package limiter

import (
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed sliding_window.lua
var slidingWindowScript string

//go:embed token_bucket.lua
var tokenBucketScript string

// NewRedisLimiter builds the distributed limiter for the given algorithm.
//
// This is the production path: state lives in Redis so every node enforces one
// shared quota. Both scripts take their clock from Redis rather than the calling
// node, so window boundaries and refill are consistent even when app servers
// have drifted.
func NewRedisLimiter(client redis.Scripter, algo Algorithm, cfg Config) (Limiter, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	switch algo {
	case SlidingWindowCounterAlgo:
		return newRedisSlidingWindow(client, cfg), nil
	case TokenBucketAlgo:
		return newRedisTokenBucket(client, cfg), nil
	default:
		return nil, fmt.Errorf("limiter: unsupported algorithm %q", algo)
	}
}

// redisKey wraps the caller-supplied key in a Cluster hash tag.
//
// The Lua scripts derive further keys from this one (per-window counters). The
// hash tag guarantees every derived key lands in the same Cluster slot, so the
// scripts stay correct if the client is swapped for a ClusterClient.
func redisKey(prefix, key string) string {
	var b strings.Builder
	b.Grow(len(prefix) + len(key) + 2)
	b.WriteString(prefix)
	b.WriteByte('{')
	b.WriteString(key)
	b.WriteByte('}')
	return b.String()
}

// runScript executes a script and validates the reply shape. A short or
// non-numeric reply means the script and this code have diverged, which is a bug
// rather than a runtime condition — surface it instead of indexing out of range.
func runScript(ctx context.Context, s *redis.Script, c redis.Scripter,
	keys []string, want int, args ...any) ([]int64, error) {

	vals, err := s.Run(ctx, c, keys, args...).Int64Slice()
	if err != nil {
		return nil, fmt.Errorf("redis eval: %w", err)
	}
	if len(vals) < want {
		return nil, fmt.Errorf("redis eval: reply has %d values, want %d", len(vals), want)
	}
	return vals, nil
}

// ── Sliding window counter ───────────────────────────────────────────────────

// RedisSlidingWindow is the distributed sliding window counter.
type RedisSlidingWindow struct {
	client   redis.Scripter
	script   *redis.Script
	cfg      Config
	windowMs int64
}

func newRedisSlidingWindow(client redis.Scripter, cfg Config) *RedisSlidingWindow {
	cfg = cfg.withDefaults()
	return &RedisSlidingWindow{
		client: client,
		// redis.Script keeps the SHA internally and retries with EVAL on
		// NOSCRIPT (after a Redis restart or SCRIPT FLUSH). Doing that by hand
		// means mutating a shared SHA field from concurrent requests, which is a
		// data race.
		script:   redis.NewScript(slidingWindowScript),
		cfg:      cfg,
		windowMs: cfg.Window.Milliseconds(),
	}
}

func (r *RedisSlidingWindow) Allow(ctx context.Context, key string) (Result, error) {
	return r.AllowN(ctx, key, 1)
}

func (r *RedisSlidingWindow) AllowN(ctx context.Context, key string, n int64) (Result, error) {
	if err := checkCost(n, r.cfg.Limit); err != nil {
		return Result{}, err
	}

	vals, err := runScript(ctx, r.script, r.client,
		[]string{redisKey("rl:", key)}, 5,
		r.cfg.Limit, r.windowMs, n)
	if err != nil {
		return Result{}, err
	}

	// { allowed, effective, remaining, reset_after_ms, retry_after_ms }
	return Result{
		Allowed:    vals[0] == 1,
		Limit:      r.cfg.Limit,
		Remaining:  vals[2],
		ResetAfter: time.Duration(vals[3]) * time.Millisecond,
		RetryAfter: time.Duration(vals[4]) * time.Millisecond,
	}, nil
}

func (r *RedisSlidingWindow) Name() string { return string(SlidingWindowCounterAlgo) }

// ── Token bucket ─────────────────────────────────────────────────────────────

// RedisTokenBucket is the distributed token bucket.
type RedisTokenBucket struct {
	client redis.Scripter
	script *redis.Script
	cfg    Config
	// refillPerMs is tokens per millisecond, formatted once to avoid
	// re-serialising a float on every request.
	refillPerMs string
	slackMs     int64
}

func newRedisTokenBucket(client redis.Scripter, cfg Config) *RedisTokenBucket {
	cfg = cfg.withDefaults()
	rate := float64(cfg.Limit) / float64(cfg.Window.Milliseconds())
	return &RedisTokenBucket{
		client:      client,
		script:      redis.NewScript(tokenBucketScript),
		cfg:         cfg,
		refillPerMs: strconv.FormatFloat(rate, 'g', -1, 64),
		slackMs:     cfg.Window.Milliseconds(),
	}
}

func (t *RedisTokenBucket) Allow(ctx context.Context, key string) (Result, error) {
	return t.AllowN(ctx, key, 1)
}

func (t *RedisTokenBucket) AllowN(ctx context.Context, key string, n int64) (Result, error) {
	if err := checkCost(n, t.cfg.BurstMax); err != nil {
		return Result{}, err
	}

	vals, err := runScript(ctx, t.script, t.client,
		[]string{redisKey("tb:", key)}, 4,
		t.refillPerMs, t.cfg.BurstMax, n, t.slackMs)
	if err != nil {
		return Result{}, err
	}

	// { allowed, remaining, reset_after_ms, retry_after_ms }
	return Result{
		Allowed:    vals[0] == 1,
		Limit:      t.cfg.Limit,
		Remaining:  vals[1],
		ResetAfter: time.Duration(vals[2]) * time.Millisecond,
		RetryAfter: time.Duration(vals[3]) * time.Millisecond,
	}, nil
}

func (t *RedisTokenBucket) Name() string { return string(TokenBucketAlgo) }
