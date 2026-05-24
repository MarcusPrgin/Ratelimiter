// Package limiter — Redis distributed implementation.
// This is the production path. The in-memory implementations are for
// single-node use or fallback. This one uses Redis for shared state
// across any number of nodes.
package limiter

import (
	"context"
	"crypto/sha1"
	_ "embed"
	"encoding/hex"
	"fmt"
	"math"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed sliding_window.lua
var slidingWindowScript string

// RedisLimiter implements distributed rate limiting backed by Redis.
// Uses a Lua script for atomic sliding window counter operations.
type RedisLimiter struct {
	client     *redis.Client
	cfg        Config
	scriptSHA  string
	scriptBody string
}

func NewRedisLimiter(client *redis.Client, cfg Config) (*RedisLimiter, error) {
	ctx := context.Background()

	// pre-load the Lua script — we reference it by SHA for efficiency
	sha, err := client.ScriptLoad(ctx, slidingWindowScript).Result()
	if err != nil {
		return nil, fmt.Errorf("loading lua script: %w", err)
	}

	return &RedisLimiter{
		client:     client,
		cfg:        cfg,
		scriptSHA:  sha,
		scriptBody: slidingWindowScript,
	}, nil
}

func (r *RedisLimiter) Allow(ctx context.Context, key string) (Result, error) {
	now := time.Now()
	windowSecs := int64(r.cfg.Window.Seconds())

	// floor to window boundary
	winStart := (now.Unix() / windowSecs) * windowSecs
	prevWinStart := winStart - windowSecs

	currKey := fmt.Sprintf("rl:%s:%d", key, winStart)
	prevKey := fmt.Sprintf("rl:%s:%d", key, prevWinStart)

	nowF := float64(now.Unix()) + float64(now.Nanosecond())/1e9

	// run the Lua script atomically
	vals, err := r.client.EvalSha(ctx, r.scriptSHA,
		[]string{currKey, prevKey},
		r.cfg.Limit,
		windowSecs,
		nowF,
		winStart,
	).Int64Slice()

	// if script was flushed from Redis, reload it
	if err != nil && isNoScriptErr(err) {
		sha, loadErr := r.client.ScriptLoad(ctx, r.scriptBody).Result()
		if loadErr != nil {
			return Result{}, fmt.Errorf("reloading script: %w", loadErr)
		}
		r.scriptSHA = sha
		vals, err = r.client.EvalSha(ctx, r.scriptSHA,
			[]string{currKey, prevKey},
			r.cfg.Limit, windowSecs, nowF, winStart,
		).Int64Slice()
	}

	if err != nil {
		return Result{}, fmt.Errorf("redis eval: %w", err)
	}

	allowed := vals[0] == 1
	currCount := vals[1]
	effective := vals[2]

	remaining := r.cfg.Limit - effective
	if remaining < 0 {
		remaining = 0
	}

	resetAt := time.Unix(winStart+windowSecs, 0)
	resetAfter := time.Until(resetAt)

	if !allowed {
		return Result{
			Allowed:    false,
			Limit:      r.cfg.Limit,
			Remaining:  0,
			ResetAfter: resetAfter,
			RetryAfter: resetAfter,
		}, nil
	}

	_ = currCount // available for debugging
	return Result{
		Allowed:    true,
		Limit:      r.cfg.Limit,
		Remaining:  remaining,
		ResetAfter: resetAfter,
	}, nil
}

func (r *RedisLimiter) Name() string { return "redis_sliding_window" }

// scriptSHAFromBody computes the SHA1 of a Lua script body — matches Redis's own calculation.
func scriptSHAFromBody(script string) string {
	h := sha1.New()
	h.Write([]byte(script))
	return hex.EncodeToString(h.Sum(nil))
}

func isNoScriptErr(err error) bool {
	return err != nil && len(err.Error()) >= 8 && err.Error()[:8] == "NOSCRIPT"
}

// windowBoundary returns the floor of t to the nearest window boundary.
func windowBoundary(t time.Time, window time.Duration) time.Time {
	windowSecs := int64(window.Seconds())
	floor := (t.Unix() / windowSecs) * windowSecs
	return time.Unix(floor, 0)
}

// elapsedFraction returns what fraction of the current window has elapsed (0.0–1.0).
func elapsedFraction(now time.Time, window time.Duration) float64 {
	boundary := windowBoundary(now, window)
	elapsed := now.Sub(boundary).Seconds()
	return math.Min(elapsed/window.Seconds(), 1.0)
}
