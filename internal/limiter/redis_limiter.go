// Package limiter — Redis distributed implementation.
// This is the production path. The in-memory implementations are for
// single-node use or fallback. This one uses Redis for shared state
// across any number of nodes.
package limiter

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed sliding_window.lua
var slidingWindowScript string

// RedisLimiter implements distributed rate limiting backed by Redis.
// Uses an atomic Lua script so multiple nodes never race on window counters.
type RedisLimiter struct {
	client     *redis.Client
	cfg        Config
	scriptSHA  string
	scriptBody string
}

func NewRedisLimiter(client *redis.Client, cfg Config) (*RedisLimiter, error) {
	ctx := context.Background()
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
	return r.AllowN(ctx, key, 1)
}

func (r *RedisLimiter) AllowN(ctx context.Context, key string, n int64) (Result, error) {
	now := time.Now()
	windowSecs := int64(r.cfg.Window.Seconds())

	winStart := (now.Unix() / windowSecs) * windowSecs
	prevWinStart := winStart - windowSecs

	currKey := fmt.Sprintf("rl:%s:%d", key, winStart)
	prevKey := fmt.Sprintf("rl:%s:%d", key, prevWinStart)

	nowF := float64(now.Unix()) + float64(now.Nanosecond())/1e9

	vals, err := r.evalScript(ctx, currKey, prevKey, r.cfg.Limit, windowSecs, nowF, winStart, n)
	if err != nil {
		return Result{}, fmt.Errorf("redis eval: %w", err)
	}

	allowed := vals[0] == 1
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

	return Result{
		Allowed:    true,
		Limit:      r.cfg.Limit,
		Remaining:  remaining,
		ResetAfter: resetAfter,
	}, nil
}

func (r *RedisLimiter) Name() string { return "redis_sliding_window" }

// evalScript runs the Lua script via EVALSHA, reloading it on NOSCRIPT errors.
func (r *RedisLimiter) evalScript(ctx context.Context, currKey, prevKey string,
	limit, windowSecs int64, nowF float64, winStart, cost int64) ([]int64, error) {
	keys := []string{currKey, prevKey}
	args := []interface{}{limit, windowSecs, nowF, winStart, cost}

	vals, err := r.client.EvalSha(ctx, r.scriptSHA, keys, args...).Int64Slice()
	if err != nil && isNoScriptErr(err) {
		sha, loadErr := r.client.ScriptLoad(ctx, r.scriptBody).Result()
		if loadErr != nil {
			return nil, fmt.Errorf("reloading script: %w", loadErr)
		}
		r.scriptSHA = sha
		vals, err = r.client.EvalSha(ctx, r.scriptSHA, keys, args...).Int64Slice()
	}
	return vals, err
}

func isNoScriptErr(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT")
}
