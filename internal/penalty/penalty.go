// Package penalty implements an exponential-backoff penalty box on top of Redis.
// When a key accumulates enough consecutive rate-limit denials it enters a penalty
// period that doubles on each subsequent violation, up to a configurable maximum.
package penalty

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config controls penalty box behaviour.
type Config struct {
	// Threshold: consecutive denials within StrikeWindow before penalty is applied.
	Threshold int64
	// StrikeWindow: rolling window in which strikes are counted.
	StrikeWindow time.Duration
	// BasePenalty: duration of the first penalty.
	BasePenalty time.Duration
	// MaxPenalty: cap; prevents infinite escalation.
	MaxPenalty time.Duration
}

func DefaultConfig() Config {
	return Config{
		Threshold:    10,
		StrikeWindow: time.Minute,
		BasePenalty:  30 * time.Second,
		MaxPenalty:   time.Hour,
	}
}

// Box is a Redis-backed penalty tracker.
type Box struct {
	rdb *redis.Client
	cfg Config
}

func New(rdb *redis.Client, cfg Config) *Box {
	return &Box{rdb: rdb, cfg: cfg}
}

// Check reports whether key is currently in penalty and how long remains.
// Returns inPenalty=false on any Redis error so the hot path is never blocked.
func (b *Box) Check(ctx context.Context, key string) (inPenalty bool, remaining time.Duration, err error) {
	ttl, err := b.rdb.TTL(ctx, b.penaltyKey(key)).Result()
	if err != nil || ttl <= 0 {
		return false, 0, err
	}
	return true, ttl, nil
}

// Record registers a rate-limit denial for key.
// If the strike count reaches Threshold, the key enters penalty with exponential backoff.
// Errors are swallowed so a Redis hiccup does not block the request path.
func (b *Box) Record(ctx context.Context, key string) {
	strikeKey := b.strikeKey(key)

	strikes, err := b.rdb.Incr(ctx, strikeKey).Result()
	if err != nil {
		return
	}
	b.rdb.Expire(ctx, strikeKey, b.cfg.StrikeWindow)

	if strikes < b.cfg.Threshold {
		return
	}

	// Threshold crossed — compute penalty using how many times we've penalised this key.
	penCountKey := b.penaltyCountKey(key)
	penaltyN, _ := b.rdb.Incr(ctx, penCountKey).Result()
	// Penalty-count key persists for MaxPenalty so repeat offenders keep escalating.
	b.rdb.Expire(ctx, penCountKey, b.cfg.MaxPenalty)

	duration := time.Duration(float64(b.cfg.BasePenalty) * math.Pow(2, float64(penaltyN-1)))
	if duration > b.cfg.MaxPenalty {
		duration = b.cfg.MaxPenalty
	}

	b.rdb.Set(ctx, b.penaltyKey(key), penaltyN, duration)
	b.rdb.Del(ctx, strikeKey) // reset strikes so next offence starts fresh
}

func (b *Box) penaltyKey(key string) string      { return fmt.Sprintf("rl:penalty:%s", key) }
func (b *Box) strikeKey(key string) string       { return fmt.Sprintf("rl:strikes:%s", key) }
func (b *Box) penaltyCountKey(key string) string { return fmt.Sprintf("rl:pencount:%s", key) }
