package limiter

import (
	"context"
	"sync"
	"time"
)

// TokenBucket allows bursting up to BurstMax tokens.
// Tokens refill at Limit per Window. AllowN consumes n tokens at once.
type TokenBucket struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	cfg     Config
}

type bucket struct {
	tokens     float64
	lastRefill time.Time
}

func NewTokenBucket(cfg Config) *TokenBucket {
	if cfg.BurstMax == 0 {
		cfg.BurstMax = cfg.Limit
	}
	return &TokenBucket{
		buckets: make(map[string]*bucket),
		cfg:     cfg,
	}
}

func (t *TokenBucket) Allow(ctx context.Context, key string) (Result, error) {
	return t.AllowN(ctx, key, 1)
}

func (t *TokenBucket) AllowN(_ context.Context, key string, n int64) (Result, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	b, ok := t.buckets[key]
	if !ok {
		b = &bucket{tokens: float64(t.cfg.BurstMax), lastRefill: now}
		t.buckets[key] = b
	}

	elapsed := now.Sub(b.lastRefill)
	refillRate := float64(t.cfg.Limit) / t.cfg.Window.Seconds()
	b.tokens += elapsed.Seconds() * refillRate
	if b.tokens > float64(t.cfg.BurstMax) {
		b.tokens = float64(t.cfg.BurstMax)
	}
	b.lastRefill = now

	need := float64(n)
	if b.tokens < need {
		retryAfter := time.Duration((need-b.tokens)/refillRate*1e9) * time.Nanosecond
		return Result{
			Allowed:    false,
			Limit:      t.cfg.Limit,
			Remaining:  0,
			ResetAfter: t.cfg.Window,
			RetryAfter: retryAfter,
		}, nil
	}

	b.tokens -= need
	return Result{
		Allowed:    true,
		Limit:      t.cfg.Limit,
		Remaining:  int64(b.tokens),
		ResetAfter: t.cfg.Window,
	}, nil
}

func (t *TokenBucket) Name() string { return "token_bucket" }
