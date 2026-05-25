// Package limiter defines the core rate limiting interface and result types.
// Every algorithm implements this interface so they can be swapped at runtime.
package limiter

import (
	"context"
	"time"
)

// Result is returned by every Allow/AllowN call.
type Result struct {
	Allowed    bool
	Limit      int64
	Remaining  int64
	ResetAfter time.Duration
	RetryAfter time.Duration
	// DeniedBy identifies which tier blocked the request. Set by ChainedLimiter;
	// empty for single-tier limiters.
	DeniedBy string
}

// Limiter is the single interface all algorithms implement.
type Limiter interface {
	// Allow is shorthand for AllowN(ctx, key, 1).
	Allow(ctx context.Context, key string) (Result, error)
	// AllowN checks whether n units of quota are available and, if so, consumes them.
	// n must be >= 1. Implementations must be safe for concurrent use.
	AllowN(ctx context.Context, key string, n int64) (Result, error)
	// Name returns the algorithm identifier used in metrics labels.
	Name() string
}

// Config holds per-limiter configuration.
type Config struct {
	Limit    int64         // max requests (or tokens) per Window
	Window   time.Duration // time window
	BurstMax int64         // token bucket: max tokens above Limit
}
