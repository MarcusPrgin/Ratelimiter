// Package limiter defines the core rate limiting interface and result types.
// Every algorithm implements this interface so they can be swapped at runtime.
package limiter

import (
	"context"
	"time"
)

// Result is returned by every Allow call.
type Result struct {
	Allowed    bool
	Limit      int64
	Remaining  int64
	ResetAfter time.Duration // how long until the window resets
	RetryAfter time.Duration // only set when Allowed == false
}

// Limiter is the single interface all algorithms implement.
type Limiter interface {
	Allow(ctx context.Context, key string) (Result, error)
	// Name returns the algorithm name for metrics labelling.
	Name() string
}

// Config holds per-key limit configuration.
type Config struct {
	Limit    int64         // max requests
	Window   time.Duration // time window
	BurstMax int64         // for token bucket: max tokens above limit
}
