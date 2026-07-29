// Package limiter defines the core rate limiting interface and the two
// supported algorithms: sliding window counter and token bucket.
//
// Each algorithm has an in-memory implementation (single node — used for local
// fallback when Redis is unreachable) and a Redis implementation (distributed,
// the production path). Both implement Limiter, so they are interchangeable.
package limiter

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Algorithm identifies a rate limiting algorithm.
type Algorithm string

const (
	// SlidingWindowCounterAlgo interpolates between the current and previous
	// fixed windows. O(1) memory per key, no boundary burst.
	SlidingWindowCounterAlgo Algorithm = "sliding_window_counter"
	// TokenBucketAlgo refills tokens continuously and permits bursts up to
	// BurstMax. O(1) memory per key.
	TokenBucketAlgo Algorithm = "token_bucket"
)

// ParseAlgorithm validates an algorithm name from configuration.
func ParseAlgorithm(s string) (Algorithm, error) {
	switch Algorithm(s) {
	case SlidingWindowCounterAlgo:
		return SlidingWindowCounterAlgo, nil
	case TokenBucketAlgo:
		return TokenBucketAlgo, nil
	default:
		return "", fmt.Errorf("unknown algorithm %q (want %q or %q)",
			s, SlidingWindowCounterAlgo, TokenBucketAlgo)
	}
}

// Sentinel errors. Callers distinguish "this request can never succeed" from
// "this request is throttled right now" — the former is a client error (400),
// the latter is a 429.
var (
	// ErrInvalidCost is returned when n < 1.
	ErrInvalidCost = errors.New("limiter: cost must be >= 1")
	// ErrCostExceedsLimit is returned when a single request asks for more quota
	// than the limit can ever grant. Retrying will never help, so surfacing this
	// as a 429 would send the client into an infinite backoff loop.
	ErrCostExceedsLimit = errors.New("limiter: cost exceeds the configured limit")
)

// Result.DeniedBy values that do not name a chain tier. The vocabulary lives here,
// next to the field, so the components that set it and the components that interpret
// it cannot drift apart.
const (
	// ShedDeniedBy marks a request dropped by adaptive load shedding.
	ShedDeniedBy = "adaptive_shed"
	// UnavailableDeniedBy marks a request refused because the limiter itself is
	// unavailable, rather than because the caller is over quota.
	UnavailableDeniedBy = "limiter_unavailable"
)

// CallerAttributable reports whether a denial reflects the caller's own behaviour.
//
// Load shedding and limiter outages are properties of the service, not of the caller.
// Treating them as the caller's fault — by counting them towards a penalty, say —
// punishes well-behaved traffic for the service's own condition, and does it at
// exactly the moment the service can least afford to shed legitimate requests.
func CallerAttributable(deniedBy string) bool {
	switch deniedBy {
	case ShedDeniedBy, UnavailableDeniedBy:
		return false
	default:
		return true
	}
}

// LimitUnknown is used for Result.Limit when no numeric limit applies —
// for example a fail-open decision made while Redis is down. The middleware
// omits X-RateLimit-* headers rather than reporting a limit it cannot vouch for.
const LimitUnknown int64 = -1

// Result is returned by every Allow/AllowN call.
type Result struct {
	// Allowed reports whether the request may proceed.
	Allowed bool
	// Limit is the applicable quota, or LimitUnknown.
	Limit int64
	// Remaining is the quota left after this call. Zero when denied.
	Remaining int64
	// ResetAfter is how long until the quota fully replenishes.
	ResetAfter time.Duration
	// RetryAfter is how long until this specific request would be admitted.
	// Only meaningful when Allowed is false. It is never larger than ResetAfter.
	RetryAfter time.Duration
	// DeniedBy identifies what blocked the request — a ChainedLimiter tier name,
	// or "adaptive_shed". Empty when allowed or when a single-tier limiter denied.
	DeniedBy string
}

// Limiter is the single interface both algorithms implement.
//
// Implementations must be safe for concurrent use by multiple goroutines.
type Limiter interface {
	// Allow is shorthand for AllowN(ctx, key, 1).
	Allow(ctx context.Context, key string) (Result, error)
	// AllowN checks whether n units of quota are available and consumes them if so.
	// Returns ErrInvalidCost if n < 1 and ErrCostExceedsLimit if n can never fit.
	AllowN(ctx context.Context, key string, n int64) (Result, error)
	// Name returns the algorithm identifier used as a metrics label. It must be
	// cheap — the hot path calls it on every request — and constant for the
	// lifetime of the limiter.
	Name() string
}

// Defaults for Config fields left at zero.
const (
	defaultMaxKeys = 1 << 20 // ~1M tracked keys per in-memory limiter
	// MinWindow is the shortest supported window. The Redis path works in
	// integer milliseconds, so anything shorter cannot be represented.
	MinWindow = time.Millisecond
)

// Config holds per-limiter configuration.
type Config struct {
	// Limit is the maximum quota units per Window.
	Limit int64
	// Window is the period over which Limit applies. Must be >= MinWindow.
	Window time.Duration
	// BurstMax is the token bucket capacity. Zero means "same as Limit"
	// (no bursting above the steady rate). Ignored by the sliding window counter.
	BurstMax int64
	// MaxKeys bounds how many distinct keys an in-memory limiter tracks.
	// Zero means defaultMaxKeys. Exceeding it evicts the least recently used
	// keys. Ignored by the Redis implementations, where Redis bounds memory.
	MaxKeys int
}

// Validate reports whether the config is usable, with an actionable message.
// Called at startup so misconfiguration fails fast instead of silently
// degrading into an allow-everything limiter.
func (c Config) Validate() error {
	if c.Limit < 1 {
		return fmt.Errorf("limit must be >= 1, got %d", c.Limit)
	}
	if c.Window < MinWindow {
		return fmt.Errorf("window must be >= %s, got %s", MinWindow, c.Window)
	}
	if c.BurstMax < 0 {
		return fmt.Errorf("burst_max must be >= 0, got %d", c.BurstMax)
	}
	if c.MaxKeys < 0 {
		return fmt.Errorf("max_keys must be >= 0, got %d", c.MaxKeys)
	}
	return nil
}

// withDefaults returns a copy with zero-valued optional fields filled in.
func (c Config) withDefaults() Config {
	if c.BurstMax == 0 {
		c.BurstMax = c.Limit
	}
	if c.MaxKeys == 0 {
		c.MaxKeys = defaultMaxKeys
	}
	return c
}

// Capacity is the largest single request this config can ever admit under the given
// algorithm: one window of quota for the sliding window counter, or the bucket's
// capacity for the token bucket.
//
// Used to reject a route whose declared cost could never be served, at startup rather
// than on every request in production.
func (c Config) Capacity(algo Algorithm) int64 {
	if algo == TokenBucketAlgo {
		return c.withDefaults().BurstMax
	}
	return c.Limit
}

// checkCost validates n against the limiter's maximum admissible request.
func checkCost(n, capacity int64) error {
	if n < 1 {
		return fmt.Errorf("%w: got %d", ErrInvalidCost, n)
	}
	if n > capacity {
		return fmt.Errorf("%w: cost %d > capacity %d", ErrCostExceedsLimit, n, capacity)
	}
	return nil
}
