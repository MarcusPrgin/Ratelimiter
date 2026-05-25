package limiter

import (
	"context"
	"fmt"
	"math"
)

// ChainTier is one enforcement layer within a ChainedLimiter.
type ChainTier struct {
	// Name identifies this tier in DeniedBy and error messages.
	Name string
	// Limiter is the rate-limit implementation for this tier.
	Limiter Limiter
	// KeyFunc transforms the request key for this tier.
	// nil means use the key unchanged (per-request limiting).
	// Use a constant-returning func for shared/global tiers.
	KeyFunc func(key string) string
}

// ChainedLimiter enforces multiple limit tiers sequentially.
// A request must pass every tier; the first denial short-circuits and sets DeniedBy.
//
// Over-count trade-off: tiers that already incremented before a later denial are
// not rolled back. The over-count is at most (n × tiers_passed) per denied event —
// negligible at typical denial rates. The alternative (two-phase peek + commit)
// requires additional Redis round trips and is not worth it here.
type ChainedLimiter struct {
	tiers []ChainTier
}

func NewChainedLimiter(tiers ...ChainTier) *ChainedLimiter {
	return &ChainedLimiter{tiers: tiers}
}

func (c *ChainedLimiter) Allow(ctx context.Context, key string) (Result, error) {
	return c.AllowN(ctx, key, 1)
}

// AllowN calls each tier in order. Returns the most restrictive (fewest Remaining)
// Result on success, or the denying tier's Result on failure.
func (c *ChainedLimiter) AllowN(ctx context.Context, key string, n int64) (Result, error) {
	minRemaining := int64(math.MaxInt64)
	var bestResult Result

	for _, tier := range c.tiers {
		tierKey := key
		if tier.KeyFunc != nil {
			tierKey = tier.KeyFunc(key)
		}
		r, err := tier.Limiter.AllowN(ctx, tierKey, n)
		if err != nil {
			return r, fmt.Errorf("tier %q: %w", tier.Name, err)
		}
		if !r.Allowed {
			r.DeniedBy = tier.Name
			return r, nil
		}
		if r.Remaining < minRemaining {
			minRemaining = r.Remaining
			bestResult = r
		}
	}
	return bestResult, nil
}

func (c *ChainedLimiter) Name() string {
	if len(c.tiers) == 0 {
		return "chained"
	}
	return "chained/" + c.tiers[0].Limiter.Name()
}
