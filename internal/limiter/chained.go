package limiter

import (
	"context"
	"errors"
	"fmt"
	"math"
)

// ChainTier is one enforcement layer within a ChainedLimiter.
type ChainTier struct {
	// Name identifies this tier in Result.DeniedBy and in metrics labels.
	Name string
	// Limiter enforces this tier.
	Limiter Limiter
	// KeyFunc maps the request key to this tier's key. Nil means use the key
	// unchanged (per-caller limiting). Return a constant for a shared tier.
	KeyFunc func(key string) string
}

// ChainedLimiter enforces several limit tiers in order — typically per-caller,
// then per-tenant, then a global ceiling. A request must pass every tier; the
// first denial short-circuits and reports itself in Result.DeniedBy so the client
// can tell "you are over your own quota" from "the service is saturated".
//
// Over-count trade-off: when a later tier denies, the tiers already consulted
// have consumed quota that the request never used. Rolling that back needs a
// two-phase peek-then-commit across every tier, which doubles the Redis round
// trips on the hot path. The error is bounded by (cost × tiers_passed) per denied
// request and self-corrects within one window, so the round trips are not worth
// it. Tiers stay ordered narrowest-first despite this: it means an abusive caller
// is stopped at its own tier and never reaches — or consumes — the shared ones.
type ChainedLimiter struct {
	tiers []ChainTier
	name  string
}

// NewChainedLimiter validates and builds a chain. At least one tier is required:
// an empty chain has no meaningful behaviour, and defaulting it either way
// (allow everything or deny everything) is a silent misconfiguration.
func NewChainedLimiter(tiers ...ChainTier) (*ChainedLimiter, error) {
	if len(tiers) == 0 {
		return nil, errors.New("limiter: chained limiter needs at least one tier")
	}
	seen := make(map[string]struct{}, len(tiers))
	for i, t := range tiers {
		if t.Name == "" {
			return nil, fmt.Errorf("limiter: chain tier %d has an empty name", i)
		}
		if t.Name == ShedDeniedBy {
			return nil, fmt.Errorf("limiter: chain tier %d uses reserved name %q", i, ShedDeniedBy)
		}
		if t.Limiter == nil {
			return nil, fmt.Errorf("limiter: chain tier %q has no limiter", t.Name)
		}
		if _, dup := seen[t.Name]; dup {
			return nil, fmt.Errorf("limiter: duplicate chain tier name %q", t.Name)
		}
		seen[t.Name] = struct{}{}
	}
	return &ChainedLimiter{
		tiers: tiers,
		// Precomputed: the metrics layer calls Name() on every request.
		name: "chained/" + tiers[0].Limiter.Name(),
	}, nil
}

func (c *ChainedLimiter) Allow(ctx context.Context, key string) (Result, error) {
	return c.AllowN(ctx, key, 1)
}

// AllowN consults each tier in order. On success it reports the most restrictive
// tier's headroom, so a client reading X-RateLimit-Remaining sees the quota that
// will actually run out first.
func (c *ChainedLimiter) AllowN(ctx context.Context, key string, n int64) (Result, error) {
	minRemaining := int64(math.MaxInt64)
	var tightest Result

	for _, tier := range c.tiers {
		tierKey := key
		if tier.KeyFunc != nil {
			tierKey = tier.KeyFunc(key)
		}

		r, err := tier.Limiter.AllowN(ctx, tierKey, n)
		if err != nil {
			return Result{}, fmt.Errorf("chain tier %q: %w", tier.Name, err)
		}
		if !r.Allowed {
			r.DeniedBy = tier.Name
			return r, nil
		}
		if r.Remaining < minRemaining {
			minRemaining = r.Remaining
			tightest = r
		}
	}
	return tightest, nil
}

// Tiers returns the configured tier names, for metric pre-registration.
func (c *ChainedLimiter) Tiers() []string {
	names := make([]string, len(c.tiers))
	for i, t := range c.tiers {
		names[i] = t.Name
	}
	return names
}

func (c *ChainedLimiter) Name() string { return c.name }

// IsCostError reports whether err means the request can never succeed, as opposed
// to being throttled right now or the backend being unavailable.
//
// The distinction drives both the HTTP status (400, not 429 — retrying a cost the
// limit can never grant loops forever) and the failure strategy, which must not
// treat a client error as a Redis outage.
func IsCostError(err error) bool {
	return errors.Is(err, ErrCostExceedsLimit) || errors.Is(err, ErrInvalidCost)
}
