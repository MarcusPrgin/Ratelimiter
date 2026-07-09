package limiter_test

import (
	"context"
	"testing"
	"time"

	"github.com/yourname/ratelimiter/internal/limiter"
)

// ── AllowN (cost-weighted) ────────────────────────────────────────────────────

func TestAllowN_CostDeductsMultipleSlots(t *testing.T) {
	cfg := limiter.Config{Limit: 10, Window: time.Second}
	ctx := context.Background()

	for _, l := range newAll(cfg) {
		t.Run(l.Name(), func(t *testing.T) {
			// cost=4 — uses 4 of 10 slots
			r, err := l.AllowN(ctx, "key", 4)
			if err != nil {
				t.Fatal(err)
			}
			if !r.Allowed {
				t.Fatal("first AllowN(4) should be allowed")
			}
			if r.Remaining != 6 {
				t.Fatalf("want remaining=6 after cost=4, got %d", r.Remaining)
			}

			// cost=4 again — 4+4=8 ≤ 10, should pass
			r, _ = l.AllowN(ctx, "key", 4)
			if !r.Allowed {
				t.Fatal("second AllowN(4) should be allowed (8 ≤ 10)")
			}

			// cost=3 — 8+3=11 > 10, must be denied
			r, _ = l.AllowN(ctx, "key", 3)
			if r.Allowed {
				t.Fatal("AllowN(3) should be denied (8+3=11 > 10)")
			}
		})
	}
}

func TestAllowN_ExactlyFillsLimit(t *testing.T) {
	cfg := limiter.Config{Limit: 10, Window: time.Second}
	ctx := context.Background()

	for _, l := range newAll(cfg) {
		t.Run(l.Name(), func(t *testing.T) {
			// cost=10 — fills the limit exactly
			r, err := l.AllowN(ctx, "key", 10)
			if err != nil {
				t.Fatal(err)
			}
			if !r.Allowed {
				t.Fatal("AllowN(10) should be allowed when limit=10")
			}
			if r.Remaining != 0 {
				t.Fatalf("want remaining=0, got %d", r.Remaining)
			}

			// cost=1 — no room left
			r, _ = l.AllowN(ctx, "key", 1)
			if r.Allowed {
				t.Fatal("next request after filling limit must be denied")
			}
		})
	}
}

func TestAllowN_CostExceedsLimit(t *testing.T) {
	cfg := limiter.Config{Limit: 5, Window: time.Second}
	ctx := context.Background()

	for _, l := range newAll(cfg) {
		t.Run(l.Name(), func(t *testing.T) {
			// cost=6 > limit=5 — must always be denied
			r, err := l.AllowN(ctx, "key", 6)
			if err != nil {
				t.Fatal(err)
			}
			if r.Allowed {
				t.Fatal("AllowN(6) with limit=5 must be denied")
			}
		})
	}
}

// ── ChainedLimiter ───────────────────────────────────────────────────────────

func TestChainedLimiter_AllTiersPass(t *testing.T) {
	ctx := context.Background()
	perKey := limiter.NewFixedWindow(limiter.Config{Limit: 10, Window: time.Second})
	global := limiter.NewFixedWindow(limiter.Config{Limit: 100, Window: time.Second})

	chain := limiter.NewChainedLimiter(
		limiter.ChainTier{Name: "per_key", Limiter: perKey},
		limiter.ChainTier{Name: "global", Limiter: global, KeyFunc: func(_ string) string { return "g" }},
	)

	r, err := chain.Allow(ctx, "user1")
	if err != nil {
		t.Fatal(err)
	}
	if !r.Allowed {
		t.Fatalf("expected allowed, DeniedBy=%q", r.DeniedBy)
	}
	if r.DeniedBy != "" {
		t.Fatalf("DeniedBy should be empty on success, got %q", r.DeniedBy)
	}
}

func TestChainedLimiter_FirstTierDenies(t *testing.T) {
	ctx := context.Background()
	tight := limiter.NewFixedWindow(limiter.Config{Limit: 1, Window: time.Second})
	loose := limiter.NewFixedWindow(limiter.Config{Limit: 1000, Window: time.Second})

	chain := limiter.NewChainedLimiter(
		limiter.ChainTier{Name: "per_key", Limiter: tight},
		limiter.ChainTier{Name: "global", Limiter: loose, KeyFunc: func(_ string) string { return "g" }},
	)

	chain.Allow(ctx, "user1") //nolint:errcheck // exhaust per_key
	r, _ := chain.Allow(ctx, "user1")
	if r.Allowed {
		t.Fatal("second request should be denied by per_key tier")
	}
	if r.DeniedBy != "per_key" {
		t.Fatalf("want DeniedBy=per_key, got %q", r.DeniedBy)
	}
}

func TestChainedLimiter_SecondTierDenies(t *testing.T) {
	ctx := context.Background()
	loose := limiter.NewFixedWindow(limiter.Config{Limit: 1000, Window: time.Second})
	tight := limiter.NewFixedWindow(limiter.Config{Limit: 1, Window: time.Second})

	chain := limiter.NewChainedLimiter(
		limiter.ChainTier{Name: "per_key", Limiter: loose},
		limiter.ChainTier{Name: "global", Limiter: tight, KeyFunc: func(_ string) string { return "g" }},
	)

	chain.Allow(ctx, "user1") //nolint:errcheck // exhaust global tier
	r, _ := chain.Allow(ctx, "user1")
	if r.Allowed {
		t.Fatal("second request should be denied by global tier")
	}
	if r.DeniedBy != "global" {
		t.Fatalf("want DeniedBy=global, got %q", r.DeniedBy)
	}
}

func TestChainedLimiter_MostRestrictiveRemainingReturned(t *testing.T) {
	ctx := context.Background()
	a := limiter.NewFixedWindow(limiter.Config{Limit: 100, Window: time.Second})
	b := limiter.NewFixedWindow(limiter.Config{Limit: 5, Window: time.Second})

	chain := limiter.NewChainedLimiter(
		limiter.ChainTier{Name: "tier_a", Limiter: a},
		limiter.ChainTier{Name: "tier_b", Limiter: b, KeyFunc: func(_ string) string { return "b" }},
	)

	r, _ := chain.Allow(ctx, "key")
	if !r.Allowed {
		t.Fatal("should be allowed")
	}
	// tier_b has remaining=4, tier_a has remaining=99 — chain should return 4
	if r.Remaining != 4 {
		t.Fatalf("want remaining=4 (most restrictive tier), got %d", r.Remaining)
	}
}

// ── AdaptiveLimiter ──────────────────────────────────────────────────────────

func TestAdaptiveLimiter_FullMultiplierPassesAll(t *testing.T) {
	ctx := context.Background()
	inner := limiter.NewFixedWindow(limiter.Config{Limit: 1000, Window: time.Second})
	al := limiter.NewAdaptiveLimiter(inner, limiter.DefaultAdaptiveConfig())
	// multiplier starts at 1.0 — nothing should be shed

	for i := 0; i < 100; i++ {
		r, err := al.Allow(ctx, "key")
		if err != nil {
			t.Fatal(err)
		}
		if !r.Allowed {
			t.Fatalf("request %d denied with multiplier=1.0", i)
		}
	}
}

func TestAdaptiveLimiter_ZeroMultiplierShedsAll(t *testing.T) {
	ctx := context.Background()
	inner := limiter.NewFixedWindow(limiter.Config{Limit: 1000, Window: time.Second})
	al := limiter.NewAdaptiveLimiter(inner, limiter.DefaultAdaptiveConfig())
	al.ForceMultiplier(0.0) // floor: everything should be shed

	shed := 0
	for i := 0; i < 500; i++ {
		r, _ := al.Allow(ctx, "key")
		if !r.Allowed {
			shed++
		}
	}
	if shed != 500 {
		t.Fatalf("expected all 500 to be shed at multiplier=0, got %d shed", shed)
	}
}

func TestAdaptiveLimiter_ForceMultiplierExposed(t *testing.T) {
	inner := limiter.NewFixedWindow(limiter.Config{Limit: 100, Window: time.Second})
	al := limiter.NewAdaptiveLimiter(inner, limiter.DefaultAdaptiveConfig())

	al.ForceMultiplier(0.5)
	if got := al.Multiplier(); got != 0.5 {
		t.Fatalf("want multiplier=0.5, got %f", got)
	}
}

func TestAdaptiveLimiter_DeniedByIsSetOnShed(t *testing.T) {
	ctx := context.Background()
	inner := limiter.NewFixedWindow(limiter.Config{Limit: 1000, Window: time.Second})
	al := limiter.NewAdaptiveLimiter(inner, limiter.DefaultAdaptiveConfig())
	al.ForceMultiplier(0.0)

	r, _ := al.Allow(ctx, "key")
	if r.DeniedBy != "adaptive_shed" {
		t.Fatalf("want DeniedBy=adaptive_shed, got %q", r.DeniedBy)
	}
}
