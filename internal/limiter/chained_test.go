package limiter_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/MarcusPrgin/Ratelimiter/internal/limiter"
)

func tierLimiter(limit int64) limiter.Limiter {
	return limiter.NewSlidingWindowCounter(limiter.Config{Limit: limit, Window: time.Hour})
}

func newChain(t *testing.T, tiers ...limiter.ChainTier) *limiter.ChainedLimiter {
	t.Helper()
	c, err := limiter.NewChainedLimiter(tiers...)
	if err != nil {
		t.Fatalf("NewChainedLimiter: %v", err)
	}
	return c
}

func TestChainAllTiersPass(t *testing.T) {
	ctx := context.Background()
	chain := newChain(t,
		limiter.ChainTier{Name: "per_key", Limiter: tierLimiter(10)},
		limiter.ChainTier{Name: "global", Limiter: tierLimiter(100),
			KeyFunc: func(string) string { return "g" }},
	)

	r, err := chain.Allow(ctx, "user1")
	if err != nil {
		t.Fatal(err)
	}
	if !r.Allowed {
		t.Fatalf("denied by %q", r.DeniedBy)
	}
	if r.DeniedBy != "" {
		t.Errorf("DeniedBy = %q on success, want empty", r.DeniedBy)
	}
}

func TestChainReportsDenyingTier(t *testing.T) {
	ctx := context.Background()

	t.Run("first tier", func(t *testing.T) {
		chain := newChain(t,
			limiter.ChainTier{Name: "per_key", Limiter: tierLimiter(1)},
			limiter.ChainTier{Name: "global", Limiter: tierLimiter(1000),
				KeyFunc: func(string) string { return "g" }},
		)
		_, _ = chain.Allow(ctx, "u")
		r, _ := chain.Allow(ctx, "u")
		if r.Allowed || r.DeniedBy != "per_key" {
			t.Errorf("allowed=%t denied_by=%q, want false/per_key", r.Allowed, r.DeniedBy)
		}
	})

	t.Run("later tier", func(t *testing.T) {
		chain := newChain(t,
			limiter.ChainTier{Name: "per_key", Limiter: tierLimiter(1000)},
			limiter.ChainTier{Name: "global", Limiter: tierLimiter(1),
				KeyFunc: func(string) string { return "g" }},
		)
		_, _ = chain.Allow(ctx, "u")
		r, _ := chain.Allow(ctx, "u")
		if r.Allowed || r.DeniedBy != "global" {
			t.Errorf("allowed=%t denied_by=%q, want false/global", r.Allowed, r.DeniedBy)
		}
	})
}

// TestChainKeyFuncIsolatesTiers is the shape of the per-tenant bug: distinct
// KeyFunc outputs must get distinct buckets. Deriving the same string for every
// caller silently turns a per-tenant tier into a second global one.
func TestChainKeyFuncIsolatesTiers(t *testing.T) {
	ctx := context.Background()
	chain := newChain(t,
		limiter.ChainTier{Name: "per_key", Limiter: tierLimiter(1000)},
		limiter.ChainTier{
			Name:    "per_tenant",
			Limiter: tierLimiter(2),
			KeyFunc: func(key string) string { return "tenant:" + key[:1] },
		},
	)

	// Two callers in tenant "a" exhaust that tenant's quota of 2.
	for _, k := range []string{"a1", "a2"} {
		if r, _ := chain.Allow(ctx, k); !r.Allowed {
			t.Fatalf("%s should be allowed", k)
		}
	}
	if r, _ := chain.Allow(ctx, "a3"); r.Allowed {
		t.Error("third caller in tenant a should be denied by the tenant tier")
	}
	// A caller in tenant "b" must be unaffected.
	if r, _ := chain.Allow(ctx, "b1"); !r.Allowed {
		t.Error("tenant b was denied by tenant a's usage — tiers are not isolated")
	}
}

func TestChainReportsMostRestrictiveRemaining(t *testing.T) {
	ctx := context.Background()
	chain := newChain(t,
		limiter.ChainTier{Name: "wide", Limiter: tierLimiter(100)},
		limiter.ChainTier{Name: "narrow", Limiter: tierLimiter(5),
			KeyFunc: func(string) string { return "n" }},
	)

	r, err := chain.Allow(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if r.Remaining != 4 {
		t.Errorf("Remaining = %d, want 4 from the narrow tier — a client sees the "+
			"quota that will actually run out first", r.Remaining)
	}
}

// TestChainConstructorRejectsBadTiers matters because the previous version
// defaulted an empty chain to a zero-valued Result, whose Allowed field is false —
// so a misconfiguration denied all traffic with no diagnostic.
func TestChainConstructorRejectsBadTiers(t *testing.T) {
	good := limiter.ChainTier{Name: "a", Limiter: tierLimiter(1)}

	tests := []struct {
		name  string
		tiers []limiter.ChainTier
	}{
		{"no tiers", nil},
		{"empty name", []limiter.ChainTier{{Name: "", Limiter: tierLimiter(1)}}},
		{"nil limiter", []limiter.ChainTier{{Name: "a"}}},
		{"duplicate names", []limiter.ChainTier{good, good}},
		{"reserved name", []limiter.ChainTier{{
			Name: limiter.ShedDeniedBy, Limiter: tierLimiter(1),
		}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := limiter.NewChainedLimiter(tc.tiers...); err == nil {
				t.Error("=> ok, want error")
			}
		})
	}
}

func TestChainPropagatesCostError(t *testing.T) {
	ctx := context.Background()
	chain := newChain(t,
		limiter.ChainTier{Name: "per_key", Limiter: tierLimiter(10)},
		limiter.ChainTier{Name: "global", Limiter: tierLimiter(5),
			KeyFunc: func(string) string { return "g" }},
	)

	// Fits the first tier but exceeds the second's capacity entirely.
	_, err := chain.AllowN(ctx, "k", 8)
	if !errors.Is(err, limiter.ErrCostExceedsLimit) {
		t.Errorf("=> %v, want ErrCostExceedsLimit wrapped through the chain", err)
	}
	if !limiter.IsCostError(err) {
		t.Error("IsCostError must see through the chain's wrapping")
	}
}

func TestChainTiersListed(t *testing.T) {
	chain := newChain(t,
		limiter.ChainTier{Name: "per_key", Limiter: tierLimiter(1)},
		limiter.ChainTier{Name: "per_tenant", Limiter: tierLimiter(1),
			KeyFunc: func(string) string { return "t" }},
	)
	got := chain.Tiers()
	want := []string{"per_key", "per_tenant"}
	if len(got) != len(want) {
		t.Fatalf("Tiers() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Tiers()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
