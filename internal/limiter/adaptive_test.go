package limiter_test

import (
	"context"
	"testing"
	"time"

	"github.com/MarcusPrgin/Ratelimiter/internal/limiter"
)

func adaptiveCfg() limiter.AdaptiveConfig { return limiter.DefaultAdaptiveConfig() }

func newAdaptive(t *testing.T, inner limiter.Limiter, cfg limiter.AdaptiveConfig) *limiter.AdaptiveLimiter {
	t.Helper()
	a, err := limiter.NewAdaptiveLimiter(inner, cfg)
	if err != nil {
		t.Fatalf("NewAdaptiveLimiter: %v", err)
	}
	return a
}

func TestAdaptiveFullMultiplierPassesEverything(t *testing.T) {
	ctx := context.Background()
	inner := limiter.NewSlidingWindowCounter(limiter.Config{Limit: 1 << 20, Window: time.Hour})
	a := newAdaptive(t, inner, adaptiveCfg())

	for i := 0; i < 200; i++ {
		r, err := a.Allow(ctx, "k")
		if err != nil {
			t.Fatal(err)
		}
		if !r.Allowed {
			t.Fatalf("request %d shed at multiplier %g", i, a.Multiplier())
		}
	}
	if a.Shed() != 0 {
		t.Errorf("shed counter = %d, want 0", a.Shed())
	}
}

func TestAdaptiveShedsAtFloor(t *testing.T) {
	ctx := context.Background()
	inner := limiter.NewSlidingWindowCounter(limiter.Config{Limit: 1 << 20, Window: time.Hour})
	a := newAdaptive(t, inner, adaptiveCfg())
	a.ForceMultiplier(0)

	const n = 300
	shed := 0
	for i := 0; i < n; i++ {
		r, _ := a.Allow(ctx, "k")
		if !r.Allowed {
			shed++
			if r.DeniedBy != limiter.ShedDeniedBy {
				t.Fatalf("DeniedBy = %q, want %q", r.DeniedBy, limiter.ShedDeniedBy)
			}
			// A shed response must tell the client when to come back, otherwise it
			// retries immediately and adds to the load being shed.
			if r.RetryAfter <= 0 {
				t.Fatal("shed result has no RetryAfter")
			}
		}
	}
	if shed != n {
		t.Errorf("shed %d of %d at multiplier 0, want all", shed, n)
	}
	if a.Shed() != uint64(n) {
		t.Errorf("shed counter = %d, want %d — the metric would read zero", a.Shed(), n)
	}
}

// TestAdaptiveAdjustsOnTimeNotVolume is the control-loop fix.
//
// Stepping the multiplier once per request ties the response rate to traffic
// volume: at high throughput it slams to the floor within a millisecond, and a
// quiet service never recovers because recovery needs one call per step. With a
// long AdjustInterval, a burst of slow calls must produce at most one step.
func TestAdaptiveAdjustsOnTimeNotVolume(t *testing.T) {
	ctx := context.Background()
	cfg := adaptiveCfg()
	// Watermarks well below any real call latency, so every sample reads as overloaded.
	cfg.LowWatermarkMs = 0.0001
	cfg.HighWatermarkMs = 0.001
	cfg.AdjustInterval = time.Hour
	cfg.DecreaseRatio = 0.5
	cfg.MinMultiplier = 0.01

	slow := &sleepyLimiter{delay: 2 * time.Millisecond}
	a := newAdaptive(t, slow, cfg)

	for i := 0; i < 50; i++ {
		if _, err := a.AllowN(ctx, "k", 1); err != nil {
			t.Fatal(err)
		}
	}

	// One step from 1.0 at ratio 0.5 is 0.5. Two or more would be <= 0.25.
	if m := a.Multiplier(); m < 0.5 {
		t.Errorf("multiplier fell to %g after 50 slow calls in one interval; "+
			"expected at most one step to 0.5", m)
	}
}

// TestAdaptiveRecoversWhenHealthy checks the additive-increase half of the loop.
func TestAdaptiveRecoversWhenHealthy(t *testing.T) {
	ctx := context.Background()
	cfg := adaptiveCfg()
	cfg.AdjustInterval = time.Millisecond
	cfg.LowWatermarkMs = 1000 // any real call counts as healthy
	cfg.HighWatermarkMs = 2000
	cfg.IncreaseStep = 0.25

	inner := limiter.NewSlidingWindowCounter(limiter.Config{Limit: 1 << 20, Window: time.Hour})
	a := newAdaptive(t, inner, cfg)
	a.ForceMultiplier(0.1)

	deadline := time.Now().Add(2 * time.Second)
	for a.Multiplier() < 1.0 && time.Now().Before(deadline) {
		for i := 0; i < 20; i++ {
			_, _ = a.Allow(ctx, "k")
		}
		time.Sleep(2 * time.Millisecond)
	}

	if m := a.Multiplier(); m < 1.0 {
		t.Errorf("multiplier recovered only to %g, want 1.0", m)
	}
}

func TestAdaptiveConfigValidation(t *testing.T) {
	inner := limiter.NewSlidingWindowCounter(limiter.Config{Limit: 1, Window: time.Second})

	base := adaptiveCfg()
	mutate := func(f func(*limiter.AdaptiveConfig)) limiter.AdaptiveConfig {
		c := base
		f(&c)
		return c
	}

	tests := []struct {
		name string
		cfg  limiter.AdaptiveConfig
		ok   bool
	}{
		{"default", base, true},
		{"watermarks inverted", mutate(func(c *limiter.AdaptiveConfig) {
			c.LowWatermarkMs, c.HighWatermarkMs = 10, 2
		}), false},
		{"watermarks equal", mutate(func(c *limiter.AdaptiveConfig) {
			c.LowWatermarkMs, c.HighWatermarkMs = 5, 5
		}), false},
		{"decrease ratio 1", mutate(func(c *limiter.AdaptiveConfig) { c.DecreaseRatio = 1 }), false},
		{"decrease ratio 0", mutate(func(c *limiter.AdaptiveConfig) { c.DecreaseRatio = 0 }), false},
		{"increase step 0", mutate(func(c *limiter.AdaptiveConfig) { c.IncreaseStep = 0 }), false},
		{"min multiplier 0", mutate(func(c *limiter.AdaptiveConfig) { c.MinMultiplier = 0 }), false},
		{"min multiplier > 1", mutate(func(c *limiter.AdaptiveConfig) { c.MinMultiplier = 1.5 }), false},
		{"alpha 0", mutate(func(c *limiter.AdaptiveConfig) { c.EWMAAlpha = 0 }), false},
		{"alpha > 1", mutate(func(c *limiter.AdaptiveConfig) { c.EWMAAlpha = 1.1 }), false},
		{"zero adjust interval", mutate(func(c *limiter.AdaptiveConfig) { c.AdjustInterval = 0 }), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := limiter.NewAdaptiveLimiter(inner, tc.cfg)
			if tc.ok && err != nil {
				t.Errorf("=> %v, want ok", err)
			}
			if !tc.ok && err == nil {
				t.Error("=> ok, want error")
			}
		})
	}
}

// TestAdaptiveConcurrentAdjustment exercises the compare-and-swap accumulators
// under the race detector. A plain load-compute-store would lose samples here.
func TestAdaptiveConcurrentAdjustment(t *testing.T) {
	ctx := context.Background()
	cfg := adaptiveCfg()
	cfg.AdjustInterval = time.Microsecond

	inner := limiter.NewSlidingWindowCounter(limiter.Config{Limit: 1 << 30, Window: time.Hour})
	a := newAdaptive(t, inner, cfg)

	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 500; j++ {
				_, _ = a.Allow(ctx, "k")
				_ = a.Multiplier()
				_ = a.EWMA()
			}
		}()
	}
	for i := 0; i < 8; i++ {
		<-done
	}

	if m := a.Multiplier(); m <= 0 || m > 1 {
		t.Errorf("multiplier %g escaped (0,1]", m)
	}
}

// sleepyLimiter always allows, after a fixed delay, so the adaptive controller sees
// a predictable latency.
type sleepyLimiter struct{ delay time.Duration }

func (s *sleepyLimiter) Allow(ctx context.Context, key string) (limiter.Result, error) {
	return s.AllowN(ctx, key, 1)
}

func (s *sleepyLimiter) AllowN(context.Context, string, int64) (limiter.Result, error) {
	time.Sleep(s.delay)
	return limiter.Result{Allowed: true, Limit: 1000, Remaining: 999}, nil
}

func (s *sleepyLimiter) Name() string { return "sleepy" }
