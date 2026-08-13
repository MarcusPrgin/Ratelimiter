package fallback_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MarcusPrgin/Ratelimiter/internal/fallback"
	"github.com/MarcusPrgin/Ratelimiter/internal/limiter"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

var errBackend = errors.New("redis: connection refused")

// flakyLimiter fails while `failing` is set, so a test can simulate an outage and
// a recovery.
type flakyLimiter struct {
	failing atomic.Bool
	calls   atomic.Int64
}

func (f *flakyLimiter) Allow(ctx context.Context, key string) (limiter.Result, error) {
	return f.AllowN(ctx, key, 1)
}

func (f *flakyLimiter) AllowN(context.Context, string, int64) (limiter.Result, error) {
	f.calls.Add(1)
	if f.failing.Load() {
		return limiter.Result{}, errBackend
	}
	return limiter.Result{Allowed: true, Limit: 100, Remaining: 99}, nil
}

func (f *flakyLimiter) Name() string { return "flaky" }

func newHandler(t *testing.T, primary, local limiter.Limiter, cfg fallback.Config) *fallback.Handler {
	t.Helper()
	h, err := fallback.New(primary, local, cfg, quietLogger())
	if err != nil {
		t.Fatalf("fallback.New: %v", err)
	}
	return h
}

func noBreaker(strategy fallback.Strategy) fallback.Config {
	return fallback.Config{Strategy: strategy}
}

func TestHealthyPrimaryPassesThrough(t *testing.T) {
	primary := &flakyLimiter{}
	h := newHandler(t, primary, nil, noBreaker(fallback.FailOpen))

	r, err := h.Allow(context.Background(), "k")
	if err != nil {
		t.Fatal(err)
	}
	if !r.Allowed || r.Limit != 100 {
		t.Errorf("result = %+v, want the primary's own decision", r)
	}
	if h.Stats().Degraded != 0 {
		t.Error("healthy call counted as degraded")
	}
}

func TestFailOpenAdmits(t *testing.T) {
	primary := &flakyLimiter{}
	primary.failing.Store(true)
	h := newHandler(t, primary, nil, noBreaker(fallback.FailOpen))

	r, err := h.Allow(context.Background(), "k")
	if err != nil {
		t.Fatalf("fail_open must handle the error, got %v", err)
	}
	if !r.Allowed {
		t.Error("fail_open denied a request while the primary was down")
	}
	if r.Limit != limiter.LimitUnknown {
		t.Errorf("Limit = %d, want LimitUnknown so no header claims a quota we "+
			"cannot vouch for", r.Limit)
	}
	if h.Stats().Degraded != 1 {
		t.Errorf("Degraded = %d, want 1", h.Stats().Degraded)
	}
}

// TestFailClosedDenies covers the most serious bug in the original: fail_closed
// returned a denial alongside an error, and the HTTP layer checked only the error
// and admitted the request. Every service that chose fail_closed — payments, auth —
// was silently failing open during a Redis outage.
func TestFailClosedDenies(t *testing.T) {
	primary := &flakyLimiter{}
	primary.failing.Store(true)
	h := newHandler(t, primary, nil, noBreaker(fallback.FailClosed))

	r, err := h.Allow(context.Background(), "k")
	if r.Allowed {
		t.Fatal("fail_closed admitted a request while the primary was down")
	}
	if !errors.Is(err, fallback.ErrUnavailable) {
		t.Errorf("err = %v, want ErrUnavailable so the caller can answer 503", err)
	}
	if r.DeniedBy != fallback.DeniedByUnavailable {
		t.Errorf("DeniedBy = %q, want %q", r.DeniedBy, fallback.DeniedByUnavailable)
	}
}

func TestLocalFallbackEnforcesLocally(t *testing.T) {
	primary := &flakyLimiter{}
	primary.failing.Store(true)
	local := limiter.NewSlidingWindowCounter(limiter.Config{Limit: 3, Window: time.Hour})
	h := newHandler(t, primary, local, noBreaker(fallback.LocalFallback))

	allowed := 0
	for i := 0; i < 10; i++ {
		r, err := h.Allow(context.Background(), "k")
		if err != nil {
			t.Fatal(err)
		}
		if r.Allowed {
			allowed++
		}
	}
	if allowed != 3 {
		t.Errorf("admitted %d, want the local limiter's 3", allowed)
	}
}

func TestLocalFallbackRequiresLocalLimiter(t *testing.T) {
	if _, err := fallback.New(&flakyLimiter{}, nil,
		noBreaker(fallback.LocalFallback), quietLogger()); err == nil {
		t.Error("local_fallback without a local limiter must be a startup error")
	}
}

// TestCostErrorIsNotTreatedAsOutage checks that a client error does not trigger the
// fallback. Otherwise a caller sending an impossible cost could drive the breaker
// open and degrade the limiter for everyone else.
func TestCostErrorIsNotTreatedAsOutage(t *testing.T) {
	primary := limiter.NewSlidingWindowCounter(limiter.Config{Limit: 5, Window: time.Hour})
	h := newHandler(t, primary, nil, fallback.Config{
		Strategy: fallback.FailOpen, BreakerThreshold: 1, BreakerCooldown: time.Minute,
	})

	for i := 0; i < 5; i++ {
		if _, err := h.AllowN(context.Background(), "k", 99); !errors.Is(err, limiter.ErrCostExceedsLimit) {
			t.Fatalf("err = %v, want ErrCostExceedsLimit passed through", err)
		}
	}
	if h.Stats().Degraded != 0 {
		t.Error("cost errors were counted as backend failures")
	}
	if h.Stats().Open {
		t.Error("cost errors tripped the circuit breaker")
	}
}

// TestCancelledContextIsNotAnOutage checks that clients hanging up does not trip
// the breaker — otherwise a client-side timeout storm degrades everyone.
func TestCancelledContextIsNotAnOutage(t *testing.T) {
	primary := &flakyLimiter{}
	primary.failing.Store(true)
	h := newHandler(t, primary, nil, fallback.Config{
		Strategy: fallback.FailClosed, BreakerThreshold: 2, BreakerCooldown: time.Minute,
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for i := 0; i < 5; i++ {
		if _, err := h.Allow(ctx, "k"); err == nil {
			t.Fatal("expected the cancellation to propagate")
		}
	}
	if h.Stats().Open {
		t.Error("cancelled requests tripped the circuit breaker")
	}
	if h.Stats().Degraded != 0 {
		t.Error("cancelled requests counted as degraded")
	}
}

// TestBreakerStopsCallingDeadPrimary is the latency fix. Without a breaker, every
// request waits out the Redis timeout before falling back, so an outage in a
// dependency that is meant to be bypassed instead adds its full timeout to every
// response.
func TestBreakerStopsCallingDeadPrimary(t *testing.T) {
	primary := &flakyLimiter{}
	primary.failing.Store(true)
	h := newHandler(t, primary, nil, fallback.Config{
		Strategy: fallback.FailOpen, BreakerThreshold: 3, BreakerCooldown: time.Minute,
	})

	for i := 0; i < 50; i++ {
		if _, err := h.Allow(context.Background(), "k"); err != nil {
			t.Fatal(err)
		}
	}

	if calls := primary.calls.Load(); calls > 4 {
		t.Errorf("primary called %d times across 50 requests, want it bypassed "+
			"after the breaker opened", calls)
	}
	if !h.Stats().Open {
		t.Error("breaker did not open")
	}
	if h.Stats().Degraded != 50 {
		t.Errorf("Degraded = %d, want 50", h.Stats().Degraded)
	}
}

func TestBreakerRecoversViaHalfOpenProbe(t *testing.T) {
	primary := &flakyLimiter{}
	primary.failing.Store(true)
	h := newHandler(t, primary, nil, fallback.Config{
		Strategy: fallback.FailOpen, BreakerThreshold: 2, BreakerCooldown: 40 * time.Millisecond,
	})

	for i := 0; i < 10; i++ {
		_, _ = h.Allow(context.Background(), "k")
	}
	if !h.Stats().Open {
		t.Fatal("breaker did not open")
	}

	primary.failing.Store(false)
	time.Sleep(60 * time.Millisecond) // let the cooldown lapse

	// The first call after the cooldown is the probe; it succeeds and closes the
	// circuit, so the next call reaches the primary and reports its real limit.
	if _, err := h.Allow(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	r, err := h.Allow(context.Background(), "k")
	if err != nil {
		t.Fatal(err)
	}
	if r.Limit != 100 {
		t.Errorf("Limit = %d after recovery, want the primary's 100 — the breaker "+
			"did not close", r.Limit)
	}
	if h.Stats().Open {
		t.Error("breaker still open after a successful probe")
	}
}

func TestBreakerDisabledWhenThresholdZero(t *testing.T) {
	primary := &flakyLimiter{}
	primary.failing.Store(true)
	h := newHandler(t, primary, nil, noBreaker(fallback.FailOpen))

	for i := 0; i < 20; i++ {
		_, _ = h.Allow(context.Background(), "k")
	}
	if primary.calls.Load() != 20 {
		t.Errorf("primary called %d times, want all 20 with the breaker disabled",
			primary.calls.Load())
	}
	if h.Stats().Open {
		t.Error("disabled breaker reported open")
	}
}

func TestParseStrategy(t *testing.T) {
	for _, s := range []string{"fail_open", "fail_closed", "local_fallback"} {
		if _, err := fallback.ParseStrategy(s); err != nil {
			t.Errorf("ParseStrategy(%q) => %v, want ok", s, err)
		}
	}
	// A typo must not resolve to a permissive default.
	for _, s := range []string{"", "failopen", "fail-open", "open", "nonsense"} {
		if _, err := fallback.ParseStrategy(s); err == nil {
			t.Errorf("ParseStrategy(%q) => ok, want error", s)
		}
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name string
		cfg  fallback.Config
		ok   bool
	}{
		{"default", fallback.DefaultConfig(), true},
		{"no breaker", fallback.Config{Strategy: fallback.FailOpen}, true},
		{"bad strategy", fallback.Config{Strategy: "nope"}, false},
		{"negative threshold", fallback.Config{
			Strategy: fallback.FailOpen, BreakerThreshold: -1}, false},
		{"breaker without cooldown", fallback.Config{
			Strategy: fallback.FailOpen, BreakerThreshold: 3}, false},
		// The breaker works in integer milliseconds. A sub-millisecond cooldown
		// truncates to zero, which does not shorten the cooldown but removes it: the
		// open deadline lands on the current instant, so every request finds it already
		// passed and probes the failing primary anyway.
		{"sub-millisecond cooldown", fallback.Config{
			Strategy:         fallback.FailOpen,
			BreakerThreshold: 3,
			BreakerCooldown:  500 * time.Microsecond,
		}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.ok && err != nil {
				t.Errorf("=> %v, want ok", err)
			}
			if !tc.ok && err == nil {
				t.Error("=> ok, want error")
			}
		})
	}
}
