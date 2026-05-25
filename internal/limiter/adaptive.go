package limiter

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// AdaptiveConfig controls the AIMD (additive-increase/multiplicative-decrease) loop.
type AdaptiveConfig struct {
	// LowWatermarkMs: below this observed latency, increase the pass-through multiplier.
	LowWatermarkMs float64
	// HighWatermarkMs: above this observed latency, decrease the pass-through multiplier.
	HighWatermarkMs float64
	// DecreaseRatio: multiplicative decrease factor when latency is high (0 < ratio < 1).
	DecreaseRatio float64
	// IncreaseStep: additive increase per healthy call.
	IncreaseStep float64
	// MinMultiplier: floor — the multiplier never drops below this value.
	MinMultiplier float64
	// EWMAAlpha: smoothing factor for latency EWMA (0 < alpha ≤ 1; lower = slower reaction).
	EWMAAlpha float64
}

func DefaultAdaptiveConfig() AdaptiveConfig {
	return AdaptiveConfig{
		LowWatermarkMs:  2.0,
		HighWatermarkMs: 10.0,
		DecreaseRatio:   0.75,
		IncreaseStep:    0.05,
		MinMultiplier:   0.1,
		EWMAAlpha:       0.1,
	}
}

// AdaptiveLimiter wraps any Limiter and probabilistically sheds load when the
// inner limiter's latency climbs above HighWatermarkMs.
//
// The pass-through multiplier is stored as atomic uint64 (float64 bits) so
// every request reads it without acquiring a lock. Only the EWMA update path,
// which runs once per inner-limiter call, takes a short mutex.
type AdaptiveLimiter struct {
	inner Limiter
	cfg   AdaptiveConfig

	// multiplierU stores math.Float64bits(multiplier) — lock-free hot-path reads.
	multiplierU atomic.Uint64

	ewmaMu sync.Mutex
	ewmaMs float64 // EWMA of inner-limiter call latency in milliseconds
}

// NewAdaptiveLimiter creates an AdaptiveLimiter wrapping inner.
// The multiplier starts at 1.0 (no shedding).
func NewAdaptiveLimiter(inner Limiter, cfg AdaptiveConfig) *AdaptiveLimiter {
	a := &AdaptiveLimiter{inner: inner, cfg: cfg}
	a.setMultiplier(1.0)
	return a
}

func (a *AdaptiveLimiter) Allow(ctx context.Context, key string) (Result, error) {
	return a.AllowN(ctx, key, 1)
}

// AllowN probabilistically rejects requests when the multiplier is below 1.0,
// then delegates to the inner limiter and records its latency for AIMD adjustment.
func (a *AdaptiveLimiter) AllowN(ctx context.Context, key string, n int64) (Result, error) {
	if m := a.Multiplier(); m < 1.0 && rand.Float64() >= m {
		return Result{
			Allowed:  false,
			DeniedBy: "adaptive_shed",
			Limit:    -1,
		}, nil
	}

	start := time.Now()
	result, err := a.inner.AllowN(ctx, key, n)
	a.recordLatency(time.Since(start))

	return result, err
}

// Multiplier returns the current pass-through fraction (0.1 – 1.0). Lock-free.
func (a *AdaptiveLimiter) Multiplier() float64 {
	return math.Float64frombits(a.multiplierU.Load())
}

// ForceMultiplier overrides the multiplier directly. Useful in tests.
func (a *AdaptiveLimiter) ForceMultiplier(m float64) { a.setMultiplier(m) }

func (a *AdaptiveLimiter) Name() string { return "adaptive/" + a.inner.Name() }

func (a *AdaptiveLimiter) recordLatency(d time.Duration) {
	ms := float64(d.Microseconds()) / 1000.0

	a.ewmaMu.Lock()
	a.ewmaMs = a.cfg.EWMAAlpha*ms + (1-a.cfg.EWMAAlpha)*a.ewmaMs
	ewma := a.ewmaMs
	a.ewmaMu.Unlock()

	cur := a.Multiplier()
	var next float64
	switch {
	case ewma > a.cfg.HighWatermarkMs:
		next = math.Max(a.cfg.MinMultiplier, cur*a.cfg.DecreaseRatio)
	case ewma < a.cfg.LowWatermarkMs:
		next = math.Min(1.0, cur+a.cfg.IncreaseStep)
	default:
		return // latency within bounds — no adjustment needed
	}
	a.setMultiplier(next)
}

func (a *AdaptiveLimiter) setMultiplier(m float64) {
	a.multiplierU.Store(math.Float64bits(m))
}
