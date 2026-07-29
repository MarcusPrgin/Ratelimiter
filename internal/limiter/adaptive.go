package limiter

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"sync/atomic"
	"time"
)

// AdaptiveConfig controls the AIMD (additive-increase / multiplicative-decrease)
// control loop.
type AdaptiveConfig struct {
	// LowWatermarkMs: below this observed latency, increase the pass-through
	// multiplier.
	LowWatermarkMs float64
	// HighWatermarkMs: above this observed latency, decrease it.
	HighWatermarkMs float64
	// DecreaseRatio: multiplicative decrease factor on overload. In (0, 1).
	DecreaseRatio float64
	// IncreaseStep: additive increase per adjustment when healthy. In (0, 1].
	IncreaseStep float64
	// MinMultiplier: floor, so the limiter never sheds everything. In (0, 1].
	MinMultiplier float64
	// EWMAAlpha: smoothing factor for the latency EWMA. In (0, 1]; lower reacts
	// more slowly.
	EWMAAlpha float64
	// AdjustInterval is the minimum time between multiplier adjustments.
	//
	// This is what makes the loop a controller rather than a counter. Adjusting
	// once per request ties the response rate to traffic volume: at 100k rps the
	// multiplier slams to the floor within a millisecond and recovers just as
	// abruptly, while a nearly idle service never recovers at all because
	// recovery needs one call per step. Clamping adjustments to a fixed interval
	// makes the decay and recovery rates depend on time instead.
	AdjustInterval time.Duration
	// ShedRetryAfter is advertised to clients as Retry-After when a request is
	// shed. Shedding is a signal about total load, not about the caller's quota,
	// so there is no per-key reset time to report.
	ShedRetryAfter time.Duration
}

func DefaultAdaptiveConfig() AdaptiveConfig {
	return AdaptiveConfig{
		LowWatermarkMs:  2.0,
		HighWatermarkMs: 10.0,
		DecreaseRatio:   0.75,
		IncreaseStep:    0.05,
		MinMultiplier:   0.1,
		EWMAAlpha:       0.1,
		AdjustInterval:  100 * time.Millisecond,
		ShedRetryAfter:  time.Second,
	}
}

// Validate reports whether the config describes a stable control loop.
func (c AdaptiveConfig) Validate() error {
	if c.HighWatermarkMs <= 0 {
		return fmt.Errorf("high_watermark_ms must be > 0, got %g", c.HighWatermarkMs)
	}
	if c.LowWatermarkMs <= 0 {
		return fmt.Errorf("low_watermark_ms must be > 0, got %g", c.LowWatermarkMs)
	}
	if c.LowWatermarkMs >= c.HighWatermarkMs {
		return fmt.Errorf("low_watermark_ms (%g) must be < high_watermark_ms (%g); "+
			"with no gap between them the multiplier oscillates every interval",
			c.LowWatermarkMs, c.HighWatermarkMs)
	}
	if c.DecreaseRatio <= 0 || c.DecreaseRatio >= 1 {
		return fmt.Errorf("decrease_ratio must be in (0,1), got %g", c.DecreaseRatio)
	}
	if c.IncreaseStep <= 0 || c.IncreaseStep > 1 {
		return fmt.Errorf("increase_step must be in (0,1], got %g", c.IncreaseStep)
	}
	if c.MinMultiplier <= 0 || c.MinMultiplier > 1 {
		return fmt.Errorf("min_multiplier must be in (0,1], got %g", c.MinMultiplier)
	}
	if c.EWMAAlpha <= 0 || c.EWMAAlpha > 1 {
		return fmt.Errorf("ewma_alpha must be in (0,1], got %g", c.EWMAAlpha)
	}
	if c.AdjustInterval <= 0 {
		return fmt.Errorf("adjust_interval must be > 0, got %s", c.AdjustInterval)
	}
	if c.ShedRetryAfter < 0 {
		return fmt.Errorf("shed_retry_after must be >= 0, got %s", c.ShedRetryAfter)
	}
	return nil
}

// AdaptiveLimiter wraps a Limiter and probabilistically sheds load when the
// inner limiter's latency climbs above HighWatermarkMs.
//
// The point is to protect Redis from a stampede: once it starts to slow down,
// sending it more traffic makes the latency worse for everyone, so the limiter
// drops a fraction of requests to keep the rest fast. AIMD is the same control
// law as TCP congestion control — back off sharply, recover gently — chosen
// because the asymmetry damps oscillation.
//
// Every field is atomic and the hot path takes no lock: reads are a single
// atomic load, and the two accumulators are updated with compare-and-swap.
type AdaptiveLimiter struct {
	inner Limiter
	cfg   AdaptiveConfig
	name  string

	// multiplierU holds math.Float64bits of the pass-through fraction.
	multiplierU atomic.Uint64
	// ewmaU holds math.Float64bits of the latency EWMA in milliseconds.
	ewmaU atomic.Uint64
	// lastAdjustMs is the epoch-ms timestamp of the last multiplier adjustment.
	lastAdjustMs atomic.Int64
	// shed counts requests dropped by shedding, for the metrics layer to publish.
	shed atomic.Uint64
}

// NewAdaptiveLimiter wraps inner. The multiplier starts at 1.0 (no shedding).
func NewAdaptiveLimiter(inner Limiter, cfg AdaptiveConfig) (*AdaptiveLimiter, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	a := &AdaptiveLimiter{
		inner: inner,
		cfg:   cfg,
		// Precomputed: Name() is called on every request by the metrics layer,
		// and concatenating on each call allocates a string per request.
		name: "adaptive/" + inner.Name(),
	}
	a.storeFloat(&a.multiplierU, 1.0)
	return a, nil
}

func (a *AdaptiveLimiter) Allow(ctx context.Context, key string) (Result, error) {
	return a.AllowN(ctx, key, 1)
}

// AllowN sheds the request with probability (1 - multiplier), otherwise delegates
// to the inner limiter and feeds its latency into the control loop.
func (a *AdaptiveLimiter) AllowN(ctx context.Context, key string, n int64) (Result, error) {
	// rand/v2's top-level generator uses per-P state, so this needs no lock.
	// The v1 global source is mutex-guarded and would serialise the hot path.
	if m := a.Multiplier(); m < 1.0 && rand.Float64() >= m {
		a.shed.Add(1)
		return Result{
			Allowed:    false,
			Limit:      LimitUnknown,
			DeniedBy:   ShedDeniedBy,
			RetryAfter: a.cfg.ShedRetryAfter,
		}, nil
	}

	start := time.Now()
	result, err := a.inner.AllowN(ctx, key, n)
	elapsed := time.Since(start)

	// A rejected cost is a client error, not a signal about backend health;
	// folding it into the EWMA would be noise.
	if err == nil || !IsCostError(err) {
		a.observe(elapsed)
	}
	return result, err
}

// Multiplier returns the current pass-through fraction. Lock-free.
func (a *AdaptiveLimiter) Multiplier() float64 { return a.loadFloat(&a.multiplierU) }

// EWMA returns the smoothed inner-limiter latency in milliseconds. Lock-free.
func (a *AdaptiveLimiter) EWMA() float64 { return a.loadFloat(&a.ewmaU) }

// Shed returns the cumulative count of shed requests.
func (a *AdaptiveLimiter) Shed() uint64 { return a.shed.Load() }

// ForceMultiplier overrides the multiplier. Exported for tests.
func (a *AdaptiveLimiter) ForceMultiplier(m float64) { a.storeFloat(&a.multiplierU, m) }

func (a *AdaptiveLimiter) Name() string { return a.name }

// observe folds one latency sample into the EWMA and, at most once per
// AdjustInterval, steps the multiplier.
func (a *AdaptiveLimiter) observe(d time.Duration) {
	ms := float64(d.Microseconds()) / 1000.0

	// CAS loop rather than a mutex. A plain load-compute-store would lose
	// concurrent samples.
	for {
		old := a.ewmaU.Load()
		cur := math.Float64frombits(old)
		next := a.cfg.EWMAAlpha*ms + (1-a.cfg.EWMAAlpha)*cur
		if a.ewmaU.CompareAndSwap(old, math.Float64bits(next)) {
			break
		}
	}

	nowMs := time.Now().UnixMilli()
	last := a.lastAdjustMs.Load()
	if nowMs-last < a.cfg.AdjustInterval.Milliseconds() {
		return
	}
	// Whichever caller wins this CAS performs the adjustment for this interval;
	// the rest return without touching the multiplier.
	if !a.lastAdjustMs.CompareAndSwap(last, nowMs) {
		return
	}
	a.adjust()
}

// adjust applies one AIMD step based on the current EWMA.
func (a *AdaptiveLimiter) adjust() {
	ewma := a.EWMA()
	for {
		old := a.multiplierU.Load()
		cur := math.Float64frombits(old)

		var next float64
		switch {
		case ewma > a.cfg.HighWatermarkMs:
			next = math.Max(a.cfg.MinMultiplier, cur*a.cfg.DecreaseRatio)
		case ewma < a.cfg.LowWatermarkMs:
			next = math.Min(1.0, cur+a.cfg.IncreaseStep)
		default:
			return // inside the deadband — hold steady
		}
		if next == cur {
			return
		}
		if a.multiplierU.CompareAndSwap(old, math.Float64bits(next)) {
			return
		}
	}
}

func (a *AdaptiveLimiter) loadFloat(u *atomic.Uint64) float64 {
	return math.Float64frombits(u.Load())
}

func (a *AdaptiveLimiter) storeFloat(u *atomic.Uint64, f float64) {
	u.Store(math.Float64bits(f))
}
