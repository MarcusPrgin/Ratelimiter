package limiter

import (
	"context"
	"math"
	"time"

	"github.com/MarcusPrgin/Ratelimiter/internal/shardmap"
)

// SlidingWindowCounter is the in-memory sliding window counter.
//
// It keeps two counters per key — the current fixed window and the previous one
// — and interpolates between them:
//
//	effective = round(prev_count × (1 - elapsed/window)) + curr_count
//
// That costs O(1) memory per key while avoiding the boundary burst a plain fixed
// window allows (where a caller can spend its whole quota at the end of one
// window and again at the start of the next, admitting 2×Limit in an instant).
//
// Windows are aligned to absolute epoch boundaries, matching RedisSlidingWindow,
// so a key sees the same boundaries whether it is served locally or from Redis.
// This is what makes the local fallback a continuation of the distributed
// limiter rather than a reset of it.
type SlidingWindowCounter struct {
	store    *shardmap.Map[swcState]
	cfg      Config
	windowMs int64
}

type swcState struct {
	prevCount int64
	currCount int64
	// winStartMs is the epoch-aligned start of the current window, in
	// milliseconds. Zero means "no window yet".
	winStartMs int64
}

// NewSlidingWindowCounter builds an in-memory sliding window counter.
// cfg must have passed Validate.
func NewSlidingWindowCounter(cfg Config) *SlidingWindowCounter {
	cfg = cfg.withDefaults()
	windowMs := cfg.Window.Milliseconds()
	return &SlidingWindowCounter{
		// After two windows both counters read zero, so the state is
		// indistinguishable from a fresh key and eviction loses nothing.
		store:    shardmap.New[swcState](cfg.MaxKeys, 2*cfg.Window),
		cfg:      cfg,
		windowMs: windowMs,
	}
}

func (s *SlidingWindowCounter) Allow(ctx context.Context, key string) (Result, error) {
	return s.AllowN(ctx, key, 1)
}

func (s *SlidingWindowCounter) AllowN(_ context.Context, key string, n int64) (Result, error) {
	if err := checkCost(n, s.cfg.Limit); err != nil {
		return Result{}, err
	}

	now := time.Now()
	nowMs := now.UnixMilli()
	winStart := nowMs - nowMs%s.windowMs
	elapsed := nowMs - winStart

	var res Result
	s.store.Do(key, now, func() swcState { return swcState{} }, func(st *swcState) {
		s.roll(st, winStart)

		prevWeight := float64(s.windowMs-elapsed) / float64(s.windowMs)
		// Round rather than truncate. Truncating systematically under-counts the
		// carried-over window, which biases the limiter toward over-admitting.
		effective := int64(float64(st.prevCount)*prevWeight+0.5) + st.currCount

		resetAfter := time.Duration(s.windowMs-elapsed) * time.Millisecond

		if effective+n > s.cfg.Limit {
			res = Result{
				Allowed:    false,
				Limit:      s.cfg.Limit,
				Remaining:  0,
				ResetAfter: resetAfter,
				RetryAfter: retryAfterForCarryover(
					effective+n-s.cfg.Limit, st.prevCount, s.windowMs, resetAfter),
			}
			return
		}

		st.currCount += n
		res = Result{
			Allowed:    true,
			Limit:      s.cfg.Limit,
			Remaining:  s.cfg.Limit - effective - n,
			ResetAfter: resetAfter,
		}
	})
	return res, nil
}

// roll advances the two counters if the window boundary has moved. A jump of
// more than one window means the previous window is entirely in the past and
// contributes nothing.
func (s *SlidingWindowCounter) roll(st *swcState, winStart int64) {
	if st.winStartMs == winStart {
		return
	}
	if st.winStartMs != 0 && winStart-st.winStartMs == s.windowMs {
		st.prevCount = st.currCount
	} else {
		st.prevCount = 0
	}
	st.currCount = 0
	st.winStartMs = winStart
}

func (s *SlidingWindowCounter) Name() string { return string(SlidingWindowCounterAlgo) }

// Keys reports the number of tracked keys, for the active-keys gauge.
func (s *SlidingWindowCounter) Keys() int { return s.store.Len() }

// retryAfterForCarryover computes how long until enough of the previous window
// ages out to fit a request that is `need` units over the limit.
//
// The carried-over contribution decays by prevCount/windowMs units per
// millisecond, so the wait is need ÷ that rate. When the previous window is
// empty nothing decays before the next boundary, so the caller waits out the
// full window.
//
// Rounded up, matching sliding_window.lua's math.ceil. Rounding to nearest sends the
// caller back up to half a millisecond early, which earns it a second denial — and,
// with the penalty box enabled, a second strike for the service's own rounding.
func retryAfterForCarryover(need, prevCount, windowMs int64, resetAfter time.Duration) time.Duration {
	if need <= 0 {
		return 0
	}
	if prevCount <= 0 {
		return resetAfter
	}
	perMs := float64(prevCount) / float64(windowMs)
	wait := time.Duration(math.Ceil(float64(need)/perMs)) * time.Millisecond
	if wait > resetAfter || wait <= 0 {
		return resetAfter
	}
	return wait
}
