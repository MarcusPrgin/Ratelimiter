package limiter

import (
	"context"
	"sync"
	"time"
)

// SlidingWindowCounter is the production-grade algorithm.
// O(1) space per key. Interpolates between current and previous window count.
// Error rate is ~0.003% in practice — what Cloudflare uses.
//
// Formula: effective_count = prev_count * (1 - elapsed/window) + curr_count
type SlidingWindowCounter struct {
	mu      sync.Mutex
	windows map[string]*swcEntry
	cfg     Config
}

type swcEntry struct {
	prevCount   int64
	currCount   int64
	windowStart time.Time
}

func NewSlidingWindowCounter(cfg Config) *SlidingWindowCounter {
	return &SlidingWindowCounter{
		windows: make(map[string]*swcEntry),
		cfg:     cfg,
	}
}

func (s *SlidingWindowCounter) Allow(_ context.Context, key string) (Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	entry, ok := s.windows[key]

	if !ok {
		entry = &swcEntry{windowStart: now}
		s.windows[key] = entry
	}

	elapsed := now.Sub(entry.windowStart)

	// rolled past one full window — advance
	if elapsed >= s.cfg.Window {
		// if we've rolled past TWO windows, prev is stale — reset
		if elapsed >= 2*s.cfg.Window {
			entry.prevCount = 0
		} else {
			entry.prevCount = entry.currCount
		}
		entry.currCount = 0
		entry.windowStart = now.Truncate(s.cfg.Window)
		elapsed = now.Sub(entry.windowStart)
	}

	// weight of previous window's contribution
	prevWeight := 1.0 - elapsed.Seconds()/s.cfg.Window.Seconds()
	effective := int64(float64(entry.prevCount)*prevWeight) + entry.currCount

	resetAfter := s.cfg.Window - elapsed

	if effective >= s.cfg.Limit {
		return Result{
			Allowed:    false,
			Limit:      s.cfg.Limit,
			Remaining:  0,
			ResetAfter: resetAfter,
			RetryAfter: resetAfter,
		}, nil
	}

	entry.currCount++
	return Result{
		Allowed:    true,
		Limit:      s.cfg.Limit,
		Remaining:  s.cfg.Limit - effective - 1,
		ResetAfter: resetAfter,
	}, nil
}

func (s *SlidingWindowCounter) Name() string { return "sliding_window_counter" }
