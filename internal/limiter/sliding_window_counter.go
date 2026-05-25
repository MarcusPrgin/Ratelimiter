package limiter

import (
	"context"
	"sync"
	"time"
)

// SlidingWindowCounter is the production-grade O(1) algorithm.
// Interpolates between current and previous window counts.
// Error rate is ~0.003% in practice — what Cloudflare uses.
//
// Formula: effective = prev_count × (1 - elapsed/window) + curr_count
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

func (s *SlidingWindowCounter) Allow(ctx context.Context, key string) (Result, error) {
	return s.AllowN(ctx, key, 1)
}

func (s *SlidingWindowCounter) AllowN(_ context.Context, key string, n int64) (Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	entry, ok := s.windows[key]
	if !ok {
		entry = &swcEntry{windowStart: now}
		s.windows[key] = entry
	}

	elapsed := now.Sub(entry.windowStart)

	if elapsed >= s.cfg.Window {
		if elapsed >= 2*s.cfg.Window {
			entry.prevCount = 0
		} else {
			entry.prevCount = entry.currCount
		}
		entry.currCount = 0
		entry.windowStart = now.Truncate(s.cfg.Window)
		elapsed = now.Sub(entry.windowStart)
	}

	prevWeight := 1.0 - elapsed.Seconds()/s.cfg.Window.Seconds()
	effective := int64(float64(entry.prevCount)*prevWeight) + entry.currCount
	resetAfter := s.cfg.Window - elapsed

	if effective+n > s.cfg.Limit {
		return Result{
			Allowed:    false,
			Limit:      s.cfg.Limit,
			Remaining:  0,
			ResetAfter: resetAfter,
			RetryAfter: resetAfter,
		}, nil
	}

	entry.currCount += n
	return Result{
		Allowed:    true,
		Limit:      s.cfg.Limit,
		Remaining:  s.cfg.Limit - effective - n,
		ResetAfter: resetAfter,
	}, nil
}

func (s *SlidingWindowCounter) Name() string { return "sliding_window_counter" }
