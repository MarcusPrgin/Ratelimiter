package limiter

import (
	"context"
	"sync"
	"time"
)

// SlidingWindowLog stores a timestamp for every request in the current window.
// Perfectly accurate but O(n) memory per key — not viable at scale.
// Included for benchmarking comparison and to illustrate the tradeoff.
type SlidingWindowLog struct {
	mu   sync.Mutex
	logs map[string][]time.Time
	cfg  Config
}

func NewSlidingWindowLog(cfg Config) *SlidingWindowLog {
	return &SlidingWindowLog{
		logs: make(map[string][]time.Time),
		cfg:  cfg,
	}
}

func (s *SlidingWindowLog) Allow(ctx context.Context, key string) (Result, error) {
	return s.AllowN(ctx, key, 1)
}

func (s *SlidingWindowLog) AllowN(_ context.Context, key string, n int64) (Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	windowStart := now.Add(-s.cfg.Window)

	// evict timestamps outside the window, reusing the backing array
	log := s.logs[key]
	valid := log[:0]
	for _, t := range log {
		if t.After(windowStart) {
			valid = append(valid, t)
		}
	}
	s.logs[key] = valid

	count := int64(len(valid))
	if count+n > s.cfg.Limit {
		var retryAfter time.Duration
		if len(valid) > 0 {
			retryAfter = valid[0].Add(s.cfg.Window).Sub(now)
		}
		return Result{
			Allowed:    false,
			Limit:      s.cfg.Limit,
			Remaining:  0,
			ResetAfter: s.cfg.Window,
			RetryAfter: retryAfter,
		}, nil
	}

	// append n timestamps; pre-grow the slice once to avoid repeat allocations
	needed := count + n
	if int64(cap(s.logs[key])) < needed {
		grown := make([]time.Time, len(s.logs[key]), needed)
		copy(grown, s.logs[key])
		s.logs[key] = grown
	}
	for i := int64(0); i < n; i++ {
		s.logs[key] = append(s.logs[key], now)
	}

	return Result{
		Allowed:    true,
		Limit:      s.cfg.Limit,
		Remaining:  s.cfg.Limit - count - n,
		ResetAfter: s.cfg.Window,
	}, nil
}

func (s *SlidingWindowLog) Name() string { return "sliding_window_log" }
