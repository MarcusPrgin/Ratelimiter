package limiter

import (
	"context"
	"sync"
	"time"
)

// SlidingWindowLog stores a timestamp for every request in the current window.
// This is perfectly accurate but uses O(n) memory per key — not viable at scale.
// Included so you can benchmark it against the counter approach and explain the tradeoff.
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

func (s *SlidingWindowLog) Allow(_ context.Context, key string) (Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	windowStart := now.Add(-s.cfg.Window)

	// evict timestamps outside the window
	log := s.logs[key]
	valid := log[:0]
	for _, t := range log {
		if t.After(windowStart) {
			valid = append(valid, t)
		}
	}
	s.logs[key] = valid

	count := int64(len(valid))
	remaining := s.cfg.Limit - count

	if count >= s.cfg.Limit {
		var retryAfter time.Duration
		if len(valid) > 0 {
			// oldest request expires first
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

	s.logs[key] = append(s.logs[key], now)
	return Result{
		Allowed:    true,
		Limit:      s.cfg.Limit,
		Remaining:  remaining - 1,
		ResetAfter: s.cfg.Window,
	}, nil
}

func (s *SlidingWindowLog) Name() string { return "sliding_window_log" }
