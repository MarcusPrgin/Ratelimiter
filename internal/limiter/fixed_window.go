package limiter

import (
	"context"
	"sync"
	"time"
)

// FixedWindow is simple and fast but vulnerable to boundary bursts:
// a caller can send 2× the limit by straddling two windows.
// Keep this implementation as a useful demo of the failure mode.
type FixedWindow struct {
	mu      sync.Mutex
	windows map[string]*fixedWindowEntry
	cfg     Config
}

type fixedWindowEntry struct {
	count     int64
	windowEnd time.Time
}

func NewFixedWindow(cfg Config) *FixedWindow {
	return &FixedWindow{
		windows: make(map[string]*fixedWindowEntry),
		cfg:     cfg,
	}
}

func (f *FixedWindow) Allow(ctx context.Context, key string) (Result, error) {
	return f.AllowN(ctx, key, 1)
}

func (f *FixedWindow) AllowN(_ context.Context, key string, n int64) (Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	now := time.Now()
	entry, ok := f.windows[key]
	if !ok || now.After(entry.windowEnd) {
		entry = &fixedWindowEntry{windowEnd: now.Add(f.cfg.Window)}
		f.windows[key] = entry
	}

	resetAfter := entry.windowEnd.Sub(now)

	if entry.count+n > f.cfg.Limit {
		return Result{
			Allowed:    false,
			Limit:      f.cfg.Limit,
			Remaining:  0,
			ResetAfter: resetAfter,
			RetryAfter: resetAfter,
		}, nil
	}

	entry.count += n
	return Result{
		Allowed:    true,
		Limit:      f.cfg.Limit,
		Remaining:  f.cfg.Limit - entry.count,
		ResetAfter: resetAfter,
	}, nil
}

func (f *FixedWindow) Name() string { return "fixed_window" }
