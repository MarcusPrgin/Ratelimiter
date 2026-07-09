package limiter_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/yourname/ratelimiter/internal/limiter"
)

// newAll returns all four implementations with the given config.
// Used in table-driven tests that must pass for every algorithm.
func newAll(cfg limiter.Config) []limiter.Limiter {
	return []limiter.Limiter{
		limiter.NewFixedWindow(cfg),
		limiter.NewSlidingWindowLog(cfg),
		limiter.NewSlidingWindowCounter(cfg),
		limiter.NewTokenBucket(cfg),
	}
}

func TestAllowUnderLimit(t *testing.T) {
	cfg := limiter.Config{Limit: 10, Window: time.Second}
	ctx := context.Background()

	for _, l := range newAll(cfg) {
		t.Run(l.Name(), func(t *testing.T) {
			for i := 0; i < 10; i++ {
				r, err := l.Allow(ctx, "user1")
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if !r.Allowed {
					t.Fatalf("request %d should be allowed, got denied", i+1)
				}
			}
		})
	}
}

func TestDenyAtLimit(t *testing.T) {
	cfg := limiter.Config{Limit: 5, Window: time.Second}
	ctx := context.Background()

	for _, l := range newAll(cfg) {
		t.Run(l.Name(), func(t *testing.T) {
			// exhaust limit
			for i := 0; i < 5; i++ {
				l.Allow(ctx, "user1") //nolint
			}
			// next should be denied
			r, err := l.Allow(ctx, "user1")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if r.Allowed {
				t.Fatal("6th request should be denied")
			}
			if r.RetryAfter <= 0 {
				t.Fatal("RetryAfter should be positive when denied")
			}
		})
	}
}

func TestKeyIsolation(t *testing.T) {
	cfg := limiter.Config{Limit: 3, Window: time.Second}
	ctx := context.Background()

	for _, l := range newAll(cfg) {
		t.Run(l.Name(), func(t *testing.T) {
			// exhaust key "a"
			for i := 0; i < 3; i++ {
				l.Allow(ctx, "a") //nolint
			}
			// key "b" should still be allowed
			r, err := l.Allow(ctx, "b")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !r.Allowed {
				t.Fatal("different key should have independent counter")
			}
		})
	}
}

func TestRemainingCountsDown(t *testing.T) {
	cfg := limiter.Config{Limit: 5, Window: time.Second}
	ctx := context.Background()

	for _, l := range newAll(cfg) {
		t.Run(l.Name(), func(t *testing.T) {
			prev := cfg.Limit
			for i := 0; i < 5; i++ {
				r, _ := l.Allow(ctx, "user1")
				if r.Remaining >= prev {
					t.Fatalf("remaining should decrease: got %d after %d was %d", r.Remaining, i, prev)
				}
				prev = r.Remaining
			}
		})
	}
}

// TestConcurrentSafety fires 1000 goroutines and checks for data races.
// Run with: go test -race ./internal/limiter/...
func TestConcurrentSafety(t *testing.T) {
	cfg := limiter.Config{Limit: 100, Window: time.Second, BurstMax: 200}
	ctx := context.Background()

	for _, l := range newAll(cfg) {
		t.Run(l.Name(), func(t *testing.T) {
			var wg sync.WaitGroup
			for i := 0; i < 1000; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					l.Allow(ctx, "concurrent-key") //nolint
				}()
			}
			wg.Wait()
		})
	}
}

// BenchmarkAlgorithms lets you compare ns/op across all four.
// Run with: go test -bench=. ./internal/limiter/...
func BenchmarkFixedWindow(b *testing.B) {
	benchLimiter(b, limiter.NewFixedWindow(limiter.Config{Limit: 1000, Window: time.Second}))
}
func BenchmarkSlidingWindowLog(b *testing.B) {
	benchLimiter(b, limiter.NewSlidingWindowLog(limiter.Config{Limit: 1000, Window: time.Second}))
}
func BenchmarkSlidingWindowCounter(b *testing.B) {
	benchLimiter(b, limiter.NewSlidingWindowCounter(limiter.Config{Limit: 1000, Window: time.Second}))
}
func BenchmarkTokenBucket(b *testing.B) {
	benchLimiter(b, limiter.NewTokenBucket(limiter.Config{Limit: 1000, Window: time.Second, BurstMax: 1000}))
}

func benchLimiter(b *testing.B, l limiter.Limiter) {
	b.Helper()
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Allow(ctx, "bench-key") //nolint
		}
	})
}
