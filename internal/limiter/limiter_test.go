package limiter_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/MarcusPrgin/Ratelimiter/internal/limiter"
)

// newRedis starts an in-process Redis. The Lua scripts run for real, including
// redis.call('TIME'), so these tests cover the distributed path rather than only
// the in-memory approximation of it.
func newRedis(t *testing.T) *redis.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// impl is one limiter implementation under test.
type impl struct {
	name  string
	algo  limiter.Algorithm
	build func(t *testing.T, cfg limiter.Config) limiter.Limiter
}

// implementations returns every Limiter that enforces a quota, so the shared
// behavioural contract is asserted against all of them rather than one.
func implementations() []impl {
	return []impl{
		{
			name: "memory/sliding_window_counter",
			algo: limiter.SlidingWindowCounterAlgo,
			build: func(_ *testing.T, cfg limiter.Config) limiter.Limiter {
				return limiter.NewSlidingWindowCounter(cfg)
			},
		},
		{
			name: "memory/token_bucket",
			algo: limiter.TokenBucketAlgo,
			build: func(_ *testing.T, cfg limiter.Config) limiter.Limiter {
				return limiter.NewTokenBucket(cfg)
			},
		},
		{
			name: "redis/sliding_window_counter",
			algo: limiter.SlidingWindowCounterAlgo,
			build: func(t *testing.T, cfg limiter.Config) limiter.Limiter {
				l, err := limiter.NewRedisLimiter(newRedis(t), limiter.SlidingWindowCounterAlgo, cfg)
				if err != nil {
					t.Fatalf("new redis sliding window: %v", err)
				}
				return l
			},
		},
		{
			name: "redis/token_bucket",
			algo: limiter.TokenBucketAlgo,
			build: func(t *testing.T, cfg limiter.Config) limiter.Limiter {
				l, err := limiter.NewRedisLimiter(newRedis(t), limiter.TokenBucketAlgo, cfg)
				if err != nil {
					t.Fatalf("new redis token bucket: %v", err)
				}
				return l
			},
		},
	}
}

// steadyConfig uses a long window so refill and window rollover cannot occur
// during a test. Tests that care about time advancing set their own window.
func steadyConfig(limit int64) limiter.Config {
	return limiter.Config{Limit: limit, Window: time.Hour}
}

func TestAllowUnderLimit(t *testing.T) {
	ctx := context.Background()
	for _, im := range implementations() {
		t.Run(im.name, func(t *testing.T) {
			l := im.build(t, steadyConfig(10))
			for i := 0; i < 10; i++ {
				r, err := l.Allow(ctx, "user1")
				if err != nil {
					t.Fatalf("request %d: %v", i+1, err)
				}
				if !r.Allowed {
					t.Fatalf("request %d of 10 denied under limit", i+1)
				}
			}
		})
	}
}

func TestDenyAtLimit(t *testing.T) {
	ctx := context.Background()
	for _, im := range implementations() {
		t.Run(im.name, func(t *testing.T) {
			l := im.build(t, steadyConfig(5))
			for i := 0; i < 5; i++ {
				if _, err := l.Allow(ctx, "user1"); err != nil {
					t.Fatalf("exhausting quota: %v", err)
				}
			}

			r, err := l.Allow(ctx, "user1")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if r.Allowed {
				t.Fatal("6th request admitted with limit 5")
			}
			if r.RetryAfter <= 0 {
				t.Error("RetryAfter must be positive on denial, or clients cannot back off")
			}
			if r.RetryAfter > r.ResetAfter {
				t.Errorf("RetryAfter (%s) exceeds ResetAfter (%s)", r.RetryAfter, r.ResetAfter)
			}
		})
	}
}

func TestKeyIsolation(t *testing.T) {
	ctx := context.Background()
	for _, im := range implementations() {
		t.Run(im.name, func(t *testing.T) {
			l := im.build(t, steadyConfig(3))
			for i := 0; i < 3; i++ {
				if _, err := l.Allow(ctx, "a"); err != nil {
					t.Fatal(err)
				}
			}
			r, err := l.Allow(ctx, "b")
			if err != nil {
				t.Fatal(err)
			}
			if !r.Allowed {
				t.Fatal("exhausting key a must not affect key b")
			}
		})
	}
}

func TestRemainingCountsDown(t *testing.T) {
	ctx := context.Background()
	for _, im := range implementations() {
		t.Run(im.name, func(t *testing.T) {
			l := im.build(t, steadyConfig(5))
			prev := int64(6)
			for i := 0; i < 5; i++ {
				r, err := l.Allow(ctx, "user1")
				if err != nil {
					t.Fatal(err)
				}
				if r.Remaining >= prev {
					t.Fatalf("request %d: remaining %d did not decrease from %d",
						i+1, r.Remaining, prev)
				}
				prev = r.Remaining
			}
			if prev != 0 {
				t.Errorf("remaining after exhausting quota = %d, want 0", prev)
			}
		})
	}
}

// TestRemainingIsZeroWhenDenied pins the Result contract on the denial path, which
// TestRemainingCountsDown does not reach: it stops at the last admitted request.
//
// The Redis token bucket used to report its leftover fractional tokens here, so a
// request denied with 3.4 tokens against a cost of 4 answered 429 alongside
// X-RateLimit-Remaining: 3 — quota the client cannot spend, and which contradicts
// the Retry-After sent with it. The other three implementations reported zero, so
// the same deployment gave different answers depending on the algorithm.
func TestRemainingIsZeroWhenDenied(t *testing.T) {
	ctx := context.Background()
	for _, im := range implementations() {
		t.Run(im.name, func(t *testing.T) {
			l := im.build(t, steadyConfig(10))

			// Leave a fractional, non-zero balance behind: 7 of 10 spent, then ask
			// for 4. The request cannot be served, but the quota is not exhausted.
			if r, err := l.AllowN(ctx, "k", 7); err != nil || !r.Allowed {
				t.Fatalf("AllowN(7) => allowed=%t err=%v, want true", r.Allowed, err)
			}

			r, err := l.AllowN(ctx, "k", 4)
			if err != nil {
				t.Fatal(err)
			}
			if r.Allowed {
				t.Fatal("AllowN(4) admitted at 7/10")
			}
			if r.Remaining != 0 {
				t.Errorf("remaining on a denial = %d, want 0 — the middleware reports "+
					"this as X-RateLimit-Remaining next to a 429", r.Remaining)
			}
		})
	}
}

// TestConcurrentExactness is the core correctness property: whatever the
// concurrency, exactly Limit requests are admitted. The suite this replaces only
// checked that concurrent access did not trip the race detector, which passes just
// as happily for a limiter that admits the wrong number of requests.
func TestConcurrentExactness(t *testing.T) {
	ctx := context.Background()
	const (
		limit   = 100
		callers = 500
	)

	for _, im := range implementations() {
		t.Run(im.name, func(t *testing.T) {
			l := im.build(t, steadyConfig(limit))

			var allowed atomic.Int64
			var wg sync.WaitGroup
			start := make(chan struct{})

			for i := 0; i < callers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start // maximise the overlap
					r, err := l.Allow(ctx, "contended")
					if err != nil {
						t.Errorf("unexpected error: %v", err)
						return
					}
					if r.Allowed {
						allowed.Add(1)
					}
				}()
			}
			close(start)
			wg.Wait()

			if got := allowed.Load(); got != limit {
				t.Errorf("admitted %d of %d concurrent requests, want exactly %d",
					got, callers, limit)
			}
		})
	}
}

func TestAllowN_CostConsumesProportionally(t *testing.T) {
	ctx := context.Background()
	for _, im := range implementations() {
		t.Run(im.name, func(t *testing.T) {
			l := im.build(t, steadyConfig(10))

			r, err := l.AllowN(ctx, "k", 4)
			if err != nil {
				t.Fatal(err)
			}
			if !r.Allowed || r.Remaining != 6 {
				t.Fatalf("AllowN(4) => allowed=%t remaining=%d, want true/6", r.Allowed, r.Remaining)
			}

			if r, err = l.AllowN(ctx, "k", 4); err != nil || !r.Allowed {
				t.Fatalf("AllowN(4) again => allowed=%t err=%v, want true (8 <= 10)", r.Allowed, err)
			}

			if r, err = l.AllowN(ctx, "k", 3); err != nil {
				t.Fatal(err)
			} else if r.Allowed {
				t.Fatal("AllowN(3) admitted at 8/10")
			}
		})
	}
}

func TestAllowN_ExactlyFillsLimit(t *testing.T) {
	ctx := context.Background()
	for _, im := range implementations() {
		t.Run(im.name, func(t *testing.T) {
			l := im.build(t, steadyConfig(10))

			r, err := l.AllowN(ctx, "k", 10)
			if err != nil {
				t.Fatal(err)
			}
			if !r.Allowed || r.Remaining != 0 {
				t.Fatalf("AllowN(10) => allowed=%t remaining=%d, want true/0", r.Allowed, r.Remaining)
			}
			if r, err = l.AllowN(ctx, "k", 1); err != nil {
				t.Fatal(err)
			} else if r.Allowed {
				t.Fatal("request admitted after the limit was filled exactly")
			}
		})
	}
}

// TestAllowN_CostExceedingCapacityIsAnError pins the distinction between "not
// right now" and "never". A cost larger than the limit can never be satisfied, so
// reporting it as a throttle would put the caller in a retry loop that cannot
// terminate; it has to surface as a client error instead.
func TestAllowN_CostExceedingCapacityIsAnError(t *testing.T) {
	ctx := context.Background()
	for _, im := range implementations() {
		t.Run(im.name, func(t *testing.T) {
			l := im.build(t, steadyConfig(5))

			_, err := l.AllowN(ctx, "k", 6)
			if !errors.Is(err, limiter.ErrCostExceedsLimit) {
				t.Fatalf("AllowN(6) with limit 5 => err=%v, want ErrCostExceedsLimit", err)
			}
			if !limiter.IsCostError(err) {
				t.Error("IsCostError must classify ErrCostExceedsLimit")
			}
		})
	}
}

func TestAllowN_RejectsNonPositiveCost(t *testing.T) {
	ctx := context.Background()
	for _, im := range implementations() {
		t.Run(im.name, func(t *testing.T) {
			l := im.build(t, steadyConfig(5))
			for _, n := range []int64{0, -1} {
				if _, err := l.AllowN(ctx, "k", n); !errors.Is(err, limiter.ErrInvalidCost) {
					t.Errorf("AllowN(%d) => err=%v, want ErrInvalidCost", n, err)
				}
			}
		})
	}
}

// TestSubSecondWindow covers windows shorter than a second. The previous Redis
// implementation derived its window from Window.Seconds() truncated to an integer,
// so any sub-second window produced a divisor of zero and panicked on the first
// request.
func TestSubSecondWindow(t *testing.T) {
	ctx := context.Background()
	for _, im := range implementations() {
		t.Run(im.name, func(t *testing.T) {
			l := im.build(t, limiter.Config{Limit: 5, Window: 200 * time.Millisecond})

			allowed := 0
			for i := 0; i < 10; i++ {
				r, err := l.Allow(ctx, "k")
				if err != nil {
					t.Fatalf("request %d: %v", i, err)
				}
				if r.Allowed {
					allowed++
				}
			}
			if allowed != 5 {
				t.Errorf("admitted %d of 10 with limit 5, want 5", allowed)
			}
		})
	}
}

// TestQuotaRecoversOverTime checks that quota actually comes back, and that it
// does so without ever exceeding the limit in the process.
func TestQuotaRecoversOverTime(t *testing.T) {
	ctx := context.Background()
	const window = 150 * time.Millisecond

	for _, im := range implementations() {
		t.Run(im.name, func(t *testing.T) {
			l := im.build(t, limiter.Config{Limit: 4, Window: window})

			for i := 0; i < 4; i++ {
				if _, err := l.Allow(ctx, "k"); err != nil {
					t.Fatal(err)
				}
			}
			if r, _ := l.Allow(ctx, "k"); r.Allowed {
				t.Fatal("quota not exhausted")
			}

			// Two full windows guarantees recovery for both algorithms: the sliding
			// window has fully rolled and the bucket has refilled to capacity.
			time.Sleep(2 * window)

			r, err := l.Allow(ctx, "k")
			if err != nil {
				t.Fatal(err)
			}
			if !r.Allowed {
				t.Errorf("quota did not recover after %s", 2*window)
			}
		})
	}
}

func TestParseAlgorithm(t *testing.T) {
	for _, name := range []string{"sliding_window_counter", "token_bucket"} {
		if _, err := limiter.ParseAlgorithm(name); err != nil {
			t.Errorf("ParseAlgorithm(%q) => %v, want ok", name, err)
		}
	}
	// The two removed algorithms must not silently resolve to a default.
	for _, name := range []string{"fixed_window", "sliding_window_log", "", "nonsense"} {
		if _, err := limiter.ParseAlgorithm(name); err == nil {
			t.Errorf("ParseAlgorithm(%q) => ok, want error", name)
		}
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name string
		cfg  limiter.Config
		ok   bool
	}{
		{"valid", limiter.Config{Limit: 10, Window: time.Second}, true},
		{"valid sub-second", limiter.Config{Limit: 10, Window: time.Millisecond}, true},
		{"zero limit", limiter.Config{Limit: 0, Window: time.Second}, false},
		{"negative limit", limiter.Config{Limit: -1, Window: time.Second}, false},
		{"zero window", limiter.Config{Limit: 10}, false},
		{"sub-millisecond window", limiter.Config{Limit: 10, Window: time.Microsecond}, false},
		{"negative burst", limiter.Config{Limit: 10, Window: time.Second, BurstMax: -1}, false},
		{"negative max keys", limiter.Config{Limit: 10, Window: time.Second, MaxKeys: -1}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.ok && err != nil {
				t.Errorf("Validate() => %v, want nil", err)
			}
			if !tc.ok && err == nil {
				t.Error("Validate() => nil, want error")
			}
		})
	}
}

// TestTokenBucketAllowsBurst distinguishes the two algorithms: a bucket that has
// been idle holds capacity up to BurstMax, which is the entire reason to choose it
// over the sliding window.
func TestTokenBucketAllowsBurst(t *testing.T) {
	ctx := context.Background()
	cfg := limiter.Config{Limit: 10, Window: time.Hour, BurstMax: 25}

	for _, name := range []string{"memory", "redis"} {
		t.Run(name, func(t *testing.T) {
			var l limiter.Limiter = limiter.NewTokenBucket(cfg)
			if name == "redis" {
				var err error
				l, err = limiter.NewRedisLimiter(newRedis(t), limiter.TokenBucketAlgo, cfg)
				if err != nil {
					t.Fatal(err)
				}
			}

			allowed := 0
			for i := 0; i < 30; i++ {
				r, err := l.Allow(ctx, "k")
				if err != nil {
					t.Fatal(err)
				}
				if r.Allowed {
					allowed++
				}
			}
			if allowed != 25 {
				t.Errorf("admitted %d, want BurstMax=25", allowed)
			}
		})
	}
}

// ── Benchmarks ───────────────────────────────────────────────────────────────

func BenchmarkSlidingWindowCounter(b *testing.B) {
	benchLimiter(b, limiter.NewSlidingWindowCounter(
		limiter.Config{Limit: 1 << 40, Window: time.Hour}))
}

func BenchmarkTokenBucket(b *testing.B) {
	benchLimiter(b, limiter.NewTokenBucket(
		limiter.Config{Limit: 1 << 40, Window: time.Hour, BurstMax: 1 << 40}))
}

// BenchmarkSlidingWindowCounterManyKeys measures the sharded path with keys spread
// across shards, which is what production traffic looks like. The single-key
// benchmark measures lock contention on one shard instead.
func BenchmarkSlidingWindowCounterManyKeys(b *testing.B) {
	l := limiter.NewSlidingWindowCounter(limiter.Config{Limit: 1 << 40, Window: time.Hour})
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			_, _ = l.Allow(ctx, keys[i&(len(keys)-1)])
		}
	})
}

// keys is a fixed pool of distinct keys, sized to a power of two so the benchmark
// can index it with a mask.
var keys = func() []string {
	k := make([]string, 1024)
	for i := range k {
		k[i] = "user:" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26)) +
			string(rune('a'+(i/676)%26))
	}
	return k
}()

func benchLimiter(b *testing.B, l limiter.Limiter) {
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = l.Allow(ctx, "bench-key")
		}
	})
}
