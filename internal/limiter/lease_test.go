package limiter_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/MarcusPrgin/Ratelimiter/internal/limiter"
)

// countingLimiter records how many calls reach the wrapped limiter, which is how
// these tests measure whether leasing actually saves round trips.
type countingLimiter struct {
	inner limiter.Limiter
	calls atomic.Int64
	units atomic.Int64
}

func (c *countingLimiter) Allow(ctx context.Context, key string) (limiter.Result, error) {
	return c.AllowN(ctx, key, 1)
}

func (c *countingLimiter) AllowN(ctx context.Context, key string, n int64) (limiter.Result, error) {
	c.calls.Add(1)
	res, err := c.inner.AllowN(ctx, key, n)
	if err == nil && res.Allowed {
		c.units.Add(n)
	}
	return res, err
}

func (c *countingLimiter) Name() string { return c.inner.Name() }

func leaseCfg(prefetch int64) limiter.LeaseConfig {
	return limiter.LeaseConfig{TTL: time.Minute, Prefetch: prefetch, NegativeCache: true}
}

// TestLeaseNeverExceedsLimit is the property the previous decision cache violated.
//
// Several LeaseCache instances share one shared limiter, standing in for several
// nodes behind a load balancer. However the requests are distributed, the number
// admitted must never exceed the configured limit — because every unit handed out
// locally was already counted centrally when the lease was drawn.
func TestLeaseNeverExceedsLimit(t *testing.T) {
	ctx := context.Background()
	const (
		limit = 100
		nodes = 4
	)

	shared := &countingLimiter{inner: limiter.NewSlidingWindowCounter(
		limiter.Config{Limit: limit, Window: time.Hour})}

	caches := make([]*limiter.LeaseCache, nodes)
	for i := range caches {
		lc, err := limiter.NewLeaseCache(shared, leaseCfg(8))
		if err != nil {
			t.Fatal(err)
		}
		caches[i] = lc
	}

	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < nodes; i++ {
		for j := 0; j < 200; j++ {
			wg.Add(1)
			go func(node int) {
				defer wg.Done()
				r, err := caches[node].Allow(ctx, "shared-key")
				if err != nil {
					t.Errorf("unexpected error: %v", err)
					return
				}
				if r.Allowed {
					allowed.Add(1)
				}
			}(i)
		}
	}
	wg.Wait()

	got := allowed.Load()
	consumed := shared.units.Load()

	// The safety property: leasing can never admit more than the shared limit,
	// because every unit handed out locally was counted centrally first.
	if got > limit {
		t.Errorf("admitted %d across %d nodes, which exceeds the limit of %d",
			got, nodes, limit)
	}
	if consumed > limit {
		t.Errorf("consumed %d units centrally, which exceeds the limit of %d",
			consumed, limit)
	}
	if got == 0 {
		t.Fatal("admitted nothing")
	}

	// Anything consumed centrally but not admitted is quota stranded in a lease that the
	// burst ended before spending.
	t.Logf("admitted %d, consumed %d centrally, %d units stranded", got, consumed, consumed-got)
	if got < limit*minBurstUtilisation/100 {
		t.Errorf("admitted only %d of %d across %d nodes — quota is being stranded in "+
			"leases nobody spends", got, limit, nodes)
	}
}

// minBurstUtilisation is the floor, as a percentage of the limit, that a simultaneous
// burst must still admit.
//
// A thundering herd on one key is the worst case for utilisation and is inherently
// stochastic: the headroom hint every goroutine reads is stale, so some still claim a
// batch the ending burst cannot spend. Measured range is 76-100% under the race
// detector. The floor is set well below that for slower CI machines while staying far
// above the ~15% that prefetching from a key's very first request produced — which is
// the regression these two tests exist to catch.
const minBurstUtilisation = 50

// TestLeaseUtilisationUnderSustainedLoad covers the realistic case, and it is exact:
// under sequential traffic leasing admits precisely the limit, no more and no less.
//
// Exactness at the boundary comes from shrinking the prefetch as the reported headroom
// runs out. Without that, the final batch claims more than remains to be spent and the
// limit under-admits by up to one batch.
func TestLeaseUtilisationUnderSustainedLoad(t *testing.T) {
	ctx := context.Background()
	const limit = 100

	// Every prefetch size must land on the limit exactly; only the number of round
	// trips should differ.
	for _, prefetch := range []int64{1, 4, 8, 16} {
		t.Run(fmt.Sprintf("prefetch=%d", prefetch), func(t *testing.T) {
			shared := &countingLimiter{inner: limiter.NewSlidingWindowCounter(
				limiter.Config{Limit: limit, Window: time.Hour})}
			lc, err := limiter.NewLeaseCache(shared, leaseCfg(prefetch))
			if err != nil {
				t.Fatal(err)
			}

			allowed := 0
			for i := 0; i < 300; i++ {
				r, err := lc.Allow(ctx, "k")
				if err != nil {
					t.Fatal(err)
				}
				if r.Allowed {
					allowed++
				}
			}

			if allowed != limit {
				t.Errorf("admitted %d of %d under sustained load, want exactly the limit "+
					"(consumed %d centrally)", allowed, limit, shared.units.Load())
			}
		})
	}
}

// TestLeaseReducesSharedCalls checks the efficiency claim: a hot key should reach
// the shared limiter roughly once per (prefetch+1) requests.
func TestLeaseReducesSharedCalls(t *testing.T) {
	ctx := context.Background()
	const (
		requests = 100
		prefetch = 9
	)

	shared := &countingLimiter{inner: limiter.NewSlidingWindowCounter(
		limiter.Config{Limit: 1 << 20, Window: time.Hour})}
	lc, err := limiter.NewLeaseCache(shared, leaseCfg(prefetch))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < requests; i++ {
		if r, err := lc.Allow(ctx, "hot"); err != nil || !r.Allowed {
			t.Fatalf("request %d: allowed=%t err=%v", i, r.Allowed, err)
		}
	}

	calls := shared.calls.Load()
	want := int64(requests / (prefetch + 1))
	if calls > want+1 {
		t.Errorf("shared limiter called %d times for %d requests, want about %d",
			calls, requests, want)
	}

	stats := lc.Stats()
	if stats.Hits+stats.Misses != requests {
		t.Errorf("stats account for %d requests, want %d", stats.Hits+stats.Misses, requests)
	}
	if hr := stats.HitRate(); hr < 0.8 {
		t.Errorf("hit rate %.2f, want >= 0.8 with prefetch=%d", hr, prefetch)
	}
}

// TestLeaseWarmsUpBeforePrefetching pins the warm-up rule: a key's first miss claims
// only what the request needs, and prefetching starts once the key is established.
//
// Prefetching immediately looks cheaper but is worse in the case that matters. In a
// simultaneous burst on a cold key, every concurrent request misses before any lease
// exists, so each claims a whole batch with nobody left to spend it — the quota is
// consumed centrally, stranded locally, and the burst is throttled far below the real
// limit.
func TestLeaseWarmsUpBeforePrefetching(t *testing.T) {
	ctx := context.Background()
	const prefetch = 9

	shared := &countingLimiter{inner: limiter.NewSlidingWindowCounter(
		limiter.Config{Limit: 1 << 20, Window: time.Hour})}
	lc, err := limiter.NewLeaseCache(shared, leaseCfg(prefetch))
	if err != nil {
		t.Fatal(err)
	}

	// First request on a cold key: one unit only.
	if _, err := lc.Allow(ctx, "cold"); err != nil {
		t.Fatal(err)
	}
	if got := shared.units.Load(); got != 1 {
		t.Errorf("first miss consumed %d units centrally, want 1 — a cold key must not "+
			"claim a batch it may never spend", got)
	}

	// Second request: the key is established, so this one claims a batch.
	if _, err := lc.Allow(ctx, "cold"); err != nil {
		t.Fatal(err)
	}
	if got := shared.units.Load(); got != 1+1+prefetch {
		t.Errorf("after two requests %d units were consumed, want %d — prefetching did "+
			"not begin once the key was established", got, 2+prefetch)
	}

	// The batch is now spendable locally: the next several requests cost nothing.
	before := shared.calls.Load()
	for i := 0; i < prefetch; i++ {
		if r, err := lc.Allow(ctx, "cold"); err != nil || !r.Allowed {
			t.Fatalf("request %d: allowed=%t err=%v", i, r.Allowed, err)
		}
	}
	if extra := shared.calls.Load() - before; extra != 0 {
		t.Errorf("%d shared calls while a lease was available, want 0", extra)
	}
}

// TestLeaseColdBurstDoesNotStrandQuota is the property the warm-up exists for: a
// simultaneous burst on a single cold key must still reach the limit, not throttle
// well below it because every concurrent miss claimed a batch nobody spent.
func TestLeaseColdBurstDoesNotStrandQuota(t *testing.T) {
	ctx := context.Background()
	const limit = 100

	shared := &countingLimiter{inner: limiter.NewSlidingWindowCounter(
		limiter.Config{Limit: limit, Window: time.Hour})}
	lc, err := limiter.NewLeaseCache(shared, leaseCfg(8))
	if err != nil {
		t.Fatal(err)
	}

	var allowed atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			r, err := lc.Allow(ctx, "cold-burst")
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

	got := allowed.Load()
	t.Logf("cold burst admitted %d of %d", got, limit)

	if got > limit {
		t.Errorf("admitted %d, exceeding the limit of %d", got, limit)
	}
	// Utilisation under a simultaneous burst is approximate; see minBurstUtilisation.
	if got < limit*minBurstUtilisation/100 {
		t.Errorf("admitted only %d of %d in a cold simultaneous burst — quota is being "+
			"stranded in leases nobody spends", got, limit)
	}
}

// TestLeaseNegativeCacheAbsorbsDenials covers the abuse case: once a caller is
// over quota, further requests should be answered locally rather than costing a
// round trip each.
func TestLeaseNegativeCacheAbsorbsDenials(t *testing.T) {
	ctx := context.Background()

	shared := &countingLimiter{inner: limiter.NewSlidingWindowCounter(
		limiter.Config{Limit: 2, Window: time.Hour})}
	lc, err := limiter.NewLeaseCache(shared, limiter.LeaseConfig{
		TTL: time.Minute, Prefetch: 0, NegativeCache: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		if r, _ := lc.Allow(ctx, "k"); !r.Allowed {
			t.Fatalf("request %d should be allowed", i)
		}
	}
	before := shared.calls.Load()

	for i := 0; i < 50; i++ {
		r, err := lc.Allow(ctx, "k")
		if err != nil {
			t.Fatal(err)
		}
		if r.Allowed {
			t.Fatalf("request %d admitted past the limit", i)
		}
	}

	if extra := shared.calls.Load() - before; extra > 2 {
		t.Errorf("50 denied requests caused %d shared-limiter calls, want the "+
			"negative cache to absorb nearly all of them", extra)
	}
}

// TestLeaseDeniedCacheExpiresWithRetryAfter checks a denial is never held past the
// point the quota would have been available again.
func TestLeaseDeniedCacheExpiresWithRetryAfter(t *testing.T) {
	ctx := context.Background()
	const window = 150 * time.Millisecond

	inner := limiter.NewSlidingWindowCounter(limiter.Config{Limit: 1, Window: window})
	lc, err := limiter.NewLeaseCache(inner, limiter.LeaseConfig{
		// A TTL far longer than the window: the per-entry cap must win.
		TTL: time.Hour, Prefetch: 0, NegativeCache: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	if r, _ := lc.Allow(ctx, "k"); !r.Allowed {
		t.Fatal("first request should be allowed")
	}
	if r, _ := lc.Allow(ctx, "k"); r.Allowed {
		t.Fatal("second request should be denied")
	}

	time.Sleep(2 * window)

	r, err := lc.Allow(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if !r.Allowed {
		t.Error("denial was cached past its retry-after, so recovered quota was not honoured")
	}
}

// TestLeasePrefetchDoesNotCauseSpuriousDenial covers the boundary case: near the
// limit, the batched claim does not fit but the bare request does. The caller must
// get the quota it actually has.
func TestLeasePrefetchDoesNotCauseSpuriousDenial(t *testing.T) {
	ctx := context.Background()

	inner := limiter.NewSlidingWindowCounter(limiter.Config{Limit: 10, Window: time.Hour})
	lc, err := limiter.NewLeaseCache(inner, limiter.LeaseConfig{
		TTL: time.Minute, Prefetch: 4, NegativeCache: false,
	})
	if err != nil {
		t.Fatal(err)
	}

	allowed := 0
	for i := 0; i < 10; i++ {
		r, err := lc.Allow(ctx, "k")
		if err != nil {
			t.Fatal(err)
		}
		if r.Allowed {
			allowed++
		}
	}
	if allowed != 10 {
		t.Errorf("admitted %d of 10 available units — the prefetch denied quota the "+
			"caller was entitled to", allowed)
	}
}

// TestLeaseCostExceedingLimitStillErrors checks the batched claim does not convert
// a client error into something else, and that a cost larger than the limit is not
// silently retried into success.
func TestLeaseCostExceedingLimitStillErrors(t *testing.T) {
	ctx := context.Background()

	inner := limiter.NewSlidingWindowCounter(limiter.Config{Limit: 5, Window: time.Hour})
	lc, err := limiter.NewLeaseCache(inner, leaseCfg(4))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := lc.AllowN(ctx, "k", 6); !errors.Is(err, limiter.ErrCostExceedsLimit) {
		t.Errorf("AllowN(6) with limit 5 => %v, want ErrCostExceedsLimit", err)
	}
	// A cost that fits exactly must still work, even though cost+prefetch does not.
	if r, err := lc.AllowN(ctx, "k", 5); err != nil || !r.Allowed {
		t.Errorf("AllowN(5) with limit 5 => allowed=%t err=%v, want admitted", r.Allowed, err)
	}
}

// TestLeaseDoesNotLeaseDegradedResults checks that a fail-open decision, which has
// no counted quota behind it, is not turned into a local lease that would outlive
// the outage.
func TestLeaseDoesNotLeaseDegradedResults(t *testing.T) {
	ctx := context.Background()

	degraded := stubLimiter{result: limiter.Result{
		Allowed: true, Limit: limiter.LimitUnknown, Remaining: limiter.LimitUnknown,
	}}
	counted := &countingLimiter{inner: &degraded}
	lc, err := limiter.NewLeaseCache(counted, leaseCfg(8))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		if _, err := lc.Allow(ctx, "k"); err != nil {
			t.Fatal(err)
		}
	}
	if calls := counted.calls.Load(); calls != 10 {
		t.Errorf("degraded results produced %d shared calls for 10 requests; they "+
			"must not be cached as leases", calls)
	}
}

func TestLeaseConfigValidation(t *testing.T) {
	inner := limiter.NewSlidingWindowCounter(limiter.Config{Limit: 1, Window: time.Second})

	tests := []struct {
		name string
		cfg  limiter.LeaseConfig
		ok   bool
	}{
		{"prefetch only", limiter.LeaseConfig{TTL: time.Second, Prefetch: 4}, true},
		{"negative cache only", limiter.LeaseConfig{TTL: time.Second, NegativeCache: true}, true},
		{"no ttl", limiter.LeaseConfig{Prefetch: 4}, false},
		{"does nothing", limiter.LeaseConfig{TTL: time.Second}, false},
		{"negative prefetch", limiter.LeaseConfig{TTL: time.Second, Prefetch: -1}, false},
		{"negative ttl", limiter.LeaseConfig{TTL: -time.Second, Prefetch: 1}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := limiter.NewLeaseCache(inner, tc.cfg)
			if tc.ok && err != nil {
				t.Errorf("NewLeaseCache => %v, want ok", err)
			}
			if !tc.ok && err == nil {
				t.Error("NewLeaseCache => ok, want error")
			}
		})
	}
}

// ── Benchmarks ───────────────────────────────────────────────────────────────
//
// All of these run against miniredis, which is in-process, so the absolute numbers
// understate a real Redis round trip by orders of magnitude. The *ratio* between a lease
// hit and a lease miss is the point, and it only widens once the shared limiter is across
// a network.

// BenchmarkRedisSlidingWindow and BenchmarkRedisTokenBucket are the baseline: the bare
// Redis path, one round trip per request.
func BenchmarkRedisSlidingWindow(b *testing.B) {
	benchSerial(b, newBenchRedis(b, limiter.SlidingWindowCounterAlgo))
}

func BenchmarkRedisTokenBucket(b *testing.B) {
	benchSerial(b, newBenchRedis(b, limiter.TokenBucketAlgo))
}

// BenchmarkLeaseOverRedisMiss wraps the same Redis limiter with leasing disabled, so
// every request still pays the round trip. Compared with the baseline it shows what the
// lease machinery itself costs.
func BenchmarkLeaseOverRedisMiss(b *testing.B) {
	benchSerial(b, newBenchLease(b, 0))
}

// BenchmarkLeaseOverRedisHit is the same stack with a prefetch large enough that nearly
// every request is served from the local lease. Against BenchmarkLeaseOverRedisMiss, the
// difference is what leasing buys.
func BenchmarkLeaseOverRedisHit(b *testing.B) {
	lc := newBenchLease(b, 1<<20)
	ctx := context.Background()

	// Warm the key up outside the timer: the first miss establishes it and the second
	// claims the batch. Without this the b.N=1 pass measures only cold misses.
	for i := 0; i < 2; i++ {
		if _, err := lc.Allow(ctx, "bench"); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := lc.Allow(ctx, "bench"); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	// Reported rather than asserted: at small b.N the warm-up misses dominate, which
	// would make an assertion fail on the calibration pass rather than on a regression.
	b.ReportMetric(lc.Stats().HitRate(), "hitrate")
}

func newBenchRedis(b *testing.B, algo limiter.Algorithm) limiter.Limiter {
	b.Helper()
	mr := miniredis.RunT(b)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	b.Cleanup(func() { _ = rdb.Close() })

	// A limit far above b.N, so this measures the admit path rather than the denial path.
	l, err := limiter.NewRedisLimiter(rdb, algo,
		limiter.Config{Limit: 1 << 40, Window: time.Hour, BurstMax: 1 << 40})
	if err != nil {
		b.Fatal(err)
	}
	return l
}

func newBenchLease(b *testing.B, prefetch int64) *limiter.LeaseCache {
	b.Helper()
	lc, err := limiter.NewLeaseCache(
		newBenchRedis(b, limiter.SlidingWindowCounterAlgo),
		limiter.LeaseConfig{TTL: time.Hour, Prefetch: prefetch, NegativeCache: true},
	)
	if err != nil {
		b.Fatal(err)
	}
	return lc
}

// benchSerial drives one key from a single goroutine, so these numbers are
// per-operation cost rather than the lock contention that benchLimiter's parallel
// version measures. Errors are checked: a benchmark that silently fails every call
// reports a very impressive ns/op.
func benchSerial(b *testing.B, l limiter.Limiter) {
	b.Helper()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := l.Allow(ctx, "bench"); err != nil {
			b.Fatal(err)
		}
	}
}

// stubLimiter returns a fixed result, for wiring tests that need a limiter with
// predetermined behaviour.
type stubLimiter struct {
	result limiter.Result
	err    error
	calls  atomic.Int64
}

func (s *stubLimiter) Allow(ctx context.Context, key string) (limiter.Result, error) {
	return s.AllowN(ctx, key, 1)
}

func (s *stubLimiter) AllowN(context.Context, string, int64) (limiter.Result, error) {
	s.calls.Add(1)
	return s.result, s.err
}

func (s *stubLimiter) Name() string { return "stub" }
