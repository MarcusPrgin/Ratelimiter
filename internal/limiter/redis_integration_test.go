package limiter_test

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/MarcusPrgin/Ratelimiter/internal/limiter"
)

// The rest of the suite runs the Lua scripts on miniredis, which is a
// reimplementation of Redis with its own Lua host. These tests run the same scripts
// against a real Redis so the two cannot quietly diverge on the behaviour the
// limiter depends on — redis.call('TIME') resolution, INCRBY on a missing key,
// PEXPIRE semantics, and how Lua numbers are converted on the way in and out.
//
// Set RATELIMITER_TEST_REDIS_ADDR to run them; CI provides a Redis service.

const testRedisEnv = "RATELIMITER_TEST_REDIS_ADDR"

func realRedis(t *testing.T) *redis.Client {
	t.Helper()

	addr := os.Getenv(testRedisEnv)
	if addr == "" {
		t.Skipf("set %s to run integration tests against a real Redis", testRedisEnv)
	}

	c := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Fatalf("cannot reach Redis at %s: %v", addr, err)
	}

	// Each test gets a private keyspace, so a shared Redis does not make tests
	// interfere with one another.
	prefix := "test:" + t.Name() + ":"
	t.Cleanup(func() {
		keys, err := c.Keys(context.Background(), "*"+prefix+"*").Result()
		if err == nil && len(keys) > 0 {
			_ = c.Del(context.Background(), keys...).Err()
		}
		_ = c.Close()
	})
	return c
}

func integrationKey(t *testing.T, name string) string {
	return "test:" + t.Name() + ":" + name
}

func TestIntegrationRedisLimitersEnforceExactly(t *testing.T) {
	ctx := context.Background()

	for _, algo := range []limiter.Algorithm{
		limiter.SlidingWindowCounterAlgo, limiter.TokenBucketAlgo,
	} {
		t.Run(string(algo), func(t *testing.T) {
			rdb := realRedis(t)
			l, err := limiter.NewRedisLimiter(rdb, algo,
				limiter.Config{Limit: 20, Window: time.Hour})
			if err != nil {
				t.Fatal(err)
			}

			key := integrationKey(t, "exact")
			allowed := 0
			for i := 0; i < 50; i++ {
				r, err := l.Allow(ctx, key)
				if err != nil {
					t.Fatalf("request %d: %v", i, err)
				}
				if r.Allowed {
					allowed++
				}
			}
			if allowed != 20 {
				t.Errorf("admitted %d of 50 with limit 20, want exactly 20", allowed)
			}
		})
	}
}

// TestIntegrationConcurrentExactness is the property the Lua scripts exist for:
// under real concurrency against real Redis, the limit holds precisely.
func TestIntegrationConcurrentExactness(t *testing.T) {
	ctx := context.Background()

	for _, algo := range []limiter.Algorithm{
		limiter.SlidingWindowCounterAlgo, limiter.TokenBucketAlgo,
	} {
		t.Run(string(algo), func(t *testing.T) {
			rdb := realRedis(t)
			// Separate limiter instances share only Redis, standing in for separate nodes.
			nodes := make([]limiter.Limiter, 4)
			for i := range nodes {
				l, err := limiter.NewRedisLimiter(rdb, algo,
					limiter.Config{Limit: 100, Window: time.Hour})
				if err != nil {
					t.Fatal(err)
				}
				nodes[i] = l
			}

			key := integrationKey(t, "concurrent")
			var allowed atomic.Int64
			var wg sync.WaitGroup
			start := make(chan struct{})

			for n := range nodes {
				for i := 0; i < 100; i++ {
					wg.Add(1)
					go func(node int) {
						defer wg.Done()
						<-start
						r, err := nodes[node].Allow(ctx, key)
						if err != nil {
							t.Errorf("node %d: %v", node, err)
							return
						}
						if r.Allowed {
							allowed.Add(1)
						}
					}(n)
				}
			}
			close(start)
			wg.Wait()

			if got := allowed.Load(); got != 100 {
				t.Errorf("admitted %d of 400 across 4 nodes, want exactly 100", got)
			}
		})
	}
}

// TestIntegrationWindowKeysExpire checks the script sets a bounded TTL. Without one,
// every window of every key accumulates in Redis forever, which is a slow memory
// leak that only shows up in production.
func TestIntegrationWindowKeysExpire(t *testing.T) {
	ctx := context.Background()
	rdb := realRedis(t)

	l, err := limiter.NewRedisLimiter(rdb, limiter.SlidingWindowCounterAlgo,
		limiter.Config{Limit: 10, Window: time.Second})
	if err != nil {
		t.Fatal(err)
	}

	key := integrationKey(t, "expiry")
	if _, err := l.Allow(ctx, key); err != nil {
		t.Fatal(err)
	}

	keys, err := rdb.Keys(ctx, "rl:{"+key+"}*").Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) == 0 {
		t.Fatal("no window key was written")
	}

	for _, k := range keys {
		ttl, err := rdb.PTTL(ctx, k).Result()
		if err != nil {
			t.Fatal(err)
		}
		if ttl <= 0 {
			t.Errorf("%s has no expiry (PTTL=%s) — window keys would accumulate forever",
				k, ttl)
		}
		if ttl > 3*time.Second {
			t.Errorf("%s TTL = %s, want at most two windows", k, ttl)
		}
	}
}

// TestIntegrationTokenBucketRefills checks the refill arithmetic against a real
// clock and real float-to-string round tripping through the Redis hash.
func TestIntegrationTokenBucketRefills(t *testing.T) {
	ctx := context.Background()
	rdb := realRedis(t)

	// 20 tokens per second, capacity 5.
	l, err := limiter.NewRedisLimiter(rdb, limiter.TokenBucketAlgo,
		limiter.Config{Limit: 20, Window: time.Second, BurstMax: 5})
	if err != nil {
		t.Fatal(err)
	}

	key := integrationKey(t, "refill")
	for i := 0; i < 5; i++ {
		if r, err := l.Allow(ctx, key); err != nil || !r.Allowed {
			t.Fatalf("request %d: allowed=%t err=%v", i, r.Allowed, err)
		}
	}
	if r, _ := l.Allow(ctx, key); r.Allowed {
		t.Fatal("bucket should be empty")
	}

	// 250ms at 20/s accrues about 5 tokens.
	time.Sleep(300 * time.Millisecond)

	r, err := l.Allow(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Allowed {
		t.Error("bucket did not refill")
	}
}

// TestIntegrationScriptSurvivesFlush covers a real operational event: SCRIPT FLUSH,
// or a Redis restart, invalidates the cached SHA. The client must reload rather than
// failing every request with NOSCRIPT.
func TestIntegrationScriptSurvivesFlush(t *testing.T) {
	ctx := context.Background()
	rdb := realRedis(t)

	l, err := limiter.NewRedisLimiter(rdb, limiter.SlidingWindowCounterAlgo,
		limiter.Config{Limit: 100, Window: time.Hour})
	if err != nil {
		t.Fatal(err)
	}

	key := integrationKey(t, "flush")
	if _, err := l.Allow(ctx, key); err != nil {
		t.Fatal(err)
	}

	if err := rdb.ScriptFlush(ctx).Err(); err != nil {
		t.Skipf("SCRIPT FLUSH not permitted: %v", err)
	}

	if _, err := l.Allow(ctx, key); err != nil {
		t.Errorf("request after SCRIPT FLUSH failed: %v — the script is not being reloaded", err)
	}
}
