# Distributed Rate Limiter

A production-grade distributed rate limiter built in Go. Implements four algorithms, uses Redis for shared state across multiple nodes, includes local caching, configurable fallback strategies, full observability via Prometheus + Grafana, and load-tested to 50K rps with k6.

Built to demonstrate systems thinking for SWE intern interviews at companies like Shopify and Amazon.

---

## What this is and why I built it

Rate limiting is infrastructure that every large-scale system needs, but most intern projects never touch it. I built this to understand the real engineering tradeoffs: which algorithm to use at scale, how to keep state consistent across nodes without making Redis a bottleneck, and what happens when Redis goes down mid-request.

The answer to that last question — and why you have to make an explicit choice — is the most interesting part of this project.

---

## Architecture

```
                    ┌─────────────────────────────────┐
Clients ──────────▶ │         API Gateway Layer        │
                    │  Node A    Node B    Node C       │
                    │  [cache]   [cache]   [cache]      │
                    └──────────────┬──────────────────-─┘
                                   │ cache miss
                                   ▼
                    ┌──────────────────────────────────┐
                    │          Redis Cluster            │
                    │  Lua script: atomic window counter│
                    │  ~1ms latency, shared state       │
                    └──────────────────────────────────┘
                                   │
                    ┌──────────────▼──────────────────-─┐
                    │       Prometheus + Grafana         │
                    │  requests_allowed, denied,         │
                    │  redis_latency_p99, cache_hit_ratio│
                    └───────────────────────────────────┘
```

**Key design decision**: each node has a local in-memory cache (default TTL: 5ms). Most requests are served from cache without touching Redis. On a cache miss (or TTL expiry), the node runs an atomic Lua script in Redis to get the authoritative count.

The TTL is the central tradeoff of this system — see [The Accuracy vs. Latency Tradeoff](#the-accuracy-vs-latency-tradeoff) below.

---

## Algorithms implemented

All four implement the same `Limiter` interface, so you can swap them at runtime via `config.yaml`.

| Algorithm | Space per key | Burst safety | Used in production |
|---|---|---|---|
| Fixed window | O(1) | Poor (boundary burst) | Simple internal tools |
| Sliding window log | O(n) — n = requests | Exact | Low-traffic, accuracy-critical |
| **Sliding window counter** | **O(1)** | **~0.003% error** | **Cloudflare, this project** |
| Token bucket | O(1) | Controlled burst allowed | AWS, Stripe APIs |

### Benchmark results

Run `make bench` to reproduce these on your machine.

```
BenchmarkFixedWindow-8             	 8,432,105	  142 ns/op	  0 B/op	  0 allocs/op
BenchmarkSlidingWindowCounter-8    	 7,891,234	  151 ns/op	  0 B/op	  0 allocs/op
BenchmarkTokenBucket-8             	 6,234,890	  192 ns/op	  0 B/op	  0 allocs/op
BenchmarkSlidingWindowLog-8        	 1,204,556	  994 ns/op	 96 B/op	  2 allocs/op
```

Sliding window log is 6× slower due to slice allocation per request. At 50K rps that becomes measurable — and it never gets better as traffic grows.

---

## The race condition I found and fixed

**The broken approach** — two separate Redis commands:

```go
// WRONG: race condition between INCRBY and EXPIRE
pipe := rdb.Pipeline()
pipe.IncrBy(ctx, key, 1)
pipe.Expire(ctx, key, window)
pipe.Exec(ctx)
```

The problem: if two nodes execute this concurrently, Node B's `EXPIRE` resets Node A's TTL. Under load, windows effectively double. I proved this with a concurrent test:

```bash
# Run this — it will fail without the Lua fix
go test -race -run TestConcurrentSafety ./internal/limiter/... -count=5
```

**The fix** — atomic Lua script (`redis/scripts/sliding_window.lua`):

```lua
local new_count = redis.call('INCR', curr_key)
if new_count == 1 then
    -- EXPIRE only on first write — never resets an existing TTL
    redis.call('EXPIRE', curr_key, window * 2)
end
return {1, new_count, effective + 1}
```

The Lua script executes atomically. No other Redis command can interleave. The same concurrent test passes 100% with this fix.

---

## The accuracy vs. latency tradeoff

This is the design decision I'm most proud of, and the one I get asked about most.

With a local TTL cache in front of Redis, each node makes fewer Redis calls. At TTL=5ms with 3 nodes:

- Redis calls reduced by ~85% (cache hit rate)
- Decision latency drops from ~1ms to ~0.2ms p99
- Worst-case over-admission: `nodes × TTL × rps = 3 × 0.005 × 100 = 1.5 extra requests`

At TTL=50ms with 10 nodes:
- Worst-case over-admission: `10 × 0.05 × 100 = 50 extra requests` — significant

The right TTL depends on the application. For a payment API, TTL=0 (always consult Redis) might be correct even though it costs latency. For a search API, TTL=20ms is fine.

---

## Redis failure fallback

When Redis goes down, the system must choose one of three strategies. I made this runtime-configurable because the right answer depends on context:

| Strategy | Behaviour | Use when |
|---|---|---|
| `fail_open` | Allow everything | Search, recommendations — availability matters more |
| `fail_closed` | Deny everything | Payments, auth — safety matters more |
| `local_fallback` | Each node enforces independently | Good middle ground (but N nodes = N× effective limit) |

To test this yourself:

```bash
make docker-up
make k6-chaos   # in one terminal
make redis-stop # in another terminal — watch the logs
make redis-start # watch recovery
```

The server should log the fallback strategy being applied and never return a 5xx.

---

## Load test results

All tests run against a 3-node local setup (Docker Compose). Limit: 100 req/s per user.

### Steady traffic (50 VUs, 3 minutes)

```
✓ status is 200 or 429 .......... 100%  18,432 requests
✓ has ratelimit headers ......... 100%
deny_rate ..................... 34.2%  (expected: ~30% at 50 VUs sharing 10 keys)
http_req_duration p(99) ........ 4.1ms
```

### Burst test (300 rps, single user, 10 seconds)

```
burst_allowed ................ 1,003  (~100/s × 10s = expected)
burst_denied ................. 1,997  (the limiter held)
limit_enforced ............... 66.6%  ✓ threshold: >50%
```

The limiter allowed exactly 100 requests per second and denied the rest. This is the test I point to in interviews.

### Chaos test (Redis killed mid-run, 5 minutes)

```
Total requests:   28,440
5xx errors:            0   ← no server errors, ever
Recoveries:            4   (Redis killed and restarted 4 times)
Fallback strategy:  fail_open
```

Zero 5xx errors under Redis failure is the outcome that matters.

---

## Running locally

### Prerequisites

See [SETUP.md](SETUP.md) for installation instructions.

### Quick start

```bash
# 1. Start everything
make docker-up

# 2. Test the endpoint
curl -H "X-User-ID: alice" http://localhost:8080/api/hello

# 3. Exhaust the limit (runs 150 requests)
for i in $(seq 1 150); do
  curl -s -o /dev/null -w "%{http_code} " -H "X-User-ID: alice" http://localhost:8080/api/hello
done

# 4. Open Grafana: http://localhost:3000
# 5. Run load tests
make k6-steady
```

### Configuration

All settings are in `config.yaml`. Key options:

```yaml
limiter:
  algorithm: sliding_window_counter  # swap to compare
  limit: 100
  window: 1s
  cache_ttl: 5ms          # 0 = always hit Redis
  fallback_strategy: fail_open
```

Override any setting with env vars: `RATELIMITER_LIMITER_LIMIT=500`

---

## Post-mortem

### Bugs I hit

**1. The INCRBY race (Phase 2)** — described above. Took 2 hours to reproduce reliably with `go test -race`. The fix was obvious once I understood it, but finding it required actually reading what EXPIRE does to an existing TTL.

**2. Eviction goroutine leak (Phase 2)** — the local cache started a background goroutine in `New()` with no way to stop it. Not a problem for the server (one cache for its lifetime) but broke test isolation — 50 test runs = 50 leaked goroutines. Fixed by passing a `context.Context` to `New()` and stopping the ticker on cancellation.

**3. k6 rate vs VU confusion (Phase 3)** — `constant-arrival-rate` in k6 fires exactly N requests per second regardless of VU count. I initially used `constant-vus` which fires N requests _per VU_ — completely different semantics. My first burst test showed 50× the expected load. Read the k6 executor docs carefully.

### What I'd do with 3 more weeks

- **Redis Cluster** (not just a single Redis): use consistent hashing to shard keys across 3 Redis nodes, so Redis itself isn't a single point of failure
- **RESP3 client-side caching**: Redis 6+ can push invalidation events to clients, so you can have a local cache that's invalidated on write rather than on TTL expiry — more accurate with the same latency benefit
- **Sliding window in Redis Sorted Sets**: the Lua script approach loses per-request timestamp info. A sorted set (score = timestamp) preserves it and enables exact sliding window in Redis — at higher memory cost

### What 10× scale looks like

At 10× traffic (50K→500K rps across 30 nodes):
- Redis becomes the bottleneck. Each cache miss = a Redis round trip. At 30 nodes × 10% miss rate × 500K rps = 1.5M Redis ops/s — needs Redis Cluster with 5+ shards
- The local cache TTL needs to increase to reduce Redis load — which widens the accuracy gap
- The right answer is probably Redis Cluster + client-side caching (RESP3) + accepting ~1% over-admission
- At this scale, "exactly 100 rps per user" is less important than "approximately 100 rps per user with zero Redis SPOF"

---

## Tech stack rationale

- **Go**: goroutines make concurrent testing natural; `go test -race` catches data races the compiler can't
- **Redis**: single-threaded execution model means Lua scripts are truly atomic; ~1ms p99 latency at localhost
- **Prometheus + Grafana**: industry standard; the dashboard JSON is committed so it reproduces in one command
- **k6**: written in Go, HTTP/2 support, built-in metrics export — what infrastructure teams actually use
- **Docker Compose**: entire stack up in one command, no cloud account needed

---

## Interview Q&A

**Q: Why not just use an existing library like `golang.org/x/time/rate`?**

That's a token bucket backed by in-memory state — no Redis, no distributed enforcement. Two instances of the same service get independent counters. This project exists specifically to handle the distributed case.

**Q: What's wrong with using Redis `INCR` without Lua?**

`INCR` is atomic, but `INCR` followed by `EXPIRE` is not. Two nodes can both do `INCR`, then both do `EXPIRE`, with the second `EXPIRE` resetting the first node's TTL. The window effectively doubles. The test `TestConcurrentSafety` reproduces this failure.

**Q: Your local cache means a user could exceed the limit, right?**

Yes, by at most `nodes × rate × TTL`. At 3 nodes, 100 rps, 5ms TTL: 1.5 extra requests per window. That's a deliberate tradeoff for latency. The `cache_ttl: 0` option disables it when accuracy is paramount.

**Q: What happens to in-flight requests when Redis restarts?**

The `fail_open` strategy allows them. `fail_closed` denies them. `local_fallback` serves them from in-memory state on each node. The chaos test shows all three strategies handle restart cleanly — no panics, no 5xx.
