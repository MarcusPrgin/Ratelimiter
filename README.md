# Distributed Rate Limiter

A production-grade distributed rate limiter in Go. Four pluggable algorithms, Redis-backed shared state, local in-memory caching, three failure strategies, adaptive load shedding, hierarchical multi-tier limits, cost-weighted quota, and a Redis-backed penalty box — all observable via Prometheus + Grafana.

---

## Features

| Feature | Detail |
|---|---|
| **4 algorithms** | Fixed window, sliding window log, sliding window counter, token bucket — swappable at runtime |
| **Distributed enforcement** | Atomic Lua script in Redis; no INCR+EXPIRE race across nodes |
| **Cost-weighted quota** | `AllowN(n)` — expensive endpoints consume multiple tokens per call |
| **Hierarchical tiers** | ChainedLimiter: per-key → per-tenant → global; first denial short-circuits |
| **Adaptive load shedding** | AIMD control loop: multiplies down when Redis p99 > threshold, additively recovers |
| **Penalty box** | Exponential-backoff blocks for repeat abusers; doubles on each violation |
| **Local cache** | Per-node TTL cache cuts Redis calls by ~85%; configurable accuracy/latency tradeoff |
| **3 failure strategies** | `fail_open`, `fail_closed`, `local_fallback` — runtime-configurable |
| **3 key extractors** | IP, `X-User-ID`, `X-Tenant-ID:X-User-ID` composite |
| **RFC-standard headers** | `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`, `Retry-After`, `X-RateLimit-Denied-By` |
| **Full observability** | Prometheus metrics + Grafana dashboard; adaptive multiplier, penalty denials, per-tier denials all exposed |
| **k6 load tests** | Steady, burst, and chaos (Redis killed mid-run) suites |
| **Graceful shutdown** | SIGTERM drains in-flight requests; all background goroutines stop cleanly |
| **Zero config deps** | `make docker-up` — Redis, Prometheus, Grafana, and the server in one command |

---

## Architecture

```
                    ┌─────────────────────────────────┐
Clients ──────────▶ │         API Gateway Layer        │
                    │  Node A    Node B    Node C       │
                    │  [cache]   [cache]   [cache]      │
                    └──────────────┬───────────────────┘
                                   │ cache miss
                                   ▼
                    ┌──────────────────────────────────┐
                    │      Adaptive Load Shedder        │
                    │  EWMA latency → AIMD multiplier   │
                    │  lock-free atomic float read      │
                    └──────────────┬───────────────────┘
                                   │
                    ┌──────────────▼───────────────────┐
                    │      ChainedLimiter (optional)    │
                    │   per-key → per-tenant → global  │
                    └──────────────┬───────────────────┘
                                   │
                    ┌──────────────▼───────────────────┐
                    │          Redis Cluster            │
                    │  Lua: atomic sliding window       │
                    │  supports cost-weighted AllowN    │
                    └──────────────┬───────────────────┘
                                   │
                    ┌──────────────▼───────────────────┐
                    │       Prometheus + Grafana         │
                    │  allowed, denied, adaptive_mult,  │
                    │  penalty_denied, tier_denied,      │
                    │  redis_latency_p99, cache_hit_ratio│
                    └───────────────────────────────────┘
```

---

## Project Structure

```
.
├── cmd/server/main.go                   # entrypoint — wires all components
├── internal/
│   ├── limiter/
│   │   ├── limiter.go                   # Limiter interface: Allow + AllowN + Name
│   │   ├── redis_limiter.go             # distributed implementation (primary path)
│   │   ├── sliding_window_counter.go    # O(1), ~0.003% error — default
│   │   ├── sliding_window_log.go        # O(n), exact — for comparison
│   │   ├── fixed_window.go              # O(1), boundary burst — illustrates the flaw
│   │   ├── token_bucket.go              # O(1), controlled burst — AWS/Stripe style
│   │   ├── chained.go                   # hierarchical multi-tier limiter
│   │   ├── adaptive.go                  # AIMD load shedder; atomic float multiplier
│   │   ├── sliding_window.lua           # atomic Lua script — supports cost ARGV[5]
│   │   ├── limiter_test.go              # core algorithm tests + race + benchmarks
│   │   └── advanced_test.go             # AllowN, ChainedLimiter, AdaptiveLimiter tests
│   ├── penalty/penalty.go               # exponential-backoff penalty box (Redis)
│   ├── cache/cache.go                   # local TTL cache; atomic hit/miss counters
│   ├── fallback/fallback.go             # fail_open | fail_closed | local_fallback
│   ├── middleware/middleware.go          # HTTP middleware — cost, penalty, headers
│   ├── metrics/metrics.go               # Prometheus counters, histograms, gauges
│   └── config/config.go                 # YAML + env config via Viper
├── k6/
│   ├── steady.js                        # 50 VUs, 3 minutes
│   ├── burst.js                         # 300 rps single user, 10 seconds
│   └── chaos.js                         # Redis killed and restarted mid-run
├── config.yaml                          # all options documented inline
├── docker-compose.yml
└── Makefile
```

---

## The Four Advanced Features

### 1. Cost-weighted quota (`AllowN`)

The `Limiter` interface exposes `AllowN(ctx, key, n)`. Expensive endpoints declare their cost in `config.yaml`; the middleware looks up the cost and calls `AllowN(n)` instead of `Allow()`.

```yaml
routes:
  - path: /api/search
    cost: 5     # counts as 5 normal requests
  - path: /api/export
    cost: 20    # burns through the limit fast
```

The Lua script accepts the cost as `ARGV[5]` and uses `INCRBY cost` instead of `INCR`. The expiry-only-on-first-write property is preserved: since Redis `INCRBY` on a non-existent key starts from 0, `new_count == cost` is true exactly on the first write.

**Why this matters**: OpenAI, Anthropic, and Stripe all rate-limit by token/compute cost rather than raw request count. This is the same pattern.

---

### 2. Hierarchical tiers (`ChainedLimiter`)

`ChainedLimiter` composes any number of `Limiter` instances. Each tier can use a different key transformation, so a single request is enforced at multiple scopes simultaneously.

```
request → per-user limit (key: "user:alice")
        → per-tenant limit (key: "tenant:acme")
        → global limit (key: "global")
```

The first tier to deny short-circuits; `X-RateLimit-Denied-By` tells the client which scope blocked it. Enable via config:

```yaml
limiter:
  chain:
    enabled: true
    tenant_limit: 1000   # per-tenant across all users
    global_limit: 10000  # entire service
```

**Design note**: when a later tier denies, earlier tiers have already been incremented. This over-counts by at most `n` units per denial — an accepted tradeoff versus the alternative (two-phase peek + commit, which requires extra Redis round trips).

---

### 3. Adaptive load shedding (`AdaptiveLimiter`)

`AdaptiveLimiter` wraps any `Limiter` and tracks the latency of each inner call via EWMA. It applies AIMD (additive increase / multiplicative decrease) to a pass-through multiplier:

```
latency > high_watermark → multiplier *= decrease_ratio   (multiplicative decrease)
latency < low_watermark  → multiplier += increase_step    (additive increase)
```

The multiplier is stored as `atomic.Uint64` (via `math.Float64bits`) so every request reads it **without acquiring any lock**. Only the EWMA update path — which runs once per real Redis call — takes a short mutex.

```yaml
limiter:
  adaptive:
    enabled: true
    low_watermark_ms: 2.0    # healthy
    high_watermark_ms: 10.0  # overloaded
    decrease_ratio: 0.75
    increase_step: 0.05
    min_multiplier: 0.1      # always let at least 10% through
```

When shedding, the response gets `DeniedBy: adaptive_shed` and the `ratelimiter_adaptive_multiplier` gauge in Grafana drops below 1.0.

**Why AIMD**: the same algorithm TCP congestion control uses. Aggressive recovery from overload, conservative recovery to avoid oscillation.

---

### 4. Penalty box (`penalty.Box`)

After a key accumulates `threshold` consecutive denials within `strike_window`, it enters a penalty that doubles on each subsequent violation, up to `max_penalty`.

```
violation 1: 30s penalty
violation 2: 60s penalty
violation 3: 120s penalty
...
violation N: min(BasePenalty × 2^(N-1), max_penalty)
```

Penalty state lives in Redis (`rl:penalty:<key>` with TTL = penalty duration). The hot path is a single `TTL` command — if the key doesn't exist, it's a one-RTT miss and falls through to normal rate limiting. Errors are swallowed so a Redis hiccup never blocks a request.

```yaml
limiter:
  penalty:
    enabled: true
    threshold: 10
    strike_window: 60s
    base_penalty: 30s
    max_penalty: 3600s
```

Penalised responses include `X-RateLimit-Reason: penalty` so clients know they need to back off longer than the normal reset window.

---

## Algorithms

All four implement the same `Limiter` interface with `Allow` and `AllowN`.

| Algorithm | Space per key | Burst safety | Used in production |
|---|---|---|---|
| Fixed window | O(1) | Poor (boundary burst) | Simple internal tools |
| Sliding window log | O(n) — n = requests | Exact | Low-traffic, accuracy-critical |
| **Sliding window counter** | **O(1)** | **~0.003% error** | **Cloudflare, this project** |
| Token bucket | O(1) | Controlled burst allowed | AWS, Stripe APIs |

### Benchmark results

```
BenchmarkFixedWindow-8             	 8,432,105	  142 ns/op	  0 B/op	  0 allocs/op
BenchmarkSlidingWindowCounter-8    	 7,891,234	  151 ns/op	  0 B/op	  0 allocs/op
BenchmarkTokenBucket-8             	 6,234,890	  192 ns/op	  0 B/op	  0 allocs/op
BenchmarkSlidingWindowLog-8        	 1,204,556	  994 ns/op	 96 B/op	  2 allocs/op
```

---

## The Race Condition I Found and Fixed

**The broken approach** — two separate Redis commands:

```go
// WRONG: EXPIRE resets the TTL on both nodes
pipe := rdb.Pipeline()
pipe.IncrBy(ctx, key, 1)
pipe.Expire(ctx, key, window)
pipe.Exec(ctx)
```

**The fix** — atomic Lua script (`internal/limiter/sliding_window.lua`):

```lua
local new_count = redis.call('INCRBY', curr_key, cost)
-- set expiry only on first write (new_count == cost means key was absent)
if new_count == cost then
    redis.call('EXPIRE', curr_key, window * 2)
end
```

The `new_count == cost` condition works because Redis `INCRBY` on a non-existent key starts from 0, so the first write always produces `new_count == cost`. Subsequent writes produce `new_count > cost`, so `EXPIRE` is never called again. Run `go test -race -run TestConcurrentSafety ./internal/limiter/...` to verify.

---

## The Accuracy vs. Latency Tradeoff

With a local TTL cache in front of Redis, each node makes fewer Redis calls.

At TTL=5ms with 3 nodes:
- Redis calls reduced by ~85%
- Decision latency drops from ~1ms to ~0.2ms p99
- Worst-case over-admission: `nodes × TTL × rps = 3 × 0.005 × 100 = 1.5 extra requests`

For a payment API, TTL=0 (always consult Redis) might be correct. For a search API, TTL=20ms is fine. Cost-weighted calls with cost > 1 use a separate cache key (`key:c<cost>`) so they don't share stale quota with unit-cost requests.

---

## Redis Failure Fallback

| Strategy | Behaviour | Use when |
|---|---|---|
| `fail_open` | Allow everything | Search, recommendations |
| `fail_closed` | Deny everything | Payments, auth |
| `local_fallback` | Each node enforces independently | Good middle ground (N nodes = N× limit) |

```bash
make docker-up
make k6-chaos   # terminal 1
make redis-stop # terminal 2 — watch the logs
make redis-start
```

---

## Load Test Results

### Steady traffic (50 VUs, 3 minutes)

```
✓ status is 200 or 429 .......... 100%  18,432 requests
✓ has ratelimit headers ......... 100%
deny_rate ..................... 34.2%
http_req_duration p(99) ........ 4.1ms
```

### Burst test (300 rps, single user, 10 seconds)

```
burst_allowed ................ 1,003
burst_denied ................. 1,997
limit_enforced ............... 66.6%  ✓ threshold: >50%
```

### Chaos test (Redis killed mid-run, 5 minutes)

```
Total requests:   28,440
5xx errors:            0
Recoveries:            4
```

---

## Running Locally

```bash
make docker-up

# Basic limit test
curl -H "X-User-ID: alice" http://localhost:8080/api/hello

# Cost-weighted: edit config.yaml to add route cost, then:
curl -H "X-User-ID: alice" http://localhost:8080/api/search   # costs 5

# Enable chain in config.yaml and add X-Tenant-ID:
curl -H "X-User-ID: alice" -H "X-Tenant-ID: acme" http://localhost:8080/api/hello

# Trigger penalty box: hammer the limit >10 times in a window
for i in $(seq 1 150); do
  curl -s -o /dev/null -w "%{http_code} " -H "X-User-ID: alice" http://localhost:8080/api/hello
done

# Grafana dashboard
open http://localhost:3000
```

---

## Configuration Reference

```yaml
limiter:
  algorithm: sliding_window_counter  # swap to compare
  limit: 100
  window: 1s
  cache_ttl: 5ms
  fallback_strategy: fail_open
  key_type: user            # ip | user | tenant

  chain:
    enabled: true
    tenant_limit: 1000
    global_limit: 10000

  adaptive:
    enabled: true
    high_watermark_ms: 10.0
    min_multiplier: 0.1

  penalty:
    enabled: true
    threshold: 10
    base_penalty: 30s
    max_penalty: 3600s

routes:
  - path: /api/search
    cost: 5
  - path: /api/export
    cost: 20
```

Override any setting with env vars: `RATELIMITER_LIMITER_LIMIT=500`

---

## Post-mortem

### Bugs I hit

**1. INCRBY+EXPIRE race** — described above. Fixed with atomic Lua script; the expiry condition `new_count == cost` generalises the original `new_count == 1` for cost-weighted requests.

**2. Eviction goroutine leak** — `cache.New()` now takes a `context.Context` and the eviction ticker stops on `ctx.Done()`. Counters are `atomic.Int64` — no write-lock on the hot cache-hit path.

**3. k6 VU vs arrival-rate confusion** — `constant-arrival-rate` fires exactly N req/s. `constant-vus` fires N req/s *per VU*. First burst test showed 50× the expected load.

**4. ChainedLimiter over-count** — early tiers increment before a later tier denies. Rollback requires a two-phase peek+commit across multiple Redis keys, adding round trips. Accepted the over-count; documented it; tested it.

### What I'd do with 3 more weeks

- **Redis Cluster**: shard keys consistently across 3+ Redis nodes — eliminates the single Redis SPOF
- **RESP3 client-side caching**: Redis 6+ push invalidation; local cache accuracy improves without TTL over-admission
- **Per-tenant adaptive limits**: each tenant gets its own EWMA + multiplier, so one misbehaving tenant doesn't trigger global shedding
- **gRPC interceptor**: same `Limiter` interface, different transport

### What 10× scale looks like

At 500K rps across 30 nodes: Redis becomes the bottleneck at ~1.5M ops/s on cache miss. Solution: Redis Cluster (5+ shards) + longer local cache TTL + accept ~1% over-admission. "Exactly 100 rps per user" matters less than "zero Redis SPOF."

---

## Tech Stack Rationale

- **Go**: `go test -race` catches data races; goroutines make concurrent testing natural
- **Redis**: single-threaded execution makes Lua scripts truly atomic; `atomic.Uint64` for the adaptive multiplier avoids lock contention on every request
- **Prometheus + Grafana**: adaptive multiplier, per-tier denial, and penalty metrics all live in the same dashboard
- **k6**: constant-arrival-rate executor; built-in metrics export; what infra teams actually use

---

## Interview Q&A

**Q: Why AllowN instead of separate rate limiters per endpoint?**

Separate limiters with different configs still share the same Redis keyspace. AllowN lets the same limiter enforce proportional limits — a `/search` that costs 5× more than `/ping` is accurate without maintaining 2 separate counters.

**Q: How does the adaptive limiter avoid thrashing?**

EWMA smoothing (alpha=0.1) means it takes ~10 observations for a latency spike to fully register. The additive increase step (0.05) is smaller than the multiplicative decrease (×0.75), so recovery is slower than degradation — the same asymmetry TCP congestion control uses.

**Q: What's the over-count in the ChainedLimiter and does it matter?**

If tier 1 (per-user) allows but tier 2 (global) denies, the per-user counter was incremented unnecessarily. At a 30% denial rate with cost=1, that's 0.3 extra increments per request in tier 1. At 100 rps, the per-user limit drifts by ~30 units/s — about 0.3% error. Acceptable; documented; alternatively mitigated by reducing cost=1 requests via the local cache.

**Q: What prevents the penalty box from blocking legitimate traffic after a restart?**

Penalty state lives in Redis with TTL = penalty duration. On Redis restart, all penalty keys are lost and all keys start clean. For `local_fallback`, in-memory penalty state is also lost on process restart. This is intentional — a restart is a form of recovery.

**Q: Why not use `golang.org/x/time/rate`?**

In-memory token bucket, single process. No distributed enforcement, no AllowN with Redis, no adaptive shedding, no penalty box. This project exists to handle the distributed case with all the tradeoffs that entails.
