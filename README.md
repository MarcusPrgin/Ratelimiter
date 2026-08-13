# Distributed Rate Limiter

[![CI](https://github.com/MarcusPrgin/Ratelimiter/actions/workflows/ci.yml/badge.svg)](https://github.com/MarcusPrgin/Ratelimiter/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/go-1.22-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![Redis](https://img.shields.io/badge/redis-5%2B-DC382D?logo=redis&logoColor=white)](https://redis.io)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A production-shaped distributed rate limiter in Go, backed by Redis.

Two algorithms enforced atomically in Lua, quota leasing that removes ~80% of Redis
round trips **without** admitting anything Redis has not already counted, and a
circuit breaker in front of three genuinely distinct failure strategies. Hierarchical
tiers, cost-weighted quota, adaptive load shedding and an escalating penalty box sit
on top, with Prometheus metrics and a provisioned Grafana dashboard.

```bash
make docker-up
curl -i -H 'X-User-ID: alice' http://localhost:8080/api/hello
```

---

## At a glance

| | |
|---|---|
| **Correctness under concurrency** | 500 concurrent requests against a limit of 100 admit **exactly** 100. Asserted, not assumed. |
| **Cost of the fast path** | A lease hit is **45.7 ns**, allocation-free — ~1,900× cheaper than a Redis round trip. |
| **Tests** | 131 tests, 8 benchmarks. 86–96% statement coverage across the core packages. |
| **CI** | Race detector, real-Redis integration, lint, benchmarks, and a Docker image that must boot and serve. |
| **Dependencies** | 5 direct: go-redis, Prometheus client, Viper, mapstructure, miniredis. |
| **Size** | ~8.2k lines of Go, of which ~4.3k are tests. |

New to the repo? [`SETUP.md`](SETUP.md) covers prerequisites and first run.
`make help` lists every target.

---

## Contents

- [Features](#features)
- [Architecture](#architecture)
- [Running the service](#running-the-service)
- [Configuration](#configuration)
- [Algorithms](#algorithms)
- [Design decisions](#design-decisions)
- [Metrics](#metrics)
- [HTTP contract](#http-contract)
- [Testing](#testing)
- [Benchmarks](#benchmarks)
- [Limitations and roadmap](#limitations-and-roadmap)
- [Tech stack](#tech-stack)
- [License](#license)

---

## Features

| Feature | Detail |
|---|---|
| **Two algorithms** | Sliding window counter and token bucket, each with an in-memory and a Redis implementation. Selected by config; the choice applies to the distributed path, not just the local one. |
| **Atomic distributed enforcement** | One Lua script per algorithm. Read, decide, write and expire happen in a single atomic step, so nodes cannot race. |
| **Quota leasing** | Claims quota from Redis in blocks and hands it out locally. Cuts round trips by roughly `prefetch/(prefetch+1)` on hot keys **without** admitting anything Redis has not already counted. |
| **Negative caching** | An over-quota caller is answered locally, so abuse costs no round trip at all. |
| **Cost-weighted quota** | `AllowN(n)`; expensive routes declare a cost and consume proportionally more. |
| **Hierarchical tiers** | Per-caller → per-tenant → global, every tier backed by Redis. First denial short-circuits and names itself. |
| **Adaptive load shedding** | AIMD on observed limiter latency, stepped on a fixed interval so behaviour does not depend on traffic volume. |
| **Circuit breaker** | After repeated failures Redis is bypassed entirely, so an outage does not add its timeout to every response. One half-open probe per cooldown tests recovery. |
| **Three failure strategies** | `fail_open`, `fail_closed`, `local_fallback` — each genuinely distinct, and validated at startup rather than defaulted. |
| **Penalty box** | Escalating exponential-backoff blocks for repeat offenders, shared through Redis. |
| **Bounded memory** | Every in-memory map is sharded, capped and LRU-evicted; key length is bounded. High key cardinality cannot exhaust the process. |
| **Spoof-resistant keys** | `X-Forwarded-For` is ignored unless you declare how many proxies you run, then read from the right. |
| **Fail-fast config** | Every enum parsed, every range checked, unknown keys rejected, all errors reported at once. `-check` validates without starting. |
| **Observability** | Prometheus metrics with denial reasons, lease effectiveness, breaker state and controller internals; provisioned Grafana dashboard. |
| **Graceful shutdown** | SIGTERM drains in-flight requests, then background work stops and Redis is closed. |
| **Tests** | Unit and integration, including the Lua scripts against both miniredis and a real Redis in CI. |

---

## Architecture

The limiter is a chain of decorators, each implementing the same `Limiter`
interface. The order is load-bearing.

```
                    HTTP request
                         │
              ┌──────────▼──────────┐
              │     middleware      │  key extraction, cost lookup, headers,
              │                     │  status mapping, panic recovery
              └──────────┬──────────┘
                         │
              ┌──────────▼──────────┐
              │    penalty box      │  outermost: a blocked caller is refused
              │                     │  before it can cost a lease or a round trip
              └──────────┬──────────┘
                         │
              ┌──────────▼──────────┐
              │    lease cache      │  a local hit costs nothing at all
              └──────────┬──────────┘
                         │
              ┌──────────▼──────────┐
              │ fallback + breaker  │  wraps what can actually fail, so it sees
              │                     │  Redis errors directly
              └─────┬────────┬──────┘
                    │        │ on failure
                    │        └──────────▶ fail_open / fail_closed / local limiter
                    │
              ┌─────▼───────────────┐
              │  adaptive shedding  │  below the lease cache on purpose — above it,
              │                     │  it would measure lease hits (~1µs) and never
              └─────┬───────────────┘  reach the watermark
                    │
              ┌─────▼───────────────┐
              │   chained tiers     │  per_key → per_tenant → global
              └─────┬───────────────┘
                    │
              ┌─────▼───────────────┐
              │       Redis         │  atomic Lua, Redis's own clock
              └─────────────────────┘
```

### Layout

```
cmd/server/main.go              entrypoint; assembles the chain above
internal/
  limiter/
    limiter.go                  Limiter interface, Result, Config, cost errors
    sliding_window_counter.go   in-memory sliding window counter
    token_bucket.go             in-memory token bucket
    redis.go                    both Redis implementations
    sliding_window.lua          atomic sliding window, Redis-clock
    token_bucket.lua            atomic token bucket, Redis-clock
    lease.go                    quota leasing + negative cache
    chained.go                  hierarchical tiers
    adaptive.go                 AIMD load shedder
  penalty/                      escalating penalty box (Redis) + Lua
  fallback/                     failure strategies + circuit breaker
  middleware/                   HTTP layer
  metrics/                      Prometheus registration
  config/                       load, default, validate
  shardmap/                     bounded sharded LRU map, shared by the above
k6/                             steady, burst and chaos load tests
grafana/, prometheus.yml        provisioned dashboard and scrape config
```

---

## Running the service

Prerequisites and platform-specific install steps are in [`SETUP.md`](SETUP.md).
The test suite needs nothing but Go.

### With Docker (full stack)

```bash
make docker-up        # app, Redis, Prometheus, Grafana, Redis exporter
make docker-logs      # follow the app
make docker-down
```

| Service | URL |
|---|---|
| API | http://localhost:8080 |
| Metrics | http://localhost:8080/metrics |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |

### Locally

```bash
make check      # validate config.yaml without starting
make run        # needs Redis on localhost:6379, or falls back per strategy
make test       # full suite; no Redis required
make test-race
```

### Exercising the features

```bash
# Basic limit — watch the headers count down
curl -i -H 'X-User-ID: alice' http://localhost:8080/api/hello

# Cost weighting: /api/search costs 5, /api/export costs 20
curl -i -H 'X-User-ID: alice' http://localhost:8080/api/search

# Exhaust the quota and inspect the refusal
for i in $(seq 1 150); do
  curl -s -o /dev/null -w '%{http_code} ' -H 'X-User-ID: bob' \
    http://localhost:8080/api/hello
done; echo

# Health: liveness never touches Redis, readiness does
curl -s http://localhost:8080/healthz; curl -s http://localhost:8080/readyz

# Watch the fallback and breaker
make k6-chaos          # terminal 1
make redis-stop        # terminal 2
make redis-start
```

Hierarchical tiers and the penalty box are off by default; enable them in
`config.yaml` (the chain also needs `key_type: tenant`).

---

## Configuration

`config.yaml` is commented in full; every key can be overridden by environment
variable — uppercase the path, replace dots with underscores, prefix
`RATELIMITER_`:

```bash
RATELIMITER_LIMITER_LIMIT=500
RATELIMITER_LIMITER_FALLBACK_STRATEGY=fail_closed
RATELIMITER_REDIS_ADDR=redis:6379
```

Validation is deliberately strict, because a misconfigured rate limiter still
returns 200s and nothing looks wrong until the thing it protects falls over. So:

- Unknown keys are an error, not a warning. A setting you believe you enabled but
  misspelled would otherwise silently not exist.
- Every enum is parsed, never defaulted. A typo in `fail_closed` must not quietly
  become allow-everything.
- Combinations are checked: a route whose cost exceeds the window capacity could
  never be served, and a tenant tier without a tenant in the key would put every
  caller in one bucket.
- All problems are reported together, so you do not fix one per restart.

```
$ ./bin/server -check
invalid configuration:
limiter.algorithm: unknown algorithm "fixed_window" (want "sliding_window_counter" or "token_bucket")
limiter.chain.enabled requires limiter.key_type: "tenant", got "ip" — without a tenant in the key every caller shares one tenant bucket
routes[0] "/api/export": cost 200 exceeds the per-window capacity 100, so every request to it would be rejected
```

---

## Algorithms

| | Sliding window counter | Token bucket |
|---|---|---|
| Memory per key | O(1), two counters | O(1), tokens + timestamp |
| Burst | None; smooth enforcement | Up to `burst_max` after idling |
| Boundary burst | No | N/A |
| Best for | Protecting a backend from load | Public APIs where bursts are a feature |
| Used by | Cloudflare | AWS, Stripe |

**Sliding window counter** keeps the current and previous fixed windows and
interpolates:

```
effective = round(prev_count × (1 − elapsed/window)) + curr_count
```

That avoids the boundary burst a plain fixed window allows, where a caller spends its
whole quota at the end of one window and again at the start of the next, admitting
2×limit in an instant. Rounding rather than truncating matters: truncation
systematically under-counts the carry-over, biasing towards over-admission.

**Token bucket** accrues `limit/window` tokens per second up to `burst_max`, computed
lazily from elapsed time, so idle keys cost nothing and there is no sweeper goroutine.

Both are aligned to the same absolute epoch boundaries in memory and in Redis, so a
key sees consistent windows whether it is served locally or centrally — which is what
makes `local_fallback` a continuation of the distributed limiter rather than a reset
of it.

---

## Design decisions

### Leasing quota instead of caching decisions

The obvious way to avoid a Redis round trip per request is to cache the decision for
a few milliseconds. It silently breaks the limit. With a 5ms TTL and one key
receiving 1000 rps, only ~200 of those requests reach Redis; the other ~800 are
admitted having consumed nothing. A limit of 100/s admits 1000/s, and the error grows
with per-key request rate — precisely the case a rate limiter exists to handle.

Leasing inverts it. On a miss the limiter asks Redis for the current request *plus*
`prefetch` extra units, so every unit handed out locally has already been counted
centrally:

- The shared count is never short, so the limit is never exceeded.
- The only residual error is unspent lease on a key that goes idle, which
  *under*-admits — the safe direction, bounded by `prefetch` per key per node.
- Concurrent misses on one key accumulate into a single lease rather than
  overwriting each other, so quota already drawn is not discarded.
- A key's *first* miss claims no prefetch; batching starts once the key is
  established. Prefetching immediately looks cheaper but is worse in the case that
  matters: in a simultaneous burst on a cold key, every concurrent request misses
  before any lease exists, so each claims a whole batch with nobody left to spend it.
  The quota is consumed centrally, stranded locally, and the burst is throttled far
  below the real limit — measurably, ~15 admitted against a limit of 100. Warming up
  costs one extra round trip per key and lifts that to 76–100.
- The prefetch shrinks as the reported headroom runs out, so the last few units are
  claimed one at a time. Under sustained traffic this makes admission **exactly** the
  limit at every prefetch size; without it the final batch over-claims and the limit
  quietly under-admits by up to one batch.

A denial is cached too, capped at its own `Retry-After` so a momentary throttle never
becomes a longer outage. That is what absorbs an abusive caller cheaply.

### One atomic step in Redis

Checking a limit in one command and incrementing in another lets two nodes both see
"one slot left" and both take it. Splitting the increment from its expiry lets a
second node reset the TTL and silently extend the window. Both are in a single Lua
script per algorithm.

The expiry is set unconditionally to a fixed absolute deadline rather than only on the
first write. Setting it once saves an O(1) command but leaks the key forever if that
single call is ever lost. Recomputing a deadline bound to the window start is
idempotent — the TTL shortens as time advances rather than being pushed out.

### Redis owns the clock

Both scripts take the time from `redis.call('TIME')` rather than from the calling
node, so every node shares one clock and window boundaries agree even when app servers
have drifted. This makes the scripts non-deterministic, which is fine on Redis 5+
where effect replication is used: the replica receives the resulting writes, not the
script. **Requires Redis 5 or newer**; the pinned image is Redis 7.

Keys are hash-tagged (`rl:{user:alice}`) so the keys a script derives all land in one
Cluster slot.

### AIMD on a fixed interval

Adaptive shedding steps the multiplier at most once per `adjust_interval`, not once
per request. Per-request stepping ties the control loop's response rate to traffic
volume: at 100k rps the multiplier slams to the floor within a millisecond, and a
nearly idle service never recovers because recovery needs one call per step. Both
accumulators are updated with compare-and-swap, so the hot path takes no lock and
concurrent samples are not lost.

### A breaker in front of the fallback

Without one, every request during a Redis outage waits out the full Redis timeout
before falling back — so an outage in a dependency that is supposed to be bypassed
instead adds its timeout to every response. After `breaker_threshold` consecutive
failures Redis is skipped entirely; one probe per cooldown tests recovery.

Client cancellations and impossible-cost errors are excluded from the breaker: the
first would trip it during a client-side timeout storm, and the second would let one
misbehaving caller degrade the limiter for everyone.

### Bounded memory everywhere

An unbounded key map is a denial-of-service vector — IP-keyed traffic from a large
botnet grows it until the process is OOM-killed. Every in-memory map is sharded 256
ways, capped, and LRU-evicted, with an idle TTL chosen so that eviction is *lossless*:
two windows for the sliding counter, a full refill for the bucket. Evicting sooner
would hand a key fresh quota, so that TTL is what stops eviction from becoming a limit
bypass. Key length is capped and hashed past the cap, since header values become Redis
key names.

### Trade-offs taken knowingly

- **Chain over-count.** When a later tier denies, earlier tiers have already consumed
  quota the request never used. Rolling that back needs a two-phase peek-then-commit
  across every tier, doubling round trips on the hot path. The error is bounded by
  `cost × tiers_passed` per denied request and self-corrects within a window. Tiers
  stay narrowest-first regardless, so an abusive caller is stopped at its own tier and
  never reaches the shared ones.
- **Local strike counting.** The penalty box counts strikes per node and only writes
  to Redis on escalation, so shedding traffic does not cost a write per denial. An
  offender spread evenly across N nodes therefore needs up to N×threshold denials to
  trip. Once tripped, the penalty is shared.

  Only denials the caller is responsible for count. Load shedding and limiter outages
  are the service's condition, so they are excluded — otherwise an overload marches
  well-behaved callers into the penalty box and keeps them locked out for minutes
  after it passes, which is the opposite of what shedding is for.
- **Penalty check interval.** A key's penalty state is re-read from Redis at most once
  per `check_interval`, so a penalty applied on one node takes up to that long to be
  honoured elsewhere — immaterial against a base penalty measured in tens of seconds,
  and it takes the common path from one round trip to zero.
- **`local_fallback` multiplies the limit.** N nodes enforcing independently admit
  roughly N× the configured limit. It keeps enforcement proportional during an outage;
  it is not exact.

---

## Metrics

| Metric | Type | Notes |
|---|---|---|
| `ratelimiter_requests_allowed_total` | counter | by `algorithm`, `key_type` |
| `ratelimiter_requests_denied_total` | counter | plus `denied_by`: `quota`, `penalty`, `adaptive_shed`, `limiter_unavailable`, `invalid_cost`, or a tier name |
| `ratelimiter_limiter_latency_seconds` | histogram | sub-millisecond buckets, so a lease hit is distinguishable from a round trip |
| `ratelimiter_requests_in_flight` | gauge | |
| `ratelimiter_lease_hits_total` / `_misses_total` | counter | round trips avoided versus paid |
| `ratelimiter_lease_hit_ratio` | gauge | |
| `ratelimiter_adaptive_multiplier` | gauge | below 1 means shedding |
| `ratelimiter_adaptive_latency_ms` | gauge | the smoothed latency driving the controller |
| `ratelimiter_adaptive_shed_total` | counter | |
| `ratelimiter_penalty_denied_total` / `_escalations_total` | counter | |
| `ratelimiter_degraded_total` | counter | requests served by the fallback strategy |
| `ratelimiter_breaker_open` | gauge | 1 while Redis is bypassed |
| `ratelimiter_tracked_keys` | gauge | by `component`; watch it against `max_keys` |
| `ratelimiter_limiter_errors_total` / `_handler_panics_total` | counter | should be zero |

Label children are resolved once at startup rather than per request, and anything
derived from live state is registered as a `CounterFunc`/`GaugeFunc` so it is computed
at scrape time — no background ticker, and no way for a metric to drift from its
source.

---

## HTTP contract

| Route | Rate limited | Notes |
|---|---|---|
| `/api/*` | yes | `/api/hello`, `/api/search` (cost 5), `/api/export` (cost 20) |
| `/healthz` | no | liveness; deliberately does **not** check Redis |
| `/readyz` | no | readiness; does check Redis |
| `/health` | no | alias for `/healthz` |
| `/metrics` | no | Prometheus |

Health and metrics are unlimited on purpose: they have to answer precisely when the
service is throttling everything else. Liveness excludes Redis so a dependency outage
does not make the orchestrator restart every node during the incident the fallback
strategy exists to survive.

**Response headers**

| Header | When |
|---|---|
| `X-RateLimit-Limit`, `-Remaining`, `-Reset` | whenever a numeric limit applies |
| `Retry-After` | on every refusal, whole seconds, rounded up, minimum 1 |
| `X-RateLimit-Denied-By` | when a specific stage or tier refused |

Headers are omitted rather than guessed while degraded: a fail-open decision has no
limit behind it, and reporting one would be a number the client may cache and act on.

**Status codes**

| Status | Meaning |
|---|---|
| 200 | admitted |
| 400 | the declared cost can never fit the limit — retrying cannot help, so not a 429 |
| 429 | over quota, in a penalty, or shed |
| 503 | `fail_closed` with the limiter unavailable — the caller's quota is intact, the limiter is down |

Errors are JSON, matching the rest of the API.

---

## Testing

```bash
make test        # everything, no external services
make test-race
make cover       # docs/coverage.html
make bench
```

131 tests and 8 benchmarks. Statement coverage on the packages that carry logic:

| Package | Coverage |
|---|---|
| `internal/shardmap` | 95.6% |
| `internal/fallback` | 92.4% |
| `internal/limiter` | 87.2% |
| `internal/config` | 86.1% |
| `internal/middleware` | 86.1% |
| `internal/penalty` | 86.0% |

`cmd/server` (63.1%) is wiring and `internal/metrics` is registration only; both are
exercised end-to-end by the Docker job in CI rather than by unit tests.

The Redis paths are covered without a running Redis: tests use `miniredis`, whose Lua
host executes the real scripts. Because it is a reimplementation, CI *also* runs the
same scripts against a real Redis so the two cannot quietly diverge on
`redis.call('TIME')` resolution, `INCRBY` against a missing key, `PEXPIRE` semantics,
or Lua number conversion. Locally:

```bash
docker run -d -p 6399:6379 redis:7-alpine
RATELIMITER_TEST_REDIS_ADDR=127.0.0.1:6399 go test -run TestIntegration -v ./internal/limiter/
```

Tests assert behaviour rather than the absence of crashes. `TestConcurrentExactness`
fires 500 concurrent requests at a limit of 100 and requires exactly 100 admitted —
a limiter that admits the wrong number passes a race-detector test just as happily.

Load tests (`make k6-steady`, `k6-burst`, `k6-chaos`) print results rather than having
them quoted here; the numbers depend entirely on your hardware, Redis placement and
network.

---

## Benchmarks

`make bench`, `-benchtime=2s`, on an Apple M5 (`darwin/arm64`). Treat them as ratios
rather than absolutes: your hardware differs, and the Redis figures run against an
in-process `miniredis`, so a real network round trip is far slower than shown.

**What leasing buys.** All three rows drive the same Redis-backed limiter:

```
BenchmarkRedisSlidingWindow-10        27342    88221 ns/op   225314 B/op   881 allocs/op
BenchmarkLeaseOverRedisMiss-10        27474    87881 ns/op   225319 B/op   881 allocs/op
BenchmarkLeaseOverRedisHit-10      52838170       45.7 ns/op      0 B/op     0 allocs/op
```

A lease hit is ~1,900× cheaper than consulting Redis, and allocation-free. The miss
row is the same stack with `prefetch: 0`, and it lands within ~1% of the bare Redis
path — so the machinery costs essentially nothing on the requests it cannot help. With
the default `prefetch: 4` roughly four in five requests to a hot key take the 45ns path
instead of the round trip, and none of them are admitted without Redis having counted
them first.

**The algorithms themselves**, in memory:

```
BenchmarkSlidingWindowCounter-10             15511006    154.8 ns/op   0 B/op   0 allocs/op
BenchmarkTokenBucket-10                      15026240    153.4 ns/op   0 B/op   0 allocs/op
BenchmarkSlidingWindowCounterManyKeys-10     92359792     30.4 ns/op   0 B/op   0 allocs/op
BenchmarkDo-10  (shardmap)                  114768879     22.4 ns/op   0 B/op   0 allocs/op
```

The many-keys case is ~5× faster than the single-key one, which is the sharding paying
off: one hot key serialises on a single shard's mutex, while realistic traffic spreads
across all 256. Everything on the hot path is allocation-free.

---

## Limitations and roadmap

Known limits, stated rather than hidden:

- **Single Redis.** No Cluster support wired up, so Redis is a single point of
  failure. The code is Cluster-*ready* — keys are hash-tagged and the limiters accept
  `redis.Scripter` — but there is no cluster client, no sharding config, and it is
  untested against Cluster.
- **No per-route limits.** Routes can override quota *cost* only. Separate limits per
  route would need a limiter instance and Redis keyspace each; the fields are absent
  rather than accepted and ignored.
- **`local_fallback` is approximate**, by N nodes as described above.
- **The Grafana stack is a local demo.** Anonymous admin access and no persistence;
  do not copy that compose file into a deployed environment.
- **No published load-test figures.** The k6 suites print their own results, and the
  numbers depend so heavily on hardware, Redis placement and network that quoting mine
  would be misleading. Run `make k6-steady` and read them off.

Next, roughly in order of value: Redis Cluster with a real sharding story; per-tenant
adaptive limits so one noisy tenant cannot trigger global shedding; a gRPC interceptor
over the same `Limiter` interface.

---

## Tech stack

- **Go 1.22** — `net/http`, `log/slog` for structured logs, `sync/atomic` for the
  lock-free paths, generics for the shared sharded map, `math/rand/v2` because its
  top-level generator uses per-P state instead of a global mutex.
- **Redis 7** — single-threaded execution is what makes a Lua script genuinely atomic,
  which is the whole basis of correctness here.
- **Prometheus + Grafana** — pull-based collectors, provisioned dashboard.
- **k6** — `constant-arrival-rate` executor for load tests that mean something.
- **miniredis** — real Lua execution in unit tests, with real Redis in CI behind it.

---

## License

MIT — see [LICENSE](LICENSE).
