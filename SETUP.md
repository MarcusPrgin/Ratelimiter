# Setup

Everything runs locally; no cloud accounts are needed.

---

## What you need

| Tool | Version | Needed for |
|---|---|---|
| Go | 1.22+ | building and testing |
| Docker Desktop | current | the full stack (Redis, Prometheus, Grafana) |
| k6 | current | load tests only |
| golangci-lint | current | `make lint` only |

**The test suite needs none of these except Go.** Redis-backed code paths are covered
by `miniredis`, which runs in-process, so `make test` works on a clean checkout.

### Go

```bash
# macOS
brew install go

# Linux
wget https://go.dev/dl/go1.22.12.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.22.12.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc && source ~/.bashrc
```

Windows: installer from https://go.dev/dl/. Verify with `go version`.

### Docker

macOS / Windows: https://www.docker.com/products/docker-desktop/

```bash
# Linux
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER && newgrp docker
```

Verify with `docker compose version` (expect v2.x).

### k6 — optional

```bash
brew install k6                          # macOS
winget install k6 --source winget        # Windows
```

Linux: https://grafana.com/docs/k6/latest/set-up/install-k6/

### golangci-lint — optional

```bash
brew install golangci-lint               # macOS
```

Otherwise: https://golangci-lint.run/usage/install/

---

## First run

```bash
cd ratelimiter

make tidy          # download and verify dependencies
make check         # validate config.yaml without starting anything
make test          # full suite — no Docker, no Redis
make docker-up     # app + Redis + Prometheus + Grafana + Redis exporter
```

Confirm it came up:

```bash
curl -s http://localhost:8080/healthz     # {"status":"ok"}
curl -s http://localhost:8080/readyz      # {"status":"ok","redis":"ok"}
curl -i -H 'X-User-ID: alice' http://localhost:8080/api/hello
curl -s http://localhost:8080/metrics | head
open http://localhost:3000                # Grafana, dashboard pre-provisioned
```

`make help` lists every target.

---

## Optional: integration tests against a real Redis

The unit tests run the Lua scripts on miniredis. To run them against real Redis as CI
does:

```bash
docker run -d --name rl-redis -p 6399:6379 redis:7-alpine

RATELIMITER_TEST_REDIS_ADDR=127.0.0.1:6399 \
  go test -run TestIntegration -v ./internal/limiter/

docker rm -f rl-redis
```

Redis 5 or newer is required, because the scripts read the clock with
`redis.call('TIME')` and rely on effect replication.

---

## Go dependencies

Downloaded automatically by `make tidy`.

| Module | Purpose |
|---|---|
| `github.com/redis/go-redis/v9` | Redis client with pooling and script management |
| `github.com/prometheus/client_golang` | metrics |
| `github.com/spf13/viper` | config file and environment loading |
| `github.com/alicebob/miniredis/v2` | in-process Redis for tests |

---

## Troubleshooting

**`invalid configuration:` on startup**
Working as intended — the message lists every problem at once. Unknown keys are
rejected too, so check for typos in `config.yaml`. Run `make check` to iterate without
starting the server.

**`connection refused` on Redis**
The container may not be healthy yet; `docker compose ps` shows status. The service
starts anyway and applies `limiter.fallback.strategy`, logging a warning — that is
deliberate, so a Redis outage does not also take this service down.

**Port 8080 already in use**
Change `server.port` in `config.yaml`, or run the load tests against another host with
`BASE_URL=http://localhost:8081 k6 run k6/steady.js`.

**Every request returns 503**
`limiter.fallback.strategy` is `fail_closed` and Redis is unreachable. That is the
strategy working. Check `/readyz` and the `ratelimiter_breaker_open` metric.

**`go: command not found` / `docker: command not found`**
Not on `PATH`; restart the terminal after installing.

**k6 reports connection refused**
The server must already be running: `make docker-up` before `make k6-steady`.
