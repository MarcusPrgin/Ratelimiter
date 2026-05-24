# Setup Guide

Everything you need to install before running this project.

---

## Prerequisites

### 1. Go 1.22+

**macOS:**
```bash
brew install go
```

**Linux:**
```bash
wget https://go.dev/dl/go1.22.0.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.22.0.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
source ~/.bashrc
```

**Windows:**
Download installer from https://go.dev/dl/ and run it.

Verify: `go version` → should show `go1.22` or higher.

---

### 2. Docker Desktop

Required for Redis, Prometheus, and Grafana containers.

**macOS / Windows:** https://www.docker.com/products/docker-desktop/

**Linux:**
```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER   # lets you run docker without sudo
newgrp docker
```

Verify: `docker compose version` → should show `v2.x.x`.

---

### 3. k6 (load testing)

**macOS:**
```bash
brew install k6
```

**Linux:**
```bash
sudo gpg -k
sudo gpg --no-default-keyring --keyring /usr/share/keyrings/k6-archive-keyring.gpg \
  --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69
echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" \
  | sudo tee /etc/apt/sources.list.d/k6.list
sudo apt-get update && sudo apt-get install k6
```

**Windows:**
```powershell
winget install k6 --source winget
```

Verify: `k6 version`

---

### 4. golangci-lint (optional, for `make lint`)

**macOS:**
```bash
brew install golangci-lint
```

**Linux / Windows:** https://golangci-lint.run/usage/install/

---

## Project setup

```bash
# 1. Clone or unzip the project
cd ratelimiter

# 2. Download Go dependencies
make deps

# 3. Run tests (no Docker needed for unit tests)
make test-race

# 4. Start the full stack
make docker-up

# 5. Verify everything is up
curl http://localhost:8080/health       # → {"status":"ok"}
curl http://localhost:9090/-/ready      # → Prometheus ready
curl http://localhost:3000              # → Grafana (open in browser)
```

---

## Dependency summary

| Tool | Version | Purpose |
|---|---|---|
| Go | 1.22+ | Language runtime |
| Docker Desktop | latest | Containers for Redis, Prometheus, Grafana |
| k6 | latest | Load testing |
| golangci-lint | latest | Optional linting |

**No cloud accounts needed.** Everything runs locally.

---

## Go module dependencies

These are downloaded automatically by `make deps` or `go mod download`:

| Package | Purpose |
|---|---|
| `github.com/redis/go-redis/v9` | Redis client with connection pooling |
| `github.com/prometheus/client_golang` | Prometheus metrics instrumentation |
| `github.com/spf13/viper` | Config file + environment variable loading |
| `gopkg.in/yaml.v3` | YAML config parsing |

---

## Troubleshooting

**`docker: command not found`**
Docker Desktop isn't installed or isn't in your PATH. Restart your terminal after installing.

**`connection refused` on Redis**
Redis container isn't healthy yet. Wait 10 seconds and retry, or run `docker compose ps` to check status.

**`go: command not found`**
Go isn't in your PATH. Add `/usr/local/go/bin` (Linux) or `$(go env GOPATH)/bin` to your `$PATH`.

**Port 8080 already in use**
Change `server.port` in `config.yaml` to `8081` and update k6 scripts accordingly.

**k6 tests fail with `connection refused`**
The server must be running before you run k6. Run `make docker-up` first, then `make k6-steady`.
