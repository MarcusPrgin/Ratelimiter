.PHONY: all build run test test-race bench lint docker-up docker-down k6-steady k6-burst k6-chaos clean deps

# ── Build ──────────────────────────────────────────────────────────────────
all: test build

build:
	go build -ldflags="-s -w" -o bin/server ./cmd/server

run:
	go run ./cmd/server -config config.yaml

# ── Test ───────────────────────────────────────────────────────────────────
test:
	go test ./... -v -timeout 30s

test-race:
	go test -race ./... -v -timeout 60s

# Run benchmarks and save output for the README
bench:
	go test -bench=. -benchmem -run='^$$' ./internal/limiter/... | tee docs/benchmarks.txt

# ── Quality ────────────────────────────────────────────────────────────────
lint:
	golangci-lint run ./...

vet:
	go vet ./...

# ── Docker ─────────────────────────────────────────────────────────────────
docker-up:
	docker compose up --build -d
	@echo "App:        http://localhost:8080"
	@echo "Prometheus: http://localhost:9090"
	@echo "Grafana:    http://localhost:3000"

docker-down:
	docker compose down

# Kill Redis to test fallback behaviour
redis-stop:
	docker compose stop redis
	@echo "Redis stopped — watch fallback behaviour"

redis-start:
	docker compose start redis
	@echo "Redis restarted — watch recovery"

# ── k6 load tests ──────────────────────────────────────────────────────────
# Requires: brew install k6  OR  https://k6.io/docs/getting-started/installation/
k6-steady:
	k6 run k6/steady.js

k6-burst:
	k6 run k6/burst.js

k6-chaos:
	@echo "Start the chaos test, then in another terminal run: make redis-stop"
	k6 run k6/chaos.js

# Save k6 results as JSON for the README
k6-bench-all:
	k6 run --out json=docs/k6-steady.json k6/steady.js
	k6 run --out json=docs/k6-burst.json k6/burst.js

# ── Deps ───────────────────────────────────────────────────────────────────
deps:
	go mod tidy
	go mod download

# ── Clean ──────────────────────────────────────────────────────────────────
clean:
	rm -rf bin/ docs/benchmarks.txt
