.PHONY: help all build run check test test-race cover bench lint vet fmt tidy \
        docker-up docker-down docker-logs redis-stop redis-start \
        k6-steady k6-burst k6-chaos clean

GO      ?= go
BIN     := bin/server
PKG     := ./...
DOCS    := docs

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

all: check test build ## Validate, test and build

# ── Build ──────────────────────────────────────────────────────────────────
build: ## Build the server binary
	$(GO) build -trimpath -ldflags="-s -w" -o $(BIN) ./cmd/server

run: ## Run the server against config.yaml
	$(GO) run ./cmd/server -config config.yaml

check: ## Validate config.yaml without starting the server
	$(GO) run ./cmd/server -config config.yaml -check

# ── Test ───────────────────────────────────────────────────────────────────
# Tests use miniredis, so the Redis-backed paths and Lua scripts are covered
# without a running Redis.
test: ## Run all tests
	$(GO) test $(PKG) -timeout 90s

test-race: ## Run all tests under the race detector
	$(GO) test -race $(PKG) -timeout 180s

cover: ## Run tests and open a coverage report
	@mkdir -p $(DOCS)
	$(GO) test $(PKG) -coverprofile=$(DOCS)/coverage.out -covermode=atomic -timeout 120s
	$(GO) tool cover -func=$(DOCS)/coverage.out | tail -1
	$(GO) tool cover -html=$(DOCS)/coverage.out -o $(DOCS)/coverage.html
	@echo "report: $(DOCS)/coverage.html"

bench: ## Run benchmarks and save the output
	@mkdir -p $(DOCS)
	$(GO) test -bench=. -benchmem -run='^$$' ./internal/... | tee $(DOCS)/benchmarks.txt

# ── Quality ────────────────────────────────────────────────────────────────
lint: ## Run golangci-lint
	golangci-lint run $(PKG)

vet: ## Run go vet
	$(GO) vet $(PKG)

fmt: ## Format all Go files
	gofmt -l -w cmd internal

tidy: ## Tidy and verify the module graph
	$(GO) mod tidy
	$(GO) mod verify

# ── Docker ─────────────────────────────────────────────────────────────────
docker-up: ## Start app, Redis, Prometheus and Grafana
	docker compose up --build -d
	@echo "App:        http://localhost:8080"
	@echo "Metrics:    http://localhost:8080/metrics"
	@echo "Prometheus: http://localhost:9090"
	@echo "Grafana:    http://localhost:3000"

docker-down: ## Stop the stack
	docker compose down

docker-logs: ## Follow the app logs
	docker compose logs -f app

redis-stop: ## Kill Redis to exercise the fallback strategy
	docker compose stop redis
	@echo "Redis stopped — the circuit breaker should open within a few requests"

redis-start: ## Bring Redis back to watch recovery
	docker compose start redis
	@echo "Redis restarted — the half-open probe should close the breaker"

# ── Load tests ─────────────────────────────────────────────────────────────
# Requires k6: brew install k6
k6-steady: ## Steady traffic at the limit boundary
	k6 run k6/steady.js

k6-burst: ## Single caller bursting well past the limit
	k6 run k6/burst.js

k6-chaos: ## Traffic while Redis is killed and restarted
	@echo "In another terminal: make redis-stop, wait, then make redis-start"
	k6 run k6/chaos.js

# ── Clean ──────────────────────────────────────────────────────────────────
clean: ## Remove build output and reports
	rm -rf bin $(DOCS)/coverage.* $(DOCS)/benchmarks.txt $(DOCS)/k6-*.json
