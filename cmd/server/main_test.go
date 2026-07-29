package main

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/MarcusPrgin/Ratelimiter/internal/config"
)

// These tests exercise the wiring in build(): the decorator order, the route table
// and the metric registration. Each component is unit-tested in its own package;
// what can only break here is how they are assembled.

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// buildApp writes a config, points it at an in-process Redis and assembles the app.
func buildApp(t *testing.T, body string) http.Handler {
	t.Helper()

	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	mr := miniredis.RunT(t)
	t.Setenv("RATELIMITER_REDIS_ADDR", mr.Addr())

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("config: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.Redis.Addr})
	t.Cleanup(func() { _ = rdb.Close() })

	app, err := build(cfg, rdb, quietLogger())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return app.handler
}

func get(h http.Handler, path string, headers map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, path, nil)
	r.RemoteAddr = "203.0.113.4:1234"
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

const baseConfig = `
limiter:
  limit: 5
  window: 1m
  key_type: user
  lease:
    enabled: false
routes:
  - path: /api/search
    cost: 5
`

func TestAPIEnforcesLimitAndSetsHeaders(t *testing.T) {
	h := buildApp(t, baseConfig)
	hdr := map[string]string{"X-User-ID": "alice"}

	for i := 0; i < 5; i++ {
		w := get(h, "/api/hello", hdr)
		if w.Code != http.StatusOK {
			t.Fatalf("request %d: status %d, want 200", i+1, w.Code)
		}
		if w.Header().Get("X-RateLimit-Limit") != "5" {
			t.Errorf("request %d: X-RateLimit-Limit = %q", i+1, w.Header().Get("X-RateLimit-Limit"))
		}
	}

	w := get(h, "/api/hello", hdr)
	if w.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d after exhausting the quota, want 429", w.Code)
	}
	if w.Header().Get("Retry-After") == "" {
		t.Error("Retry-After missing on 429")
	}
}

// TestRouteCostIsApplied checks the cost table reaches the limiter. The documented
// examples also have to resolve: previously /api/search was rate limited and then
// 404'd, because the route did not exist.
func TestRouteCostIsApplied(t *testing.T) {
	h := buildApp(t, baseConfig)
	hdr := map[string]string{"X-User-ID": "bob"}

	// cost 5 against a limit of 5 consumes the whole window in one request.
	if w := get(h, "/api/search", hdr); w.Code != http.StatusOK {
		t.Fatalf("/api/search status = %d, want 200 (the route must exist)", w.Code)
	}
	if w := get(h, "/api/hello", hdr); w.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429 — the cost-5 route did not consume the quota", w.Code)
	}
}

func TestPerCallerIsolation(t *testing.T) {
	h := buildApp(t, baseConfig)

	for i := 0; i < 5; i++ {
		get(h, "/api/hello", map[string]string{"X-User-ID": "carol"})
	}
	if w := get(h, "/api/hello", map[string]string{"X-User-ID": "carol"}); w.Code != http.StatusTooManyRequests {
		t.Fatalf("carol status = %d, want 429", w.Code)
	}
	if w := get(h, "/api/hello", map[string]string{"X-User-ID": "dave"}); w.Code != http.StatusOK {
		t.Errorf("dave status = %d, want 200 — quotas are not isolated per caller", w.Code)
	}
}

// TestHealthAndMetricsAreNotRateLimited matters operationally: these endpoints have
// to answer precisely when the service is throttling everything else.
func TestHealthAndMetricsAreNotRateLimited(t *testing.T) {
	h := buildApp(t, baseConfig)
	hdr := map[string]string{"X-User-ID": "eve"}

	for i := 0; i < 20; i++ {
		get(h, "/api/hello", hdr)
	}
	if w := get(h, "/api/hello", hdr); w.Code != http.StatusTooManyRequests {
		t.Fatalf("expected the caller to be throttled, got %d", w.Code)
	}

	for _, path := range []string{"/healthz", "/readyz", "/health", "/metrics"} {
		if w := get(h, path, hdr); w.Code != http.StatusOK {
			t.Errorf("%s status = %d, want 200 while API traffic is throttled", path, w.Code)
		}
	}
}

func TestReadinessReportsRedisFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(baseConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	mr := miniredis.RunT(t)
	t.Setenv("RATELIMITER_REDIS_ADDR", mr.Addr())

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: cfg.Redis.Addr})
	t.Cleanup(func() { _ = rdb.Close() })

	app, err := build(cfg, rdb, quietLogger())
	if err != nil {
		t.Fatal(err)
	}

	mr.Close() // Redis is now unreachable

	// Readiness includes Redis, so it must fail...
	if w := get(app.handler, "/readyz", nil); w.Code != http.StatusServiceUnavailable {
		t.Errorf("/readyz status = %d with Redis down, want 503", w.Code)
	}
	// ...while liveness must not, or the orchestrator restarts every node during a
	// dependency outage the fallback strategy is designed to ride out.
	if w := get(app.handler, "/healthz", nil); w.Code != http.StatusOK {
		t.Errorf("/healthz status = %d with Redis down, want 200", w.Code)
	}
}

// TestMetricsExposeTheDocumentedSeries guards against a dashboard referring to a
// metric nothing ever registers — which is what happened to the adaptive-shed and
// active-keys counters.
func TestMetricsExposeTheDocumentedSeries(t *testing.T) {
	cfg := `
limiter:
  limit: 5
  window: 1m
  key_type: tenant
  lease:
    enabled: true
    ttl: 50ms
    prefetch: 2
    negative_cache: true
  chain:
    enabled: true
    tenant_limit: 100
    global_limit: 1000
  adaptive:
    enabled: true
  penalty:
    enabled: true
`
	h := buildApp(t, cfg)
	get(h, "/api/hello", map[string]string{"X-Tenant-ID": "acme", "X-User-ID": "alice"})

	body := get(h, "/metrics", nil).Body.String()

	for _, want := range []string{
		"ratelimiter_requests_allowed_total",
		"ratelimiter_requests_denied_total",
		"ratelimiter_limiter_latency_seconds",
		"ratelimiter_requests_in_flight",
		"ratelimiter_adaptive_multiplier",
		"ratelimiter_adaptive_shed_total",
		"ratelimiter_adaptive_latency_ms",
		"ratelimiter_penalty_denied_total",
		"ratelimiter_penalty_escalations_total",
		"ratelimiter_lease_hits_total",
		"ratelimiter_lease_misses_total",
		"ratelimiter_lease_hit_ratio",
		"ratelimiter_degraded_total",
		"ratelimiter_breaker_open",
		"ratelimiter_tracked_keys",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("%s is not exported", want)
		}
	}

	// Chain tier names must be pre-registered as denied_by values, or a tier denial
	// lands in the "other" bucket.
	for _, tier := range []string{"per_key", "per_tenant", "global"} {
		if !strings.Contains(body, `denied_by="`+tier+`"`) {
			t.Errorf("denied_by=%q is not pre-registered", tier)
		}
	}
}

// TestChainedTiersIsolateTenants is the end-to-end form of the per-tenant key bug.
// The tenant tier must bind per tenant, not collapse every tenant into one bucket.
func TestChainedTiersIsolateTenants(t *testing.T) {
	cfg := `
limiter:
  limit: 100
  window: 1m
  key_type: tenant
  lease:
    enabled: false
  chain:
    enabled: true
    tenant_limit: 100
    global_limit: 10000
`
	h := buildApp(t, cfg)

	// Two callers inside tenant acme, using distinct per-caller quotas.
	for _, user := range []string{"alice", "bob"} {
		w := get(h, "/api/hello", map[string]string{"X-Tenant-ID": "acme", "X-User-ID": user})
		if w.Code != http.StatusOK {
			t.Fatalf("acme/%s status = %d, want 200", user, w.Code)
		}
	}
	// A different tenant must be unaffected by acme's usage.
	w := get(h, "/api/hello", map[string]string{"X-Tenant-ID": "globex", "X-User-ID": "alice"})
	if w.Code != http.StatusOK {
		t.Errorf("globex status = %d, want 200 — tenants are sharing one bucket", w.Code)
	}
}

// TestFailClosedRejectsWhenRedisIsDown is the top-to-bottom version of the most
// serious bug: with fail_closed and Redis unreachable, API traffic must be refused.
func TestFailClosedRejectsWhenRedisIsDown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := `
limiter:
  limit: 5
  window: 1m
  key_type: user
  lease:
    enabled: false
  fallback:
    strategy: fail_closed
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	mr := miniredis.RunT(t)
	t.Setenv("RATELIMITER_REDIS_ADDR", mr.Addr())

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: cfg.Redis.Addr})
	t.Cleanup(func() { _ = rdb.Close() })

	app, err := build(cfg, rdb, quietLogger())
	if err != nil {
		t.Fatal(err)
	}

	mr.Close()

	w := get(app.handler, "/api/hello", map[string]string{"X-User-ID": "alice"})
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d with fail_closed and Redis down, want 503", w.Code)
	}
}

// TestFailOpenAdmitsWhenRedisIsDown is the counterpart, so the two strategies are
// pinned to genuinely different behaviour rather than both defaulting to permissive.
func TestFailOpenAdmitsWhenRedisIsDown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := `
limiter:
  limit: 5
  window: 1m
  key_type: user
  lease:
    enabled: false
  fallback:
    strategy: fail_open
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	mr := miniredis.RunT(t)
	t.Setenv("RATELIMITER_REDIS_ADDR", mr.Addr())

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: cfg.Redis.Addr})
	t.Cleanup(func() { _ = rdb.Close() })

	app, err := build(cfg, rdb, quietLogger())
	if err != nil {
		t.Fatal(err)
	}

	mr.Close()

	for i := 0; i < 20; i++ {
		w := get(app.handler, "/api/hello", map[string]string{"X-User-ID": "alice"})
		if w.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d with fail_open and Redis down, want 200",
				i+1, w.Code)
		}
	}
}

// allFeaturesConfig turns on every optional stage at once, which is the wiring nothing
// else exercises: the decorator chain is assembled conditionally, so a stage can be
// individually correct and still be composed in the wrong order.
const allFeaturesConfig = `
limiter:
  algorithm: %s
  limit: 20
  window: 1m
  burst_max: %d
  key_type: tenant
  fallback:
    strategy: local_fallback
  lease:
    enabled: true
    ttl: 50ms
    prefetch: 3
    negative_cache: true
  chain:
    enabled: true
    tenant_limit: 40
    global_limit: 100
  adaptive:
    enabled: true
  penalty:
    enabled: true
    threshold: 5
    strike_window: 60s
    base_penalty: 30s
    max_penalty: 5m
routes:
  - path: /api/search
    cost: 4
`

// TestFullStackEnforcesLimit runs the whole chain — penalty box, lease cache, fallback,
// breaker, adaptive shedding, three Redis tiers — for both algorithms, and requires the
// per-caller limit to hold end to end.
func TestFullStackEnforcesLimit(t *testing.T) {
	for _, tc := range []struct {
		algo  string
		burst int
	}{
		{"sliding_window_counter", 0},
		{"token_bucket", 20},
	} {
		t.Run(tc.algo, func(t *testing.T) {
			h := buildApp(t, fmt.Sprintf(allFeaturesConfig, tc.algo, tc.burst))
			hdr := map[string]string{"X-Tenant-ID": "acme", "X-User-ID": "alice"}

			allowed, denied, other := 0, 0, 0
			for i := 0; i < 60; i++ {
				switch w := get(h, "/api/hello", hdr); w.Code {
				case http.StatusOK:
					allowed++
				case http.StatusTooManyRequests:
					denied++
				default:
					other++
					t.Errorf("request %d: unexpected status %d: %s", i, w.Code, w.Body.String())
				}
			}

			if other != 0 {
				t.Fatalf("%d requests failed unexpectedly", other)
			}
			// The per-caller limit is 20. Anything materially above it means a stage in
			// the chain is handing out quota nothing counted.
			if allowed > 20 {
				t.Errorf("admitted %d of 60 against a per-caller limit of 20", allowed)
			}
			if allowed == 0 {
				t.Error("admitted nothing — a stage is refusing everything")
			}
			if denied == 0 {
				t.Error("refused nothing — the limit is not being enforced")
			}
		})
	}
}

// TestFullStackTenantTierBinds checks the middle tier actually binds: separate callers
// inside one tenant share the tenant ceiling even though each is under its own limit.
func TestFullStackTenantTierBinds(t *testing.T) {
	h := buildApp(t, fmt.Sprintf(allFeaturesConfig, "sliding_window_counter", 0))

	// Per-caller limit 20, tenant limit 40. Three callers at 20 each would be 60,
	// so the tenant tier has to refuse the excess.
	total := 0
	for _, user := range []string{"alice", "bob", "carol"} {
		for i := 0; i < 20; i++ {
			w := get(h, "/api/hello", map[string]string{
				"X-Tenant-ID": "acme", "X-User-ID": user,
			})
			if w.Code == http.StatusOK {
				total++
			}
		}
	}
	if total > 40 {
		t.Errorf("tenant admitted %d against a tenant limit of 40", total)
	}

	// A different tenant must still have its own headroom.
	w := get(h, "/api/hello", map[string]string{"X-Tenant-ID": "globex", "X-User-ID": "alice"})
	if w.Code != http.StatusOK {
		t.Errorf("second tenant got %d — tenants are sharing a bucket", w.Code)
	}
}

// TestFullStackPenaltyEscalates checks the outermost stage engages end to end: a caller
// that keeps hammering past its quota is eventually blocked by the penalty box rather
// than merely throttled, and says so.
func TestFullStackPenaltyEscalates(t *testing.T) {
	h := buildApp(t, fmt.Sprintf(allFeaturesConfig, "sliding_window_counter", 0))
	hdr := map[string]string{"X-Tenant-ID": "acme", "X-User-ID": "abuser"}

	sawPenalty := false
	for i := 0; i < 200; i++ {
		w := get(h, "/api/hello", hdr)
		if w.Code == http.StatusTooManyRequests &&
			w.Header().Get("X-RateLimit-Denied-By") == "penalty" {
			sawPenalty = true
			break
		}
	}
	if !sawPenalty {
		t.Error("a caller well past its quota never entered the penalty box")
	}
}

// TestFullStackCostRouteConsumesProportionally checks the cost table survives the whole
// chain, including the lease cache's batched claims.
func TestFullStackCostRouteConsumesProportionally(t *testing.T) {
	h := buildApp(t, fmt.Sprintf(allFeaturesConfig, "sliding_window_counter", 0))
	hdr := map[string]string{"X-Tenant-ID": "acme", "X-User-ID": "spender"}

	// Limit 20, cost 4: five requests exhaust the caller's quota.
	allowed := 0
	for i := 0; i < 10; i++ {
		if w := get(h, "/api/search", hdr); w.Code == http.StatusOK {
			allowed++
		}
	}
	if allowed > 5 {
		t.Errorf("admitted %d cost-4 requests against a limit of 20, want at most 5", allowed)
	}
	if allowed == 0 {
		t.Error("admitted no cost-4 requests at all")
	}
}

func TestBuildRejectsBothAlgorithms(t *testing.T) {
	for _, algo := range []string{"sliding_window_counter", "token_bucket"} {
		t.Run(algo, func(t *testing.T) {
			body := "limiter:\n  algorithm: " + algo + "\n  limit: 10\n  window: 1m\n  burst_max: 20\n"
			h := buildApp(t, body)
			if w := get(h, "/api/hello", nil); w.Code != http.StatusOK {
				t.Errorf("status = %d, want 200", w.Code)
			}
		})
	}
}
