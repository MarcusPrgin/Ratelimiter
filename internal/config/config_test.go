package config_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/MarcusPrgin/Ratelimiter/internal/config"
)

// write puts a config file in a temp dir and returns its path.
func write(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// minimal is a valid config with everything optional left to defaults.
const minimal = `
limiter:
  limit: 100
  window: 1s
`

// TestShippedConfigIsValid loads the repository's own config.yaml. A committed
// config that fails its own validation is a broken default for every new deployment.
func TestShippedConfigIsValid(t *testing.T) {
	cfg, err := config.Load("../../config.yaml")
	if err != nil {
		t.Fatalf("the shipped config.yaml does not validate: %v", err)
	}
	if cfg.Limiter.Limit < 1 {
		t.Errorf("limit = %d", cfg.Limiter.Limit)
	}
	if cfg.Algorithm() == "" {
		t.Error("algorithm did not resolve")
	}
}

func TestDefaultsApplied(t *testing.T) {
	cfg, err := config.Load(write(t, minimal))
	if err != nil {
		t.Fatal(err)
	}

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"server.port", cfg.Server.Port, 8080},
		{"server.shutdown_timeout", cfg.Server.ShutdownTimeout, 15 * time.Second},
		{"redis.addr", cfg.Redis.Addr, "localhost:6379"},
		{"limiter.algorithm", cfg.Limiter.Algorithm, "sliding_window_counter"},
		{"limiter.key_type", cfg.Limiter.KeyType, "ip"},
		{"limiter.fallback.strategy", cfg.Limiter.Fallback.Strategy, "fail_open"},
		{"limiter.lease.enabled", cfg.Limiter.Lease.Enabled, true},
		{"metrics.path", cfg.Metrics.Path, "/metrics"},
		{"log.format", cfg.Log.Format, "json"},
		// Trusting X-Forwarded-For must be opt-in.
		{"limiter.trusted_proxy_hops", cfg.Limiter.TrustedProxyHops, 0},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

// TestUnknownKeyRejected is the typo guard. Silently ignoring an unrecognised key
// means a setting someone believed they had enabled quietly not existing.
func TestUnknownKeyRejected(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"misspelled top level", "limitr:\n  limit: 10\n"},
		{"misspelled nested", minimal + "\n  fallbacK_strategy: fail_open\n"},
		{"stale flat key", minimal + "\n  cache_ttl: 5ms\n"},
		{"unsupported per-route limit", minimal + "\nroutes:\n  - path: /api/x\n    cost: 2\n    limit: 10\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := config.Load(write(t, tc.body)); err == nil {
				t.Error("=> ok, want an error naming the unknown key")
			}
		})
	}
}

// TestRemovedAlgorithmsRejected checks the two deleted algorithms fail loudly rather
// than silently falling back to the default.
func TestRemovedAlgorithmsRejected(t *testing.T) {
	for _, algo := range []string{"fixed_window", "sliding_window_log"} {
		t.Run(algo, func(t *testing.T) {
			_, err := config.Load(write(t, "limiter:\n  limit: 10\n  window: 1s\n  algorithm: "+algo+"\n"))
			if err == nil {
				t.Fatal("=> ok, want an error")
			}
			if !strings.Contains(err.Error(), "algorithm") {
				t.Errorf("error does not mention the algorithm: %v", err)
			}
		})
	}
}

// TestBadFallbackStrategyRejected is the highest-stakes validation here: a typo
// silently defaulting to permissive behaviour would turn a payment service's
// fail_closed into allow-everything.
func TestBadFallbackStrategyRejected(t *testing.T) {
	for _, s := range []string{"failclosed", "fail-closed", "closed", "open", ""} {
		t.Run("strategy="+s, func(t *testing.T) {
			body := minimal + "\n  fallback:\n    strategy: \"" + s + "\"\n"
			if _, err := config.Load(write(t, body)); err == nil {
				t.Error("=> ok, want an error")
			}
		})
	}
}

func TestInvalidValuesRejected(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"zero limit", "limiter:\n  limit: 0\n  window: 1s\n"},
		{"negative limit", "limiter:\n  limit: -5\n  window: 1s\n"},
		{"sub-millisecond window", "limiter:\n  limit: 10\n  window: 100us\n"},
		{"bad key type", minimal + "\n  key_type: username\n"},
		{"negative proxy hops", minimal + "\n  trusted_proxy_hops: -1\n"},
		{"port out of range", "server:\n  port: 99999\n" + minimal},
		{"zero shutdown timeout", "server:\n  shutdown_timeout: 0s\n" + minimal},
		{"empty redis addr", "redis:\n  addr: \"\"\n" + minimal},
		{"idle conns above pool", "redis:\n  pool_size: 4\n  min_idle_conns: 10\n" + minimal},
		{"bad log level", minimal + "\nlog:\n  level: verbose\n"},
		{"bad log format", minimal + "\nlog:\n  format: xml\n"},
		{"metrics path without slash", minimal + "\nmetrics:\n  path: metrics\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := config.Load(write(t, tc.body)); err == nil {
				t.Error("=> ok, want an error")
			}
		})
	}
}

// TestChainRequiresTenantKey covers a misconfiguration that looks like it works: a
// per-tenant tier with no tenant in the key puts every caller in one bucket, so the
// tier silently behaves as a second global limit.
func TestChainRequiresTenantKey(t *testing.T) {
	body := `
limiter:
  limit: 100
  window: 1s
  key_type: ip
  chain:
    enabled: true
    tenant_limit: 1000
    global_limit: 10000
`
	_, err := config.Load(write(t, body))
	if err == nil {
		t.Fatal("=> ok, want an error about key_type")
	}
	if !strings.Contains(err.Error(), "key_type") {
		t.Errorf("error does not point at key_type: %v", err)
	}
}

func TestChainLimitOrderingValidated(t *testing.T) {
	tests := []struct {
		name                  string
		limit, tenant, global int64
	}{
		{"tenant below per-caller", 100, 50, 10000},
		{"global below tenant", 100, 1000, 500},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body := strings.NewReplacer(
				"$LIMIT", strconv.FormatInt(tc.limit, 10),
				"$TENANT", strconv.FormatInt(tc.tenant, 10),
				"$GLOBAL", strconv.FormatInt(tc.global, 10),
			).Replace(`
limiter:
  limit: $LIMIT
  window: 1s
  key_type: tenant
  chain:
    enabled: true
    tenant_limit: $TENANT
    global_limit: $GLOBAL
`)
			if _, err := config.Load(write(t, body)); err == nil {
				t.Error("=> ok, want an error about tier ordering")
			}
		})
	}
}

// TestRouteCostValidated stops a route that could never be served from reaching
// production, where it would return 400 for every request.
func TestRouteCostValidated(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"cost above capacity", "limiter:\n  limit: 10\n  window: 1s\nroutes:\n  - path: /api/x\n    cost: 50\n"},
		{"zero cost", minimal + "\nroutes:\n  - path: /api/x\n    cost: 0\n"},
		{"negative cost", minimal + "\nroutes:\n  - path: /api/x\n    cost: -1\n"},
		{"path without slash", minimal + "\nroutes:\n  - path: api/x\n    cost: 2\n"},
		{"empty path", minimal + "\nroutes:\n  - path: \"\"\n    cost: 2\n"},
		{"duplicate paths", minimal + "\nroutes:\n  - path: /api/x\n    cost: 2\n  - path: /api/x\n    cost: 3\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := config.Load(write(t, tc.body)); err == nil {
				t.Error("=> ok, want an error")
			}
		})
	}
}

// TestRouteCostCheckedAgainstEveryTier covers a gap that only appears with the token
// bucket and the chain together: burst_max can exceed a tier's limit, so a cost between
// the two is admitted by the per-caller tier and then permanently rejected by a later
// one. Checking only the per-caller capacity lets that reach production, where the route
// returns 400 forever.
func TestRouteCostCheckedAgainstEveryTier(t *testing.T) {
	// burst_max 200 admits a cost of 150, but the tenant tier caps at 100.
	tooBigForTier := `
limiter:
  algorithm: token_bucket
  limit: 100
  window: 1s
  burst_max: 200
  key_type: tenant
  chain:
    enabled: true
    tenant_limit: 100
    global_limit: 1000
routes:
  - path: /api/heavy
    cost: 150
`
	_, err := config.Load(write(t, tooBigForTier))
	if err == nil {
		t.Error("=> ok, want an error: the cost exceeds the tenant tier's capacity")
	}

	// The same cost is fine once every tier can accommodate it.
	fitsAllTiers := strings.Replace(tooBigForTier, "tenant_limit: 100", "tenant_limit: 200", 1)
	if _, err := config.Load(write(t, fitsAllTiers)); err != nil {
		t.Errorf("valid config rejected: %v", err)
	}
}

func TestTokenBucketBurstValidated(t *testing.T) {
	// A bucket smaller than one window of quota can never hold the configured rate.
	body := "limiter:\n  algorithm: token_bucket\n  limit: 100\n  window: 1s\n  burst_max: 50\n"
	if _, err := config.Load(write(t, body)); err == nil {
		t.Error("=> ok, want an error about burst_max below limit")
	}

	body = "limiter:\n  algorithm: token_bucket\n  limit: 100\n  window: 1s\n  burst_max: 250\n"
	if _, err := config.Load(write(t, body)); err != nil {
		t.Errorf("valid token bucket config rejected: %v", err)
	}
}

// TestAllErrorsReportedTogether checks the operator gets the whole list rather than
// fixing one problem per restart.
func TestAllErrorsReportedTogether(t *testing.T) {
	body := `
limiter:
  limit: 0
  window: 1s
  algorithm: nonsense
  key_type: nonsense
  fallback:
    strategy: nonsense
`
	_, err := config.Load(write(t, body))
	if err == nil {
		t.Fatal("=> ok, want errors")
	}
	msg := err.Error()
	for _, want := range []string{"algorithm", "key_type", "strategy", "limit"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message omits %q:\n%s", want, msg)
		}
	}
}

func TestEnvOverride(t *testing.T) {
	t.Setenv("RATELIMITER_LIMITER_LIMIT", "777")
	t.Setenv("RATELIMITER_LIMITER_FALLBACK_STRATEGY", "fail_closed")
	t.Setenv("RATELIMITER_REDIS_ADDR", "redis.internal:6379")

	cfg, err := config.Load(write(t, minimal))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Limiter.Limit != 777 {
		t.Errorf("limit = %d, want 777 from the environment", cfg.Limiter.Limit)
	}
	if cfg.Limiter.Fallback.Strategy != "fail_closed" {
		t.Errorf("strategy = %q, want fail_closed", cfg.Limiter.Fallback.Strategy)
	}
	if cfg.Redis.Addr != "redis.internal:6379" {
		t.Errorf("redis.addr = %q", cfg.Redis.Addr)
	}
}

// TestEnvOverrideIsValidated checks environment values go through the same checks as
// file values, rather than bypassing them.
func TestEnvOverrideIsValidated(t *testing.T) {
	t.Setenv("RATELIMITER_LIMITER_FALLBACK_STRATEGY", "whoops")
	if _, err := config.Load(write(t, minimal)); err == nil {
		t.Error("=> ok, want the environment override validated too")
	}
}

func TestMissingFileIsAnError(t *testing.T) {
	if _, err := config.Load(filepath.Join(t.TempDir(), "absent.yaml")); err == nil {
		t.Error("=> ok, want an error for a missing config file")
	}
}

func TestCostMap(t *testing.T) {
	body := minimal + "\nroutes:\n  - path: /api/a\n    cost: 5\n  - path: /api/b\n    cost: 1\n"
	cfg, err := config.Load(write(t, body))
	if err != nil {
		t.Fatal(err)
	}
	m := cfg.CostMap()
	if m["/api/a"] != 5 {
		t.Errorf("cost for /api/a = %d, want 5", m["/api/a"])
	}
	// Cost 1 is the default, so it need not occupy a map entry on the hot path.
	if _, ok := m["/api/b"]; ok {
		t.Error("cost 1 should not be stored")
	}
}

func TestParseLogLevel(t *testing.T) {
	for _, s := range []string{"debug", "info", "warn", "warning", "error", "INFO", " info "} {
		if _, err := config.ParseLogLevel(s); err != nil {
			t.Errorf("ParseLogLevel(%q) => %v, want ok", s, err)
		}
	}
	for _, s := range []string{"", "trace", "fatal"} {
		if _, err := config.ParseLogLevel(s); err == nil {
			t.Errorf("ParseLogLevel(%q) => ok, want error", s)
		}
	}
}
