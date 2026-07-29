package middleware_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/MarcusPrgin/Ratelimiter/internal/fallback"
	"github.com/MarcusPrgin/Ratelimiter/internal/limiter"
	"github.com/MarcusPrgin/Ratelimiter/internal/metrics"
	"github.com/MarcusPrgin/Ratelimiter/internal/middleware"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newRecorder() *metrics.Recorder {
	return metrics.NewRecorder(prometheus.NewRegistry(), "test", "ip", []string{
		fallback.DeniedByUnavailable, limiter.ShedDeniedBy, "per_key",
	})
}

// okHandler reports whether it was reached, which is how these tests distinguish
// "admitted" from "refused".
type okHandler struct{ reached bool }

func (h *okHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	h.reached = true
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"ok":true}`))
}

func newMW(t *testing.T, l limiter.Limiter, cfg middleware.Config) *middleware.RateLimiter {
	t.Helper()
	if cfg.KeyMode == "" {
		cfg.KeyMode = middleware.KeyByIP
	}
	mw, err := middleware.New(l, cfg, newRecorder(), quietLogger())
	if err != nil {
		t.Fatalf("middleware.New: %v", err)
	}
	return mw
}

func do(h http.Handler, req *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func request(headers map[string]string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/api/hello", nil)
	r.RemoteAddr = "203.0.113.9:54321"
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	return r
}

func TestAdmittedRequestReachesHandlerWithHeaders(t *testing.T) {
	l := limiter.NewSlidingWindowCounter(limiter.Config{Limit: 10, Window: time.Minute})
	next := &okHandler{}
	h := newMW(t, l, middleware.Config{}).Handler(next)

	w := do(h, request(nil))

	if w.Code != http.StatusOK || !next.reached {
		t.Fatalf("status=%d reached=%t, want 200/true", w.Code, next.reached)
	}
	if got := w.Header().Get("X-RateLimit-Limit"); got != "10" {
		t.Errorf("X-RateLimit-Limit = %q, want 10", got)
	}
	if got := w.Header().Get("X-RateLimit-Remaining"); got != "9" {
		t.Errorf("X-RateLimit-Remaining = %q, want 9", got)
	}
	if w.Header().Get("X-RateLimit-Reset") == "" {
		t.Error("X-RateLimit-Reset missing")
	}
}

func TestDeniedRequestReturns429(t *testing.T) {
	l := limiter.NewSlidingWindowCounter(limiter.Config{Limit: 1, Window: time.Minute})
	next := &okHandler{}
	h := newMW(t, l, middleware.Config{}).Handler(next)

	do(h, request(nil))
	next.reached = false
	w := do(h, request(nil))

	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", w.Code)
	}
	if next.reached {
		t.Error("denied request still reached the handler")
	}

	// Retry-After must be a whole number of seconds and at least 1: rounding down to
	// 0 tells the client to retry immediately, producing another denial.
	ra := w.Header().Get("Retry-After")
	secs, err := strconv.Atoi(ra)
	if err != nil {
		t.Fatalf("Retry-After = %q, want an integer", ra)
	}
	if secs < 1 {
		t.Errorf("Retry-After = %d, want >= 1", secs)
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, want JSON to match the rest of the API", ct)
	}
	var body struct {
		Error  string `json:"error"`
		Status int    `json:"status"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if body.Status != http.StatusTooManyRequests {
		t.Errorf("body status = %d, want 429", body.Status)
	}
}

// TestFailClosedReturns503 is the end-to-end proof of the original bug. With
// fail_closed and a dead limiter, the request must be refused. The previous
// middleware called next.ServeHTTP on any limiter error, so this admitted traffic.
func TestFailClosedReturns503(t *testing.T) {
	dead := &erroringLimiter{}
	fb, err := fallback.New(dead, nil,
		fallback.Config{Strategy: fallback.FailClosed}, quietLogger())
	if err != nil {
		t.Fatal(err)
	}

	next := &okHandler{}
	h := newMW(t, fb, middleware.Config{}).Handler(next)
	w := do(h, request(nil))

	if next.reached {
		t.Fatal("fail_closed admitted a request while the limiter was down")
	}
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 — the caller is not over quota, the "+
			"limiter is down", w.Code)
	}
	if w.Header().Get("Retry-After") == "" {
		t.Error("Retry-After missing on 503")
	}
}

func TestFailOpenAdmitsWithoutClaimingAQuota(t *testing.T) {
	dead := &erroringLimiter{}
	fb, err := fallback.New(dead, nil,
		fallback.Config{Strategy: fallback.FailOpen}, quietLogger())
	if err != nil {
		t.Fatal(err)
	}

	next := &okHandler{}
	h := newMW(t, fb, middleware.Config{}).Handler(next)
	w := do(h, request(nil))

	if !next.reached || w.Code != http.StatusOK {
		t.Fatalf("status=%d reached=%t, want 200/true", w.Code, next.reached)
	}
	// No limit is knowable while degraded, so advertising one would be a number the
	// client may cache and act on.
	if got := w.Header().Get("X-RateLimit-Limit"); got != "" {
		t.Errorf("X-RateLimit-Limit = %q while degraded, want absent", got)
	}
}

// TestImpossibleCostReturns400 pins the distinction between throttled and
// impossible. A 429 here would put the client in a retry loop that can never
// succeed.
func TestImpossibleCostReturns400(t *testing.T) {
	l := limiter.NewSlidingWindowCounter(limiter.Config{Limit: 5, Window: time.Minute})
	next := &okHandler{}
	h := newMW(t, l, middleware.Config{
		Cost: func(*http.Request) int64 { return 50 },
	}).Handler(next)

	w := do(h, request(nil))

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for a cost the limit can never grant", w.Code)
	}
	if next.reached {
		t.Error("handler reached despite an impossible cost")
	}
}

func TestPanicIsRecovered(t *testing.T) {
	l := limiter.NewSlidingWindowCounter(limiter.Config{Limit: 10, Window: time.Minute})
	boom := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	})
	h := newMW(t, l, middleware.Config{}).Handler(boom)

	// Without recovery this panic would unwind past the server and kill the process,
	// taking every other in-flight request with it.
	w := do(h, request(nil))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", w.Code)
	}
}

func TestAbortHandlerPanicIsNotSwallowed(t *testing.T) {
	l := limiter.NewSlidingWindowCounter(limiter.Config{Limit: 10, Window: time.Minute})
	abort := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(http.ErrAbortHandler)
	})
	h := newMW(t, l, middleware.Config{}).Handler(abort)

	defer func() {
		if v := recover(); v != http.ErrAbortHandler {
			t.Errorf("recovered %v, want ErrAbortHandler to propagate to net/http", v)
		}
	}()
	do(h, request(nil))
}

// ── Key extraction ───────────────────────────────────────────────────────────

// TestXForwardedForIgnoredByDefault covers the spoofing hole. Trusting the header
// unconditionally lets a caller choose a fresh limit key per request, and so an
// unlimited quota.
func TestXForwardedForIgnoredByDefault(t *testing.T) {
	l := limiter.NewSlidingWindowCounter(limiter.Config{Limit: 2, Window: time.Minute})
	next := &okHandler{}
	h := newMW(t, l, middleware.Config{TrustedProxyHops: 0}).Handler(next)

	allowed := 0
	for i := 0; i < 10; i++ {
		// A different forged header every time.
		w := do(h, request(map[string]string{
			"X-Forwarded-For": "10.0.0." + strconv.Itoa(i),
		}))
		if w.Code == http.StatusOK {
			allowed++
		}
	}
	if allowed != 2 {
		t.Errorf("admitted %d of 10 with rotating X-Forwarded-For values, want 2 — "+
			"the header is letting callers pick their own quota", allowed)
	}
}

// TestXForwardedForCountsFromTheRight checks the client address is located by
// counting trusted hops from the right, which is the part a caller cannot forge.
func TestXForwardedForCountsFromTheRight(t *testing.T) {
	l := limiter.NewSlidingWindowCounter(limiter.Config{Limit: 2, Window: time.Minute})
	next := &okHandler{}
	// One proxy in front: it appended the real peer, so the last entry is the client.
	h := newMW(t, l, middleware.Config{TrustedProxyHops: 1}).Handler(next)

	allowed := 0
	for i := 0; i < 10; i++ {
		// The client forges a prefix; our proxy appended the true address last.
		w := do(h, request(map[string]string{
			"X-Forwarded-For": "10.9.9." + strconv.Itoa(i) + ", 198.51.100.7",
		}))
		if w.Code == http.StatusOK {
			allowed++
		}
	}
	if allowed != 2 {
		t.Errorf("admitted %d of 10, want 2 — the forged left-hand entries are "+
			"being used as the key", allowed)
	}
}

func TestKeyModes(t *testing.T) {
	tests := []struct {
		name    string
		mode    middleware.KeyMode
		headers map[string]string
		other   map[string]string
		shared  bool // do the two header sets share a quota?
	}{
		{
			name:    "user separates by X-User-ID",
			mode:    middleware.KeyByUser,
			headers: map[string]string{"X-User-ID": "alice"},
			other:   map[string]string{"X-User-ID": "bob"},
			shared:  false,
		},
		{
			name:    "user falls back to IP when the header is absent",
			mode:    middleware.KeyByUser,
			headers: nil,
			other:   nil,
			shared:  true,
		},
		{
			name:    "tenant separates by tenant",
			mode:    middleware.KeyByTenant,
			headers: map[string]string{"X-Tenant-ID": "acme", "X-User-ID": "alice"},
			other:   map[string]string{"X-Tenant-ID": "globex", "X-User-ID": "alice"},
			shared:  false,
		},
		{
			name:    "tenant separates users inside one tenant",
			mode:    middleware.KeyByTenant,
			headers: map[string]string{"X-Tenant-ID": "acme", "X-User-ID": "alice"},
			other:   map[string]string{"X-Tenant-ID": "acme", "X-User-ID": "bob"},
			shared:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l := limiter.NewSlidingWindowCounter(limiter.Config{Limit: 1, Window: time.Minute})
			h := newMW(t, l, middleware.Config{KeyMode: tc.mode}).Handler(&okHandler{})

			if w := do(h, request(tc.headers)); w.Code != http.StatusOK {
				t.Fatalf("first request status = %d, want 200", w.Code)
			}
			w := do(h, request(tc.other))
			denied := w.Code == http.StatusTooManyRequests
			if denied != tc.shared {
				t.Errorf("second request denied=%t, want shared=%t", denied, tc.shared)
			}
		})
	}
}

// TestOversizedKeyIsBounded covers the memory-exhaustion angle: header values end
// up as Redis key names, so their length has to be capped.
func TestOversizedKeyIsBounded(t *testing.T) {
	l := limiter.NewSlidingWindowCounter(limiter.Config{Limit: 1, Window: time.Minute})
	h := newMW(t, l, middleware.Config{KeyMode: middleware.KeyByUser}).Handler(&okHandler{})

	huge := strings.Repeat("x", 100_000)
	if w := do(h, request(map[string]string{"X-User-ID": huge})); w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	// Same oversized id: hashing must be deterministic, or the caller gets a fresh
	// quota on every request.
	if w := do(h, request(map[string]string{"X-User-ID": huge})); w.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429 — the bounded key is not stable", w.Code)
	}
	// A different oversized id must not collide with the first.
	other := strings.Repeat("y", 100_000)
	if w := do(h, request(map[string]string{"X-User-ID": other})); w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 — distinct long keys are colliding", w.Code)
	}
}

// capturingLimiter records the key it was asked about, so tests can assert on the
// key the middleware actually derived.
type capturingLimiter struct {
	keys []string
}

func (c *capturingLimiter) Allow(ctx context.Context, key string) (limiter.Result, error) {
	return c.AllowN(ctx, key, 1)
}

func (c *capturingLimiter) AllowN(_ context.Context, key string, _ int64) (limiter.Result, error) {
	c.keys = append(c.keys, key)
	return limiter.Result{Allowed: true, Limit: 100, Remaining: 99}, nil
}

func (c *capturingLimiter) Name() string { return "capturing" }

// TestDerivedKeysAreBoundedAndStructured covers both halves of key derivation at once.
//
// Bounded: header values become Redis key names, so an unbounded one is a cheap way to
// exhaust Redis memory.
//
// Structured: the "tenant:…|user:…" shape has to survive bounding. Hashing the finished
// key instead of its components would erase the tenant prefix, so TenantOf could not
// recover the tenant and the chain's per-tenant tier would put every oversized-header
// caller into one shared bucket.
func TestDerivedKeysAreBoundedAndStructured(t *testing.T) {
	huge := strings.Repeat("x", 100_000)
	other := strings.Repeat("y", 100_000)

	tests := []struct {
		name    string
		mode    middleware.KeyMode
		headers map[string]string
		tenant  string // expected TenantOf result
	}{
		{"ip", middleware.KeyByIP, nil, ""},
		{"user", middleware.KeyByUser, map[string]string{"X-User-ID": huge}, ""},
		{
			name: "tenant with oversized ids",
			mode: middleware.KeyByTenant,
			headers: map[string]string{
				"X-Tenant-ID": huge, "X-User-ID": other,
			},
			// The tenant must still be recoverable, and must be the bounded form of
			// the tenant id rather than empty.
			tenant: "recoverable",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cap := &capturingLimiter{}
			h := newMW(t, cap, middleware.Config{KeyMode: tc.mode}).Handler(&okHandler{})
			do(h, request(tc.headers))

			if len(cap.keys) != 1 {
				t.Fatalf("limiter saw %d keys, want 1", len(cap.keys))
			}
			key := cap.keys[0]

			if len(key) > middleware.MaxKeyLen {
				t.Errorf("derived key is %d bytes, over the %d bound: %q",
					len(key), middleware.MaxKeyLen, key)
			}
			if strings.Contains(key, huge) || strings.Contains(key, other) {
				t.Error("an oversized header value was embedded in the key verbatim")
			}

			got := middleware.TenantOf(key)
			if tc.tenant == "" {
				if got != "" {
					t.Errorf("TenantOf(%q) = %q, want empty for a non-tenant key", key, got)
				}
				return
			}
			if got == "" {
				t.Errorf("TenantOf(%q) = empty — the tenant is not recoverable, so every "+
					"oversized-header caller would share one tenant bucket", key)
			}
		})
	}
}

// TestOversizedTenantsStayIsolated is the consequence of the above: two different
// oversized tenant ids must not collapse into the same tenant.
func TestOversizedTenantsStayIsolated(t *testing.T) {
	cap := &capturingLimiter{}
	h := newMW(t, cap, middleware.Config{KeyMode: middleware.KeyByTenant}).Handler(&okHandler{})

	do(h, request(map[string]string{
		"X-Tenant-ID": strings.Repeat("a", 5000), "X-User-ID": "u",
	}))
	do(h, request(map[string]string{
		"X-Tenant-ID": strings.Repeat("b", 5000), "X-User-ID": "u",
	}))

	if len(cap.keys) != 2 {
		t.Fatalf("limiter saw %d keys, want 2", len(cap.keys))
	}
	a, b := middleware.TenantOf(cap.keys[0]), middleware.TenantOf(cap.keys[1])
	if a == "" || b == "" {
		t.Fatalf("tenants not recoverable: %q, %q", a, b)
	}
	if a == b {
		t.Errorf("two distinct oversized tenant ids both mapped to tenant %q", a)
	}
}

func TestTenantOf(t *testing.T) {
	tests := []struct{ key, want string }{
		{"tenant:acme|user:alice", "acme"},
		{"tenant:acme|user:", "acme"},
		{"tenant:acme", "acme"},
		{"user:alice", ""},
		{"ip:203.0.113.9", ""},
		{"", ""},
		// Regression guard for the original bug: taking everything before the first
		// colon yields the literal "tenant", collapsing every tenant into one bucket.
		{"tenant:globex|user:bob", "globex"},
	}
	for _, tc := range tests {
		if got := middleware.TenantOf(tc.key); got != tc.want {
			t.Errorf("TenantOf(%q) = %q, want %q", tc.key, got, tc.want)
		}
	}
}

func TestParseKeyMode(t *testing.T) {
	for _, s := range []string{"ip", "user", "tenant"} {
		if _, err := middleware.ParseKeyMode(s); err != nil {
			t.Errorf("ParseKeyMode(%q) => %v, want ok", s, err)
		}
	}
	for _, s := range []string{"", "IP", "userid", "nonsense"} {
		if _, err := middleware.ParseKeyMode(s); err == nil {
			t.Errorf("ParseKeyMode(%q) => ok, want error", s)
		}
	}
}

func TestConfigValidation(t *testing.T) {
	if err := (middleware.Config{KeyMode: "bogus"}).Validate(); err == nil {
		t.Error("bogus key mode accepted")
	}
	if err := (middleware.Config{KeyMode: middleware.KeyByIP, TrustedProxyHops: -1}).Validate(); err == nil {
		t.Error("negative trusted_proxy_hops accepted")
	}
	if err := (middleware.Config{KeyMode: middleware.KeyByIP}).Validate(); err != nil {
		t.Errorf("valid config rejected: %v", err)
	}
}

func TestNewRejectsMissingDependencies(t *testing.T) {
	cfg := middleware.Config{KeyMode: middleware.KeyByIP}
	l := limiter.NewSlidingWindowCounter(limiter.Config{Limit: 1, Window: time.Second})

	if _, err := middleware.New(nil, cfg, newRecorder(), quietLogger()); err == nil {
		t.Error("nil limiter accepted")
	}
	if _, err := middleware.New(l, cfg, nil, quietLogger()); err == nil {
		t.Error("nil recorder accepted")
	}
}

// erroringLimiter always fails, standing in for an unreachable Redis.
type erroringLimiter struct{}

func (e *erroringLimiter) Allow(ctx context.Context, key string) (limiter.Result, error) {
	return e.AllowN(ctx, key, 1)
}

func (e *erroringLimiter) AllowN(context.Context, string, int64) (limiter.Result, error) {
	return limiter.Result{}, io.ErrUnexpectedEOF
}

func (e *erroringLimiter) Name() string { return "erroring" }
