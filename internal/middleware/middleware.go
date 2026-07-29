// Package middleware provides the HTTP layer for rate limiting: it derives the
// limit key from the request, consults the limiter, sets the X-RateLimit-* headers
// and maps the decision onto a status code.
package middleware

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/MarcusPrgin/Ratelimiter/internal/fallback"
	"github.com/MarcusPrgin/Ratelimiter/internal/limiter"
	"github.com/MarcusPrgin/Ratelimiter/internal/metrics"
)

// KeyMode selects how a caller is identified.
type KeyMode string

const (
	// KeyByIP limits per client IP.
	KeyByIP KeyMode = "ip"
	// KeyByUser limits per X-User-ID, falling back to IP when absent.
	KeyByUser KeyMode = "user"
	// KeyByTenant limits per X-Tenant-ID + X-User-ID composite.
	KeyByTenant KeyMode = "tenant"
)

// ParseKeyMode validates a key mode from configuration.
func ParseKeyMode(s string) (KeyMode, error) {
	switch KeyMode(s) {
	case KeyByIP, KeyByUser, KeyByTenant:
		return KeyMode(s), nil
	default:
		return "", fmt.Errorf("unknown key_type %q (want %q, %q or %q)",
			s, KeyByIP, KeyByUser, KeyByTenant)
	}
}

const (
	// maxComponentLen bounds one client-supplied component of a key — a user id, a
	// tenant id, an address.
	//
	// These come from client-controlled headers and end up as Redis key names.
	// Unbounded, a caller can send a megabyte-long X-User-ID and have the service
	// store it, under as many distinct values as it likes: a cheap way to exhaust
	// Redis memory rather than merely burn quota.
	maxComponentLen = 48

	// MaxKeyLen is the resulting bound on a whole derived key. The longest shape is
	// the tenant composite, "tenant:" + component + "|user:" + component, so this holds
	// by construction; TestDerivedKeysAreBoundedAndStructured checks it stays that way.
	MaxKeyLen = len("tenant:") + maxComponentLen + len("|user:") + maxComponentLen
)

// CostFunc returns how many quota units a request consumes.
type CostFunc func(r *http.Request) int64

// Config configures the middleware.
type Config struct {
	// KeyMode selects how callers are identified.
	KeyMode KeyMode
	// TrustedProxyHops is how many reverse proxies sit in front of this service.
	//
	// Zero means ignore X-Forwarded-For entirely and use the socket peer address.
	// That is the safe default: trusting the header unconditionally lets any
	// caller pick its own rate limit key, and thereby its own quota, just by
	// varying the header. Set this to the actual number of proxies so the client
	// address can be located by counting from the right; see clientIP.
	TrustedProxyHops int
	// Cost returns the quota cost of a request. Nil means every request costs 1.
	Cost CostFunc
}

// Validate reports whether the config is usable.
func (c Config) Validate() error {
	if _, err := ParseKeyMode(string(c.KeyMode)); err != nil {
		return err
	}
	if c.TrustedProxyHops < 0 {
		return fmt.Errorf("trusted_proxy_hops must be >= 0, got %d", c.TrustedProxyHops)
	}
	return nil
}

// RateLimiter is the HTTP middleware.
type RateLimiter struct {
	limiter limiter.Limiter
	cfg     Config
	rec     *metrics.Recorder
	log     *slog.Logger
	cost    CostFunc
}

// New builds the middleware.
func New(l limiter.Limiter, cfg Config, rec *metrics.Recorder, log *slog.Logger) (*RateLimiter, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if l == nil {
		return nil, errors.New("middleware: limiter is required")
	}
	if rec == nil {
		return nil, errors.New("middleware: metrics recorder is required")
	}
	if log == nil {
		log = slog.Default()
	}
	cost := cfg.Cost
	if cost == nil {
		cost = func(*http.Request) int64 { return 1 }
	}
	return &RateLimiter{limiter: l, cfg: cfg, rec: rec, log: log, cost: cost}, nil
}

// Handler wraps next with rate limiting. Call it once at startup and reuse the
// result: building it per request allocates a closure on the hot path.
func (rl *RateLimiter) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rl.rec.InFlightAdd(1)
		defer rl.rec.InFlightAdd(-1)

		// A panic in the wrapped handler would otherwise kill the process and
		// take every in-flight request with it.
		defer rl.recoverPanic(w, r)

		key, keyType := rl.extractKey(r)
		cost := rl.cost(r)

		start := time.Now()
		res, err := rl.limiter.AllowN(r.Context(), key, cost)
		rl.rec.ObserveLatency(time.Since(start))

		if err != nil {
			rl.handleError(w, r, err, keyType)
			return
		}

		rl.setHeaders(w, res)

		if !res.Allowed {
			rl.rec.Deny(res.DeniedBy)
			rl.writeError(w, http.StatusTooManyRequests, denialMessage(res.DeniedBy))
			return
		}

		rl.rec.Allow()
		next.ServeHTTP(w, r)
	})
}

// handleError maps a limiter error onto a response.
//
// Every branch here is a deliberate status choice. The version this replaces
// called next.ServeHTTP on any error, which meant a Redis outage admitted every
// request regardless of the configured strategy — fail_closed silently became
// fail_open, on exactly the services that chose it because unmetered requests are
// dangerous.
func (rl *RateLimiter) handleError(w http.ResponseWriter, r *http.Request,
	err error, keyType string) {

	switch {
	case limiter.IsCostError(err):
		// The request asks for more quota than the limit can ever grant. 429 would
		// send the client into a retry loop that can never succeed.
		rl.rec.Deny(metrics.DeniedByInvalidCost)
		rl.writeError(w, http.StatusBadRequest,
			"request cost exceeds the maximum quota for this endpoint")

	case errors.Is(err, fallback.ErrUnavailable):
		// fail_closed with the limiter down. The caller is not over quota and its
		// own state is intact, so this is a server-side outage: 503, not 429.
		rl.rec.Deny(fallback.DeniedByUnavailable)
		w.Header().Set("Retry-After", "1")
		rl.writeError(w, http.StatusServiceUnavailable,
			"rate limiter unavailable, request rejected")

	case r.Context().Err() != nil:
		// The client went away mid-request. Nothing useful to write.
		rl.rec.Error()

	default:
		rl.rec.Error()
		rl.log.ErrorContext(r.Context(), "limiter error", "error", err, "key_type", keyType)
		rl.writeError(w, http.StatusInternalServerError, "rate limiter error")
	}
}

func (rl *RateLimiter) recoverPanic(w http.ResponseWriter, r *http.Request) {
	v := recover()
	if v == nil {
		return
	}
	// net/http uses this sentinel to abort a connection deliberately; swallowing
	// it would break that contract.
	if v == http.ErrAbortHandler {
		panic(v)
	}
	rl.rec.Panic()
	rl.log.ErrorContext(r.Context(), "recovered panic in handler",
		"panic", v, "method", r.Method, "path", r.URL.Path)
	rl.writeError(w, http.StatusInternalServerError, "internal error")
}

// setHeaders emits the rate limit headers.
func (rl *RateLimiter) setHeaders(w http.ResponseWriter, res limiter.Result) {
	h := w.Header()
	// A negative limit means "no number we can vouch for" — during a fail-open
	// degradation, for instance. Reporting a limit then would be a lie the client
	// may cache or act on.
	if res.Limit > 0 {
		h.Set("X-RateLimit-Limit", strconv.FormatInt(res.Limit, 10))
		h.Set("X-RateLimit-Remaining", strconv.FormatInt(max(res.Remaining, 0), 10))
		h.Set("X-RateLimit-Reset", strconv.Itoa(ceilSeconds(res.ResetAfter)))
	}
	if res.DeniedBy != "" {
		h.Set("X-RateLimit-Denied-By", res.DeniedBy)
	}
	if !res.Allowed {
		// RFC 9110 requires whole seconds. Round up: rounding down tells the
		// client to retry before the quota is actually available, which produces a
		// second denial and, with the penalty box enabled, another strike.
		retry := res.RetryAfter
		if retry <= 0 {
			retry = res.ResetAfter
		}
		h.Set("Retry-After", strconv.Itoa(max(ceilSeconds(retry), 1)))
	}
}

// writeError sends a JSON error body, matching the API's content type rather than
// http.Error's text/plain.
func (rl *RateLimiter) writeError(w http.ResponseWriter, status int, msg string) {
	h := w.Header()
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Error  string `json:"error"`
		Status int    `json:"status"`
	}{Error: msg, Status: status})
}

// extractKey derives the limit key and the key type label.
//
// Each client-supplied component is bounded individually rather than the finished key
// being hashed as a whole. Hashing the whole thing would destroy the "tenant:…|user:…"
// structure, so TenantOf could not recover the tenant and the chain's per-tenant tier
// would put every oversized-header caller into a single shared bucket.
func (rl *RateLimiter) extractKey(r *http.Request) (key, keyType string) {
	switch rl.cfg.KeyMode {
	case KeyByUser:
		if uid := header(r, "X-User-ID"); uid != "" {
			return "user:" + boundComponent(uid), "user"
		}
		return "ip:" + boundComponent(clientIP(r, rl.cfg.TrustedProxyHops)), "ip"

	case KeyByTenant:
		tid := header(r, "X-Tenant-ID")
		if tid == "" {
			if uid := header(r, "X-User-ID"); uid != "" {
				return "user:" + boundComponent(uid), "user"
			}
			return "ip:" + boundComponent(clientIP(r, rl.cfg.TrustedProxyHops)), "ip"
		}
		// The tenant id comes first and is delimited, so TenantOf can recover it
		// from the composite key without ambiguity.
		return "tenant:" + boundComponent(tid) +
			"|user:" + boundComponent(header(r, "X-User-ID")), "tenant"

	default:
		return "ip:" + boundComponent(clientIP(r, rl.cfg.TrustedProxyHops)), "ip"
	}
}

// TenantOf extracts the tenant id from a key built by KeyByTenant, or "" if the
// key is not tenant-scoped. Used by the chained limiter's per-tenant tier.
func TenantOf(key string) string {
	const prefix = "tenant:"
	if !strings.HasPrefix(key, prefix) {
		return ""
	}
	rest := key[len(prefix):]
	if i := strings.IndexByte(rest, '|'); i >= 0 {
		return rest[:i]
	}
	return rest
}

// header returns a trimmed header value.
func header(r *http.Request, name string) string {
	return strings.TrimSpace(r.Header.Get(name))
}

// boundComponent caps one client-supplied key component, hashing anything longer.
//
// Hashing rather than truncating keeps distinct long values distinct: truncation would
// merge every caller sharing a prefix into one quota, which is both a bypass (share
// someone else's headroom) and a denial of service (exhaust it for them).
func boundComponent(v string) string {
	if len(v) <= maxComponentLen {
		return v
	}
	sum := sha256.Sum256([]byte(v))
	return "h" + hex.EncodeToString(sum[:12])
}

// clientIP resolves the caller's address.
//
// With hops > 0 the address is located by counting from the *right* of
// X-Forwarded-For. Each proxy appends the address it received the connection
// from, so the rightmost hops entries were written by our own infrastructure and
// cannot be forged; the caller is the entry immediately to their left. Reading
// the leftmost entry instead — the common shortcut — takes whatever the client
// put there, letting it choose a fresh rate limit key per request.
func clientIP(r *http.Request, hops int) string {
	peer := peerIP(r)
	if hops <= 0 {
		return peer
	}

	xff := r.Header.Get("X-Forwarded-For")
	if xff == "" {
		return peer
	}

	parts := strings.Split(xff, ",")
	idx := len(parts) - hops
	if idx < 0 {
		// Fewer entries than expected: the chain is shorter than configured, so
		// the leftmost entry is the closest thing to the origin we can trust.
		idx = 0
	}
	if ip := net.ParseIP(strings.TrimSpace(parts[idx])); ip != nil {
		return ip.String()
	}
	return peer
}

func peerIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func denialMessage(deniedBy string) string {
	switch deniedBy {
	case "":
		return "rate limit exceeded"
	case limiter.ShedDeniedBy:
		return "service is shedding load, retry shortly"
	default:
		return "rate limit exceeded (" + deniedBy + ")"
	}
}

func ceilSeconds(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	return int(math.Ceil(d.Seconds()))
}
