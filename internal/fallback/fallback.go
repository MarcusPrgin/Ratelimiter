// Package fallback decides what happens when the shared limiter is unavailable.
package fallback

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/MarcusPrgin/Ratelimiter/internal/limiter"
)

// Strategy controls behaviour when the primary limiter fails.
type Strategy string

const (
	// FailOpen admits everything while the primary is unavailable. Correct when
	// rejecting real traffic costs more than briefly serving over the limit —
	// search, recommendations, content APIs.
	FailOpen Strategy = "fail_open"
	// FailClosed rejects everything while the primary is unavailable. Correct when
	// an unmetered request is dangerous — payments, auth, anything that spends
	// money or grants access.
	FailClosed Strategy = "fail_closed"
	// LocalFallback has each node enforce the limit from its own memory. Keeps
	// enforcement roughly proportional, at N nodes = N× the intended limit.
	LocalFallback Strategy = "local_fallback"
)

// DeniedByUnavailable is reported when a request is refused because the limiter itself
// is down rather than because the caller is over quota. The value is owned by the
// limiter package, which defines what Result.DeniedBy may contain.
const DeniedByUnavailable = limiter.UnavailableDeniedBy

// ErrUnavailable is returned under FailClosed so the caller can answer 503
// instead of 429: the caller did nothing wrong and its quota is intact.
var ErrUnavailable = errors.New("fallback: rate limiter unavailable")

// ParseStrategy validates a strategy name from configuration.
//
// An unrecognised value must be a startup error, never a default. The failure
// mode of guessing here is the worst one available: a typo in a payment service's
// config silently turning fail_closed into allow-everything.
func ParseStrategy(s string) (Strategy, error) {
	switch Strategy(s) {
	case FailOpen, FailClosed, LocalFallback:
		return Strategy(s), nil
	default:
		return "", fmt.Errorf("unknown fallback strategy %q (want %q, %q or %q)",
			s, FailOpen, FailClosed, LocalFallback)
	}
}

// Config configures the handler.
type Config struct {
	Strategy Strategy
	// BreakerThreshold is how many consecutive primary failures open the circuit.
	// Zero disables the breaker.
	BreakerThreshold int64
	// BreakerCooldown is how long the circuit stays open before a single probe is
	// allowed through.
	BreakerCooldown time.Duration
}

func DefaultConfig() Config {
	return Config{
		Strategy:         FailOpen,
		BreakerThreshold: 5,
		BreakerCooldown:  2 * time.Second,
	}
}

// Validate reports whether the config is usable.
func (c Config) Validate() error {
	if _, err := ParseStrategy(string(c.Strategy)); err != nil {
		return err
	}
	if c.BreakerThreshold < 0 {
		return fmt.Errorf("breaker_threshold must be >= 0, got %d", c.BreakerThreshold)
	}
	if c.BreakerThreshold > 0 && c.BreakerCooldown <= 0 {
		return fmt.Errorf("breaker_cooldown must be > 0 when the breaker is enabled, got %s",
			c.BreakerCooldown)
	}
	return nil
}

// Stats is a snapshot for the metrics layer.
type Stats struct {
	// Degraded is how many requests were served by the fallback strategy.
	Degraded uint64
	// Open reports whether the circuit is currently open.
	Open bool
}

// Handler wraps a primary Limiter with a failure strategy and a circuit breaker.
type Handler struct {
	primary  limiter.Limiter
	local    limiter.Limiter
	strategy Strategy
	breaker  *breaker
	log      *slog.Logger

	degraded atomic.Uint64
}

// New builds a Handler. local is required only for LocalFallback.
func New(primary, local limiter.Limiter, cfg Config, log *slog.Logger) (*Handler, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if primary == nil {
		return nil, errors.New("fallback: primary limiter is required")
	}
	if cfg.Strategy == LocalFallback && local == nil {
		return nil, errors.New("fallback: local limiter is required for local_fallback")
	}
	if log == nil {
		log = slog.Default()
	}
	return &Handler{
		primary:  primary,
		local:    local,
		strategy: cfg.Strategy,
		breaker:  newBreaker(cfg.BreakerThreshold, cfg.BreakerCooldown),
		log:      log,
	}, nil
}

func (h *Handler) Allow(ctx context.Context, key string) (limiter.Result, error) {
	return h.AllowN(ctx, key, 1)
}

// AllowN consults the primary limiter and applies the configured strategy if it
// fails.
//
// A handled backend failure returns a decision with a nil error. Returning both a
// decision and an error invites the caller to check only the error and admit the
// request, which is how fail_closed silently becomes fail_open.
func (h *Handler) AllowN(ctx context.Context, key string, n int64) (limiter.Result, error) {
	nowMs := time.Now().UnixMilli()

	if !h.breaker.allow(nowMs) {
		// Circuit open: skip the primary entirely. Without this every request
		// waits out the full Redis timeout before falling back, so an outage in a
		// dependency that is supposed to be bypassed instead adds its timeout to
		// every response.
		return h.degrade(ctx, key, n, nil)
	}

	res, err := h.primary.AllowN(ctx, key, n)
	if err == nil {
		h.breaker.success()
		return res, nil
	}

	// A cost the limit can never grant is the caller's error, not an outage.
	if limiter.IsCostError(err) {
		h.breaker.success()
		return limiter.Result{}, err
	}
	// A cancelled or expired request context is the caller going away. Counting it
	// as a backend failure would trip the breaker during a client-side timeout
	// storm and degrade everyone else.
	if ctx.Err() != nil {
		return limiter.Result{}, err
	}

	h.breaker.failure(nowMs)
	return h.degrade(ctx, key, n, err)
}

// degrade applies the configured strategy. cause is nil when the circuit was
// already open, so there is no fresh error to report.
func (h *Handler) degrade(ctx context.Context, key string, n int64, cause error) (limiter.Result, error) {
	h.degraded.Add(1)
	if cause != nil {
		h.log.WarnContext(ctx, "primary limiter failed, applying fallback",
			"strategy", string(h.strategy), "error", cause)
	}

	switch h.strategy {
	case FailOpen:
		return limiter.Result{
			Allowed:   true,
			Limit:     limiter.LimitUnknown,
			Remaining: limiter.LimitUnknown,
		}, nil

	case FailClosed:
		return limiter.Result{
			Allowed:  false,
			Limit:    limiter.LimitUnknown,
			DeniedBy: DeniedByUnavailable,
		}, ErrUnavailable

	case LocalFallback:
		return h.local.AllowN(ctx, key, n)
	}

	// Unreachable: New validates the strategy. Fail closed rather than inventing a
	// permissive default for a value that cannot occur.
	return limiter.Result{Allowed: false, Limit: limiter.LimitUnknown},
		fmt.Errorf("fallback: unhandled strategy %q", h.strategy)
}

// Stats returns a snapshot for the metrics layer.
func (h *Handler) Stats() Stats {
	return Stats{
		Degraded: h.degraded.Load(),
		Open:     h.breaker.isOpen(time.Now().UnixMilli()),
	}
}

func (h *Handler) Name() string { return h.primary.Name() }

// ── Circuit breaker ──────────────────────────────────────────────────────────

// breaker trips after threshold consecutive failures and, once tripped, lets a
// single probe through per cooldown to test recovery.
type breaker struct {
	threshold  int64
	cooldownMs int64

	failures atomic.Int64
	// openUntilMs is zero when the circuit is closed.
	openUntilMs atomic.Int64
}

func newBreaker(threshold int64, cooldown time.Duration) *breaker {
	return &breaker{threshold: threshold, cooldownMs: cooldown.Milliseconds()}
}

// allow reports whether the primary should be consulted.
func (b *breaker) allow(nowMs int64) bool {
	if b.threshold <= 0 {
		return true
	}
	until := b.openUntilMs.Load()
	if until == 0 {
		return true
	}
	if nowMs < until {
		return false
	}
	// Half-open. The CAS lets exactly one caller per cooldown probe the primary,
	// so recovery is tested without a thundering herd against a struggling Redis.
	return b.openUntilMs.CompareAndSwap(until, nowMs+b.cooldownMs)
}

func (b *breaker) success() {
	if b.threshold <= 0 {
		return
	}
	if b.failures.Load() != 0 {
		b.failures.Store(0)
	}
	if b.openUntilMs.Load() != 0 {
		b.openUntilMs.Store(0)
	}
}

func (b *breaker) failure(nowMs int64) {
	if b.threshold <= 0 {
		return
	}
	if b.failures.Add(1) >= b.threshold {
		b.openUntilMs.Store(nowMs + b.cooldownMs)
	}
}

func (b *breaker) isOpen(nowMs int64) bool {
	until := b.openUntilMs.Load()
	return until != 0 && nowMs < until
}
