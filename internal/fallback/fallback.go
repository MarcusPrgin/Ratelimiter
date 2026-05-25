// Package fallback defines what happens when the primary limiter (Redis) is unavailable.
package fallback

import (
	"context"
	"errors"
	"log/slog"

	"github.com/yourname/ratelimiter/internal/limiter"
)

// Strategy controls behaviour when the primary limiter fails.
type Strategy string

const (
	// FailOpen allows all requests when the primary limiter is unavailable.
	FailOpen Strategy = "fail_open"
	// FailClosed denies all requests when the primary limiter is unavailable.
	FailClosed Strategy = "fail_closed"
	// LocalFallback uses the in-memory limiter when the primary is down.
	// Each node enforces independently — N nodes = N× the effective limit.
	LocalFallback Strategy = "local_fallback"
)

// Handler wraps a primary Limiter with a configurable fallback strategy.
type Handler struct {
	primary  limiter.Limiter
	local    limiter.Limiter
	strategy Strategy
}

func New(primary, local limiter.Limiter, strategy Strategy) *Handler {
	return &Handler{primary: primary, local: local, strategy: strategy}
}

func (h *Handler) Allow(ctx context.Context, key string) (limiter.Result, error) {
	return h.AllowN(ctx, key, 1)
}

func (h *Handler) AllowN(ctx context.Context, key string, n int64) (limiter.Result, error) {
	result, err := h.primary.AllowN(ctx, key, n)
	if err == nil {
		return result, nil
	}

	slog.Warn("primary limiter failed, applying fallback",
		"strategy", h.strategy, "error", err)

	switch h.strategy {
	case FailOpen:
		return limiter.Result{Allowed: true, Limit: -1, Remaining: -1}, nil
	case FailClosed:
		return limiter.Result{Allowed: false, Limit: -1}, errors.New("rate limiter unavailable")
	case LocalFallback:
		return h.local.AllowN(ctx, key, n)
	default:
		return limiter.Result{Allowed: true}, nil
	}
}

func (h *Handler) Name() string { return h.primary.Name() + "_with_fallback" }
