// Package fallback defines what happens when Redis is unavailable.
// This is the question every interviewer will ask — have a clear answer.
package fallback

import (
	"context"
	"errors"
	"log/slog"

	"github.com/yourname/ratelimiter/internal/limiter"
)

// Strategy controls behaviour when Redis is down.
type Strategy string

const (
	// FailOpen allows all requests when Redis is unavailable.
	// Use for: search APIs, recommendations — availability > accuracy.
	FailOpen Strategy = "fail_open"

	// FailClosed denies all requests when Redis is unavailable.
	// Use for: payment APIs, auth — safety > availability.
	FailClosed Strategy = "fail_closed"

	// LocalFallback uses the in-memory limiter when Redis is down.
	// Best of both worlds, but each node enforces independently —
	// N nodes = N× the effective limit during outage.
	LocalFallback Strategy = "local_fallback"
)

// Handler wraps a primary Limiter with a fallback strategy.
type Handler struct {
	primary  limiter.Limiter
	local    limiter.Limiter // used for LocalFallback
	strategy Strategy
}

func New(primary, local limiter.Limiter, strategy Strategy) *Handler {
	return &Handler{
		primary:  primary,
		local:    local,
		strategy: strategy,
	}
}

func (h *Handler) Allow(ctx context.Context, key string) (limiter.Result, error) {
	result, err := h.primary.Allow(ctx, key)
	if err == nil {
		return result, nil
	}

	// Redis is down — apply fallback strategy
	slog.Warn("primary limiter failed, applying fallback",
		"strategy", h.strategy,
		"error", err,
	)

	switch h.strategy {
	case FailOpen:
		return limiter.Result{Allowed: true, Limit: -1, Remaining: -1}, nil

	case FailClosed:
		return limiter.Result{
			Allowed: false,
			Limit:   -1,
			Remaining: 0,
		}, errors.New("rate limiter unavailable")

	case LocalFallback:
		return h.local.Allow(ctx, key)

	default:
		return limiter.Result{Allowed: true}, nil
	}
}

func (h *Handler) Name() string {
	return h.primary.Name() + "_with_fallback"
}
