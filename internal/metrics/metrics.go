// Package metrics registers and exposes Prometheus metrics.
// Labels: algorithm (which limiter), key_type (ip|user|tenant).
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	RequestsAllowed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ratelimiter_requests_allowed_total",
		Help: "Total requests allowed through the rate limiter.",
	}, []string{"algorithm", "key_type"})

	RequestsDenied = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ratelimiter_requests_denied_total",
		Help: "Total requests denied by the rate limiter.",
	}, []string{"algorithm", "key_type"})

	RedisLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ratelimiter_redis_latency_seconds",
		Help:    "Redis round-trip latency in seconds.",
		Buckets: []float64{.0001, .0005, .001, .005, .01, .025, .05, .1, .25},
	}, []string{"operation"})

	CacheHitRatio = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ratelimiter_cache_hit_ratio",
		Help: "Ratio of local cache hits to total lookups (0–1).",
	}, []string{"algorithm"})

	ActiveKeys = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ratelimiter_active_keys",
		Help: "Number of currently tracked rate limit keys.",
	}, []string{"algorithm"})

	// AdaptiveMultiplier tracks the current pass-through fraction (0.1–1.0).
	// A value below 1.0 means the adaptive limiter is shedding load.
	AdaptiveMultiplier = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ratelimiter_adaptive_multiplier",
		Help: "Current adaptive pass-through multiplier (1.0 = no shedding).",
	}, []string{"algorithm"})

	// AdaptiveShed counts requests rejected by the adaptive shedding logic
	// (distinct from normal rate-limit denials).
	AdaptiveShed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ratelimiter_adaptive_shed_total",
		Help: "Requests dropped by adaptive load shedding.",
	}, []string{"algorithm"})

	// PenaltyDenied counts requests blocked by the penalty box.
	PenaltyDenied = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ratelimiter_penalty_denied_total",
		Help: "Requests denied because the key is in the penalty box.",
	}, []string{"algorithm", "key_type"})

	// ChainTierDenied counts per-tier denials in a ChainedLimiter.
	ChainTierDenied = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ratelimiter_chain_tier_denied_total",
		Help: "Requests denied by a specific chain tier.",
	}, []string{"tier"})
)

func ObserveRedis(operation string, start time.Time) {
	RedisLatency.WithLabelValues(operation).Observe(time.Since(start).Seconds())
}

func RecordAllow(algorithm, keyType string) {
	RequestsAllowed.WithLabelValues(algorithm, keyType).Inc()
}

func RecordDeny(algorithm, keyType string) {
	RequestsDenied.WithLabelValues(algorithm, keyType).Inc()
}

func UpdateCacheHitRatio(algorithm string, ratio float64) {
	CacheHitRatio.WithLabelValues(algorithm).Set(ratio / 100)
}

func RecordAdaptiveShed(algorithm string) {
	AdaptiveShed.WithLabelValues(algorithm).Inc()
}

func RecordPenaltyDeny(algorithm, keyType string) {
	PenaltyDenied.WithLabelValues(algorithm, keyType).Inc()
}

func RecordChainTierDenied(tier string) {
	ChainTierDenied.WithLabelValues(tier).Inc()
}
