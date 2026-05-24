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
)

// ObserveRedis records a Redis operation latency.
func ObserveRedis(operation string, start time.Time) {
	RedisLatency.WithLabelValues(operation).Observe(time.Since(start).Seconds())
}

// RecordAllow increments the allowed counter.
func RecordAllow(algorithm, keyType string) {
	RequestsAllowed.WithLabelValues(algorithm, keyType).Inc()
}

// RecordDeny increments the denied counter.
func RecordDeny(algorithm, keyType string) {
	RequestsDenied.WithLabelValues(algorithm, keyType).Inc()
}

// UpdateCacheHitRatio sets the current cache hit ratio gauge.
func UpdateCacheHitRatio(algorithm string, ratio float64) {
	CacheHitRatio.WithLabelValues(algorithm).Set(ratio / 100)
}
