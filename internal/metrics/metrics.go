// Package metrics registers and exposes Prometheus metrics.
//
// Two conventions here are deliberate.
//
// First, label children are resolved once at startup rather than per request.
// WithLabelValues hashes the label set and takes a read lock on every call; on the
// hot path that is pure overhead when the labels — algorithm, key type — are fixed
// for the process lifetime.
//
// Second, anything derived from live component state is registered as a
// CounterFunc or GaugeFunc rather than pushed from a background ticker. The value
// is then computed at scrape time, so it cannot go stale, drift from the source,
// or keep a goroutine alive for the life of the process.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	// DeniedByQuota is the denied_by label for an ordinary over-quota denial, as
	// opposed to a penalty, a load shed, or a backend outage.
	DeniedByQuota = "quota"
	// DeniedByInvalidCost labels a request rejected because its declared cost can
	// never fit the limit. It is a client error, not a throttle, and is kept
	// separate so it cannot be mistaken for capacity pressure on a dashboard.
	DeniedByInvalidCost = "invalid_cost"
	// deniedByOther catches any reason not registered at startup.
	deniedByOther = "other"
)

// Recorder holds pre-resolved metric children for the request path.
type Recorder struct {
	allowed prometheus.Counter
	// denied is keyed by denied_by. Populated at construction and never written
	// to afterwards, so concurrent reads need no lock.
	denied      map[string]prometheus.Counter
	deniedOther prometheus.Counter

	latency  prometheus.Observer
	inFlight prometheus.Gauge
	panics   prometheus.Counter
	errors   prometheus.Counter
}

// NewRecorder registers the request-path metrics.
//
// deniedBy must list every reason this deployment can produce — chain tier names,
// plus the built-in penalty/shed/unavailable reasons. Pre-resolving them keeps the
// hot path allocation-free; anything unlisted still lands in an "other" bucket
// rather than being dropped.
func NewRecorder(reg prometheus.Registerer, algorithm, keyType string, deniedBy []string) *Recorder {
	f := promauto.With(reg)
	labels := prometheus.Labels{"algorithm": algorithm, "key_type": keyType}

	allowedVec := f.NewCounterVec(prometheus.CounterOpts{
		Name: "ratelimiter_requests_allowed_total",
		Help: "Requests admitted by the rate limiter.",
	}, []string{"algorithm", "key_type"})

	deniedVec := f.NewCounterVec(prometheus.CounterOpts{
		Name: "ratelimiter_requests_denied_total",
		Help: "Requests refused by the rate limiter, labelled by what refused them.",
	}, []string{"algorithm", "key_type", "denied_by"})

	latencyVec := f.NewHistogramVec(prometheus.HistogramOpts{
		Name: "ratelimiter_limiter_latency_seconds",
		Help: "Time spent in the limiter chain, including any Redis round trip.",
		// Sub-millisecond buckets matter: a local lease hit should be ~1µs and a
		// Redis round trip ~1ms, and default buckets cannot tell them apart.
		Buckets: []float64{
			.000_01, .000_05, .000_1, .000_5, .001, .002, .005, .01, .025, .05, .1, .25, 1,
		},
	}, []string{"algorithm"})

	r := &Recorder{
		allowed: allowedVec.With(labels),
		denied:  make(map[string]prometheus.Counter, len(deniedBy)+1),
		latency: latencyVec.WithLabelValues(algorithm),
		inFlight: f.NewGauge(prometheus.GaugeOpts{
			Name: "ratelimiter_requests_in_flight",
			Help: "Requests currently inside the rate limiting middleware.",
		}),
		panics: f.NewCounter(prometheus.CounterOpts{
			Name: "ratelimiter_handler_panics_total",
			Help: "Panics recovered by the rate limiting middleware.",
		}),
		errors: f.NewCounter(prometheus.CounterOpts{
			Name: "ratelimiter_limiter_errors_total",
			Help: "Limiter calls that returned an error the fallback did not handle.",
		}),
	}

	for _, reason := range append([]string{DeniedByQuota, DeniedByInvalidCost, deniedByOther}, deniedBy...) {
		if _, ok := r.denied[reason]; ok {
			continue
		}
		r.denied[reason] = deniedVec.With(prometheus.Labels{
			"algorithm": algorithm, "key_type": keyType, "denied_by": reason,
		})
	}
	r.deniedOther = r.denied[deniedByOther]

	return r
}

// Allow records an admitted request.
func (r *Recorder) Allow() { r.allowed.Inc() }

// Deny records a refused request. reason is Result.DeniedBy, or empty for an
// ordinary over-quota denial.
func (r *Recorder) Deny(reason string) {
	if reason == "" {
		reason = DeniedByQuota
	}
	if c, ok := r.denied[reason]; ok {
		c.Inc()
		return
	}
	// An unregistered reason is a wiring bug, not a reason to lose the count.
	r.deniedOther.Inc()
}

// ObserveLatency records how long the limiter chain took.
func (r *Recorder) ObserveLatency(d time.Duration) { r.latency.Observe(d.Seconds()) }

// InFlightAdd adjusts the in-flight gauge.
func (r *Recorder) InFlightAdd(delta float64) { r.inFlight.Add(delta) }

// Panic records a recovered panic.
func (r *Recorder) Panic() { r.panics.Inc() }

// Error records an unhandled limiter error.
func (r *Recorder) Error() { r.errors.Inc() }

// Sources supplies live component state to pull-based collectors. Every field is
// optional; nil fields are skipped, so a deployment with the adaptive limiter or
// penalty box disabled simply does not export those series.
type Sources struct {
	// Counters — must be monotonically increasing.
	AdaptiveShed       func() float64
	PenaltyDenied      func() float64
	PenaltyEscalations func() float64
	LeaseHits          func() float64
	LeaseMisses        func() float64
	Degraded           func() float64

	// Gauges.
	AdaptiveMultiplier func() float64
	AdaptiveLatencyMs  func() float64
	LeaseHitRatio      func() float64
	BreakerOpen        func() float64
	// TrackedKeys reports in-memory key cardinality per component, so the bounded
	// maps can be watched approaching their cap.
	TrackedKeys map[string]func() float64
}

// RegisterSources wires pull-based collectors to live component state.
func RegisterSources(reg prometheus.Registerer, algorithm string, s Sources) {
	f := promauto.With(reg)
	labels := prometheus.Labels{"algorithm": algorithm}

	counter := func(name, help string, fn func() float64) {
		if fn == nil {
			return
		}
		f.NewCounterFunc(prometheus.CounterOpts{
			Name: name, Help: help, ConstLabels: labels,
		}, fn)
	}
	gauge := func(name, help string, fn func() float64) {
		if fn == nil {
			return
		}
		f.NewGaugeFunc(prometheus.GaugeOpts{
			Name: name, Help: help, ConstLabels: labels,
		}, fn)
	}

	counter("ratelimiter_adaptive_shed_total",
		"Requests dropped by adaptive load shedding.", s.AdaptiveShed)
	counter("ratelimiter_penalty_denied_total",
		"Requests refused because the key is in the penalty box.", s.PenaltyDenied)
	counter("ratelimiter_penalty_escalations_total",
		"Times a key entered or re-entered the penalty box.", s.PenaltyEscalations)
	counter("ratelimiter_lease_hits_total",
		"Requests answered from a local quota lease or cached denial.", s.LeaseHits)
	counter("ratelimiter_lease_misses_total",
		"Requests that had to consult the shared limiter.", s.LeaseMisses)
	counter("ratelimiter_degraded_total",
		"Requests served by the fallback strategy while the primary was unavailable.",
		s.Degraded)

	gauge("ratelimiter_adaptive_multiplier",
		"Adaptive pass-through fraction; 1.0 means no shedding.", s.AdaptiveMultiplier)
	gauge("ratelimiter_adaptive_latency_ms",
		"Smoothed limiter latency driving the adaptive controller, in milliseconds.",
		s.AdaptiveLatencyMs)
	gauge("ratelimiter_lease_hit_ratio",
		"Fraction of requests answered locally, 0-1.", s.LeaseHitRatio)
	gauge("ratelimiter_breaker_open",
		"1 while the primary-limiter circuit breaker is open.", s.BreakerOpen)

	// GaugeVec has no Func variant, so each component gets its own collector,
	// distinguished by a const label so they still form one metric family.
	for component, fn := range s.TrackedKeys {
		if fn == nil {
			continue
		}
		f.NewGaugeFunc(prometheus.GaugeOpts{
			Name:        "ratelimiter_tracked_keys",
			Help:        "Keys held in a bounded in-memory map, by component.",
			ConstLabels: prometheus.Labels{"algorithm": algorithm, "component": component},
		}, fn)
	}
}
