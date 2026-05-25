// Package middleware provides HTTP middleware for rate limiting.
// Extracts key by IP, X-User-ID, or X-Tenant-ID, calls the limiter,
// sets X-RateLimit-* headers on every response, returns 429 when denied.
package middleware

import (
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/yourname/ratelimiter/internal/cache"
	"github.com/yourname/ratelimiter/internal/limiter"
	"github.com/yourname/ratelimiter/internal/metrics"
	"github.com/yourname/ratelimiter/internal/penalty"
)

// KeyExtractor determines the rate limit key from a request.
type KeyExtractor func(r *http.Request) (key, keyType string)

// CostFunc returns the quota cost for a request. Defaults to 1 if nil.
type CostFunc func(r *http.Request) int64

func ByIP(r *http.Request) (string, string) {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return "ip:" + xff, "ip"
	}
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	return "ip:" + ip, "ip"
}

func ByUserID(r *http.Request) (string, string) {
	uid := r.Header.Get("X-User-ID")
	if uid == "" {
		return ByIP(r)
	}
	return "user:" + uid, "user"
}

func ByTenant(r *http.Request) (string, string) {
	tid := r.Header.Get("X-Tenant-ID")
	uid := r.Header.Get("X-User-ID")
	if tid == "" {
		return ByUserID(r)
	}
	return fmt.Sprintf("tenant:%s:user:%s", tid, uid), "tenant"
}

// RateLimiter is the HTTP middleware.
type RateLimiter struct {
	limiter   limiter.Limiter
	cache     *cache.LocalCache  // nil = no local cache
	extractor KeyExtractor
	costFn    CostFunc
	penalty   *penalty.Box // nil = no penalty box
}

// New creates a rate limiter middleware.
// Pass nil for localCache to always hit Redis.
// Pass nil for penaltyBox to disable the penalty feature.
func New(l limiter.Limiter, localCache *cache.LocalCache, extractor KeyExtractor,
	costFn CostFunc, penaltyBox *penalty.Box) *RateLimiter {
	if extractor == nil {
		extractor = ByIP
	}
	if costFn == nil {
		costFn = func(_ *http.Request) int64 { return 1 }
	}
	return &RateLimiter{
		limiter:   l,
		cache:     localCache,
		extractor: extractor,
		costFn:    costFn,
		penalty:   penaltyBox,
	}
}

// Handler wraps the given http.Handler with rate limiting.
func (rl *RateLimiter) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, keyType := rl.extractor(r)
		cost := rl.costFn(r)

		// ── Penalty box check ────────────────────────────────────────────────
		// Fast Redis TTL lookup; errors are non-fatal (fail open for penalty).
		if rl.penalty != nil {
			if inPenalty, remaining, err := rl.penalty.Check(r.Context(), key); inPenalty && err == nil {
				w.Header().Set("X-RateLimit-Reason", "penalty")
				w.Header().Set("Retry-After", strconv.Itoa(int(remaining.Seconds())+1))
				http.Error(w, "rate limit exceeded (penalty box)", http.StatusTooManyRequests)
				metrics.RecordPenaltyDeny(rl.limiter.Name(), keyType)
				return
			}
		}

		// ── Local cache check ────────────────────────────────────────────────
		// Include cost in cache key so different-cost callers don't share entries.
		cacheKey := key
		if cost != 1 {
			cacheKey = fmt.Sprintf("%s:c%d", key, cost)
		}
		if rl.cache != nil {
			if cached, ok := rl.cache.Get(cacheKey); ok {
				metrics.UpdateCacheHitRatio(rl.limiter.Name(), rl.cache.HitRate())
				setHeaders(w, cached)
				if !cached.Allowed {
					http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
					metrics.RecordDeny(rl.limiter.Name(), keyType)
					return
				}
				next.ServeHTTP(w, r)
				return
			}
		}

		// ── Limiter call (Redis or in-memory) ────────────────────────────────
		start := time.Now()
		result, err := rl.limiter.AllowN(r.Context(), key, cost)
		metrics.ObserveRedis("allow", start)

		if err != nil {
			// limiter error — fail open so a Redis blip doesn't cause a 500
			next.ServeHTTP(w, r)
			return
		}

		if rl.cache != nil {
			rl.cache.Set(cacheKey, result)
		}

		setHeaders(w, result)

		if !result.Allowed {
			// Record denial in penalty box before responding.
			if rl.penalty != nil {
				rl.penalty.Record(r.Context(), key)
			}
			// Emit per-chain-tier metric when a specific tier denied.
			if result.DeniedBy != "" {
				metrics.RecordChainTierDenied(result.DeniedBy)
			}
			metrics.RecordDeny(rl.limiter.Name(), keyType)
			w.Header().Set("Retry-After", strconv.Itoa(int(result.RetryAfter.Seconds())+1))
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		metrics.RecordAllow(rl.limiter.Name(), keyType)
		next.ServeHTTP(w, r)
	})
}

func setHeaders(w http.ResponseWriter, r limiter.Result) {
	if r.Limit > 0 {
		w.Header().Set("X-RateLimit-Limit", strconv.FormatInt(r.Limit, 10))
		w.Header().Set("X-RateLimit-Remaining", strconv.FormatInt(r.Remaining, 10))
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(int64(r.ResetAfter.Seconds()), 10))
	}
	if r.DeniedBy != "" {
		w.Header().Set("X-RateLimit-Denied-By", r.DeniedBy)
	}
}
