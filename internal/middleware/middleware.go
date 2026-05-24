// Package middleware provides HTTP middleware for rate limiting.
// Extracts key by IP or X-User-ID header, calls the limiter,
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
)

// KeyExtractor determines the rate limit key from a request.
type KeyExtractor func(r *http.Request) (key, keyType string)

// ByIP extracts the client IP address as the key.
func ByIP(r *http.Request) (string, string) {
	// honour X-Forwarded-For when behind a proxy
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return "ip:" + xff, "ip"
	}
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	return "ip:" + ip, "ip"
}

// ByUserID extracts the X-User-ID header as the key.
func ByUserID(r *http.Request) (string, string) {
	uid := r.Header.Get("X-User-ID")
	if uid == "" {
		return ByIP(r) // fall back to IP
	}
	return "user:" + uid, "user"
}

// ByTenant extracts X-Tenant-ID for multi-tenant rate limiting.
func ByTenant(r *http.Request) (string, string) {
	tid := r.Header.Get("X-Tenant-ID")
	uid := r.Header.Get("X-User-ID")
	if tid == "" {
		return ByUserID(r)
	}
	return fmt.Sprintf("tenant:%s:user:%s", tid, uid), "tenant"
}

// RateLimiter is the middleware handler.
type RateLimiter struct {
	limiter   limiter.Limiter
	cache     *cache.LocalCache // nil = no local cache
	extractor KeyExtractor
}

// New creates a new rate limiter middleware.
// Pass nil for localCache to skip caching and always hit Redis.
func New(l limiter.Limiter, localCache *cache.LocalCache, extractor KeyExtractor) *RateLimiter {
	if extractor == nil {
		extractor = ByIP
	}
	return &RateLimiter{
		limiter:   l,
		cache:     localCache,
		extractor: extractor,
	}
}

// Handler wraps the given http.Handler with rate limiting.
func (rl *RateLimiter) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, keyType := rl.extractor(r)

		var result limiter.Result
		var err error

		// check local cache first
		if rl.cache != nil {
			if cached, ok := rl.cache.Get(key); ok {
				result = cached
				metrics.UpdateCacheHitRatio(rl.limiter.Name(), rl.cache.HitRate())
				setHeaders(w, result)
				if !result.Allowed {
					http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
					metrics.RecordDeny(rl.limiter.Name(), keyType)
					return
				}
				next.ServeHTTP(w, r)
				return
			}
		}

		// cache miss — go to Redis
		start := time.Now()
		result, err = rl.limiter.Allow(r.Context(), key)
		metrics.ObserveRedis("allow", start)

		if err != nil {
			// limiter error — apply fail-open by default here
			// (production: check fallback.Handler instead)
			next.ServeHTTP(w, r)
			return
		}

		// populate cache for next TTL period
		if rl.cache != nil {
			rl.cache.Set(key, result)
		}

		setHeaders(w, result)

		if !result.Allowed {
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
}
