// Package cache provides a lightweight in-memory TTL cache that sits in front
// of Redis. When a key is in cache, we skip the Redis round trip entirely.
// TTL is configurable — the tradeoff is accuracy vs latency (see README).
package cache

import (
	"sync"
	"time"

	"github.com/yourname/ratelimiter/internal/limiter"
)

type entry struct {
	result    limiter.Result
	expiresAt time.Time
}

// LocalCache is a thread-safe in-memory cache for rate limit results.
// Keys expire after TTL, after which the next call goes to Redis.
type LocalCache struct {
	mu      sync.RWMutex
	entries map[string]*entry
	ttl     time.Duration

	// metrics — read these from outside to populate Prometheus gauges
	Hits   int64
	Misses int64
}

func New(ttl time.Duration) *LocalCache {
	c := &LocalCache{
		entries: make(map[string]*entry),
		ttl:     ttl,
	}
	// background goroutine evicts expired entries every TTL period
	go c.evictLoop()
	return c
}

// Get returns the cached result for key, if present and not expired.
func (c *LocalCache) Get(key string) (limiter.Result, bool) {
	c.mu.RLock()
	e, ok := c.entries[key]
	c.mu.RUnlock()

	if !ok || time.Now().After(e.expiresAt) {
		c.mu.Lock()
		c.Misses++
		c.mu.Unlock()
		return limiter.Result{}, false
	}

	c.mu.Lock()
	c.Hits++
	c.mu.Unlock()
	return e.result, true
}

// Set stores a result for key with the configured TTL.
func (c *LocalCache) Set(key string, result limiter.Result) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = &entry{
		result:    result,
		expiresAt: time.Now().Add(c.ttl),
	}
}

// HitRate returns cache hit percentage. Safe to call from multiple goroutines.
func (c *LocalCache) HitRate() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	total := c.Hits + c.Misses
	if total == 0 {
		return 0
	}
	return float64(c.Hits) / float64(total) * 100
}

func (c *LocalCache) evictLoop() {
	ticker := time.NewTicker(c.ttl)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		c.mu.Lock()
		for k, e := range c.entries {
			if now.After(e.expiresAt) {
				delete(c.entries, k)
			}
		}
		c.mu.Unlock()
	}
}
