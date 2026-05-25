// Package cache provides a lightweight in-memory TTL cache that sits in front
// of Redis. When a key is in cache, we skip the Redis round trip entirely.
// TTL is configurable — the tradeoff is accuracy vs latency (see README).
package cache

import (
	"context"
	"sync"
	"sync/atomic"
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
	hits    atomic.Int64
	misses  atomic.Int64
}

// New creates a LocalCache whose background eviction goroutine runs until ctx is cancelled.
func New(ctx context.Context, ttl time.Duration) *LocalCache {
	c := &LocalCache{
		entries: make(map[string]*entry),
		ttl:     ttl,
	}
	go c.evictLoop(ctx)
	return c
}

// Get returns the cached result for key, if present and not expired.
func (c *LocalCache) Get(key string) (limiter.Result, bool) {
	c.mu.RLock()
	e, ok := c.entries[key]
	c.mu.RUnlock()

	if !ok || time.Now().After(e.expiresAt) {
		c.misses.Add(1)
		return limiter.Result{}, false
	}

	c.hits.Add(1)
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
	hits := c.hits.Load()
	misses := c.misses.Load()
	total := hits + misses
	if total == 0 {
		return 0
	}
	return float64(hits) / float64(total) * 100
}

func (c *LocalCache) evictLoop(ctx context.Context) {
	ticker := time.NewTicker(c.ttl)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
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
}
