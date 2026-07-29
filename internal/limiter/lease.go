package limiter

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/MarcusPrgin/Ratelimiter/internal/shardmap"
)

// LeaseConfig configures LeaseCache.
type LeaseConfig struct {
	// TTL bounds how long a lease or a cached denial may be held. It is further
	// capped per entry by the quota's own reset/retry deadline, so a lease never
	// outlives the window whose quota it draws on.
	TTL time.Duration
	// Prefetch is how many quota units beyond the current request to claim from
	// the shared limiter on a miss. Zero disables leasing and leaves only the
	// negative cache.
	//
	// Redis calls for a hot key drop by roughly Prefetch/(Prefetch+1). The cost is
	// that a key which goes idle mid-lease leaves its remaining units unspent.
	Prefetch int64
	// NegativeCache caches denials for min(TTL, RetryAfter). This is what absorbs
	// an abusive caller: once over quota, that caller is answered locally instead
	// of costing a Redis round trip per request.
	NegativeCache bool
	// MaxKeys bounds tracked keys. Zero means defaultMaxKeys.
	MaxKeys int
}

// Validate reports whether the config is usable.
func (c LeaseConfig) Validate() error {
	if c.TTL < 0 {
		return fmt.Errorf("ttl must be >= 0, got %s", c.TTL)
	}
	if c.Prefetch < 0 {
		return fmt.Errorf("prefetch must be >= 0, got %d", c.Prefetch)
	}
	if c.MaxKeys < 0 {
		return fmt.Errorf("max_keys must be >= 0, got %d", c.MaxKeys)
	}
	return nil
}

// Enabled reports whether the config does anything at all.
func (c LeaseConfig) Enabled() bool {
	return c.TTL > 0 && (c.Prefetch > 0 || c.NegativeCache)
}

// LeaseStats is a snapshot of cache effectiveness.
type LeaseStats struct {
	// Hits are requests answered locally, without touching the shared limiter.
	Hits uint64
	// Misses are requests that had to consult the shared limiter.
	Misses uint64
	// Keys is the number of tracked keys.
	Keys int
}

// HitRate returns the fraction of requests served locally, in [0, 1].
func (s LeaseStats) HitRate() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total)
}

// LeaseCache is a Limiter decorator that cuts round trips to the shared limiter
// by claiming quota in blocks and handing it out locally.
//
// It is deliberately *not* a cache of decisions. Caching the decision — the
// obvious implementation — silently breaks the limit: with a 5ms TTL and one key
// receiving 1000 rps, only ~200 of those requests ever reach Redis and the other
// ~800 are admitted having consumed nothing, so a limit of 100/s admits 1000/s.
// The error grows with per-key request rate, which is exactly the case a rate
// limiter exists to handle.
//
// Leasing inverts that. On a miss it asks the shared limiter for the current
// request *plus* Prefetch extra units, so the units handed out locally have
// already been counted centrally. The shared count is therefore never short and
// the limit is never exceeded. The residual error is unspent lease on a key that
// goes idle, which under-admits by at most Prefetch units per key per TTL — the
// safe direction, and bounded.
type LeaseCache struct {
	inner Limiter
	cfg   LeaseConfig
	name  string

	entries *shardmap.Map[leaseEntry]

	hits   atomic.Uint64
	misses atomic.Uint64
}

type leaseEntry struct {
	// units is quota already consumed in the shared limiter and not yet handed out.
	units int64
	// remaining is the headroom the shared limiter last reported for this key, used
	// to keep a prefetch from claiming quota that is nearly exhausted. Negative means
	// unknown.
	remaining int64
	// denied marks a negative entry; units is then meaningless.
	denied   bool
	limit    int64
	deniedBy string
	// Absolute deadlines, so headers recomputed on a hit count down rather than
	// repeating the value captured when the entry was written.
	expiresAt time.Time
	resetAt   time.Time
	retryAt   time.Time
}

// NewLeaseCache wraps inner. cfg.Enabled must be true; callers that do not want
// caching should use inner directly.
func NewLeaseCache(inner Limiter, cfg LeaseConfig) (*LeaseCache, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if !cfg.Enabled() {
		return nil, fmt.Errorf("lease cache: ttl=%s prefetch=%d negative_cache=%t does nothing",
			cfg.TTL, cfg.Prefetch, cfg.NegativeCache)
	}
	maxKeys := cfg.MaxKeys
	if maxKeys == 0 {
		maxKeys = defaultMaxKeys
	}
	return &LeaseCache{
		inner:   inner,
		cfg:     cfg,
		name:    inner.Name(),
		entries: shardmap.New[leaseEntry](maxKeys, cfg.TTL),
	}, nil
}

func (l *LeaseCache) Allow(ctx context.Context, key string) (Result, error) {
	return l.AllowN(ctx, key, 1)
}

func (l *LeaseCache) AllowN(ctx context.Context, key string, n int64) (Result, error) {
	if n < 1 {
		return Result{}, fmt.Errorf("%w: got %d", ErrInvalidCost, n)
	}

	now := time.Now()
	res, decided, hint := l.local(key, n, now)
	if decided {
		l.hits.Add(1)
		return res, nil
	}
	l.misses.Add(1)

	res, granted, err := l.draw(ctx, key, n, hint)
	if err != nil {
		return Result{}, err
	}
	l.store(key, n, granted, res, now)
	return res, nil
}

// drawHint is what local() learned about a key, for the miss path to size its claim by.
type drawHint struct {
	// known reports whether the key was tracked at all.
	known bool
	// remaining is the headroom the shared limiter last reported, or -1 if unknown.
	remaining int64
}

// local answers from an existing lease or negative entry. It reports whether the
// request was decided locally, plus what it learned about the key — which is what tells
// the miss path whether this key has shown it can spend a lease, and how much headroom
// is left to claim against.
func (l *LeaseCache) local(key string, n int64, now time.Time) (res Result, decided bool, hint drawHint) {
	hint.remaining = -1
	hint.known = l.entries.Update(key, now, func(e *leaseEntry) bool {
		hint.remaining = e.remaining
		if now.After(e.expiresAt) {
			return false // stale — drop it and go to the shared limiter
		}
		if e.denied {
			res = Result{
				Allowed:    false,
				Limit:      e.limit,
				DeniedBy:   e.deniedBy,
				ResetAfter: nonNegative(e.resetAt.Sub(now)),
				RetryAfter: nonNegative(e.retryAt.Sub(now)),
			}
			decided = true
			return true
		}
		if e.units >= n {
			e.units -= n
			res = Result{
				Allowed:    true,
				Limit:      e.limit,
				Remaining:  e.units,
				ResetAfter: nonNegative(e.resetAt.Sub(now)),
			}
			decided = true
			return true
		}
		// Not enough lease left to cover this request on its own. Leave the entry
		// alone and fall through to the shared limiter; store accumulates the new
		// batch onto these units rather than replacing them, so the remainder stays
		// spendable by a later, smaller request instead of being discarded.
		return true
	})
	return res, decided, hint
}

// draw claims quota from the shared limiter, returning the result and how many units
// were actually consumed there.
//
// hint.known reports whether this key was already tracked. A key's first miss claims
// only what the request needs, and prefetching begins once the key is established.
// Prefetching immediately looks cheaper but is worse under the case that matters: in a
// simultaneous burst on a cold key, every concurrent request misses before any lease
// exists, so each one claims a whole batch and there is nobody left to spend them. The
// quota is consumed centrally and stranded locally, and the burst is throttled well
// below the real limit. Warming up first costs one extra round trip per key and makes
// that case exact.
func (l *LeaseCache) draw(ctx context.Context, key string, n int64, hint drawHint) (Result, int64, error) {
	want := n
	if hint.known {
		prefetch := l.cfg.Prefetch
		// Do not batch-claim quota that is nearly gone. Close to the limit a batch
		// cannot be spent before the window turns, so claiming it strands quota that
		// another node could have used — and the boundary is exactly where accuracy
		// matters most. Shrinking the prefetch here makes the tail exact.
		if hint.remaining >= 0 && hint.remaining < prefetch {
			prefetch = hint.remaining
		}
		want = n + prefetch
	}

	res, err := l.inner.AllowN(ctx, key, want)
	if err != nil {
		// The batch may exceed the configured limit even though the bare request
		// does not. Retry without the prefetch rather than failing the request.
		if !IsCostError(err) || want == n {
			return Result{}, 0, err
		}
		res, err = l.inner.AllowN(ctx, key, n)
		if err != nil {
			return Result{}, 0, err
		}
		return res, n, nil
	}

	// A denial may be caused by the prefetch rather than the request itself.
	// Retry bare so a caller is never refused quota it actually had.
	if !res.Allowed && want > n {
		res, err = l.inner.AllowN(ctx, key, n)
		if err != nil {
			return Result{}, 0, err
		}
		return res, n, nil
	}
	return res, want, nil
}

// store records the leftover lease, or the denial, for subsequent requests.
func (l *LeaseCache) store(key string, n, granted int64, res Result, now time.Time) {
	switch {
	case res.Allowed:
		// A result with no numeric limit came from a degraded path — a fail-open
		// decision made while the shared limiter was unreachable. There is no
		// counted quota behind it, so there is nothing to lease, and caching it
		// would keep admitting requests after Redis has recovered.
		if res.Limit <= 0 {
			return
		}
		// spare may be zero — on a key's first miss, draw claims no prefetch. The
		// entry is still written, with no units, to mark the key as established so the
		// next miss does prefetch. Without that marker a key never warms up and every
		// request pays a round trip.
		spare := granted - n
		if spare < 0 {
			return
		}
		// A lease draws on a specific window's quota, so it must not outlive it.
		ttl := l.cfg.TTL
		if res.ResetAfter > 0 && res.ResetAfter < ttl {
			ttl = res.ResetAfter
		}
		if ttl <= 0 {
			return
		}

		fresh := leaseEntry{
			units:     spare,
			remaining: res.Remaining,
			limit:     res.Limit,
			expiresAt: now.Add(ttl),
			resetAt:   now.Add(res.ResetAfter),
		}
		// create returns the zero entry: Do runs fn on newly created values too, so
		// seeding it with `fresh` here would have fn add `spare` to an entry that
		// already contains it, granting twice the quota that was actually consumed.
		l.entries.Do(key, now, func() leaseEntry { return leaseEntry{} }, func(cur *leaseEntry) {
			// Accumulate rather than replace. Concurrent misses on one key each draw
			// their own batch; overwriting would discard whatever the previous lease
			// still held — quota already consumed centrally and then thrown away, so
			// the limit under-admits far more than the design intends.
			if cur.denied || cur.expiresAt.IsZero() || now.After(cur.expiresAt) {
				*cur = fresh
				return
			}
			cur.units += spare
			cur.remaining = res.Remaining
			cur.limit = res.Limit
			// Keep the earlier deadline of the two: a lease must not outlive the
			// window whose quota it draws on.
			if fresh.expiresAt.Before(cur.expiresAt) {
				cur.expiresAt = fresh.expiresAt
			}
			if fresh.resetAt.Before(cur.resetAt) {
				cur.resetAt = fresh.resetAt
			}
		})
		return

	case l.cfg.NegativeCache:
		// Never hold a denial past the moment it would have been admitted —
		// that would turn a momentary throttle into a longer outage.
		ttl := l.cfg.TTL
		if res.RetryAfter > 0 && res.RetryAfter < ttl {
			ttl = res.RetryAfter
		}
		if ttl <= 0 {
			return
		}
		// A denial replaces whatever was there: any lease is moot once the shared
		// limiter has started refusing this key.
		denial := leaseEntry{
			denied:    true,
			remaining: 0,
			limit:     res.Limit,
			deniedBy:  res.DeniedBy,
			expiresAt: now.Add(ttl),
			resetAt:   now.Add(res.ResetAfter),
			retryAt:   now.Add(res.RetryAfter),
		}
		l.entries.Do(key, now,
			func() leaseEntry { return denial },
			func(cur *leaseEntry) { *cur = denial })
	}
}

// Stats returns a snapshot for the metrics layer.
func (l *LeaseCache) Stats() LeaseStats {
	return LeaseStats{
		Hits:   l.hits.Load(),
		Misses: l.misses.Load(),
		Keys:   l.entries.Len(),
	}
}

// Name reports the wrapped limiter's name: leasing is a transport optimisation,
// not a different algorithm, so it should not fragment metric labels.
func (l *LeaseCache) Name() string { return l.name }

func nonNegative(d time.Duration) time.Duration {
	if d < 0 {
		return 0
	}
	return d
}
