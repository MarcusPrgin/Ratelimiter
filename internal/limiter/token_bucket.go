package limiter

import (
	"context"
	"math"
	"time"

	"github.com/MarcusPrgin/Ratelimiter/internal/shardmap"
)

// TokenBucket is the in-memory token bucket.
//
// Tokens accrue continuously at Limit/Window per second and accumulate up to
// BurstMax. A caller that has been idle can therefore spend a burst of up to
// BurstMax at once, then is held to the steady refill rate — the behaviour AWS
// and Stripe expose on their APIs.
//
// Refill is computed lazily from the elapsed time on each call rather than by a
// background ticker, so idle keys cost nothing and there is no per-key goroutine.
type TokenBucket struct {
	store *shardmap.Map[tbState]
	cfg   Config
	// refillPerMs is tokens gained per millisecond, precomputed.
	refillPerMs float64
}

type tbState struct {
	tokens float64
	// lastMs is when tokens was last recomputed, in epoch milliseconds.
	// Zero means "not yet initialised".
	lastMs int64
}

// NewTokenBucket builds an in-memory token bucket. cfg must have passed Validate.
func NewTokenBucket(cfg Config) *TokenBucket {
	cfg = cfg.withDefaults()
	refillPerMs := float64(cfg.Limit) / float64(cfg.Window.Milliseconds())
	// A bucket that has refilled to capacity is indistinguishable from a fresh
	// one, so it is safe to evict then. One extra window of slack keeps
	// near-full buckets around rather than churning them.
	idleTTL := time.Duration(float64(cfg.BurstMax)/refillPerMs)*time.Millisecond + cfg.Window
	return &TokenBucket{
		store:       shardmap.New[tbState](cfg.MaxKeys, idleTTL),
		cfg:         cfg,
		refillPerMs: refillPerMs,
	}
}

func (t *TokenBucket) Allow(ctx context.Context, key string) (Result, error) {
	return t.AllowN(ctx, key, 1)
}

func (t *TokenBucket) AllowN(_ context.Context, key string, n int64) (Result, error) {
	if err := checkCost(n, t.cfg.BurstMax); err != nil {
		return Result{}, err
	}

	now := time.Now()
	nowMs := now.UnixMilli()
	capacity := float64(t.cfg.BurstMax)

	var res Result
	t.store.Do(key, now,
		func() tbState { return tbState{tokens: capacity, lastMs: nowMs} },
		func(st *tbState) {
			// Refill for the time since the last call. Guard against a
			// non-monotonic wall clock stepping backwards: a negative delta
			// would subtract tokens.
			if st.lastMs == 0 {
				st.lastMs = nowMs
			}
			if delta := nowMs - st.lastMs; delta > 0 {
				st.tokens = math.Min(capacity, st.tokens+float64(delta)*t.refillPerMs)
				st.lastMs = nowMs
			} else if delta < 0 {
				st.lastMs = nowMs
			}

			need := float64(n)
			// Time for the bucket to fill completely, which is when ResetAfter
			// expires — i.e. when the caller regains its full burst allowance.
			resetAfter := t.durationFor(capacity - st.tokens)

			if st.tokens < need {
				res = Result{
					Allowed:    false,
					Limit:      t.cfg.Limit,
					Remaining:  0,
					ResetAfter: resetAfter,
					RetryAfter: t.durationFor(need - st.tokens),
				}
				return
			}

			st.tokens -= need
			res = Result{
				Allowed:   true,
				Limit:     t.cfg.Limit,
				Remaining: int64(st.tokens),
				// Recompute: consuming tokens pushes the full-refill point out.
				ResetAfter: t.durationFor(capacity - st.tokens),
			}
		})
	return res, nil
}

// durationFor returns how long it takes to accrue the given number of tokens,
// rounded up so a caller never retries a millisecond too early.
func (t *TokenBucket) durationFor(tokens float64) time.Duration {
	if tokens <= 0 {
		return 0
	}
	return time.Duration(math.Ceil(tokens/t.refillPerMs)) * time.Millisecond
}

func (t *TokenBucket) Name() string { return string(TokenBucketAlgo) }

// Keys reports the number of tracked keys, for the active-keys gauge.
func (t *TokenBucket) Keys() int { return t.store.Len() }
