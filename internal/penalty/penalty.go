// Package penalty implements an exponential-backoff penalty box.
//
// A caller that keeps hammering a limit after being denied is not going to be
// stopped by more 429s — it costs the service a Redis round trip per request
// either way. The penalty box escalates: once a key accumulates Threshold denials
// inside StrikeWindow it is blocked outright for BasePenalty, and each subsequent
// offence doubles that up to MaxPenalty.
package penalty

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/MarcusPrgin/Ratelimiter/internal/limiter"
	"github.com/MarcusPrgin/Ratelimiter/internal/shardmap"
)

//go:embed penalty.lua
var recordScript string

// DeniedBy is the Result.DeniedBy value set on a penalty-box denial. The value is
// owned by the limiter package, which defines what Result.DeniedBy may contain and
// which names a chain tier may therefore not reuse.
const DeniedBy = limiter.PenaltyDeniedBy

// Client is the subset of a Redis client the penalty box needs. Both
// *redis.Client and *redis.ClusterClient satisfy it.
type Client interface {
	redis.Scripter
	PTTL(ctx context.Context, key string) *redis.DurationCmd
}

// Config controls penalty box behaviour.
type Config struct {
	// Threshold is how many denials inside StrikeWindow trigger a penalty.
	Threshold int64
	// StrikeWindow is the fixed window in which strikes accumulate.
	StrikeWindow time.Duration
	// BasePenalty is the duration of a first offence.
	BasePenalty time.Duration
	// MaxPenalty caps escalation.
	MaxPenalty time.Duration
	// CheckInterval bounds how often one key's penalty state is re-read from
	// Redis.
	//
	// Without it, enforcing a shared penalty costs a Redis round trip on every
	// request, including the overwhelming majority from well-behaved callers that
	// have no penalty at all. Caching the "not penalised" answer for
	// CheckInterval means a penalty applied on one node takes up to that long to
	// be honoured by the others — immaterial against a BasePenalty measured in
	// tens of seconds, and it takes the common path from one round trip to zero.
	CheckInterval time.Duration
	// MaxKeys bounds locally tracked keys.
	MaxKeys int
}

func DefaultConfig() Config {
	return Config{
		Threshold:     10,
		StrikeWindow:  time.Minute,
		BasePenalty:   30 * time.Second,
		MaxPenalty:    time.Hour,
		CheckInterval: time.Second,
		MaxKeys:       1 << 20,
	}
}

// Validate reports whether the config is usable.
func (c Config) Validate() error {
	if c.Threshold < 1 {
		return fmt.Errorf("threshold must be >= 1, got %d", c.Threshold)
	}
	if c.StrikeWindow <= 0 {
		return fmt.Errorf("strike_window must be > 0, got %s", c.StrikeWindow)
	}
	// Penalties are handed to Redis as integer millisecond TTLs, so anything shorter
	// truncates to zero. PEXPIRE rejects a zero TTL outright, which fails the whole
	// escalation script — the penalty box would count strikes and then never apply a
	// penalty, logging a warning per offence.
	if c.BasePenalty < time.Millisecond {
		return fmt.Errorf("base_penalty must be >= %s, got %s", time.Millisecond, c.BasePenalty)
	}
	if c.MaxPenalty < c.BasePenalty {
		return fmt.Errorf("max_penalty (%s) must be >= base_penalty (%s)",
			c.MaxPenalty, c.BasePenalty)
	}
	if c.CheckInterval < 0 {
		return fmt.Errorf("check_interval must be >= 0, got %s", c.CheckInterval)
	}
	if c.MaxKeys < 0 {
		return fmt.Errorf("max_keys must be >= 0, got %d", c.MaxKeys)
	}
	return nil
}

// Stats is a snapshot for the metrics layer.
type Stats struct {
	// Denied is how many requests the box has blocked.
	Denied uint64
	// Escalations is how many times a key has entered or re-entered a penalty.
	Escalations uint64
	// Keys is the number of locally tracked keys.
	Keys int
}

// Box is a Limiter decorator that blocks keys in penalty before they reach the
// wrapped limiter.
type Box struct {
	inner  limiter.Limiter
	rdb    Client
	cfg    Config
	script *redis.Script
	log    *slog.Logger

	state *shardmap.Map[keyState]

	denied      atomic.Uint64
	escalations atomic.Uint64
}

type keyState struct {
	// penalisedUntil is when the active penalty expires; zero if none.
	penalisedUntil time.Time
	// checkedAt is when Redis was last consulted for this key.
	checkedAt time.Time
	// strikes counts denials seen by this node in the current local window.
	strikes int64
	// windowEnd closes the local strike window.
	windowEnd time.Time
}

// New wraps inner with a penalty box.
func New(inner limiter.Limiter, rdb Client, cfg Config, log *slog.Logger) (*Box, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if inner == nil {
		return nil, errors.New("penalty: inner limiter is required")
	}
	if rdb == nil {
		return nil, errors.New("penalty: redis client is required")
	}
	if log == nil {
		log = slog.Default()
	}
	maxKeys := cfg.MaxKeys
	if maxKeys == 0 {
		maxKeys = 1 << 20
	}
	return &Box{
		inner:  inner,
		rdb:    rdb,
		cfg:    cfg,
		script: redis.NewScript(recordScript),
		log:    log,
		// Local state matters only while a penalty or strike window is live.
		state: shardmap.New[keyState](maxKeys, cfg.MaxPenalty+cfg.StrikeWindow),
	}, nil
}

func (b *Box) Allow(ctx context.Context, key string) (limiter.Result, error) {
	return b.AllowN(ctx, key, 1)
}

func (b *Box) AllowN(ctx context.Context, key string, n int64) (limiter.Result, error) {
	if until, ok := b.penalised(ctx, key); ok {
		b.denied.Add(1)
		remaining := time.Until(until)
		if remaining < 0 {
			remaining = 0
		}
		return limiter.Result{
			Allowed:    false,
			Limit:      limiter.LimitUnknown,
			DeniedBy:   DeniedBy,
			ResetAfter: remaining,
			RetryAfter: remaining,
		}, nil
	}

	res, err := b.inner.AllowN(ctx, key, n)
	if err != nil {
		return res, err
	}
	// Only denials the caller is responsible for count as strikes. Load shedding and
	// limiter outages are the service's condition, not the caller's: counting them
	// would march well-behaved callers into the penalty box during an overload, and
	// then keep them out for minutes after it passed.
	if !res.Allowed && limiter.CallerAttributable(res.DeniedBy) {
		b.recordStrike(ctx, key)
	}
	return res, nil
}

// penalised reports whether key is currently blocked, consulting Redis at most
// once per CheckInterval.
func (b *Box) penalised(ctx context.Context, key string) (time.Time, bool) {
	now := time.Now()

	var (
		until   time.Time
		blocked bool
		known   bool
	)
	b.state.Update(key, now, func(st *keyState) bool {
		if !st.penalisedUntil.IsZero() {
			if now.Before(st.penalisedUntil) {
				until, blocked, known = st.penalisedUntil, true, true
				return true
			}
			st.penalisedUntil = time.Time{} // expired
		}
		if b.cfg.CheckInterval > 0 && now.Sub(st.checkedAt) < b.cfg.CheckInterval {
			known = true // recently confirmed clear
		}
		return true
	})
	if known {
		return until, blocked
	}

	// Cache miss or stale: ask Redis.
	ttl, err := b.rdb.PTTL(ctx, b.baseKey(key)+":p").Result()
	if err != nil {
		// A Redis problem must not block traffic. The limiter itself already has
		// a configured failure strategy; the penalty box defers to it rather than
		// imposing a second, contradictory one.
		b.log.WarnContext(ctx, "penalty lookup failed, treating key as clear",
			"error", err)
		return time.Time{}, false
	}

	if ttl > 0 {
		until = now.Add(ttl)
		blocked = true
	}
	b.remember(key, now, until)
	return until, blocked
}

// remember records the outcome of a Redis lookup.
func (b *Box) remember(key string, now time.Time, until time.Time) {
	fresh := keyState{penalisedUntil: until, checkedAt: now}
	b.state.Do(key, now, func() keyState { return fresh }, func(st *keyState) {
		st.penalisedUntil = until
		st.checkedAt = now
	})
}

// recordStrike counts a denial locally and escalates through Redis once this
// node has seen Threshold denials inside StrikeWindow.
//
// Strikes are counted per node rather than per round trip to Redis: writing every
// denial to Redis would cost an extra command on exactly the traffic the box
// exists to shed. The trade-off is that an offender spread evenly across N nodes
// needs up to N×Threshold denials to trip. Once tripped, the penalty is shared, so
// escalation is global even though detection is local.
func (b *Box) recordStrike(ctx context.Context, key string) {
	now := time.Now()

	var trip bool
	// create returns the zero state: Do runs fn on newly created values too, so
	// pre-seeding strikes here would count the first denial twice and trip the
	// threshold one strike early.
	b.state.Do(key, now,
		func() keyState { return keyState{} },
		func(st *keyState) {
			// A tumbling window: the deadline only moves once it has actually passed.
			// Extending it on every strike would make the window slide, so it never
			// closes while traffic continues and any caller that stays busy long
			// enough trips the threshold however sparse its denials are.
			if st.windowEnd.IsZero() || now.After(st.windowEnd) {
				st.strikes = 0
				st.windowEnd = now.Add(b.cfg.StrikeWindow)
			}
			st.strikes++
			if st.strikes >= b.cfg.Threshold {
				trip = true
				st.strikes = 0
				st.windowEnd = now.Add(b.cfg.StrikeWindow)
			}
		})
	if !trip {
		return
	}

	vals, err := b.script.Run(ctx, b.rdb,
		[]string{b.baseKey(key)},
		b.cfg.BasePenalty.Milliseconds(),
		b.cfg.MaxPenalty.Milliseconds(),
	).Int64Slice()
	if err != nil {
		b.log.WarnContext(ctx, "penalty escalation failed", "error", err)
		return
	}
	if len(vals) < 2 {
		return
	}

	penalty := time.Duration(vals[0]) * time.Millisecond
	b.escalations.Add(1)
	b.remember(key, now, now.Add(penalty))
	b.log.InfoContext(ctx, "key entered penalty box",
		"penalty", penalty, "offence", vals[1])
}

// Stats returns a snapshot for the metrics layer.
func (b *Box) Stats() Stats {
	return Stats{
		Denied:      b.denied.Load(),
		Escalations: b.escalations.Load(),
		Keys:        b.state.Len(),
	}
}

// Name reports the wrapped limiter's name; the penalty box is a gate in front of
// an algorithm, not an algorithm of its own.
func (b *Box) Name() string { return b.inner.Name() }

// baseKey returns the hash-tagged base key the Lua script derives sub-keys from.
func (b *Box) baseKey(key string) string { return "pen:{" + key + "}" }
