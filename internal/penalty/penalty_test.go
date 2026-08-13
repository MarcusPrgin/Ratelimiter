package penalty_test

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/MarcusPrgin/Ratelimiter/internal/limiter"
	"github.com/MarcusPrgin/Ratelimiter/internal/penalty"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newRedis(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	return c, mr
}

// switchLimiter allows or denies on command, so a test can drive the strike count
// without depending on a real limiter's timing.
type switchLimiter struct {
	deny  atomic.Bool
	calls atomic.Int64
}

func (s *switchLimiter) Allow(ctx context.Context, key string) (limiter.Result, error) {
	return s.AllowN(ctx, key, 1)
}

func (s *switchLimiter) AllowN(context.Context, string, int64) (limiter.Result, error) {
	s.calls.Add(1)
	if s.deny.Load() {
		return limiter.Result{
			Allowed: false, Limit: 10, ResetAfter: time.Second, RetryAfter: time.Second,
		}, nil
	}
	return limiter.Result{Allowed: true, Limit: 10, Remaining: 9}, nil
}

func (s *switchLimiter) Name() string { return "switch" }

func testConfig() penalty.Config {
	c := penalty.DefaultConfig()
	c.Threshold = 3
	c.StrikeWindow = time.Minute
	c.BasePenalty = 200 * time.Millisecond
	c.MaxPenalty = 2 * time.Second
	// Always consult Redis, so tests are not affected by the local check cache.
	c.CheckInterval = 0
	return c
}

func newBox(t *testing.T, inner limiter.Limiter, rdb penalty.Client, cfg penalty.Config) *penalty.Box {
	t.Helper()
	b, err := penalty.New(inner, rdb, cfg, quietLogger())
	if err != nil {
		t.Fatalf("penalty.New: %v", err)
	}
	return b
}

func TestBelowThresholdNoPenalty(t *testing.T) {
	ctx := context.Background()
	rdb, _ := newRedis(t)
	inner := &switchLimiter{}
	box := newBox(t, inner, rdb, testConfig())

	inner.deny.Store(true)
	// Two denials with a threshold of 3 must not escalate.
	for i := 0; i < 2; i++ {
		if _, err := box.Allow(ctx, "k"); err != nil {
			t.Fatal(err)
		}
	}

	inner.deny.Store(false)
	r, err := box.Allow(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if !r.Allowed {
		t.Errorf("denied by %q below the threshold", r.DeniedBy)
	}
	if box.Stats().Escalations != 0 {
		t.Errorf("Escalations = %d, want 0", box.Stats().Escalations)
	}
}

// TestThresholdCountsExactly guards the off-by-one that pre-seeding the strike
// counter introduces: with the first denial counted twice, a threshold of 3 trips
// after 2 denials.
func TestThresholdCountsExactly(t *testing.T) {
	ctx := context.Background()
	rdb, _ := newRedis(t)
	inner := &switchLimiter{}
	cfg := testConfig() // Threshold 3
	box := newBox(t, inner, rdb, cfg)

	inner.deny.Store(true)
	for i := 0; i < 2; i++ {
		if _, err := box.Allow(ctx, "k"); err != nil {
			t.Fatal(err)
		}
	}
	if got := box.Stats().Escalations; got != 0 {
		t.Fatalf("escalated after 2 denials with threshold 3 (Escalations=%d)", got)
	}

	if _, err := box.Allow(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if got := box.Stats().Escalations; got != 1 {
		t.Errorf("Escalations = %d after exactly 3 denials, want 1", got)
	}
}

func TestPenalisedKeyIsBlockedWithoutCallingInner(t *testing.T) {
	ctx := context.Background()
	rdb, _ := newRedis(t)
	inner := &switchLimiter{}
	box := newBox(t, inner, rdb, testConfig())

	inner.deny.Store(true)
	for i := 0; i < 3; i++ {
		if _, err := box.Allow(ctx, "k"); err != nil {
			t.Fatal(err)
		}
	}

	// Even with the limiter now admitting, the penalty must hold.
	inner.deny.Store(false)
	before := inner.calls.Load()

	r, err := box.Allow(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if r.Allowed {
		t.Fatal("penalised key was admitted")
	}
	if r.DeniedBy != penalty.DeniedBy {
		t.Errorf("DeniedBy = %q, want %q", r.DeniedBy, penalty.DeniedBy)
	}
	if r.RetryAfter <= 0 {
		t.Error("penalty denial must report how long to wait")
	}
	if inner.calls.Load() != before {
		t.Error("penalised request still reached the wrapped limiter — the box should " +
			"refuse it before it costs a round trip")
	}
	if box.Stats().Denied == 0 {
		t.Error("Denied stat not recorded")
	}
}

// TestNonAttributableDenialsDoNotCountAsStrikes is the fairness rule.
//
// Load shedding and limiter outages are the service's condition, not the caller's.
// Counting them as strikes marches well-behaved callers into the penalty box during an
// overload — and then keeps them locked out for minutes after it has passed, which is
// the opposite of what shedding is for.
func TestNonAttributableDenialsDoNotCountAsStrikes(t *testing.T) {
	ctx := context.Background()

	for _, deniedBy := range []string{limiter.ShedDeniedBy, limiter.UnavailableDeniedBy} {
		t.Run(deniedBy, func(t *testing.T) {
			rdb, _ := newRedis(t)
			inner := &fixedDenyLimiter{deniedBy: deniedBy}
			cfg := testConfig() // Threshold 3
			box := newBox(t, inner, rdb, cfg)

			// Far more denials than the threshold.
			for i := 0; i < 50; i++ {
				r, err := box.Allow(ctx, "innocent")
				if err != nil {
					t.Fatal(err)
				}
				if r.DeniedBy != deniedBy {
					t.Fatalf("request %d: DeniedBy = %q, want %q", i, r.DeniedBy, deniedBy)
				}
			}

			if got := box.Stats().Escalations; got != 0 {
				t.Errorf("Escalations = %d after 50 %s denials, want 0 — the caller is "+
					"being penalised for the service's own condition", got, deniedBy)
			}
			if got := box.Stats().Denied; got != 0 {
				t.Errorf("Denied = %d, want 0: the box never blocked this caller", got)
			}
		})
	}
}

// TestQuotaDenialsStillCountAsStrikes is the counterpart, so the attribution rule cannot
// be satisfied by simply never striking.
func TestQuotaDenialsStillCountAsStrikes(t *testing.T) {
	ctx := context.Background()
	rdb, _ := newRedis(t)

	// An empty DeniedBy is an ordinary over-quota denial, and a chain tier name is
	// still the caller exceeding a limit that applies to it.
	for _, deniedBy := range []string{"", "per_key", "per_tenant"} {
		t.Run("denied_by="+deniedBy, func(t *testing.T) {
			inner := &fixedDenyLimiter{deniedBy: deniedBy}
			box := newBox(t, inner, rdb, testConfig()) // Threshold 3

			for i := 0; i < 3; i++ {
				if _, err := box.Allow(ctx, "abuser-"+deniedBy); err != nil {
					t.Fatal(err)
				}
			}
			if got := box.Stats().Escalations; got != 1 {
				t.Errorf("Escalations = %d after 3 quota denials, want 1", got)
			}
		})
	}
}

// fixedDenyLimiter always denies with a fixed DeniedBy value.
type fixedDenyLimiter struct {
	deniedBy string
	calls    atomic.Int64
}

func (f *fixedDenyLimiter) Allow(ctx context.Context, key string) (limiter.Result, error) {
	return f.AllowN(ctx, key, 1)
}

func (f *fixedDenyLimiter) AllowN(context.Context, string, int64) (limiter.Result, error) {
	f.calls.Add(1)
	return limiter.Result{
		Allowed:    false,
		Limit:      limiter.LimitUnknown,
		DeniedBy:   f.deniedBy,
		ResetAfter: time.Second,
		RetryAfter: time.Second,
	}, nil
}

func (f *fixedDenyLimiter) Name() string { return "fixed-deny" }

func TestPenaltyExpires(t *testing.T) {
	ctx := context.Background()
	rdb, mr := newRedis(t)
	inner := &switchLimiter{}
	cfg := testConfig()
	cfg.BasePenalty = 100 * time.Millisecond
	box := newBox(t, inner, rdb, cfg)

	inner.deny.Store(true)
	for i := 0; i < 3; i++ {
		_, _ = box.Allow(ctx, "k")
	}
	inner.deny.Store(false)

	if r, _ := box.Allow(ctx, "k"); r.Allowed {
		t.Fatal("expected the key to be in penalty")
	}

	// The box tracks the deadline locally against the real clock, while miniredis
	// only ages keys when told to, so both clocks have to move.
	time.Sleep(150 * time.Millisecond)
	mr.FastForward(150 * time.Millisecond)

	r, err := box.Allow(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if !r.Allowed {
		t.Error("penalty did not expire")
	}
}

func TestPenaltyKeysAreIsolated(t *testing.T) {
	ctx := context.Background()
	rdb, _ := newRedis(t)
	inner := &switchLimiter{}
	box := newBox(t, inner, rdb, testConfig())

	inner.deny.Store(true)
	for i := 0; i < 3; i++ {
		_, _ = box.Allow(ctx, "abuser")
	}
	inner.deny.Store(false)

	if r, _ := box.Allow(ctx, "abuser"); r.Allowed {
		t.Error("abuser was not penalised")
	}
	if r, _ := box.Allow(ctx, "innocent"); !r.Allowed {
		t.Error("an unrelated key was caught by another key's penalty")
	}
}

// TestEscalationDoublesPenalty checks the backoff actually escalates rather than
// re-applying the base penalty.
func TestEscalationDoublesPenalty(t *testing.T) {
	ctx := context.Background()
	rdb, mr := newRedis(t)
	inner := &switchLimiter{}
	cfg := testConfig()
	cfg.BasePenalty = 100 * time.Millisecond
	cfg.MaxPenalty = 10 * time.Second
	box := newBox(t, inner, rdb, cfg)
	inner.deny.Store(true)

	trip := func() time.Duration {
		for i := 0; i < 3; i++ {
			_, _ = box.Allow(ctx, "k")
		}
		ttl, err := rdb.PTTL(ctx, "pen:{k}:p").Result()
		if err != nil {
			t.Fatalf("PTTL: %v", err)
		}
		return ttl
	}

	first := trip()
	if first <= 0 {
		t.Fatalf("first penalty TTL = %s, want > 0", first)
	}

	// Wait out the first penalty, which is what a repeat offender does. The offence
	// count has a much longer TTL, so it survives and the next penalty escalates.
	time.Sleep(first + 50*time.Millisecond)
	mr.FastForward(first + 50*time.Millisecond)

	second := trip()
	if second < first*2 {
		t.Errorf("second penalty %s, want at least double the first %s", second, first)
	}
}

// TestEscalationClampsWithoutOverflow covers the arithmetic bug: base × 2^n
// overflows a double, and the original expression produced a negative duration,
// which Redis rejects as an invalid TTL. Seeding a high escalation count reaches
// that regime directly.
func TestEscalationClampsWithoutOverflow(t *testing.T) {
	ctx := context.Background()
	rdb, _ := newRedis(t)
	inner := &switchLimiter{}
	cfg := testConfig()
	cfg.BasePenalty = time.Second
	cfg.MaxPenalty = 5 * time.Second
	box := newBox(t, inner, rdb, cfg)

	// Pretend this key has already offended 1000 times.
	if err := rdb.Set(ctx, "pen:{k}:n", 1000, time.Hour).Err(); err != nil {
		t.Fatal(err)
	}

	inner.deny.Store(true)
	for i := 0; i < 3; i++ {
		if _, err := box.Allow(ctx, "k"); err != nil {
			t.Fatalf("escalation at a high offence count failed: %v", err)
		}
	}

	ttl, err := rdb.PTTL(ctx, "pen:{k}:p").Result()
	if err != nil {
		t.Fatalf("PTTL: %v — a non-finite penalty was written as the TTL", err)
	}
	if ttl <= 0 {
		t.Fatalf("penalty TTL = %s, want a positive duration clamped to max", ttl)
	}
	if ttl > cfg.MaxPenalty {
		t.Errorf("penalty TTL = %s, want <= max_penalty %s", ttl, cfg.MaxPenalty)
	}
}

// TestStrikeWindowIsTumblingNotSliding covers the window bug. If the window's
// deadline is pushed out by every strike, it never closes while traffic continues,
// so a caller making sparse, well-spaced denials still accumulates strikes forever
// and eventually trips the threshold it never actually exceeded.
//
// Threshold is 3 here. Strikes are spread so that no window ever holds 3, which
// means a correct tumbling window never escalates.
func TestStrikeWindowIsTumblingNotSliding(t *testing.T) {
	ctx := context.Background()
	rdb, _ := newRedis(t)
	inner := &switchLimiter{}
	cfg := testConfig() // Threshold 3
	cfg.StrikeWindow = 100 * time.Millisecond
	box := newBox(t, inner, rdb, cfg)

	inner.deny.Store(true)
	// Two strikes per window, then let the window lapse. A sliding window would
	// carry all six forward and escalate; a tumbling one resets to zero each time.
	for round := 0; round < 3; round++ {
		_, _ = box.Allow(ctx, "k")
		_, _ = box.Allow(ctx, "k")
		time.Sleep(cfg.StrikeWindow + 40*time.Millisecond)
	}

	if got := box.Stats().Escalations; got != 0 {
		t.Errorf("Escalations = %d after 6 denials spread 2 per window with "+
			"threshold 3 — strikes are carrying across windows", got)
	}
}

// TestStrikesAccumulateWithinOneWindow is the counterpart: inside a single window,
// strikes must add up and trip the threshold.
func TestStrikesAccumulateWithinOneWindow(t *testing.T) {
	ctx := context.Background()
	rdb, _ := newRedis(t)
	inner := &switchLimiter{}
	cfg := testConfig() // Threshold 3
	cfg.StrikeWindow = 10 * time.Second
	box := newBox(t, inner, rdb, cfg)

	inner.deny.Store(true)
	for i := 0; i < 3; i++ {
		_, _ = box.Allow(ctx, "k")
	}
	if got := box.Stats().Escalations; got != 1 {
		t.Errorf("Escalations = %d after 3 denials in one window, want 1", got)
	}
}

// TestRedisFailureDoesNotBlockTraffic checks the box defers to the limiter's own
// failure strategy rather than imposing a second, contradictory one.
func TestRedisFailureDoesNotBlockTraffic(t *testing.T) {
	ctx := context.Background()
	rdb, mr := newRedis(t)
	inner := &switchLimiter{}
	box := newBox(t, inner, rdb, testConfig())

	mr.Close() // Redis is now unreachable

	r, err := box.Allow(ctx, "k")
	if err != nil {
		t.Fatalf("penalty lookup failure surfaced as an error: %v", err)
	}
	if !r.Allowed {
		t.Error("a Redis outage caused the penalty box to block traffic")
	}
}

// TestCheckIntervalAvoidsRoundTrips covers the efficiency of the common path: a key
// with no penalty should not cost a Redis lookup on every request.
func TestCheckIntervalAvoidsRoundTrips(t *testing.T) {
	ctx := context.Background()
	rdb, _ := newRedis(t)
	inner := &switchLimiter{}
	cfg := testConfig()
	cfg.CheckInterval = time.Minute
	box := newBox(t, inner, rdb, cfg)

	for i := 0; i < 100; i++ {
		if r, err := box.Allow(ctx, "k"); err != nil || !r.Allowed {
			t.Fatalf("request %d: allowed=%t err=%v", i, r.Allowed, err)
		}
	}
	if inner.calls.Load() != 100 {
		t.Errorf("wrapped limiter called %d times, want 100", inner.calls.Load())
	}
}

func TestConfigValidation(t *testing.T) {
	base := penalty.DefaultConfig()
	mutate := func(f func(*penalty.Config)) penalty.Config {
		c := base
		f(&c)
		return c
	}

	tests := []struct {
		name string
		cfg  penalty.Config
		ok   bool
	}{
		{"default", base, true},
		{"zero threshold", mutate(func(c *penalty.Config) { c.Threshold = 0 }), false},
		{"zero strike window", mutate(func(c *penalty.Config) { c.StrikeWindow = 0 }), false},
		{"zero base penalty", mutate(func(c *penalty.Config) { c.BasePenalty = 0 }), false},
		// Penalties reach Redis as integer millisecond TTLs, so a sub-millisecond one
		// truncates to zero and PEXPIRE rejects it — the box would count strikes and
		// then fail every escalation, never actually penalising anyone.
		{"sub-millisecond base penalty", mutate(func(c *penalty.Config) {
			c.BasePenalty = 500 * time.Microsecond
		}), false},
		{"max below base", mutate(func(c *penalty.Config) {
			c.BasePenalty, c.MaxPenalty = time.Minute, time.Second
		}), false},
		{"negative check interval", mutate(func(c *penalty.Config) {
			c.CheckInterval = -time.Second
		}), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.ok && err != nil {
				t.Errorf("=> %v, want ok", err)
			}
			if !tc.ok && err == nil {
				t.Error("=> ok, want error")
			}
		})
	}
}

func TestNewRejectsMissingDependencies(t *testing.T) {
	rdb, _ := newRedis(t)
	inner := &switchLimiter{}
	cfg := penalty.DefaultConfig()

	if _, err := penalty.New(nil, rdb, cfg, quietLogger()); err == nil {
		t.Error("nil inner limiter accepted")
	}
	if _, err := penalty.New(inner, nil, cfg, quietLogger()); err == nil {
		t.Error("nil redis client accepted")
	}
}
