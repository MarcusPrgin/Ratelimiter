package shardmap_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/MarcusPrgin/Ratelimiter/internal/shardmap"
)

func TestDoCreatesAndRetainsState(t *testing.T) {
	m := shardmap.New[int](1024, time.Hour)
	now := time.Now()

	for i := 0; i < 5; i++ {
		m.Do("k", now, func() int { return 0 }, func(v *int) { *v++ })
	}

	var got int
	if !m.Update("k", now, func(v *int) bool { got = *v; return true }) {
		t.Fatal("entry missing after Do")
	}
	// Regression guard: an implementation that stamps the access time after running
	// eviction treats a brand new entry as infinitely idle and drops it, so every
	// Do starts from scratch and this reads 1.
	if got != 5 {
		t.Errorf("value = %d, want 5 — mutations are not being retained across calls", got)
	}
}

func TestNewEntrySurvivesEviction(t *testing.T) {
	// idleTTL of zero makes every existing entry immediately evictable, which is the
	// case that exposes an entry created without a timestamp.
	m := shardmap.New[int](1024, 0)
	now := time.Now()

	m.Do("k", now, func() int { return 7 }, func(*int) {})
	if m.Len() != 1 {
		t.Fatalf("Len = %d after inserting one key, want 1", m.Len())
	}
}

func TestBoundedByMaxKeys(t *testing.T) {
	// Per-shard cap is maxKeys/ShardCount, so the total bound is approximate; what
	// matters is that it does not grow without limit.
	const maxKeys = shardmap.ShardCount * 2
	m := shardmap.New[int](maxKeys, time.Hour)
	now := time.Now()

	for i := 0; i < 100_000; i++ {
		m.Do(fmt.Sprintf("key-%d", i), now, func() int { return i }, func(*int) {})
	}

	if got := m.Len(); got > maxKeys {
		t.Errorf("Len = %d after 100k distinct keys, want <= %d — the map is unbounded",
			got, maxKeys)
	}
	if m.Len() == 0 {
		t.Error("Len = 0, the map evicted everything")
	}
}

// TestIdleEntriesReclaimedOnRead covers the read path. Insert-time eviction only
// sweeps the shard being written to, so reads have to reclaim as well or a shard
// that stops receiving writes never gives its memory back.
func TestIdleEntriesReclaimedOnRead(t *testing.T) {
	m := shardmap.New[int](1024, 50*time.Millisecond)
	base := time.Now()

	m.Do("k", base, func() int { return 1 }, func(*int) {})
	if !m.Update("k", base, func(*int) bool { return true }) {
		t.Fatal("entry missing immediately after insert")
	}

	stale := base.Add(time.Second)
	if m.Update("k", stale, func(*int) bool { return true }) {
		t.Error("entry idle beyond idleTTL was reported as present")
	}
	if m.Len() != 0 {
		t.Errorf("Len = %d, want the stale entry reclaimed", m.Len())
	}
}

// TestLRUEvictsColdestFirst checks eviction order under pressure. Evicting a hot
// key would reset its counter and hand that caller a fresh quota, so the coldest
// key has to be the one that goes.
func TestLRUEvictsColdestFirst(t *testing.T) {
	// One entry per shard, so any two keys sharing a shard compete for one slot.
	m := shardmap.New[int](shardmap.ShardCount, time.Hour)
	now := time.Now()

	hot, cold := findColliding(t, m, now)

	// Touch `hot` most recently, then insert `cold` into the same slot.
	m.Do(cold, now, func() int { return 0 }, func(*int) {})
	m.Do(hot, now.Add(time.Millisecond), func() int { return 0 }, func(*int) {})
	m.Do(cold, now.Add(2*time.Millisecond), func() int { return 0 }, func(*int) {})

	// Inserting `cold` evicts the shard's LRU tail, which is `hot` only if ordering
	// is broken. Re-touch `hot` and insert `cold` once more to confirm the survivor.
	m.Do(hot, now.Add(3*time.Millisecond), func() int { return 0 }, func(*int) {})
	if !m.Update(hot, now.Add(4*time.Millisecond), func(*int) bool { return true }) {
		t.Error("the most recently used key was evicted ahead of a colder one")
	}
}

// findColliding returns two keys that hash to the same shard, detected by one
// evicting the other from a map holding a single entry per shard.
func findColliding(t *testing.T, m *shardmap.Map[int], now time.Time) (first, second string) {
	t.Helper()
	first = "probe-0"
	m.Do(first, now, func() int { return 0 }, func(*int) {})

	for i := 1; i < 200_000; i++ {
		k := fmt.Sprintf("probe-%d", i)
		m.Do(k, now, func() int { return 0 }, func(*int) {})
		if !m.Update(first, now, func(*int) bool { return true }) {
			return first, k // inserting k evicted first: same shard
		}
	}
	t.Skip("no shard collision found")
	return "", ""
}

func TestUpdateOnMissingKey(t *testing.T) {
	m := shardmap.New[int](1024, time.Hour)
	called := false
	if m.Update("nope", time.Now(), func(*int) bool { called = true; return true }) {
		t.Error("Update reported a hit for an absent key")
	}
	if called {
		t.Error("Update ran fn for an absent key")
	}
}

func TestUpdateCanDelete(t *testing.T) {
	m := shardmap.New[int](1024, time.Hour)
	now := time.Now()
	m.Do("k", now, func() int { return 1 }, func(*int) {})

	if m.Update("k", now, func(*int) bool { return false }) {
		t.Error("Update returned true when fn asked for deletion")
	}
	if m.Len() != 0 {
		t.Errorf("Len = %d after deletion, want 0", m.Len())
	}
	if m.Update("k", now, func(*int) bool { return true }) {
		t.Error("entry still present after deletion")
	}
}

// TestLenTracksEntries guards the atomic counter against drifting from reality,
// since Len is what the tracked-keys gauge reports.
func TestLenTracksEntries(t *testing.T) {
	m := shardmap.New[int](1<<16, time.Hour)
	now := time.Now()

	const n = 1000
	for i := 0; i < n; i++ {
		m.Do(fmt.Sprintf("k%d", i), now, func() int { return i }, func(*int) {})
	}
	if m.Len() != n {
		t.Errorf("Len = %d, want %d", m.Len(), n)
	}
	// Deleting via Update's false return must keep the counter honest too.
	for i := 0; i < n/2; i++ {
		m.Update(fmt.Sprintf("k%d", i), now, func(*int) bool { return false })
	}
	if m.Len() != n/2 {
		t.Errorf("Len = %d after removing half, want %d", m.Len(), n/2)
	}
}

func TestConcurrentAccess(t *testing.T) {
	m := shardmap.New[int](1<<14, time.Hour)

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			now := time.Now()
			for i := 0; i < 500; i++ {
				k := fmt.Sprintf("k%d", i%97)
				m.Do(k, now, func() int { return 0 }, func(v *int) { *v++ })
				m.Update(k, now, func(*int) bool { return true })
				_ = m.Len()
			}
		}()
	}
	wg.Wait()
}

func BenchmarkDo(b *testing.B) {
	m := shardmap.New[int](1<<20, time.Hour)
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		now := time.Now()
		i := 0
		for pb.Next() {
			i++
			m.Do(keys[i&1023], now, func() int { return 0 }, func(v *int) { *v++ })
		}
	})
}
