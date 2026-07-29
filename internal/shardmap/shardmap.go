// Package shardmap provides a bounded, sharded key→state map with LRU eviction.
//
// It backs both the in-memory limiters and the local decision cache, which have
// the same two requirements that a plain `map` guarded by one mutex fails:
//
//   - Throughput. A single mutex serialises every request in the process. Keys
//     are independent of one another, so sharding by key hash spreads the
//     contention across ShardCount independent locks.
//   - Bounded memory. An unbounded map is a denial-of-service vector: IP-keyed
//     traffic from a large botnet grows it until the process is OOM-killed. Each
//     shard caps its entry count and evicts its least-recently-used keys.
//
// Eviction is only safe if the caller picks idleTTL as the point at which an
// entry stops affecting behaviour — two windows for a sliding window counter, a
// full refill for a token bucket. Evicting sooner would hand a key fresh quota,
// so idleTTL is what stops LRU eviction from becoming a limit bypass.
package shardmap

import (
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"
)

// ShardCount must be a power of two so the reduction is a mask rather than a
// division.
const ShardCount = 256

// Map is a bounded, sharded map. It is safe for concurrent use.
type Map[V any] struct {
	seed        maphash.Seed
	shards      [ShardCount]shard[V]
	maxPerShard int
	idleTTL     time.Duration
	// entries tracks the live count so Len is O(1) rather than locking every
	// shard — it is read once per metrics scrape, but locking 256 mutexes to
	// answer it would briefly stall the hot path.
	entries atomic.Int64
}

type shard[V any] struct {
	mu      sync.Mutex
	entries map[string]*slot[V]
	// Intrusive LRU list, most-recently-used at head.
	head, tail *slot[V]
}

type slot[V any] struct {
	key        string
	val        V
	lastAccess time.Time
	prev, next *slot[V]
}

// New builds a Map holding at most maxKeys entries (distributed across shards),
// evicting entries idle for longer than idleTTL.
func New[V any](maxKeys int, idleTTL time.Duration) *Map[V] {
	perShard := maxKeys / ShardCount
	if perShard < 1 {
		perShard = 1
	}
	m := &Map[V]{
		seed:        maphash.MakeSeed(),
		maxPerShard: perShard,
		idleTTL:     idleTTL,
	}
	for i := range m.shards {
		m.shards[i].entries = make(map[string]*slot[V])
	}
	return m
}

// Do runs fn against the state for key while holding that key's shard lock,
// creating it with create if absent. All mutation happens inside fn, so callers
// never need their own lock — and cannot accidentally hold a reference to the
// state after the lock is released.
//
// fn always runs, including on a value that create just returned. An accumulating
// fn must therefore treat create's result as an empty starting point, not as a
// value already carrying this call's contribution — otherwise the first call for a
// key counts twice. The safe pattern is for create to return the zero value and
// for fn to hold all the logic.
func (m *Map[V]) Do(key string, now time.Time, create func() V, fn func(*V)) {
	sh := m.shardFor(key)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, ok := sh.entries[key]
	if !ok {
		// lastAccess must be stamped before evict runs. A zero timestamp reads as
		// infinitely idle, so the entry just created would be the first thing
		// evicted — leaving the map permanently empty and every caller looking
		// like a brand new key with a full quota.
		e = &slot[V]{key: key, val: create(), lastAccess: now}
		sh.entries[key] = e
		sh.pushFront(e)
		m.entries.Add(1)
		m.entries.Add(int64(-sh.evict(m.maxPerShard, m.idleTTL, now)))
	} else {
		sh.moveToFront(e)
		e.lastAccess = now
	}

	fn(&e.val)
}

// Update runs fn against an existing entry and reports whether one was found.
// It never creates an entry. fn returns false to delete the entry — used by the
// cache to drop a hit that turned out to be expired.
//
// An entry idle past idleTTL is treated as absent and removed. Insert-time
// eviction only sweeps the shard being written to, so a shard receiving no writes
// would otherwise hold its entries indefinitely; making reads self-healing means
// stale state is reclaimed by either access path without a background sweeper.
func (m *Map[V]) Update(key string, now time.Time, fn func(*V) bool) bool {
	sh := m.shardFor(key)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, ok := sh.entries[key]
	if !ok {
		return false
	}
	if now.Sub(e.lastAccess) > m.idleTTL {
		sh.remove(e)
		m.entries.Add(-1)
		return false
	}
	if !fn(&e.val) {
		sh.remove(e)
		m.entries.Add(-1)
		return false
	}
	e.lastAccess = now
	sh.moveToFront(e)
	return true
}

// Len reports the number of live entries.
func (m *Map[V]) Len() int { return int(m.entries.Load()) }

func (m *Map[V]) shardFor(key string) *shard[V] {
	return &m.shards[maphash.String(m.seed, key)&(ShardCount-1)]
}

// evict drops entries from the LRU tail — first anything idle past idleTTL
// (lossless, since that state no longer affects decisions), then, if the shard is
// still over cap, the coldest keys regardless of age. It returns how many were
// removed.
//
// Called with the shard lock held and only on insert, so the cost is amortised
// across inserts instead of being paid by a periodic full scan. A periodic sweep
// is what the naive version does, and at a 5ms TTL that is 200 full-map scans a
// second, each holding the lock for the length of the map.
func (sh *shard[V]) evict(maxEntries int, idleTTL time.Duration, now time.Time) int {
	removed := 0
	for sh.tail != nil && now.Sub(sh.tail.lastAccess) > idleTTL {
		sh.remove(sh.tail)
		removed++
	}
	for len(sh.entries) > maxEntries && sh.tail != nil {
		sh.remove(sh.tail)
		removed++
	}
	return removed
}

func (sh *shard[V]) pushFront(e *slot[V]) {
	e.prev, e.next = nil, sh.head
	if sh.head != nil {
		sh.head.prev = e
	}
	sh.head = e
	if sh.tail == nil {
		sh.tail = e
	}
}

func (sh *shard[V]) moveToFront(e *slot[V]) {
	if sh.head == e {
		return
	}
	sh.unlink(e)
	sh.pushFront(e)
}

func (sh *shard[V]) remove(e *slot[V]) {
	sh.unlink(e)
	delete(sh.entries, e.key)
}

func (sh *shard[V]) unlink(e *slot[V]) {
	if e.prev != nil {
		e.prev.next = e.next
	} else if sh.head == e {
		sh.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else if sh.tail == e {
		sh.tail = e.prev
	}
	e.prev, e.next = nil, nil
}
