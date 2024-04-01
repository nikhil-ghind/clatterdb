// Package memtable implements an in-memory sorted buffer for recent writes.
//
// The memtable is the write-side of the LSM tree. All writes go to the active
// memtable (after WAL). When the memtable exceeds a size threshold, it becomes
// immutable and is flushed to an SSTable on disk.
//
// Implementation uses a skip list for O(log n) inserts and ordered iteration.
// Data is organized by series key, then by timestamp within each series.
package memtable

import (
	"math/rand"
	"sync"
	"sync/atomic"

	"github.com/clatterdb/internal/storage"
)

const (
	maxLevel    = 16
	probability = 0.25
	// DefaultMaxSize is the threshold at which the memtable should be flushed.
	DefaultMaxSize = 4 * 1024 * 1024 // 4 MB
)

// skipListNode is a node in the skip list, keyed by (seriesKey, timestamp).
type skipListNode struct {
	sample storage.Sample
	next   []*skipListNode
}

func newNode(sample storage.Sample, level int) *skipListNode {
	return &skipListNode{
		sample: sample,
		next:   make([]*skipListNode, level),
	}
}

// Memtable is a write-optimized in-memory buffer backed by a skip list.
type Memtable struct {
	mu       sync.RWMutex
	head     *skipListNode
	level    int
	size     atomic.Int64
	count    atomic.Int64
	immutable bool
}

func New() *Memtable {
	return &Memtable{
		head:  newNode(storage.Sample{}, maxLevel),
		level: 1,
	}
}

// Insert adds a sample to the memtable. Returns false if the memtable is immutable.
func (m *Memtable) Insert(sample storage.Sample) bool {
	if m.immutable {
		return false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := sample.Series.Key()
	ts := sample.DataPoint.Timestamp

	update := make([]*skipListNode, maxLevel)
	current := m.head

	for i := m.level - 1; i >= 0; i-- {
		for current.next[i] != nil && m.less(current.next[i].sample, key, ts) {
			current = current.next[i]
		}
		update[i] = current
	}

	// Check for duplicate (same key + timestamp) — overwrite
	if next := current.next[0]; next != nil {
		nextKey := next.sample.Series.Key()
		if nextKey == key && next.sample.DataPoint.Timestamp == ts {
			next.sample = sample
			return true
		}
	}

	newLevel := m.randomLevel()
	if newLevel > m.level {
		for i := m.level; i < newLevel; i++ {
			update[i] = m.head
		}
		m.level = newLevel
	}

	node := newNode(sample, newLevel)
	for i := 0; i < newLevel; i++ {
		node.next[i] = update[i].next[i]
		update[i].next[i] = node
	}

	// Approximate size tracking
	sampleSize := int64(len(key) + 8 + 8 + 8) // key + ts + val + txn
	m.size.Add(sampleSize)
	m.count.Add(1)

	return true
}

// Query returns all data points for a series within the time range.
func (m *Memtable) Query(seriesKey string, tr storage.TimeRange) []storage.DataPoint {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var results []storage.DataPoint
	current := m.head

	// Skip to the first node with matching key and ts >= tr.Start
	for i := m.level - 1; i >= 0; i-- {
		for current.next[i] != nil && m.less(current.next[i].sample, seriesKey, tr.Start) {
			current = current.next[i]
		}
	}

	// Iterate forward collecting matches
	current = current.next[0]
	for current != nil {
		key := current.sample.Series.Key()
		ts := current.sample.DataPoint.Timestamp

		if key > seriesKey {
			break
		}
		if key == seriesKey && ts >= tr.Start && ts <= tr.End && !current.sample.IsDeleted {
			results = append(results, current.sample.DataPoint)
		}
		if key == seriesKey && ts > tr.End {
			break
		}
		current = current.next[0]
	}

	return results
}

// Iterator returns all samples in sorted order (for flushing to SSTable).
func (m *Memtable) Iterator() []storage.Sample {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var samples []storage.Sample
	current := m.head.next[0]
	for current != nil {
		samples = append(samples, current.sample)
		current = current.next[0]
	}
	return samples
}

// Freeze makes the memtable immutable (no more writes accepted).
func (m *Memtable) Freeze() {
	m.immutable = true
}

// IsFrozen returns whether the memtable is immutable.
func (m *Memtable) IsFrozen() bool {
	return m.immutable
}

// Size returns the approximate size of the memtable in bytes.
func (m *Memtable) Size() int64 {
	return m.size.Load()
}

// Count returns the number of samples in the memtable.
func (m *Memtable) Count() int64 {
	return m.count.Load()
}

// ShouldFlush returns true if the memtable exceeds the size threshold.
func (m *Memtable) ShouldFlush(maxSize int64) bool {
	return m.size.Load() >= maxSize
}

func (m *Memtable) less(sample storage.Sample, key string, ts int64) bool {
	sKey := sample.Series.Key()
	if sKey != key {
		return sKey < key
	}
	return sample.DataPoint.Timestamp < ts
}

func (m *Memtable) randomLevel() int {
	level := 1
	for level < maxLevel && rand.Float64() < probability {
		level++
	}
	return level
}
