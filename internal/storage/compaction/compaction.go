// Package compaction implements leveled and time-window compaction strategies
// for managing SSTables on disk.
//
// Leveled compaction: SSTables are organized into levels (L0, L1, L2, ...).
// L0 is the landing zone for memtable flushes (may overlap). Higher levels are
// non-overlapping and get progressively larger. When a level exceeds its size
// threshold, overlapping tables are merged into the next level.
//
// Time-window compaction: Optimized for time-series — groups SSTables by time
// window (e.g., 1 hour) and compacts within each window. Never merges across
// windows, preserving time-locality for efficient range scans and TTL deletion.
package compaction

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/clatterdb/internal/storage"
	"github.com/clatterdb/internal/storage/sstable"
	"github.com/google/uuid"
)

// Strategy defines which compaction strategy to use.
type Strategy string

const (
	Leveled    Strategy = "leveled"
	TimeWindow Strategy = "time_window"
)

const (
	L0MaxTables    = 4   // trigger compaction when L0 has this many tables
	LevelSizeRatio = 10  // each level is 10x the size of the previous
	L1MaxSize      = 64 * 1024 * 1024  // 64 MB
)

// TableInfo tracks an SSTable's position in the level hierarchy.
type TableInfo struct {
	Meta  sstable.SSTableMeta
	Level int
}

// Manager manages compaction of SSTables across levels.
type Manager struct {
	mu       sync.Mutex
	dataDir  string
	strategy Strategy
	levels   map[int][]TableInfo // level → tables at that level
	ttl      time.Duration       // retention period; 0 means no TTL
}

func NewManager(dataDir string, strategy Strategy, ttl time.Duration) *Manager {
	return &Manager{
		dataDir:  dataDir,
		strategy: strategy,
		levels:   make(map[int][]TableInfo),
		ttl:      ttl,
	}
}

// AddTable registers a newly flushed SSTable at L0.
func (m *Manager) AddTable(meta sstable.SSTableMeta) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.levels[0] = append(m.levels[0], TableInfo{Meta: meta, Level: 0})
}

// AddTableAtLevel registers a table at a specific level (after compaction).
func (m *Manager) AddTableAtLevel(meta sstable.SSTableMeta, level int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.levels[level] = append(m.levels[level], TableInfo{Meta: meta, Level: level})
}

// NeedsCompaction returns true if any level needs compaction.
func (m *Manager) NeedsCompaction() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	// L0 trigger: too many tables
	if len(m.levels[0]) >= L0MaxTables {
		return true
	}

	// Higher levels: size-based trigger
	for level := 1; level <= m.maxLevel(); level++ {
		maxSize := m.levelMaxSize(level)
		currentSize := m.levelSize(level)
		if currentSize > maxSize {
			return true
		}
	}

	return false
}

// Run executes one round of compaction.
func (m *Manager) Run() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// TTL-based deletion first
	if m.ttl > 0 {
		m.deleteTTLExpired()
	}

	switch m.strategy {
	case Leveled:
		return m.runLeveled()
	case TimeWindow:
		return m.runTimeWindow()
	default:
		return m.runLeveled()
	}
}

func (m *Manager) runLeveled() error {
	// Check L0 first
	if len(m.levels[0]) >= L0MaxTables {
		return m.compactLevel(0)
	}

	// Check higher levels
	for level := 1; level <= m.maxLevel(); level++ {
		if m.levelSize(level) > m.levelMaxSize(level) {
			return m.compactLevel(level)
		}
	}

	return nil
}

// compactLevel merges tables from `level` into `level+1`.
func (m *Manager) compactLevel(level int) error {
	tables := m.levels[level]
	if len(tables) == 0 {
		return nil
	}

	log.Printf("[compaction] compacting L%d (%d tables) → L%d", level, len(tables), level+1)

	// Find overlapping tables in the next level
	var inputMetas []sstable.SSTableMeta
	for _, t := range tables {
		inputMetas = append(inputMetas, t.Meta)
	}

	// Also include overlapping tables from level+1
	nextLevel := m.levels[level+1]
	minTime, maxTime := m.timeRange(inputMetas)
	var overlapping []int
	for i, t := range nextLevel {
		tr := storage.TimeRange{Start: t.Meta.MinTime, End: t.Meta.MaxTime}
		if tr.Overlaps(storage.TimeRange{Start: minTime, End: maxTime}) {
			inputMetas = append(inputMetas, t.Meta)
			overlapping = append(overlapping, i)
		}
	}

	// Merge all input tables
	merged, err := m.mergeTables(inputMetas)
	if err != nil {
		return fmt.Errorf("merge tables: %w", err)
	}

	// Write output SSTable at level+1
	outputPath := filepath.Join(m.dataDir, fmt.Sprintf("L%d_%s.sst", level+1, uuid.New().String()[:8]))
	meta, err := sstable.Flush(outputPath, merged)
	if err != nil {
		return fmt.Errorf("flush compacted: %w", err)
	}

	// Remove old tables from level
	for _, t := range tables {
		os.Remove(t.Meta.Path)
	}
	m.levels[level] = nil

	// Remove overlapping tables from level+1
	newNextLevel := make([]TableInfo, 0)
	overlapSet := make(map[int]bool)
	for _, idx := range overlapping {
		overlapSet[idx] = true
		os.Remove(nextLevel[idx].Meta.Path)
	}
	for i, t := range nextLevel {
		if !overlapSet[i] {
			newNextLevel = append(newNextLevel, t)
		}
	}

	// Add output table
	newNextLevel = append(newNextLevel, TableInfo{Meta: *meta, Level: level + 1})
	m.levels[level+1] = newNextLevel

	log.Printf("[compaction] L%d → L%d complete: %d samples in output", level, level+1, meta.NumSamples)
	return nil
}

// runTimeWindow groups tables by time window and compacts within each window.
func (m *Manager) runTimeWindow() error {
	windowDuration := int64(3600 * 1000) // 1 hour in milliseconds

	// Group all tables by time window
	windows := make(map[int64][]TableInfo)
	for level, tables := range m.levels {
		for _, t := range tables {
			windowKey := t.Meta.MinTime / windowDuration
			t.Level = level
			windows[windowKey] = append(windows[windowKey], t)
		}
	}

	// Compact windows that have too many tables
	for windowKey, tables := range windows {
		if len(tables) < 3 {
			continue // not enough to justify compaction
		}

		log.Printf("[compaction] time-window %d: compacting %d tables", windowKey, len(tables))

		var metas []sstable.SSTableMeta
		for _, t := range tables {
			metas = append(metas, t.Meta)
		}

		merged, err := m.mergeTables(metas)
		if err != nil {
			return err
		}

		outputPath := filepath.Join(m.dataDir, fmt.Sprintf("tw_%d_%s.sst", windowKey, uuid.New().String()[:8]))
		meta, err := sstable.Flush(outputPath, merged)
		if err != nil {
			return err
		}

		// Remove old tables
		for _, t := range tables {
			os.Remove(t.Meta.Path)
			m.removeTable(t)
		}

		// Add compacted table at L1
		m.levels[1] = append(m.levels[1], TableInfo{Meta: *meta, Level: 1})
	}

	return nil
}

// mergeTables reads all input SSTables and merge-sorts their samples.
func (m *Manager) mergeTables(metas []sstable.SSTableMeta) ([]storage.Sample, error) {
	var allSamples []storage.Sample

	for _, meta := range metas {
		reader, err := sstable.OpenReader(meta.Path)
		if err != nil {
			return nil, fmt.Errorf("open %s: %w", meta.Path, err)
		}

		samples, err := reader.ScanAll()
		reader.Close()
		if err != nil {
			return nil, fmt.Errorf("scan %s: %w", meta.Path, err)
		}
		allSamples = append(allSamples, samples...)
	}

	// Sort by (key, timestamp)
	sort.Slice(allSamples, func(i, j int) bool {
		ki, kj := allSamples[i].Series.Key(), allSamples[j].Series.Key()
		if ki != kj {
			return ki < kj
		}
		return allSamples[i].DataPoint.Timestamp < allSamples[j].DataPoint.Timestamp
	})

	// Deduplicate: keep the latest version (highest TxnID) for each (key, ts)
	deduped := make([]storage.Sample, 0, len(allSamples))
	for i, s := range allSamples {
		if i > 0 {
			prev := allSamples[i-1]
			if s.Series.Key() == prev.Series.Key() &&
				s.DataPoint.Timestamp == prev.DataPoint.Timestamp {
				// Keep newer version (replace last entry if this has higher txn)
				if s.TxnID > prev.TxnID {
					deduped[len(deduped)-1] = s
				}
				continue
			}
		}
		// Drop tombstones during compaction (the delete has been applied)
		if s.IsDeleted {
			continue
		}
		deduped = append(deduped, s)
	}

	return deduped, nil
}

// deleteTTLExpired removes SSTables whose max timestamp is older than the TTL.
func (m *Manager) deleteTTLExpired() {
	cutoff := time.Now().UnixMilli() - m.ttl.Milliseconds()

	for level, tables := range m.levels {
		var kept []TableInfo
		for _, t := range tables {
			if t.Meta.MaxTime < cutoff {
				log.Printf("[compaction] TTL expired: removing %s (maxTime=%d < cutoff=%d)",
					t.Meta.Path, t.Meta.MaxTime, cutoff)
				os.Remove(t.Meta.Path)
			} else {
				kept = append(kept, t)
			}
		}
		m.levels[level] = kept
	}
}

// GetAllTables returns all tracked SSTable metadata across levels.
func (m *Manager) GetAllTables() []TableInfo {
	m.mu.Lock()
	defer m.mu.Unlock()

	var all []TableInfo
	for _, tables := range m.levels {
		all = append(all, tables...)
	}
	return all
}

func (m *Manager) removeTable(target TableInfo) {
	for level, tables := range m.levels {
		for i, t := range tables {
			if t.Meta.Path == target.Meta.Path {
				m.levels[level] = append(tables[:i], tables[i+1:]...)
				return
			}
		}
	}
}

func (m *Manager) maxLevel() int {
	max := 0
	for l := range m.levels {
		if l > max {
			max = l
		}
	}
	return max
}

func (m *Manager) levelSize(level int) int64 {
	var total int64
	for _, t := range m.levels[level] {
		total += t.Meta.SizeBytes
	}
	return total
}

func (m *Manager) levelMaxSize(level int) int64 {
	size := int64(L1MaxSize)
	for i := 1; i < level; i++ {
		size *= LevelSizeRatio
	}
	return size
}

func (m *Manager) timeRange(metas []sstable.SSTableMeta) (int64, int64) {
	minT := int64(1<<63 - 1)
	maxT := int64(-(1 << 63))
	for _, meta := range metas {
		if meta.MinTime < minT {
			minT = meta.MinTime
		}
		if meta.MaxTime > maxT {
			maxT = meta.MaxTime
		}
	}
	return minT, maxT
}
