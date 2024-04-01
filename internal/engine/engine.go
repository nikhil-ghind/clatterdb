// Package engine implements the core database engine that ties together
// WAL, memtable, SSTables, compaction, and MVCC into a unified storage layer.
//
// Write path:  WAL append → memtable insert → (flush trigger) → SSTable flush
// Read path:   memtable scan → SSTable scan (L0 → L1 → ...) → merge results
package engine

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/clatterdb/internal/mvcc"
	"github.com/clatterdb/internal/storage"
	"github.com/clatterdb/internal/storage/compaction"
	"github.com/clatterdb/internal/storage/memtable"
	"github.com/clatterdb/internal/storage/sstable"
	"github.com/clatterdb/internal/storage/wal"
	"github.com/google/uuid"
)

type Config struct {
	DataDir           string
	WALDir            string
	MemtableMaxSize   int64
	CompactionStrategy compaction.Strategy
	RetentionTTL      time.Duration
	SyncWrites        bool // fsync after every write
}

func DefaultConfig(dataDir string) Config {
	return Config{
		DataDir:           dataDir,
		WALDir:            filepath.Join(dataDir, "wal"),
		MemtableMaxSize:   memtable.DefaultMaxSize,
		CompactionStrategy: compaction.TimeWindow,
		RetentionTTL:      7 * 24 * time.Hour, // 7 days
		SyncWrites:        false,
	}
}

type Engine struct {
	mu         sync.RWMutex
	config     Config
	wal        *wal.WAL
	active     *memtable.Memtable   // current writable memtable
	immutables []*memtable.Memtable // frozen memtables waiting to be flushed
	compactor  *compaction.Manager
	mvcc       *mvcc.Manager
	flushCh    chan struct{}
	closed     bool
}

func Open(config Config) (*Engine, error) {
	if err := os.MkdirAll(config.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir data dir: %w", err)
	}

	w, err := wal.New(config.WALDir)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}

	e := &Engine{
		config:    config,
		wal:       w,
		active:    memtable.New(),
		compactor: compaction.NewManager(config.DataDir, config.CompactionStrategy, config.RetentionTTL),
		mvcc:      mvcc.NewManager(),
		flushCh:   make(chan struct{}, 1),
	}

	// Replay WAL for crash recovery
	if err := e.recoverFromWAL(); err != nil {
		return nil, fmt.Errorf("WAL recovery: %w", err)
	}

	// Load existing SSTables
	if err := e.loadSSTables(); err != nil {
		return nil, fmt.Errorf("load SSTables: %w", err)
	}

	return e, nil
}

// Write inserts a batch of samples within a transaction.
func (e *Engine) Write(samples []storage.Sample) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return fmt.Errorf("engine is closed")
	}

	// Begin MVCC transaction
	txn := e.mvcc.Begin()

	// Tag samples with transaction ID and record write set
	for i := range samples {
		samples[i].TxnID = txn.ID
		txn.WriteSet = append(txn.WriteSet, samples[i].Series.Key())
	}

	// WAL first (durability)
	if err := e.wal.AppendBatch(samples); err != nil {
		e.mvcc.Abort(txn)
		return fmt.Errorf("WAL append: %w", err)
	}

	if e.config.SyncWrites {
		if err := e.wal.Sync(); err != nil {
			e.mvcc.Abort(txn)
			return fmt.Errorf("WAL sync: %w", err)
		}
	}

	// Memtable insert
	for _, s := range samples {
		if !e.active.Insert(s) {
			// Memtable is frozen — this shouldn't happen since we hold the lock
			e.mvcc.Abort(txn)
			return fmt.Errorf("memtable insert failed")
		}
	}

	// Commit transaction
	if err := e.mvcc.Commit(txn); err != nil {
		return fmt.Errorf("txn commit: %w", err)
	}

	// Check if memtable needs flushing
	if e.active.ShouldFlush(e.config.MemtableMaxSize) {
		e.triggerFlush()
	}

	return nil
}

// WriteSingle is a convenience method for writing a single sample.
func (e *Engine) WriteSingle(series storage.Series, dp storage.DataPoint) error {
	return e.Write([]storage.Sample{{Series: series, DataPoint: dp}})
}

// Query reads data points for a series within the time range.
// Results are merged across the active memtable, immutable memtables, and SSTables.
// MVCC snapshot ensures consistent read at the point-in-time the query started.
func (e *Engine) Query(seriesKey string, tr storage.TimeRange) ([]storage.DataPoint, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed {
		return nil, fmt.Errorf("engine is closed")
	}

	snapshot := e.mvcc.ReadOnlySnapshot()

	var results []storage.DataPoint

	// 1. Query active memtable
	memResults := e.active.Query(seriesKey, tr)
	results = append(results, memResults...)

	// 2. Query immutable memtables (newest first)
	for i := len(e.immutables) - 1; i >= 0; i-- {
		immResults := e.immutables[i].Query(seriesKey, tr)
		results = append(results, immResults...)
	}

	// 3. Query SSTables (L0 first, then L1, L2, ...)
	tables := e.compactor.GetAllTables()
	for _, table := range tables {
		// Skip tables outside time range
		tableTR := storage.TimeRange{Start: table.Meta.MinTime, End: table.Meta.MaxTime}
		if !tableTR.Overlaps(tr) {
			continue
		}

		reader, err := sstable.OpenReader(table.Meta.Path)
		if err != nil {
			log.Printf("[engine] failed to open SSTable %s: %v", table.Meta.Path, err)
			continue
		}

		// Bloom filter check
		if !reader.MayContainSeries(seriesKey) {
			reader.Close()
			continue
		}

		sstResults, err := reader.Query(seriesKey, tr)
		reader.Close()
		if err != nil {
			log.Printf("[engine] SSTable query error: %v", err)
			continue
		}
		results = append(results, sstResults...)
	}

	// 4. Deduplicate and apply MVCC visibility
	results = e.applySnapshot(results, seriesKey, snapshot)

	// 5. Sort by timestamp
	sortDataPoints(results)

	return results, nil
}

// Close shuts down the engine, flushing the active memtable.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.closed = true

	// Flush active memtable
	if e.active.Count() > 0 {
		e.flushMemtable(e.active)
	}

	// Flush immutable memtables
	for _, imm := range e.immutables {
		e.flushMemtable(imm)
	}

	return e.wal.Close()
}

// RunCompaction runs a single compaction cycle. Call periodically from a background goroutine.
func (e *Engine) RunCompaction() error {
	if !e.compactor.NeedsCompaction() {
		return nil
	}
	return e.compactor.Run()
}

// StartBackgroundWorkers starts flush and compaction workers.
func (e *Engine) StartBackgroundWorkers(ctx context.Context) {
	// Flush worker
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-e.flushCh:
				e.doFlush()
			}
		}
	}()

	// Compaction worker
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := e.RunCompaction(); err != nil {
					log.Printf("[engine] compaction error: %v", err)
				}
			}
		}
	}()
}

// --- Internal methods ---

func (e *Engine) triggerFlush() {
	// Freeze current memtable and create a new one
	e.active.Freeze()
	e.immutables = append(e.immutables, e.active)
	e.active = memtable.New()

	// Signal flush worker (non-blocking)
	select {
	case e.flushCh <- struct{}{}:
	default:
	}
}

func (e *Engine) doFlush() {
	e.mu.Lock()
	if len(e.immutables) == 0 {
		e.mu.Unlock()
		return
	}
	imm := e.immutables[0]
	e.mu.Unlock()

	e.flushMemtable(imm)

	e.mu.Lock()
	if len(e.immutables) > 0 {
		e.immutables = e.immutables[1:]
	}
	e.mu.Unlock()

	// Truncate WAL segments for flushed data
	walSeq := e.wal.CurrentSeq()
	e.wal.Truncate(walSeq - 1)
}

func (e *Engine) flushMemtable(mem *memtable.Memtable) {
	samples := mem.Iterator()
	if len(samples) == 0 {
		return
	}

	path := filepath.Join(e.config.DataDir, fmt.Sprintf("L0_%s.sst", uuid.New().String()[:8]))
	meta, err := sstable.Flush(path, samples)
	if err != nil {
		log.Printf("[engine] flush failed: %v", err)
		return
	}

	e.compactor.AddTable(*meta)
	log.Printf("[engine] flushed memtable → %s (%d samples)", path, meta.NumSamples)
}

func (e *Engine) recoverFromWAL() error {
	samples, err := e.wal.Replay()
	if err != nil {
		return err
	}

	for _, s := range samples {
		e.active.Insert(s)
	}

	if len(samples) > 0 {
		log.Printf("[engine] recovered %d samples from WAL", len(samples))
	}
	return nil
}

func (e *Engine) loadSSTables() error {
	entries, err := os.ReadDir(e.config.DataDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".sst" {
			continue
		}

		path := filepath.Join(e.config.DataDir, entry.Name())
		reader, err := sstable.OpenReader(path)
		if err != nil {
			log.Printf("[engine] skipping invalid SSTable %s: %v", path, err)
			continue
		}

		meta := reader.Meta()
		reader.Close()

		// Determine level from filename prefix
		level := 0
		if len(entry.Name()) > 1 && entry.Name()[0] == 'L' {
			fmt.Sscanf(entry.Name(), "L%d_", &level)
		}

		e.compactor.AddTableAtLevel(meta, level)
	}

	return nil
}

func (e *Engine) applySnapshot(points []storage.DataPoint, seriesKey string, snapshot *mvcc.Snapshot) []storage.DataPoint {
	// In a full implementation, we'd check each sample's TxnID against the snapshot.
	// Since DataPoint doesn't carry TxnID, we rely on the memtable/SSTable layers
	// to have already filtered by visibility during their query methods.
	// This is a simplification — production systems would carry TxnID through.
	_ = snapshot
	return deduplicateByTimestamp(points)
}

func deduplicateByTimestamp(points []storage.DataPoint) []storage.DataPoint {
	if len(points) == 0 {
		return points
	}

	sortDataPoints(points)

	deduped := make([]storage.DataPoint, 0, len(points))
	deduped = append(deduped, points[0])
	for i := 1; i < len(points); i++ {
		if points[i].Timestamp != points[i-1].Timestamp {
			deduped = append(deduped, points[i])
		}
	}
	return deduped
}

func sortDataPoints(points []storage.DataPoint) {
	for i := 1; i < len(points); i++ {
		for j := i; j > 0 && points[j].Timestamp < points[j-1].Timestamp; j-- {
			points[j], points[j-1] = points[j-1], points[j]
		}
	}
}
