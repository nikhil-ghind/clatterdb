// Package mvcc implements Multi-Version Concurrency Control for snapshot isolation.
//
// Each write transaction gets a monotonically increasing transaction ID.
// Each read transaction gets a snapshot — a set of committed transaction IDs
// visible at the time the read began. Reads never block writes; writes never
// block reads.
//
// Isolation level: Snapshot Isolation (SI)
// - Readers see a consistent snapshot as of their start time
// - Writers see their own uncommitted writes
// - Write-write conflicts are detected at commit time
package mvcc

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// TxnStatus represents the state of a transaction.
type TxnStatus int

const (
	TxnActive    TxnStatus = iota
	TxnCommitted
	TxnAborted
)

// Transaction represents an MVCC transaction.
type Transaction struct {
	ID        uint64
	Status    TxnStatus
	StartTxn  uint64   // the txn ID at the time this txn started (for snapshot)
	WriteSet  []string // series keys written by this txn (for conflict detection)
}

// Snapshot represents a read-consistent view of the database.
type Snapshot struct {
	VisibleUpTo uint64            // all committed txns with ID <= this are visible
	ActiveTxns  map[uint64]bool   // txns that were active when snapshot was taken (invisible)
}

// IsVisible returns true if a sample written by the given txn ID is visible in this snapshot.
func (s *Snapshot) IsVisible(txnID uint64) bool {
	if txnID > s.VisibleUpTo {
		return false
	}
	return !s.ActiveTxns[txnID]
}

// Manager coordinates MVCC transactions.
type Manager struct {
	mu          sync.Mutex
	nextTxnID   atomic.Uint64
	activeTxns  map[uint64]*Transaction
	committedTo uint64 // highest committed txn ID (watermark)
}

func NewManager() *Manager {
	m := &Manager{
		activeTxns: make(map[uint64]*Transaction),
	}
	m.nextTxnID.Store(1)
	return m
}

// Begin starts a new transaction and returns its ID.
func (m *Manager) Begin() *Transaction {
	txnID := m.nextTxnID.Add(1) - 1

	m.mu.Lock()
	defer m.mu.Unlock()

	txn := &Transaction{
		ID:       txnID,
		Status:   TxnActive,
		StartTxn: m.committedTo,
	}
	m.activeTxns[txnID] = txn
	return txn
}

// Snapshot creates a read snapshot for the given transaction.
func (m *Manager) Snapshot(txn *Transaction) *Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	active := make(map[uint64]bool, len(m.activeTxns))
	for id := range m.activeTxns {
		if id != txn.ID { // a txn can always see its own writes
			active[id] = true
		}
	}

	return &Snapshot{
		VisibleUpTo: m.committedTo,
		ActiveTxns:  active,
	}
}

// Commit attempts to commit a transaction.
// Returns an error if a write-write conflict is detected.
func (m *Manager) Commit(txn *Transaction) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if txn.Status != TxnActive {
		return fmt.Errorf("txn %d is not active (status=%d)", txn.ID, txn.Status)
	}

	// Write-write conflict detection:
	// Check if any other transaction committed writes to the same series keys
	// after this transaction started.
	for id, other := range m.activeTxns {
		if id == txn.ID || other.Status != TxnCommitted {
			continue
		}
		if id > txn.StartTxn && m.hasConflict(txn.WriteSet, other.WriteSet) {
			txn.Status = TxnAborted
			delete(m.activeTxns, txn.ID)
			return fmt.Errorf("write-write conflict: txn %d conflicts with txn %d", txn.ID, id)
		}
	}

	txn.Status = TxnCommitted
	delete(m.activeTxns, txn.ID)

	// Advance committed watermark
	if txn.ID > m.committedTo {
		m.committedTo = txn.ID
	}

	return nil
}

// Abort aborts a transaction.
func (m *Manager) Abort(txn *Transaction) {
	m.mu.Lock()
	defer m.mu.Unlock()

	txn.Status = TxnAborted
	delete(m.activeTxns, txn.ID)
}

// CurrentTxnID returns the latest transaction ID (for read-only queries).
func (m *Manager) CurrentTxnID() uint64 {
	return m.nextTxnID.Load() - 1
}

// ReadOnlySnapshot creates a snapshot for a read-only query (no transaction overhead).
func (m *Manager) ReadOnlySnapshot() *Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	active := make(map[uint64]bool, len(m.activeTxns))
	for id := range m.activeTxns {
		active[id] = true
	}

	return &Snapshot{
		VisibleUpTo: m.committedTo,
		ActiveTxns:  active,
	}
}

func (m *Manager) hasConflict(writeSet1, writeSet2 []string) bool {
	if len(writeSet1) == 0 || len(writeSet2) == 0 {
		return false
	}
	set := make(map[string]bool, len(writeSet2))
	for _, key := range writeSet2 {
		set[key] = true
	}
	for _, key := range writeSet1 {
		if set[key] {
			return true
		}
	}
	return false
}
