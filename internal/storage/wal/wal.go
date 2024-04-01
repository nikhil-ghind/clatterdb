// Package wal implements a write-ahead log for crash recovery.
//
// Every write is first appended to the WAL before being applied to the memtable.
// On crash, the WAL is replayed to reconstruct the memtable state.
//
// WAL file format:
//   [record_len(4)][crc32(4)][record_bytes(record_len)]...
//
// WAL files are rotated when the current segment exceeds MaxSegmentSize.
// Old segments are deleted after the corresponding memtable has been flushed to SSTable.
package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/clatterdb/internal/storage"
)

const (
	MaxSegmentSize = 64 * 1024 * 1024 // 64 MB per segment
	headerSize     = 8                // 4 bytes length + 4 bytes CRC
)

type WAL struct {
	mu         sync.Mutex
	dir        string
	current    *os.File
	currentSize int64
	segmentSeq  int
}

func New(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir wal dir: %w", err)
	}

	w := &WAL{dir: dir}

	// Find the highest existing segment sequence number
	segments, err := w.listSegments()
	if err != nil {
		return nil, err
	}
	if len(segments) > 0 {
		w.segmentSeq = segments[len(segments)-1].seq + 1
	}

	if err := w.rotate(); err != nil {
		return nil, err
	}

	return w, nil
}

// Append writes a sample to the WAL. Must be called before memtable insertion.
func (w *WAL) Append(sample storage.Sample) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data := storage.EncodeSample(sample)
	record := w.encodeRecord(data)

	n, err := w.current.Write(record)
	if err != nil {
		return fmt.Errorf("wal write: %w", err)
	}
	w.currentSize += int64(n)

	// Rotate if segment is full
	if w.currentSize >= MaxSegmentSize {
		if err := w.rotate(); err != nil {
			return fmt.Errorf("wal rotate: %w", err)
		}
	}

	return nil
}

// AppendBatch writes multiple samples atomically.
func (w *WAL) AppendBatch(samples []storage.Sample) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var batch []byte
	for _, s := range samples {
		data := storage.EncodeSample(s)
		batch = append(batch, w.encodeRecord(data)...)
	}

	n, err := w.current.Write(batch)
	if err != nil {
		return fmt.Errorf("wal batch write: %w", err)
	}
	w.currentSize += int64(n)

	if w.currentSize >= MaxSegmentSize {
		if err := w.rotate(); err != nil {
			return err
		}
	}

	return nil
}

// Sync forces a flush to disk (fsync).
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.current.Sync()
}

// Replay reads all WAL segments and returns all samples for memtable reconstruction.
func (w *WAL) Replay() ([]storage.Sample, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	segments, err := w.listSegments()
	if err != nil {
		return nil, err
	}

	var allSamples []storage.Sample
	for _, seg := range segments {
		samples, err := w.replaySegment(seg.path)
		if err != nil {
			log.Printf("[wal] warning: partial replay of %s: %v", seg.path, err)
			// Continue with what we recovered — partial crash recovery
		}
		allSamples = append(allSamples, samples...)
	}

	log.Printf("[wal] replayed %d samples from %d segments", len(allSamples), len(segments))
	return allSamples, nil
}

// Truncate removes WAL segments older than the given sequence number.
// Called after a successful memtable flush to SSTable.
func (w *WAL) Truncate(upToSeq int) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	segments, err := w.listSegments()
	if err != nil {
		return err
	}

	for _, seg := range segments {
		if seg.seq <= upToSeq {
			if err := os.Remove(seg.path); err != nil {
				return fmt.Errorf("remove segment %s: %w", seg.path, err)
			}
			log.Printf("[wal] truncated segment %s", seg.path)
		}
	}
	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.current != nil {
		return w.current.Close()
	}
	return nil
}

func (w *WAL) CurrentSeq() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.segmentSeq - 1
}

// --- Internal methods ---

func (w *WAL) rotate() error {
	if w.current != nil {
		if err := w.current.Sync(); err != nil {
			return err
		}
		w.current.Close()
	}

	path := filepath.Join(w.dir, fmt.Sprintf("wal_%06d.log", w.segmentSeq))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open new segment: %w", err)
	}

	w.current = f
	w.currentSize = 0
	w.segmentSeq++
	return nil
}

func (w *WAL) encodeRecord(data []byte) []byte {
	checksum := crc32.ChecksumIEEE(data)
	record := make([]byte, headerSize+len(data))
	binary.BigEndian.PutUint32(record[0:4], uint32(len(data)))
	binary.BigEndian.PutUint32(record[4:8], checksum)
	copy(record[8:], data)
	return record
}

func (w *WAL) replaySegment(path string) ([]storage.Sample, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var samples []storage.Sample
	header := make([]byte, headerSize)

	for {
		_, err := io.ReadFull(f, header)
		if err == io.EOF {
			break
		}
		if err != nil {
			return samples, fmt.Errorf("read header: %w", err)
		}

		recordLen := binary.BigEndian.Uint32(header[0:4])
		expectedCRC := binary.BigEndian.Uint32(header[4:8])

		data := make([]byte, recordLen)
		_, err = io.ReadFull(f, data)
		if err != nil {
			return samples, fmt.Errorf("read record: %w", err)
		}

		// Verify CRC
		actualCRC := crc32.ChecksumIEEE(data)
		if actualCRC != expectedCRC {
			return samples, fmt.Errorf("CRC mismatch at offset: expected %d, got %d", expectedCRC, actualCRC)
		}

		sample, _, err := storage.DecodeSample(data)
		if err != nil {
			return samples, fmt.Errorf("decode sample: %w", err)
		}
		samples = append(samples, sample)
	}

	return samples, nil
}

type segmentInfo struct {
	path string
	seq  int
}

func (w *WAL) listSegments() ([]segmentInfo, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, err
	}

	var segments []segmentInfo
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), "wal_") || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		var seq int
		if _, err := fmt.Sscanf(e.Name(), "wal_%06d.log", &seq); err != nil {
			continue
		}
		segments = append(segments, segmentInfo{
			path: filepath.Join(w.dir, e.Name()),
			seq:  seq,
		})
	}
	sort.Slice(segments, func(i, j int) bool { return segments[i].seq < segments[j].seq })
	return segments, nil
}
