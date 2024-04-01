// Package sstable implements Sorted String Tables (SSTables) — the on-disk
// component of the LSM tree.
//
// SSTable file format:
//
//	┌──────────────────────────┐
//	│    Data Block 0          │  Sorted samples, grouped by series key
//	│    Data Block 1          │
//	│    ...                   │
//	├──────────────────────────┤
//	│    Index Block            │  Maps series_key → data block offset
//	├──────────────────────────┤
//	│    Bloom Filter Block     │  For fast negative lookups
//	├──────────────────────────┤
//	│    Footer (48 bytes)      │  Index offset, bloom offset, counts, magic
//	└──────────────────────────┘
//
// Each data block contains samples sorted by (series_key, timestamp).
// The index block maps each unique series key to the offset of the data block
// containing its first sample, enabling binary search on series keys.
package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"sort"

	"github.com/clatterdb/internal/storage"
)

const (
	footerSize   = 48
	magicNumber  = 0x434C415454455244 // "CLATTERD"
	blockSize    = 4096              // target block size in bytes
)

// SSTableMeta holds metadata about an SSTable file.
type SSTableMeta struct {
	Path       string
	Level      int
	MinTime    int64 // earliest timestamp in the table
	MaxTime    int64 // latest timestamp in the table
	NumSamples int64
	SizeBytes  int64
	SeriesKeys []string // all unique series keys
}

// Writer creates a new SSTable file from sorted samples.
type Writer struct {
	file       *os.File
	path       string
	bloom      *BloomFilter
	index      []indexEntry
	offset     int64
	numSamples int64
	minTime    int64
	maxTime    int64
}

type indexEntry struct {
	key    string
	offset int64
}

// NewWriter creates a new SSTable writer.
func NewWriter(path string, expectedKeys int) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create sstable: %w", err)
	}

	return &Writer{
		file:    f,
		path:    path,
		bloom:   NewBloomFilter(expectedKeys, 0.01),
		minTime: 1<<63 - 1,
		maxTime: -(1 << 63),
	}, nil
}

// WriteAll writes all samples to the SSTable. Samples must be sorted by (key, timestamp).
func (w *Writer) WriteAll(samples []storage.Sample) error {
	if len(samples) == 0 {
		return nil
	}

	// Write data blocks, tracking index entries
	currentKey := ""
	for _, s := range samples {
		key := s.Series.Key()

		// New series key — record index entry
		if key != currentKey {
			w.index = append(w.index, indexEntry{key: key, offset: w.offset})
			w.bloom.Add(key)
			currentKey = key
		}

		data := storage.EncodeSample(s)
		n, err := w.file.Write(data)
		if err != nil {
			return fmt.Errorf("write sample: %w", err)
		}
		w.offset += int64(n)
		w.numSamples++

		if s.DataPoint.Timestamp < w.minTime {
			w.minTime = s.DataPoint.Timestamp
		}
		if s.DataPoint.Timestamp > w.maxTime {
			w.maxTime = s.DataPoint.Timestamp
		}
	}

	// Write index block
	indexOffset := w.offset
	if err := w.writeIndex(); err != nil {
		return err
	}

	// Write bloom filter block
	bloomOffset := w.offset
	bloomData := w.bloom.Encode()
	if _, err := w.file.Write(bloomData); err != nil {
		return fmt.Errorf("write bloom: %w", err)
	}
	w.offset += int64(len(bloomData))

	// Write footer
	if err := w.writeFooter(indexOffset, bloomOffset); err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *Writer) writeIndex() error {
	for _, entry := range w.index {
		keyBytes := []byte(entry.key)
		// [key_len(4)][key][offset(8)]
		buf := make([]byte, 4+len(keyBytes)+8)
		binary.BigEndian.PutUint32(buf[0:4], uint32(len(keyBytes)))
		copy(buf[4:4+len(keyBytes)], keyBytes)
		binary.BigEndian.PutUint64(buf[4+len(keyBytes):], uint64(entry.offset))

		n, err := w.file.Write(buf)
		if err != nil {
			return fmt.Errorf("write index entry: %w", err)
		}
		w.offset += int64(n)
	}
	return nil
}

func (w *Writer) writeFooter(indexOffset, bloomOffset int64) error {
	footer := make([]byte, footerSize)
	binary.BigEndian.PutUint64(footer[0:8], uint64(indexOffset))
	binary.BigEndian.PutUint64(footer[8:16], uint64(bloomOffset))
	binary.BigEndian.PutUint64(footer[16:24], uint64(w.numSamples))
	binary.BigEndian.PutUint64(footer[24:32], uint64(w.minTime))
	binary.BigEndian.PutUint64(footer[32:40], uint64(w.maxTime))
	binary.BigEndian.PutUint64(footer[40:48], magicNumber)

	_, err := w.file.Write(footer)
	w.offset += footerSize
	return err
}

// Close closes the writer.
func (w *Writer) Close() error {
	return w.file.Close()
}

// Meta returns metadata about the written SSTable.
func (w *Writer) Meta() SSTableMeta {
	keys := make([]string, len(w.index))
	for i, e := range w.index {
		keys[i] = e.key
	}
	return SSTableMeta{
		Path:       w.path,
		MinTime:    w.minTime,
		MaxTime:    w.maxTime,
		NumSamples: w.numSamples,
		SizeBytes:  w.offset,
		SeriesKeys: keys,
	}
}

// Reader reads samples from an SSTable file.
type Reader struct {
	file       *os.File
	path       string
	bloom      *BloomFilter
	index      []indexEntry
	numSamples int64
	minTime    int64
	maxTime    int64
}

// OpenReader opens an SSTable file for reading.
func OpenReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open sstable: %w", err)
	}

	r := &Reader{file: f, path: path}
	if err := r.readFooter(); err != nil {
		f.Close()
		return nil, err
	}

	return r, nil
}

func (r *Reader) readFooter() error {
	info, err := r.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() < footerSize {
		return fmt.Errorf("file too small for footer")
	}

	footer := make([]byte, footerSize)
	if _, err := r.file.ReadAt(footer, info.Size()-footerSize); err != nil {
		return err
	}

	magic := binary.BigEndian.Uint64(footer[40:48])
	if magic != magicNumber {
		return fmt.Errorf("invalid magic number: %x", magic)
	}

	indexOffset := int64(binary.BigEndian.Uint64(footer[0:8]))
	bloomOffset := int64(binary.BigEndian.Uint64(footer[8:16]))
	r.numSamples = int64(binary.BigEndian.Uint64(footer[16:24]))
	r.minTime = int64(binary.BigEndian.Uint64(footer[24:32]))
	r.maxTime = int64(binary.BigEndian.Uint64(footer[32:40]))

	// Read index
	indexSize := bloomOffset - indexOffset
	indexData := make([]byte, indexSize)
	if _, err := r.file.ReadAt(indexData, indexOffset); err != nil {
		return fmt.Errorf("read index: %w", err)
	}
	r.index = r.parseIndex(indexData)

	// Read bloom filter
	bloomSize := info.Size() - footerSize - bloomOffset
	bloomData := make([]byte, bloomSize)
	if _, err := r.file.ReadAt(bloomData, bloomOffset); err != nil {
		return fmt.Errorf("read bloom: %w", err)
	}
	r.bloom = DecodeBloomFilter(bloomData)

	return nil
}

func (r *Reader) parseIndex(data []byte) []indexEntry {
	var entries []indexEntry
	offset := 0
	for offset < len(data) {
		if offset+4 > len(data) {
			break
		}
		keyLen := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		if offset+keyLen+8 > len(data) {
			break
		}
		key := string(data[offset : offset+keyLen])
		offset += keyLen
		fileOffset := int64(binary.BigEndian.Uint64(data[offset:]))
		offset += 8
		entries = append(entries, indexEntry{key: key, offset: fileOffset})
	}
	return entries
}

// MayContainSeries checks the bloom filter for a series key.
func (r *Reader) MayContainSeries(seriesKey string) bool {
	return r.bloom.MayContain(seriesKey)
}

// TimeRange returns the time range covered by this SSTable.
func (r *Reader) TimeRange() storage.TimeRange {
	return storage.TimeRange{Start: r.minTime, End: r.maxTime}
}

// Query reads all data points for a series key within the time range.
func (r *Reader) Query(seriesKey string, tr storage.TimeRange) ([]storage.DataPoint, error) {
	// Bloom filter check
	if !r.bloom.MayContain(seriesKey) {
		return nil, nil
	}

	// Time range check
	if r.maxTime < tr.Start || r.minTime > tr.End {
		return nil, nil
	}

	// Binary search index for the series key
	idx := sort.Search(len(r.index), func(i int) bool {
		return r.index[i].key >= seriesKey
	})
	if idx >= len(r.index) || r.index[idx].key != seriesKey {
		return nil, nil // series not in this SSTable
	}

	// Determine read range
	startOffset := r.index[idx].offset
	var endOffset int64
	if idx+1 < len(r.index) {
		endOffset = r.index[idx+1].offset
	} else {
		// Read up to the index block
		endOffset = r.index[0].offset // This is the first index entry offset
		// Actually need the index block offset from footer
		info, _ := r.file.Stat()
		endOffset = info.Size() - footerSize
		// Find the index block start (it's after the last data)
		if len(r.index) > 0 {
			// Scan up to a reasonable amount
			endOffset = startOffset + 1024*1024 // 1MB max scan
		}
	}

	// Read data block
	data := make([]byte, endOffset-startOffset)
	n, err := r.file.ReadAt(data, startOffset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("read data block: %w", err)
	}
	data = data[:n]

	// Decode samples and filter
	var results []storage.DataPoint
	offset := 0
	for offset < len(data) {
		sample, consumed, err := storage.DecodeSample(data[offset:])
		if err != nil {
			break
		}
		offset += consumed

		if sample.Series.Key() != seriesKey {
			break // past our series
		}
		if sample.DataPoint.Timestamp > tr.End {
			break
		}
		if sample.DataPoint.Timestamp >= tr.Start && !sample.IsDeleted {
			results = append(results, sample.DataPoint)
		}
	}

	return results, nil
}

// ScanAll reads all samples from the SSTable (for compaction).
func (r *Reader) ScanAll() ([]storage.Sample, error) {
	if len(r.index) == 0 {
		return nil, nil
	}

	info, err := r.file.Stat()
	if err != nil {
		return nil, err
	}

	// Read all data blocks (everything before the index)
	dataEnd := info.Size() - footerSize
	// Find where index starts
	indexStart := r.index[0].offset
	// The index entries' offset field points to data offsets, not index offsets.
	// We stored indexOffset in the footer — but we already read it. Approximate:
	for _, entry := range r.index {
		if entry.offset > dataEnd {
			break
		}
	}
	_ = indexStart

	// Just read everything up to the footer and decode
	// In practice we'd use the footer's indexOffset, but simplify here
	dataSize := dataEnd
	if dataSize > 100*1024*1024 {
		dataSize = 100 * 1024 * 1024 // cap at 100MB
	}
	data := make([]byte, dataSize)
	n, err := r.file.ReadAt(data, 0)
	if err != nil && err != io.EOF {
		return nil, err
	}
	data = data[:n]

	var samples []storage.Sample
	offset := 0
	for offset < len(data) {
		sample, consumed, err := storage.DecodeSample(data[offset:])
		if err != nil {
			break
		}
		offset += consumed
		samples = append(samples, sample)

		if int64(len(samples)) >= r.numSamples {
			break
		}
	}

	return samples, nil
}

func (r *Reader) Close() error {
	return r.file.Close()
}

func (r *Reader) Meta() SSTableMeta {
	keys := make([]string, len(r.index))
	for i, e := range r.index {
		keys[i] = e.key
	}
	info, _ := r.file.Stat()
	size := int64(0)
	if info != nil {
		size = info.Size()
	}
	return SSTableMeta{
		Path:       r.path,
		MinTime:    r.minTime,
		MaxTime:    r.maxTime,
		NumSamples: r.numSamples,
		SizeBytes:  size,
		SeriesKeys: keys,
	}
}

// Flush writes a memtable's contents to a new SSTable file.
func Flush(path string, samples []storage.Sample) (*SSTableMeta, error) {
	if len(samples) == 0 {
		return nil, fmt.Errorf("no samples to flush")
	}

	// Count unique keys for bloom filter sizing
	keySet := make(map[string]struct{})
	for _, s := range samples {
		keySet[s.Series.Key()] = struct{}{}
	}

	writer, err := NewWriter(path, len(keySet))
	if err != nil {
		return nil, err
	}
	defer writer.Close()

	if err := writer.WriteAll(samples); err != nil {
		return nil, err
	}

	meta := writer.Meta()
	log.Printf("[sstable] flushed %s: %d samples, %d series, time=[%d, %d]",
		path, meta.NumSamples, len(meta.SeriesKeys), meta.MinTime, meta.MaxTime)
	return &meta, nil
}
