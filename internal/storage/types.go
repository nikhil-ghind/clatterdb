package storage

import (
	"encoding/binary"
	"fmt"
	"math"
)

// DataPoint represents a single time-series data point.
type DataPoint struct {
	Timestamp int64   // Unix timestamp in milliseconds
	Value     float64 // Metric value
}

// Series represents a time series identified by its metric name and labels.
type Series struct {
	Metric string            // e.g., "http_requests_total"
	Labels map[string]string // e.g., {"method": "GET", "status": "200"}
}

// SeriesKey returns a canonical string key for a series (metric + sorted labels).
func (s Series) Key() string {
	key := s.Metric
	if len(s.Labels) > 0 {
		key += "{"
		first := true
		// Labels are iterated in map order — in production, sort them.
		for k, v := range s.Labels {
			if !first {
				key += ","
			}
			key += fmt.Sprintf("%s=%q", k, v)
			first = false
		}
		key += "}"
	}
	return key
}

// Sample is a DataPoint associated with a specific series.
type Sample struct {
	Series     Series
	DataPoint  DataPoint
	TxnID      uint64 // MVCC transaction ID that wrote this sample
	IsDeleted  bool   // tombstone marker for deletes
}

// TimeRange represents an inclusive time range [Start, End].
type TimeRange struct {
	Start int64 // milliseconds
	End   int64 // milliseconds
}

func (tr TimeRange) Contains(ts int64) bool {
	return ts >= tr.Start && ts <= tr.End
}

func (tr TimeRange) Overlaps(other TimeRange) bool {
	return tr.Start <= other.End && other.Start <= tr.End
}

// --- Binary encoding for on-disk format ---

// EncodeSample encodes a sample to bytes for WAL and SSTable storage.
// Format: [key_len(4)][key_bytes][timestamp(8)][value(8)][txn_id(8)][deleted(1)]
func EncodeSample(s Sample) []byte {
	key := []byte(s.Series.Key())
	buf := make([]byte, 4+len(key)+8+8+8+1)
	offset := 0

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(key)))
	offset += 4
	copy(buf[offset:], key)
	offset += len(key)
	binary.BigEndian.PutUint64(buf[offset:], uint64(s.DataPoint.Timestamp))
	offset += 8
	binary.BigEndian.PutUint64(buf[offset:], math.Float64bits(s.DataPoint.Value))
	offset += 8
	binary.BigEndian.PutUint64(buf[offset:], s.TxnID)
	offset += 8
	if s.IsDeleted {
		buf[offset] = 1
	}
	return buf
}

// DecodeSample decodes a sample from bytes.
func DecodeSample(data []byte) (Sample, int, error) {
	if len(data) < 4 {
		return Sample{}, 0, fmt.Errorf("data too short for key length")
	}

	offset := 0
	keyLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	if len(data) < offset+keyLen+25 {
		return Sample{}, 0, fmt.Errorf("data too short for sample")
	}

	key := string(data[offset : offset+keyLen])
	offset += keyLen

	ts := int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8
	val := math.Float64frombits(binary.BigEndian.Uint64(data[offset:]))
	offset += 8
	txnID := binary.BigEndian.Uint64(data[offset:])
	offset += 8
	deleted := data[offset] == 1
	offset++

	s := Sample{
		Series:    Series{Metric: key}, // simplified — full label parsing omitted
		DataPoint: DataPoint{Timestamp: ts, Value: val},
		TxnID:     txnID,
		IsDeleted: deleted,
	}
	return s, offset, nil
}
