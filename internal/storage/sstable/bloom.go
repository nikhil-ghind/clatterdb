// Package sstable implements sorted string tables for persistent storage.
//
// This file implements a bloom filter for fast negative lookups —
// quickly determining that a series key is NOT in an SSTable,
// avoiding unnecessary disk reads.
package sstable

import (
	"math"

	"github.com/spaolacci/murmur3"
)

// BloomFilter is a space-efficient probabilistic data structure for set membership.
type BloomFilter struct {
	bits    []byte
	numBits uint64
	numHash int
}

// NewBloomFilter creates a bloom filter sized for n expected items
// with the given false positive rate.
func NewBloomFilter(n int, fpRate float64) *BloomFilter {
	if n <= 0 {
		n = 1
	}
	if fpRate <= 0 {
		fpRate = 0.01
	}

	// Optimal number of bits: m = -n * ln(p) / (ln(2)^2)
	numBits := uint64(-float64(n) * math.Log(fpRate) / (math.Ln2 * math.Ln2))
	if numBits < 64 {
		numBits = 64
	}

	// Optimal number of hash functions: k = (m/n) * ln(2)
	numHash := int(math.Ceil(float64(numBits) / float64(n) * math.Ln2))
	if numHash < 1 {
		numHash = 1
	}
	if numHash > 30 {
		numHash = 30
	}

	return &BloomFilter{
		bits:    make([]byte, (numBits+7)/8),
		numBits: numBits,
		numHash: numHash,
	}
}

// Add inserts a key into the bloom filter.
func (bf *BloomFilter) Add(key string) {
	h1, h2 := bf.hash(key)
	for i := 0; i < bf.numHash; i++ {
		pos := (h1 + uint64(i)*h2) % bf.numBits
		bf.bits[pos/8] |= 1 << (pos % 8)
	}
}

// MayContain returns true if the key might be in the set (possible false positive),
// or false if the key is definitely NOT in the set (no false negatives).
func (bf *BloomFilter) MayContain(key string) bool {
	h1, h2 := bf.hash(key)
	for i := 0; i < bf.numHash; i++ {
		pos := (h1 + uint64(i)*h2) % bf.numBits
		if bf.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

// Encode serializes the bloom filter to bytes for on-disk storage.
func (bf *BloomFilter) Encode() []byte {
	// Format: [numBits(8)][numHash(4)][bits...]
	buf := make([]byte, 12+len(bf.bits))
	buf[0] = byte(bf.numBits >> 56)
	buf[1] = byte(bf.numBits >> 48)
	buf[2] = byte(bf.numBits >> 40)
	buf[3] = byte(bf.numBits >> 32)
	buf[4] = byte(bf.numBits >> 24)
	buf[5] = byte(bf.numBits >> 16)
	buf[6] = byte(bf.numBits >> 8)
	buf[7] = byte(bf.numBits)
	buf[8] = byte(bf.numHash >> 24)
	buf[9] = byte(bf.numHash >> 16)
	buf[10] = byte(bf.numHash >> 8)
	buf[11] = byte(bf.numHash)
	copy(buf[12:], bf.bits)
	return buf
}

// DecodeBloomFilter deserializes a bloom filter from bytes.
func DecodeBloomFilter(data []byte) *BloomFilter {
	if len(data) < 12 {
		return NewBloomFilter(1, 0.01)
	}
	numBits := uint64(data[0])<<56 | uint64(data[1])<<48 | uint64(data[2])<<40 |
		uint64(data[3])<<32 | uint64(data[4])<<24 | uint64(data[5])<<16 |
		uint64(data[6])<<8 | uint64(data[7])
	numHash := int(data[8])<<24 | int(data[9])<<16 | int(data[10])<<8 | int(data[11])

	bits := make([]byte, len(data)-12)
	copy(bits, data[12:])

	return &BloomFilter{
		bits:    bits,
		numBits: numBits,
		numHash: numHash,
	}
}

func (bf *BloomFilter) hash(key string) (uint64, uint64) {
	h := murmur3.New128()
	h.Write([]byte(key))
	return h.Sum128()
}
