// Package cluster implements distributed partitioning for ClatterDB.
//
// Data is partitioned by time-based sharding: each node owns a set of
// time ranges. Writes are routed to the node responsible for the sample's
// timestamp. Queries that span multiple time ranges are fanned out to
// all relevant nodes and results are merged.
//
// Node discovery and partition assignment use consistent hashing for
// automatic rebalancing when nodes join or leave.
package cluster

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
	"time"
)

const (
	virtualNodes    = 128 // virtual nodes per physical node for consistent hashing
	defaultReplicas = 2   // replication factor
)

// NodeInfo describes a cluster node.
type NodeInfo struct {
	ID      string `json:"id"`
	Address string `json:"address"` // host:port for gRPC
	Status  string `json:"status"`  // "active", "draining", "down"
}

// Partition represents a time-range shard assigned to a set of nodes.
type Partition struct {
	ID        int    `json:"id"`
	StartTime int64  `json:"start_time"` // milliseconds
	EndTime   int64  `json:"end_time"`
	Owners    []string `json:"owners"` // node IDs (first is primary)
}

// ConsistentHash implements consistent hashing for partition-to-node assignment.
type ConsistentHash struct {
	mu       sync.RWMutex
	ring     []hashEntry
	nodes    map[string]*NodeInfo
	replicas int
}

type hashEntry struct {
	hash   uint32
	nodeID string
}

func NewConsistentHash(replicas int) *ConsistentHash {
	if replicas <= 0 {
		replicas = defaultReplicas
	}
	return &ConsistentHash{
		nodes:    make(map[string]*NodeInfo),
		replicas: replicas,
	}
}

// AddNode adds a node to the hash ring.
func (ch *ConsistentHash) AddNode(node *NodeInfo) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	ch.nodes[node.ID] = node

	for i := 0; i < virtualNodes; i++ {
		key := fmt.Sprintf("%s-%d", node.ID, i)
		hash := crc32.ChecksumIEEE([]byte(key))
		ch.ring = append(ch.ring, hashEntry{hash: hash, nodeID: node.ID})
	}

	sort.Slice(ch.ring, func(i, j int) bool {
		return ch.ring[i].hash < ch.ring[j].hash
	})
}

// RemoveNode removes a node from the hash ring.
func (ch *ConsistentHash) RemoveNode(nodeID string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	delete(ch.nodes, nodeID)

	var newRing []hashEntry
	for _, entry := range ch.ring {
		if entry.nodeID != nodeID {
			newRing = append(newRing, entry)
		}
	}
	ch.ring = newRing
}

// GetNodes returns the nodes responsible for the given key (primary + replicas).
func (ch *ConsistentHash) GetNodes(key string) []string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.ring) == 0 {
		return nil
	}

	hash := crc32.ChecksumIEEE([]byte(key))
	idx := sort.Search(len(ch.ring), func(i int) bool {
		return ch.ring[i].hash >= hash
	})
	if idx == len(ch.ring) {
		idx = 0
	}

	// Collect distinct nodes
	seen := make(map[string]bool)
	var nodes []string
	for i := 0; i < len(ch.ring) && len(nodes) < ch.replicas+1; i++ {
		entry := ch.ring[(idx+i)%len(ch.ring)]
		if !seen[entry.nodeID] {
			seen[entry.nodeID] = true
			nodes = append(nodes, entry.nodeID)
		}
	}

	return nodes
}

// Partitioner manages time-based partitioning across cluster nodes.
type Partitioner struct {
	mu          sync.RWMutex
	hash        *ConsistentHash
	partitions  []Partition
	windowSize  time.Duration // time window per partition (e.g., 6 hours)
}

func NewPartitioner(windowSize time.Duration, replicas int) *Partitioner {
	if windowSize <= 0 {
		windowSize = 6 * time.Hour
	}
	return &Partitioner{
		hash:       NewConsistentHash(replicas),
		windowSize: windowSize,
	}
}

// AddNode registers a new cluster node.
func (p *Partitioner) AddNode(node *NodeInfo) {
	p.hash.AddNode(node)
}

// RemoveNode removes a cluster node and triggers rebalancing.
func (p *Partitioner) RemoveNode(nodeID string) {
	p.hash.RemoveNode(nodeID)
}

// GetWriteTargets returns the node IDs that should receive a write for the given timestamp.
func (p *Partitioner) GetWriteTargets(timestamp int64) []string {
	partitionKey := p.partitionKey(timestamp)
	return p.hash.GetNodes(partitionKey)
}

// GetQueryTargets returns all node IDs that need to be queried for the given time range.
// This fans out to all nodes that own partitions overlapping the range.
func (p *Partitioner) GetQueryTargets(start, end int64) []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	nodeSet := make(map[string]bool)
	windowMs := p.windowSize.Milliseconds()

	// Walk through all partition windows in the range
	partStart := (start / windowMs) * windowMs
	for ts := partStart; ts <= end; ts += windowMs {
		key := p.partitionKey(ts)
		nodes := p.hash.GetNodes(key)
		for _, n := range nodes {
			nodeSet[n] = true
		}
	}

	nodes := make([]string, 0, len(nodeSet))
	for n := range nodeSet {
		nodes = append(nodes, n)
	}
	return nodes
}

// GetPartitionForTime returns the partition that covers the given timestamp.
func (p *Partitioner) GetPartitionForTime(timestamp int64) Partition {
	windowMs := p.windowSize.Milliseconds()
	start := (timestamp / windowMs) * windowMs
	end := start + windowMs - 1

	key := p.partitionKey(timestamp)
	owners := p.hash.GetNodes(key)

	return Partition{
		ID:        int(timestamp / windowMs),
		StartTime: start,
		EndTime:   end,
		Owners:    owners,
	}
}

func (p *Partitioner) partitionKey(timestamp int64) string {
	windowMs := p.windowSize.Milliseconds()
	windowID := timestamp / windowMs
	return fmt.Sprintf("partition-%d", windowID)
}

// GetNode returns info about a specific node.
func (p *Partitioner) GetNode(nodeID string) (*NodeInfo, bool) {
	p.hash.mu.RLock()
	defer p.hash.mu.RUnlock()
	node, ok := p.hash.nodes[nodeID]
	return node, ok
}

// GetAllNodes returns all registered nodes.
func (p *Partitioner) GetAllNodes() []*NodeInfo {
	p.hash.mu.RLock()
	defer p.hash.mu.RUnlock()
	nodes := make([]*NodeInfo, 0, len(p.hash.nodes))
	for _, n := range p.hash.nodes {
		nodes = append(nodes, n)
	}
	return nodes
}
