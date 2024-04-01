// Package cluster provides the query router that fans out queries to
// the correct cluster nodes based on time-range partitioning.
package cluster

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/clatterdb/internal/storage"
)

// RemoteQueryFunc is a function that queries a remote node.
// In production, this would be a gRPC call.
type RemoteQueryFunc func(ctx context.Context, nodeID, seriesKey string, tr storage.TimeRange) ([]storage.DataPoint, error)

// Router handles partition-aware query routing across cluster nodes.
type Router struct {
	localNodeID  string
	partitioner  *Partitioner
	remoteQuery  RemoteQueryFunc
	queryTimeout time.Duration
}

func NewRouter(localNodeID string, partitioner *Partitioner, remoteQuery RemoteQueryFunc) *Router {
	return &Router{
		localNodeID:  localNodeID,
		partitioner:  partitioner,
		remoteQuery:  remoteQuery,
		queryTimeout: 5 * time.Second,
	}
}

// QueryResult holds the merged result from a distributed query.
type QueryResult struct {
	DataPoints  []storage.DataPoint
	NodesQueried int
	NodesResponded int
	Errors      []string
}

// DistributedQuery fans out a query to all relevant nodes and merges results.
func (r *Router) DistributedQuery(ctx context.Context, seriesKey string, tr storage.TimeRange) (*QueryResult, error) {
	// Find all nodes that own data for this time range
	targetNodes := r.partitioner.GetQueryTargets(tr.Start, tr.End)

	if len(targetNodes) == 0 {
		return &QueryResult{}, nil
	}

	ctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	type nodeResult struct {
		nodeID string
		points []storage.DataPoint
		err    error
	}

	results := make(chan nodeResult, len(targetNodes))
	var wg sync.WaitGroup

	for _, nodeID := range targetNodes {
		wg.Add(1)
		go func(nid string) {
			defer wg.Done()

			points, err := r.remoteQuery(ctx, nid, seriesKey, tr)
			results <- nodeResult{nodeID: nid, points: points, err: err}
		}(nodeID)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// Merge results
	qr := &QueryResult{
		NodesQueried: len(targetNodes),
	}

	for nr := range results {
		if nr.err != nil {
			log.Printf("[router] node %s query error: %v", nr.nodeID, nr.err)
			qr.Errors = append(qr.Errors, nr.err.Error())
			continue
		}
		qr.NodesResponded++
		qr.DataPoints = append(qr.DataPoints, nr.points...)
	}

	// Sort and deduplicate merged results
	sort.Slice(qr.DataPoints, func(i, j int) bool {
		return qr.DataPoints[i].Timestamp < qr.DataPoints[j].Timestamp
	})
	qr.DataPoints = dedup(qr.DataPoints)

	return qr, nil
}

// RouteWrite determines which nodes should receive a write and forwards it.
func (r *Router) RouteWrite(timestamp int64) []string {
	return r.partitioner.GetWriteTargets(timestamp)
}

// IsLocalPartition returns true if this node owns the partition for the given timestamp.
func (r *Router) IsLocalPartition(timestamp int64) bool {
	targets := r.partitioner.GetWriteTargets(timestamp)
	for _, t := range targets {
		if t == r.localNodeID {
			return true
		}
	}
	return false
}

func dedup(points []storage.DataPoint) []storage.DataPoint {
	if len(points) == 0 {
		return points
	}
	result := []storage.DataPoint{points[0]}
	for i := 1; i < len(points); i++ {
		if points[i].Timestamp != points[i-1].Timestamp {
			result = append(result, points[i])
		}
	}
	return result
}
