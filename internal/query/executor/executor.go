// Package executor walks the physical plan tree and produces query results.
//
// Each plan node type has an Execute method that returns data points.
// The executor processes the tree bottom-up:
//   1. Leaf nodes (scan) read from memtable or SSTables
//   2. Merge nodes combine results from children
//   3. Downsample/Aggregate nodes transform the merged stream
package executor

import (
	"fmt"
	"math"
	"sort"

	"github.com/clatterdb/internal/engine"
	"github.com/clatterdb/internal/query/parser"
	"github.com/clatterdb/internal/query/planner"
	"github.com/clatterdb/internal/storage"
	"github.com/clatterdb/internal/storage/sstable"
)

// Result holds the output of a query execution.
type Result struct {
	SeriesKey  string
	DataPoints []storage.DataPoint
}

// Executor walks the plan tree and executes the query.
type Executor struct {
	engine *engine.Engine
}

func New(eng *engine.Engine) *Executor {
	return &Executor{engine: eng}
}

// Execute runs the physical plan and returns results.
func (e *Executor) Execute(node *planner.PlanNode) (*Result, error) {
	points, err := e.executeNode(node)
	if err != nil {
		return nil, err
	}

	return &Result{
		SeriesKey:  node.SeriesKey,
		DataPoints: points,
	}, nil
}

func (e *Executor) executeNode(node *planner.PlanNode) ([]storage.DataPoint, error) {
	switch node.Type {
	case planner.NodeMemtableScan:
		return e.execMemtableScan(node)
	case planner.NodeSSTableScan:
		return e.execSSTableScan(node)
	case planner.NodeMerge:
		return e.execMerge(node)
	case planner.NodeDownsample:
		return e.execDownsample(node)
	case planner.NodeAggregate:
		return e.execAggregate(node)
	default:
		return nil, fmt.Errorf("unknown node type: %d", node.Type)
	}
}

func (e *Executor) execMemtableScan(node *planner.PlanNode) ([]storage.DataPoint, error) {
	// Delegate to the engine's query which already handles memtable scanning.
	// In a real implementation, the executor would access the memtable directly.
	return e.engine.Query(node.SeriesKey, node.TimeRange)
}

func (e *Executor) execSSTableScan(node *planner.PlanNode) ([]storage.DataPoint, error) {
	reader, err := sstable.OpenReader(node.SSTPath)
	if err != nil {
		return nil, fmt.Errorf("open SSTable %s: %w", node.SSTPath, err)
	}
	defer reader.Close()

	if !reader.MayContainSeries(node.SeriesKey) {
		return nil, nil
	}

	return reader.Query(node.SeriesKey, node.TimeRange)
}

func (e *Executor) execMerge(node *planner.PlanNode) ([]storage.DataPoint, error) {
	var allPoints []storage.DataPoint

	for _, child := range node.Children {
		points, err := e.executeNode(child)
		if err != nil {
			return nil, err
		}
		allPoints = append(allPoints, points...)
	}

	// Sort by timestamp
	sort.Slice(allPoints, func(i, j int) bool {
		return allPoints[i].Timestamp < allPoints[j].Timestamp
	})

	// Deduplicate (keep last value for each timestamp)
	return dedup(allPoints), nil
}

func (e *Executor) execDownsample(node *planner.PlanNode) ([]storage.DataPoint, error) {
	if len(node.Children) != 1 {
		return nil, fmt.Errorf("downsample expects 1 child, got %d", len(node.Children))
	}

	input, err := e.executeNode(node.Children[0])
	if err != nil {
		return nil, err
	}

	if len(input) == 0 || node.Interval <= 0 {
		return input, nil
	}

	// Group points into time buckets
	buckets := make(map[int64][]float64)
	var bucketKeys []int64

	for _, dp := range input {
		bucket := (dp.Timestamp / node.Interval) * node.Interval
		if _, exists := buckets[bucket]; !exists {
			bucketKeys = append(bucketKeys, bucket)
		}
		buckets[bucket] = append(buckets[bucket], dp.Value)
	}

	sort.Slice(bucketKeys, func(i, j int) bool { return bucketKeys[i] < bucketKeys[j] })

	// Apply aggregation function to each bucket
	result := make([]storage.DataPoint, 0, len(bucketKeys))
	for _, bucket := range bucketKeys {
		values := buckets[bucket]
		aggValue := applyAgg(node.AggFunc, values)
		result = append(result, storage.DataPoint{
			Timestamp: bucket,
			Value:     aggValue,
		})
	}

	return result, nil
}

func (e *Executor) execAggregate(node *planner.PlanNode) ([]storage.DataPoint, error) {
	if len(node.Children) != 1 {
		return nil, fmt.Errorf("aggregate expects 1 child, got %d", len(node.Children))
	}

	input, err := e.executeNode(node.Children[0])
	if err != nil {
		return nil, err
	}

	if len(input) == 0 {
		return nil, nil
	}

	// Special case: RATE computes per-second rate of change
	if node.AggFunc == parser.AggRate {
		return computeRate(input), nil
	}

	// Aggregate all values into a single result
	values := make([]float64, len(input))
	for i, dp := range input {
		values[i] = dp.Value
	}

	aggValue := applyAgg(node.AggFunc, values)
	return []storage.DataPoint{{
		Timestamp: input[len(input)-1].Timestamp,
		Value:     aggValue,
	}}, nil
}

func applyAgg(fn parser.AggFunc, values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	switch fn {
	case parser.AggAvg:
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum / float64(len(values))

	case parser.AggSum:
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum

	case parser.AggMin:
		min := values[0]
		for _, v := range values[1:] {
			if v < min {
				min = v
			}
		}
		return min

	case parser.AggMax:
		max := values[0]
		for _, v := range values[1:] {
			if v > max {
				max = v
			}
		}
		return max

	case parser.AggCount:
		return float64(len(values))

	case parser.AggLast:
		return values[len(values)-1]

	default:
		return values[len(values)-1]
	}
}

// computeRate computes per-second rate of change between consecutive points.
func computeRate(points []storage.DataPoint) []storage.DataPoint {
	if len(points) < 2 {
		return nil
	}

	rates := make([]storage.DataPoint, 0, len(points)-1)
	for i := 1; i < len(points); i++ {
		dtMs := float64(points[i].Timestamp - points[i-1].Timestamp)
		if dtMs <= 0 {
			continue
		}
		dv := points[i].Value - points[i-1].Value
		rate := dv / (dtMs / 1000.0) // per second

		// Handle counter resets (value decreased = counter wrapped)
		if rate < 0 {
			rate = math.Max(0, points[i].Value/(dtMs/1000.0))
		}

		rates = append(rates, storage.DataPoint{
			Timestamp: points[i].Timestamp,
			Value:     rate,
		})
	}

	return rates
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
