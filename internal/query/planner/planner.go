// Package planner translates a parsed query into a physical execution plan.
//
// Plan tree:
//
//   AggregateNode (optional — AVG, SUM, RATE, etc.)
//     └── DownsampleNode (optional — group by time interval)
//           └── MergeNode (merge-sort results from multiple sources)
//                 ├── MemtableScanNode
//                 ├── SSTableScanNode (L0-table-1)
//                 ├── SSTableScanNode (L0-table-2)
//                 └── SSTableScanNode (L1-table-1)
//
// The planner performs predicate pushdown (time range) to scan nodes
// and prunes SSTables that don't overlap the query time range.
package planner

import (
	"fmt"

	"github.com/clatterdb/internal/query/parser"
	"github.com/clatterdb/internal/storage"
	"github.com/clatterdb/internal/storage/compaction"
)

// NodeType identifies the type of plan node.
type NodeType int

const (
	NodeMemtableScan NodeType = iota
	NodeSSTableScan
	NodeMerge
	NodeDownsample
	NodeAggregate
)

// PlanNode is a node in the physical execution plan tree.
type PlanNode struct {
	Type       NodeType
	Children   []*PlanNode
	SeriesKey  string
	TimeRange  storage.TimeRange
	SSTPath    string            // for SSTableScan nodes
	Interval   int64             // for Downsample nodes (milliseconds)
	AggFunc    parser.AggFunc    // for Aggregate/Downsample nodes
}

// Planner creates physical execution plans from parsed queries.
type Planner struct {
	tables []compaction.TableInfo
}

func New(tables []compaction.TableInfo) *Planner {
	return &Planner{tables: tables}
}

// Plan creates a physical execution plan for the given query.
func (p *Planner) Plan(q *parser.Query) (*PlanNode, error) {
	if q.Metric == "" {
		return nil, fmt.Errorf("query must specify a metric")
	}

	seriesKey := q.SeriesKey()
	tr := storage.TimeRange{Start: q.Start, End: q.End}

	// Build leaf scan nodes
	var scanNodes []*PlanNode

	// Always scan memtable
	scanNodes = append(scanNodes, &PlanNode{
		Type:      NodeMemtableScan,
		SeriesKey: seriesKey,
		TimeRange: tr,
	})

	// Scan SSTables that overlap the time range (predicate pushdown)
	for _, table := range p.tables {
		tableTR := storage.TimeRange{Start: table.Meta.MinTime, End: table.Meta.MaxTime}
		if !tableTR.Overlaps(tr) {
			continue // prune — SSTable doesn't overlap query range
		}
		scanNodes = append(scanNodes, &PlanNode{
			Type:      NodeSSTableScan,
			SeriesKey: seriesKey,
			TimeRange: tr,
			SSTPath:   table.Meta.Path,
		})
	}

	// Merge node
	var root *PlanNode
	if len(scanNodes) == 1 {
		root = scanNodes[0]
	} else {
		root = &PlanNode{
			Type:      NodeMerge,
			Children:  scanNodes,
			SeriesKey: seriesKey,
			TimeRange: tr,
		}
	}

	// Downsample node (if requested)
	if q.Downsample > 0 {
		aggFunc := q.Aggregate
		if aggFunc == "" {
			aggFunc = parser.AggAvg // default downsample aggregation
		}
		root = &PlanNode{
			Type:      NodeDownsample,
			Children:  []*PlanNode{root},
			SeriesKey: seriesKey,
			TimeRange: tr,
			Interval:  q.Downsample.Milliseconds(),
			AggFunc:   aggFunc,
		}
	} else if q.Aggregate != "" {
		// Aggregate without downsample — aggregate over the entire range
		root = &PlanNode{
			Type:      NodeAggregate,
			Children:  []*PlanNode{root},
			SeriesKey: seriesKey,
			TimeRange: tr,
			AggFunc:   q.Aggregate,
		}
	}

	return root, nil
}

// Explain returns a human-readable string representation of the plan.
func Explain(node *PlanNode, indent int) string {
	prefix := ""
	for i := 0; i < indent; i++ {
		prefix += "  "
	}

	var desc string
	switch node.Type {
	case NodeMemtableScan:
		desc = fmt.Sprintf("MemtableScan(key=%s, range=[%d,%d])",
			node.SeriesKey, node.TimeRange.Start, node.TimeRange.End)
	case NodeSSTableScan:
		desc = fmt.Sprintf("SSTableScan(key=%s, path=%s, range=[%d,%d])",
			node.SeriesKey, node.SSTPath, node.TimeRange.Start, node.TimeRange.End)
	case NodeMerge:
		desc = fmt.Sprintf("Merge(%d sources)", len(node.Children))
	case NodeDownsample:
		desc = fmt.Sprintf("Downsample(interval=%dms, agg=%s)", node.Interval, node.AggFunc)
	case NodeAggregate:
		desc = fmt.Sprintf("Aggregate(func=%s)", node.AggFunc)
	}

	result := prefix + desc + "\n"
	for _, child := range node.Children {
		result += Explain(child, indent+1)
	}
	return result
}
