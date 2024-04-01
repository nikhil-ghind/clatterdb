// Package parser implements a query language parser for ClatterDB.
//
// Query language (simplified PromQL-like):
//
//   SELECT metric_name{label="value"} [start:end] [DOWNSAMPLE interval] [AGG func]
//
// Examples:
//   SELECT http_requests_total{method="GET"} [1h]
//   SELECT cpu_usage{host="web-1"} [2024-01-01:2024-01-02] DOWNSAMPLE 5m AVG
//   SELECT memory_bytes [30m] DOWNSAMPLE 1m MAX
//   SELECT disk_io_total{device="sda"} [1d] RATE
package parser

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// QueryType represents the type of query.
type QueryType int

const (
	InstantQuery QueryType = iota
	RangeQuery
)

// AggFunc represents an aggregation function.
type AggFunc string

const (
	AggNone  AggFunc = ""
	AggAvg   AggFunc = "AVG"
	AggSum   AggFunc = "SUM"
	AggMin   AggFunc = "MIN"
	AggMax   AggFunc = "MAX"
	AggCount AggFunc = "COUNT"
	AggRate  AggFunc = "RATE" // per-second rate of change
	AggLast  AggFunc = "LAST"
)

// Query is the parsed representation of a query string.
type Query struct {
	Type       QueryType
	Metric     string
	Labels     map[string]string
	Start      int64         // milliseconds
	End        int64         // milliseconds
	Downsample time.Duration // 0 means no downsampling
	Aggregate  AggFunc
}

var (
	// Matches: metric_name{key="value", key2="value2"}
	metricPattern = regexp.MustCompile(`^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?$`)
	// Matches: key="value"
	labelPattern = regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)="([^"]*)"`)
	// Matches: [30m] or [1h] or [1d] or [2024-01-01:2024-01-02]
	rangePattern = regexp.MustCompile(`\[([^\]]+)\]`)
	// Matches: DOWNSAMPLE 5m
	downsamplePattern = regexp.MustCompile(`(?i)DOWNSAMPLE\s+(\d+[smhd])`)
)

// Parse parses a query string into a Query struct.
func Parse(input string) (*Query, error) {
	input = strings.TrimSpace(input)

	// Strip SELECT prefix if present
	if strings.HasPrefix(strings.ToUpper(input), "SELECT ") {
		input = strings.TrimSpace(input[7:])
	}

	q := &Query{
		Type:   RangeQuery,
		Labels: make(map[string]string),
		End:    time.Now().UnixMilli(),
	}

	// Extract aggregation function
	for _, agg := range []AggFunc{AggAvg, AggSum, AggMin, AggMax, AggCount, AggRate, AggLast} {
		upper := strings.ToUpper(input)
		if strings.HasSuffix(upper, " "+string(agg)) {
			q.Aggregate = agg
			input = strings.TrimSpace(input[:len(input)-len(agg)-1])
			break
		}
	}

	// Extract downsample
	if matches := downsamplePattern.FindStringSubmatch(input); len(matches) == 2 {
		dur, err := parseDuration(matches[1])
		if err != nil {
			return nil, fmt.Errorf("invalid downsample duration %q: %w", matches[1], err)
		}
		q.Downsample = dur
		input = downsamplePattern.ReplaceAllString(input, "")
		input = strings.TrimSpace(input)
	}

	// Extract time range [...]
	if matches := rangePattern.FindStringSubmatch(input); len(matches) == 2 {
		rangeStr := matches[1]
		if err := q.parseTimeRange(rangeStr); err != nil {
			return nil, fmt.Errorf("invalid time range %q: %w", rangeStr, err)
		}
		input = rangePattern.ReplaceAllString(input, "")
		input = strings.TrimSpace(input)
	} else {
		// Default: last 1 hour
		q.Start = q.End - 3600*1000
	}

	// Parse metric name and labels
	if err := q.parseMetric(input); err != nil {
		return nil, err
	}

	return q, nil
}

func (q *Query) parseMetric(s string) error {
	s = strings.TrimSpace(s)
	matches := metricPattern.FindStringSubmatch(s)
	if len(matches) < 2 {
		return fmt.Errorf("invalid metric selector: %q", s)
	}

	q.Metric = matches[1]

	if len(matches) >= 3 && matches[2] != "" {
		labelsStr := matches[2][1 : len(matches[2])-1] // strip { }
		labelMatches := labelPattern.FindAllStringSubmatch(labelsStr, -1)
		for _, lm := range labelMatches {
			q.Labels[lm[1]] = lm[2]
		}
	}

	return nil
}

func (q *Query) parseTimeRange(s string) error {
	// Check for absolute range: start:end
	if strings.Contains(s, ":") {
		parts := strings.SplitN(s, ":", 2)
		startTime, err := parseTimestamp(parts[0])
		if err != nil {
			return fmt.Errorf("invalid start time: %w", err)
		}
		endTime, err := parseTimestamp(parts[1])
		if err != nil {
			return fmt.Errorf("invalid end time: %w", err)
		}
		q.Start = startTime
		q.End = endTime
		return nil
	}

	// Relative range: e.g., "30m", "1h", "7d"
	dur, err := parseDuration(s)
	if err != nil {
		return err
	}
	q.Start = q.End - dur.Milliseconds()
	return nil
}

// SeriesKey returns the series key for this query (metric + labels).
func (q *Query) SeriesKey() string {
	if len(q.Labels) == 0 {
		return q.Metric
	}
	key := q.Metric + "{"
	first := true
	for k, v := range q.Labels {
		if !first {
			key += ","
		}
		key += fmt.Sprintf("%s=%q", k, v)
		first = false
	}
	key += "}"
	return key
}

func parseDuration(s string) (time.Duration, error) {
	if len(s) < 2 {
		return 0, fmt.Errorf("duration too short: %q", s)
	}

	unit := s[len(s)-1]
	numStr := s[:len(s)-1]
	num, err := strconv.Atoi(numStr)
	if err != nil {
		return 0, fmt.Errorf("invalid number in duration: %w", err)
	}

	switch unit {
	case 's':
		return time.Duration(num) * time.Second, nil
	case 'm':
		return time.Duration(num) * time.Minute, nil
	case 'h':
		return time.Duration(num) * time.Hour, nil
	case 'd':
		return time.Duration(num) * 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("unknown duration unit: %c", unit)
	}
}

func parseTimestamp(s string) (int64, error) {
	s = strings.TrimSpace(s)

	// Try RFC3339
	t, err := time.Parse(time.RFC3339, s)
	if err == nil {
		return t.UnixMilli(), nil
	}

	// Try date only
	t, err = time.Parse("2006-01-02", s)
	if err == nil {
		return t.UnixMilli(), nil
	}

	// Try unix timestamp
	ts, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		if ts > 1e12 {
			return ts, nil // already milliseconds
		}
		return ts * 1000, nil // seconds → milliseconds
	}

	return 0, fmt.Errorf("cannot parse timestamp: %q", s)
}
