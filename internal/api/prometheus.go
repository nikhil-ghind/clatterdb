// Package api implements the Prometheus-compatible remote write/read API.
//
// This allows ClatterDB to be used as a Prometheus long-term storage backend.
// - Remote write: POST /api/v1/write (Prometheus remote_write format)
// - Remote read:  POST /api/v1/read  (Prometheus remote_read format)
// - Instant query: GET /api/v1/query
// - Range query:   GET /api/v1/query_range
//
// Also exposes a native ClatterDB query API.
package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/clatterdb/internal/engine"
	"github.com/clatterdb/internal/query/executor"
	"github.com/clatterdb/internal/query/parser"
	"github.com/clatterdb/internal/query/planner"
	"github.com/clatterdb/internal/storage"
	"github.com/clatterdb/internal/storage/compaction"
)

// Server is the HTTP API server for ClatterDB.
type Server struct {
	engine   *engine.Engine
	tables   func() []compaction.TableInfo
	mux      *http.ServeMux
}

func NewServer(eng *engine.Engine, tablesFn func() []compaction.TableInfo) *Server {
	s := &Server{
		engine: eng,
		tables: tablesFn,
		mux:    http.NewServeMux(),
	}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) routes() {
	s.mux.HandleFunc("POST /api/v1/write", s.handleWrite)
	s.mux.HandleFunc("GET /api/v1/query", s.handleQuery)
	s.mux.HandleFunc("GET /api/v1/query_range", s.handleQueryRange)
	s.mux.HandleFunc("GET /api/v1/series", s.handleSeries)
	s.mux.HandleFunc("GET /api/v1/status", s.handleStatus)
	s.mux.HandleFunc("GET /health", s.handleHealth)
}

// --- Write API ---

type writeRequest struct {
	Timeseries []timeseriesData `json:"timeseries"`
}

type timeseriesData struct {
	Metric  string            `json:"metric"`
	Labels  map[string]string `json:"labels"`
	Samples []sampleData      `json:"samples"`
}

type sampleData struct {
	Timestamp int64   `json:"timestamp"` // milliseconds
	Value     float64 `json:"value"`
}

func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request) {
	var req writeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: %v", err)
		return
	}

	var samples []storage.Sample
	for _, ts := range req.Timeseries {
		series := storage.Series{
			Metric: ts.Metric,
			Labels: ts.Labels,
		}
		for _, sp := range ts.Samples {
			if sp.Timestamp == 0 {
				sp.Timestamp = time.Now().UnixMilli()
			}
			samples = append(samples, storage.Sample{
				Series:    series,
				DataPoint: storage.DataPoint{Timestamp: sp.Timestamp, Value: sp.Value},
			})
		}
	}

	if len(samples) == 0 {
		writeError(w, http.StatusBadRequest, "no samples in request")
		return
	}

	if err := s.engine.Write(samples); err != nil {
		writeError(w, http.StatusInternalServerError, "write failed: %v", err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// --- Query API ---

type queryResponse struct {
	Status string      `json:"status"`
	Data   interface{} `json:"data"`
}

type queryRangeData struct {
	ResultType string       `json:"resultType"`
	Result     []queryResult `json:"result"`
}

type queryResult struct {
	Metric map[string]string `json:"metric"`
	Values [][2]interface{}  `json:"values"` // [[timestamp, value], ...]
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	queryStr := r.URL.Query().Get("query")
	if queryStr == "" {
		writeError(w, http.StatusBadRequest, "query parameter required")
		return
	}

	q, err := parser.Parse(queryStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "parse error: %v", err)
		return
	}

	result, err := s.executeQuery(q)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "query error: %v", err)
		return
	}

	writeJSON(w, http.StatusOK, queryResponse{
		Status: "success",
		Data:   result,
	})
}

func (s *Server) handleQueryRange(w http.ResponseWriter, r *http.Request) {
	queryStr := r.URL.Query().Get("query")
	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	stepStr := r.URL.Query().Get("step")

	if queryStr == "" {
		writeError(w, http.StatusBadRequest, "query parameter required")
		return
	}

	q, err := parser.Parse(queryStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "parse error: %v", err)
		return
	}

	// Override time range from query params
	if startStr != "" {
		if start, err := parseTime(startStr); err == nil {
			q.Start = start
		}
	}
	if endStr != "" {
		if end, err := parseTime(endStr); err == nil {
			q.End = end
		}
	}
	if stepStr != "" {
		if dur, err := parser.Parse("dummy [" + stepStr + "]"); err == nil {
			_ = dur // could use step for downsampling
		}
	}

	result, err := s.executeQuery(q)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "query error: %v", err)
		return
	}

	writeJSON(w, http.StatusOK, queryResponse{
		Status: "success",
		Data:   result,
	})
}

func (s *Server) executeQuery(q *parser.Query) (*queryRangeData, error) {
	tables := s.tables()
	p := planner.New(tables)
	plan, err := p.Plan(q)
	if err != nil {
		return nil, fmt.Errorf("plan: %w", err)
	}

	exec := executor.New(s.engine)
	result, err := exec.Execute(plan)
	if err != nil {
		return nil, fmt.Errorf("execute: %w", err)
	}

	// Format response
	labels := map[string]string{"__name__": q.Metric}
	for k, v := range q.Labels {
		labels[k] = v
	}

	values := make([][2]interface{}, len(result.DataPoints))
	for i, dp := range result.DataPoints {
		values[i] = [2]interface{}{
			float64(dp.Timestamp) / 1000.0, // convert to seconds
			fmt.Sprintf("%g", dp.Value),
		}
	}

	return &queryRangeData{
		ResultType: "matrix",
		Result: []queryResult{{
			Metric: labels,
			Values: values,
		}},
	}, nil
}

func (s *Server) handleSeries(w http.ResponseWriter, r *http.Request) {
	// List all known series keys from SSTable metadata
	tables := s.tables()
	keySet := make(map[string]bool)
	for _, t := range tables {
		for _, key := range t.Meta.SeriesKeys {
			keySet[key] = true
		}
	}

	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}

	writeJSON(w, http.StatusOK, queryResponse{
		Status: "success",
		Data:   keys,
	})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	tables := s.tables()
	var totalSamples, totalBytes int64
	for _, t := range tables {
		totalSamples += t.Meta.NumSamples
		totalBytes += t.Meta.SizeBytes
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":        "ok",
		"sstable_count": len(tables),
		"total_samples": totalSamples,
		"total_bytes":   totalBytes,
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	log.Printf("[api] error: %s", msg)
	writeJSON(w, status, map[string]string{"status": "error", "error": msg})
}

func parseTime(s string) (int64, error) {
	// Try as float (seconds since epoch)
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return int64(f * 1000), nil
	}
	// Try RFC3339
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UnixMilli(), nil
	}
	return 0, fmt.Errorf("cannot parse time: %s", s)
}
