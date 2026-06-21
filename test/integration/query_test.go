package integration

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/coleYab/chronodb/internal/engine"
	"github.com/coleYab/chronodb/internal/index"
	"github.com/coleYab/chronodb/internal/query"
)

func TestQueryAcrossSources(t *testing.T) {
	dir := t.TempDir()
	cfg := engine.DefaultConfig()
	cfg.WALPath = filepath.Join(dir, "test.wal")
	cfg.ManifestPath = filepath.Join(dir, "manifest.json")
	cfg.DataDir = dir
	cfg.MemtableSize = 400
	cfg.FlushInterval = 50 * time.Millisecond

	e, cancel, done := runEngine(t, cfg)
	sid := uint64(1)

	for i := 0; i < 60; i++ {
		submitWrite(t, e, sid, int64(i), float64(i))
	}

	time.Sleep(500 * time.Millisecond)

	for i := 60; i < 70; i++ {
		submitWrite(t, e, sid, int64(i), float64(i))
	}

	segPaths := []string{}
	for _, entry := range e.Manifest().List() {
		segPaths = append(segPaths, entry.Path)
	}

	segPts := query.ReadSegments(segPaths, sid)
	activePts := e.GetPoints(sid)
	frozenPts := e.FrozenPoints(sid)

	merged := query.Merge(segPts, activePts, frozenPts)
	filtered := query.Filter(merged, 0, 69)

	if len(filtered) != 70 {
		t.Fatalf("expected 70 points, got %d (segments: %d, active: %d, frozen: %d)",
			len(filtered), len(segPts), len(activePts), len(frozenPts))
	}

	for i, pt := range filtered {
		if pt.Timestamp != int64(i) || pt.Value != float64(i) {
			t.Fatalf("point %d: expected (%d,%f), got (%d,%f)",
				i, int64(i), float64(i), pt.Timestamp, pt.Value)
		}
	}

	buckets := query.Aggregate(filtered, 10)
	if len(buckets) != 7 {
		t.Fatalf("expected 7 buckets for 0-69 range, got %d", len(buckets))
	}
	if buckets[0].Count != 10 || buckets[6].Count != 10 {
		t.Fatalf("expected first and last bucket count 10, got %d and %d",
			buckets[0].Count, buckets[6].Count)
	}

	cancel()
	<-done
}

func TestQueryAggregation(t *testing.T) {
	dir := t.TempDir()
	cfg := engine.DefaultConfig()
	cfg.WALPath = filepath.Join(dir, "test.wal")
	cfg.ManifestPath = filepath.Join(dir, "manifest.json")
	cfg.DataDir = dir
	cfg.MemtableSize = 800
	cfg.FlushInterval = 100 * time.Millisecond

	e, cancel, done := runEngine(t, cfg)
	sid := uint64(1)

	for i := 0; i < 100; i++ {
		submitWrite(t, e, sid, int64(i), float64(i))
	}

	e.Sync()
	time.Sleep(500 * time.Millisecond)

	segPaths := []string{}
	for _, entry := range e.Manifest().List() {
		segPaths = append(segPaths, entry.Path)
	}

	segPts := query.ReadSegments(segPaths, sid)
	activePts := e.GetPoints(sid)
	frozenPts := e.FrozenPoints(sid)
	merged := query.Merge(segPts, activePts, frozenPts)
	filtered := query.Filter(merged, 0, 99)

	if len(filtered) != 100 {
		t.Fatalf("expected 100 points, got %d", len(filtered))
	}

	buckets := query.Aggregate(filtered, 10)
	if len(buckets) != 10 {
		t.Fatalf("expected 10 buckets, got %d", len(buckets))
	}

	for i, b := range buckets {
		expectedSum := float64(i*10+(i*10+9)) * 10 / 2
		if b.Sum != expectedSum {
			t.Fatalf("bucket %d [%d]: expected sum %f, got %f", i, b.Start, expectedSum, b.Sum)
		}
		if b.Count != 10 {
			t.Fatalf("bucket %d [%d]: expected count 10, got %d", i, b.Start, b.Count)
		}
		if b.Min != float64(i*10) {
			t.Fatalf("bucket %d [%d]: expected min %f, got %f", i, b.Start, float64(i*10), b.Min)
		}
		if b.Max != float64(i*10+9) {
			t.Fatalf("bucket %d [%d]: expected max %f, got %f", i, b.Start, float64(i*10+9), b.Max)
		}
	}

	cancel()
	<-done
}

func TestQueryEngineDispatch(t *testing.T) {
	dir := t.TempDir()
	cfg := engine.DefaultConfig()
	cfg.WALPath = filepath.Join(dir, "test.wal")
	cfg.ManifestPath = filepath.Join(dir, "manifest.json")
	cfg.DataDir = dir
	cfg.MemtableSize = 800
	cfg.FlushInterval = 100 * time.Millisecond

	e, cancel, done := runEngine(t, cfg)
	sid := uint64(42)

	// Populate index with the series
	idx := index.New()
	idx.Insert(sid, "cpu_load", map[string]string{"host": "s1", "env": "prod"})
	e.SetIndex(idx)

	for i := 0; i < 20; i++ {
		submitWrite(t, e, sid, int64(i), float64(i))
	}

	e.Sync()

	// Query through engine dispatch
	reqCh := make(chan engine.Response, 1)
	cmd := engine.Command{
		Kind: engine.QueryCmd,
		Payload: engine.QueryPayload{
			Metric:      "cpu_load",
			TagFilters:  map[string]string{"host": "s1"},
			StartTime:   time.UnixMilli(5),
			EndTime:     time.UnixMilli(14),
			Aggregation: "sum",
			BucketWidth: time.Second,
		},
		RespCh: reqCh,
	}
	if err := e.Submit(cmd); err != nil {
		t.Fatal(err)
	}
	resp := <-reqCh
	if resp.Err != nil {
		t.Fatal(resp.Err)
	}

	results, ok := resp.Data.([]query.Result)
	if !ok {
		t.Fatalf("expected []query.Result, got %T", resp.Data)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].SeriesID != sid {
		t.Fatalf("expected series %d, got %d", sid, results[0].SeriesID)
	}

	cancel()
	<-done
}
