package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	httpapi "github.com/coleYab/chronodb/api/http"
	"github.com/coleYab/chronodb/internal/engine"
	"github.com/coleYab/chronodb/internal/index"
	"github.com/coleYab/chronodb/internal/manifest"
	"github.com/coleYab/chronodb/internal/memtable"
	"github.com/coleYab/chronodb/internal/segment"
	"github.com/coleYab/chronodb/internal/series"
)

type testServer struct {
	engine   *engine.Engine
	registry *series.Registry
	http     *httptest.Server
	cancel   context.CancelFunc
	done     chan struct{}
}

func newTestServer(t *testing.T, dir string) *testServer {
	cfg := engine.DefaultConfig()
	cfg.WALPath = filepath.Join(dir, "test.wal")
	cfg.ManifestPath = filepath.Join(dir, "manifest.json")
	cfg.DataDir = dir
	cfg.FlushInterval = 1 * time.Hour

	e, err := engine.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		e.Run(ctx)
		close(done)
	}()

	reg := series.NewRegistry()
	idx := index.New()
	e.SetIndex(idx)
	h := httpapi.NewHandler(e, reg, idx)

	return &testServer{
		engine:   e,
		registry: reg,
		http:     httptest.NewServer(h),
		cancel:   cancel,
		done:     done,
	}
}

func (s *testServer) close() {
	s.http.Close()
	s.cancel()
	<-s.done
}

func (s *testServer) post(path string, body interface{}) (*http.Response, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		return nil, err
	}
	return s.http.Client().Post(s.http.URL+path, "application/json", &buf)
}

func (s *testServer) get(path string) (*http.Response, error) {
	return s.http.Client().Get(s.http.URL + path)
}

func TestEndToEnd(t *testing.T) {
	dir := t.TempDir()
	srv := newTestServer(t, dir)
	defer srv.close()

	// Phase 1: Write 100K points across 1K series
	t.Log("writing 100K points across 1K series...")
	for i := 0; i < 1000; i++ {
		host := fmt.Sprintf("host-%d", i%50)
		region := fmt.Sprintf("region-%d", i%5)
		metric := "cpu_usage"
		if i%2 == 0 {
			metric = "mem_usage"
		}

		body := map[string]interface{}{
			"series": []map[string]interface{}{
				{
					"metric": metric,
					"tags":   map[string]string{"host": host, "region": region},
					"points": []map[string]interface{}{
						{"timestamp": 100, "value": float64(i) * 1.0},
						{"timestamp": 200, "value": float64(i) * 2.0},
					},
				},
			},
		}

		resp, err := srv.post("/write", body)
		if err != nil {
			t.Fatalf("write request failed at series %d: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("write failed at series %d: %d", i, resp.StatusCode)
		}
	}

	srv.engine.Sync()

	// Verify metrics endpoint
	t.Log("verifying metrics list...")
	resp, err := srv.get("/metrics")
	if err != nil {
		t.Fatal(err)
	}
	var metrics []string
	json.NewDecoder(resp.Body).Decode(&metrics)
	resp.Body.Close()
	if len(metrics) != 2 {
		t.Fatalf("expected 2 metrics, got %d: %v", len(metrics), metrics)
	}

	// Phase 2: Query with tag filters
	t.Log("running query with tag filters...")
	qBody := map[string]interface{}{
		"metric": "cpu_usage",
		"tags":   map[string]string{"region": "region-0"},
		"start":  "1970-01-01T00:00:00Z",
		"end":    "1970-01-01T00:00:01Z",
	}

	resp, err = srv.post("/query", qBody)
	if err != nil {
		t.Fatal(err)
	}
	var queryResult struct {
		Results []struct {
			SeriesID uint64           `json:"series_id"`
			Buckets  []json.RawMessage `json:"buckets"`
			Err      string            `json:"error"`
		} `json:"results"`
	}
	json.NewDecoder(resp.Body).Decode(&queryResult)
	resp.Body.Close()

	if len(queryResult.Results) == 0 {
		t.Fatal("expected query results")
	}
	t.Logf("query returned %d results", len(queryResult.Results))

	// Phase 3: Durability — kill and restart
	t.Log("testing crash recovery durability...")
	srv.close()

	e2Cfg := engine.DefaultConfig()
	e2Cfg.WALPath = filepath.Join(dir, "test.wal")
	e2Cfg.ManifestPath = filepath.Join(dir, "manifest.json")
	e2Cfg.DataDir = dir
	e2Cfg.FlushInterval = 1 * time.Hour

	e2, err := engine.New(e2Cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.Recover(); err != nil {
		t.Fatal(err)
	}

	if e2.MemtableSize() == 0 {
		t.Fatal("expected recovered data in memtable")
	}
	e2.Close()
}

func TestEndToEndRetention(t *testing.T) {
	dir := t.TempDir()
	srv := newTestServer(t, dir)
	defer srv.close()

	// Write segments via the server
	body := map[string]interface{}{
		"series": []map[string]interface{}{
			{
				"metric": "test_metric",
				"tags":   map[string]string{"env": "test"},
				"points": []map[string]interface{}{
					{"timestamp": 100, "value": 1.0},
				},
			},
		},
	}

	resp, err := srv.post("/write", body)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("write failed: %d", resp.StatusCode)
	}

	// Create a segment with old data
	oldSeg := filepath.Join(dir, "old.seg")
	var segList []segment.SeriesPoints
	segList = append(segList, segment.SeriesPoints{
		SeriesID: 1,
		Points:   []memtable.Point{{Timestamp: 100, Value: 1.0}},
	})
	w, _ := segment.NewWriter(oldSeg)
	w.Write(segList, 100, 100)
	w.Close()

	srv.engine.Manifest().Add(manifest.SegmentEntry{
		Path:       oldSeg,
		BlockStart: 100,
		BlockEnd:   100,
	})

	// Run retention
	retRespCh := make(chan engine.Response, 1)
	retCmd := engine.Command{
		Kind: engine.RetentionCmd,
		Payload: engine.RetentionPayload{
			Cutoff: time.Now(),
		},
		RespCh: retRespCh,
	}
	if err := srv.engine.Submit(retCmd); err != nil {
		t.Fatal(err)
	}
	<-retRespCh

	if _, err := os.Stat(oldSeg); !os.IsNotExist(err) {
		t.Error("old segment should have been deleted by retention")
	}

	if srv.engine.Manifest().NumSegments() != 0 {
		t.Fatalf("expected 0 segments after retention, got %d", srv.engine.Manifest().NumSegments())
	}
}
