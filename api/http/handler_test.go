package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coleYab/chronodb/internal/engine"
	"github.com/coleYab/chronodb/internal/index"
	"github.com/coleYab/chronodb/internal/memtable"
	"github.com/coleYab/chronodb/internal/series"
)

func setupTestServer(t *testing.T) (*Handler, *engine.Engine, context.CancelFunc) {
	dir := t.TempDir()
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
	go e.Run(ctx)

	reg := series.NewRegistry()
	idx := index.New()
	e.SetIndex(idx)
	h := NewHandler(e, reg, idx)
	return h, e, cancel
}

func doRequest(h *Handler, method, path, body string) *httptest.ResponseRecorder {
	var buf *bytes.Buffer
	if body != "" {
		buf = bytes.NewBufferString(body)
	} else {
		buf = bytes.NewBuffer(nil)
	}
	req := httptest.NewRequest(method, path, buf)
	w := httptest.NewRecorder()

	switch {
	case path == "/write" || strings.HasPrefix(path, "/write"):
		h.handleWrite(w, req)
	case path == "/query" || strings.HasPrefix(path, "/query"):
		h.handleQuery(w, req)
	case path == "/metrics" || strings.HasPrefix(path, "/metrics"):
		h.handleListMetrics(w, req)
	case path == "/engine/metrics" || strings.HasPrefix(path, "/engine/metrics"):
		h.handleEngineMetrics(w, req)
	case strings.HasPrefix(path, "/series"):
		h.handleSeries(w, req)
	case path == "/healthz" || strings.HasPrefix(path, "/healthz"):
		h.handleHealthz(w, req)
	case path == "/docs" || strings.HasPrefix(path, "/docs"):
		h.handleDocs(w, req)
	}
	return w
}

func TestWriteEndpoint(t *testing.T) {
	h, e, cancel := setupTestServer(t)
	defer cancel()

	body := `{
		"series": [{
			"metric": "cpu_usage",
			"tags": {"host": "server1"},
			"points": [
				{"timestamp": 100, "value": 0.5},
				{"timestamp": 200, "value": 0.8}
			]
		}]
	}`

	w := doRequest(h, "POST", "/write", body)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp writeResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Written != 1 {
		t.Fatalf("expected 1 series written, got %d", resp.Written)
	}
	if resp.Error != "" {
		t.Fatalf("unexpected error: %s", resp.Error)
	}

	metrics := h.registry.ListMetrics()
	if len(metrics) != 1 || metrics[0] != "cpu_usage" {
		t.Fatalf("expected [cpu_usage], got %v", metrics)
	}

	_ = e
}

func TestWriteInvalidJSON(t *testing.T) {
	h, _, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(h, "POST", "/write", "not json")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestWriteEmptyBody(t *testing.T) {
	h, _, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(h, "POST", "/write", `{}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestWriteWrongMethod(t *testing.T) {
	h, _, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(h, "GET", "/write", "")
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestQueryEndpoint(t *testing.T) {
	h, e, cancel := setupTestServer(t)
	defer cancel()

	// Pre-populate registry and index
	seriesID, _ := h.registry.GetOrCreate("cpu_usage", map[string]string{"host": "server1"})
	h.index.Insert(seriesID, "cpu_usage", map[string]string{"host": "server1"})

	// Write some data
	cmd := engine.Command{
		Kind: engine.WriteCmd,
		Payload: engine.WritePayload{
			SeriesID: seriesID,
			Points:   []memtable.Point{{Timestamp: 100, Value: 1.0}, {Timestamp: 200, Value: 2.0}},
		},
		RespCh: make(chan engine.Response, 1),
	}
	e.Submit(cmd)
	<-cmd.RespCh

	body := `{
		"metric": "cpu_usage",
		"tags": {"host": "server1"},
		"start": "1970-01-01T00:00:00Z",
		"end": "1970-01-01T00:00:01Z"
	}`

	w := doRequest(h, "POST", "/query", body)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp queryResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Results) == 0 {
		t.Fatal("expected results")
	}
}

func TestHealthz(t *testing.T) {
	h, _, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(h, "GET", "/healthz", "")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var body map[string]string
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "ok" {
		t.Fatalf("expected status ok, got %v", body)
	}
}

func TestMetrics(t *testing.T) {
	h, _, cancel := setupTestServer(t)
	defer cancel()

	h.index.Insert(1, "cpu", nil)
	h.index.Insert(2, "mem", nil)

	w := doRequest(h, "GET", "/metrics", "")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var metrics []string
	if err := json.NewDecoder(w.Body).Decode(&metrics); err != nil {
		t.Fatal(err)
	}
	if len(metrics) != 2 {
		t.Fatalf("expected 2 metrics, got %d", len(metrics))
	}
}

func TestDocs(t *testing.T) {
	h, _, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(h, "GET", "/docs", "")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "text/html; charset=utf-8" {
		t.Fatalf("expected text/html, got %s", ct)
	}
	if !strings.Contains(w.Body.String(), "ChronoDB API") {
		t.Fatal("expected docs page content")
	}
}
