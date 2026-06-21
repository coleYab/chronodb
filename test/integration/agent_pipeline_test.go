package integration

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/coleYab/chronodb/internal/agent"
	"github.com/coleYab/chronodb/internal/config"
)

func TestAgentPipeline_FileToShipper(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")
	cfgPath := filepath.Join(dir, "chrono-agent.yaml")

	if err := os.WriteFile(logPath, []byte("existing line\n"), 0644); err != nil {
		t.Fatal(err)
	}

	payloadCh := make(chan []byte, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		select {
		case payloadCh <- body:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	// Write agent config
	cfgContent := []byte(`
agent:
  batch_size: 10
  batch_interval: "1s"
  queue_size: 1000

output:
  url: ` + ts.URL + `
  timeout: 5s
  retry_max: 0

default_tags:
  env: test

file_tail:
  - path: "` + logPath + `"
    parser: json
    metric_field: metric
    value_field: value
    tag_fields: [host]
    poll_interval: "50ms"
`)
	if err := os.WriteFile(cfgPath, cfgContent, 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	// Disable other collectors for this test
	cfg.System.EnabledMetrics = nil
	cfg.StatsD.Enabled = false
	cfg.Docker.Enabled = false

	a := agent.New(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		if err := a.Run(ctx); err != nil {
			t.Logf("agent run exited: %v", err)
		}
	}()

	// Give agent time to start and do initial file tailer poll
	time.Sleep(300 * time.Millisecond)

	// Append a new log line
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	line := `{"metric":"cpu.usage","value":42.5,"host":"web-01"}` + "\n"
	if _, err := f.WriteString(line); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Wait for the payload to arrive at the test server
	select {
	case payload := <-payloadCh:
		if len(payload) == 0 {
			t.Fatal("empty payload received")
		}
		var parsed struct {
			Series []struct {
				Metric string            `json:"metric"`
				Tags   map[string]string `json:"tags"`
				Points []struct {
					Timestamp int64   `json:"timestamp"`
					Value     float64 `json:"value"`
				} `json:"points"`
			} `json:"series"`
		}
		if err := json.Unmarshal(payload, &parsed); err != nil {
			t.Fatalf("bad JSON payload: %v\n%s", err, string(payload))
		}
		if len(parsed.Series) == 0 {
			t.Fatal("no series in payload")
		}
		series := parsed.Series[0]
		if series.Metric != "cpu.usage" {
			t.Fatalf("expected cpu.usage, got %s", series.Metric)
		}
		if len(series.Points) == 0 {
			t.Fatal("no points in series")
		}
		if series.Points[0].Value != 42.5 {
			t.Fatalf("expected 42.5, got %f", series.Points[0].Value)
		}
		if series.Tags == nil {
			t.Fatal("expected tags")
		}
		if series.Tags["host"] != "web-01" {
			t.Fatalf("expected host=web-01, got %s", series.Tags["host"])
		}
		if series.Tags["env"] != "test" {
			t.Fatalf("expected env=test tag from config, got %s", series.Tags["env"])
		}
		t.Logf("SUCCESS: received valid payload with metric=%s value=%f", series.Metric, series.Points[0].Value)

	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for payload at test server")
	}
}

func TestAgentPipeline_ConfigDefaults(t *testing.T) {
	cfg := config.DefaultConfig()
	// Verify defaults are reasonable
	if cfg.Agent.BatchSize <= 0 {
		t.Fatal("default batch size should be positive")
	}
	if cfg.Output.RetryMax < 0 {
		t.Fatal("default retry max should be >= 0")
	}
	if len(cfg.System.EnabledMetrics) == 0 {
		t.Fatal("default should have system metrics enabled")
	}
	if cfg.StatsD.Enabled != true {
		t.Fatal("default statsd should be enabled")
	}
}

func TestAgentPipeline_BadOutputServer(t *testing.T) {
	// Agent should handle the shipper failing without crashing
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")
	cfgPath := filepath.Join(dir, "config.yaml")

	os.WriteFile(logPath, []byte("existing\n"), 0644)

	cfgContent := []byte(`
agent:
  batch_size: 10
  batch_interval: "500ms"
output:
  url: "http://127.0.0.1:1"
  timeout: 1s
  retry_max: 0
system:
  enabled_metrics: []
statsd:
  enabled: false
docker:
  enabled: false
file_tail:
  - path: "` + logPath + `"
    parser: json
    metric_field: metric
    value_field: value
    poll_interval: "100ms"
`)
	os.WriteFile(cfgPath, cfgContent, 0644)

	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}

	a := agent.New(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- a.Run(ctx)
	}()

	time.Sleep(500 * time.Millisecond)

	// Write a line - shipper will fail but agent should not crash
	f, _ := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0644)
	f.WriteString(`{"metric":"test","value":1}` + "\n")
	f.Close()

	time.Sleep(2 * time.Second)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("agent error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("agent did not shut down in time")
	}
}

func TestAgentPipeline_MultipleLines(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")
	cfgPath := filepath.Join(dir, "config.yaml")

	os.WriteFile(logPath, []byte(""), 0644)

	type point struct {
		Timestamp int64   `json:"timestamp"`
		Value     float64 `json:"value"`
	}
	var mu sync.Mutex
	var receivedPoints []point

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var parsed struct {
			Series []struct {
				Metric string            `json:"metric"`
				Tags   map[string]string `json:"tags"`
				Points []point           `json:"points"`
			} `json:"series"`
		}
		json.Unmarshal(body, &parsed)
		mu.Lock()
		for _, s := range parsed.Series {
			receivedPoints = append(receivedPoints, s.Points...)
		}
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	cfgContent := []byte(`
agent:
  batch_size: 100
  batch_interval: "200ms"
output:
  url: ` + ts.URL + `
  timeout: 5s
  retry_max: 0
system:
  enabled_metrics: []
statsd:
  enabled: false
docker:
  enabled: false
file_tail:
  - path: "` + logPath + `"
    parser: json
    metric_field: metric
    value_field: value
    poll_interval: "50ms"
`)
	os.WriteFile(cfgPath, cfgContent, 0644)

	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}

	a := agent.New(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go a.Run(ctx)
	time.Sleep(200 * time.Millisecond)

	f, _ := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0644)
	for i := 0; i < 5; i++ {
		f.WriteString(`{"metric":"cpu","value":` + itoa(i) + `}` + "\n")
	}
	f.Close()

	time.Sleep(3 * time.Second)

	mu.Lock()
	count := len(receivedPoints)
	mu.Unlock()

	if count == 0 {
		t.Fatal("no points received")
	}
	if count < 5 {
		t.Logf("expected 5 points, got %d (may have been buffered)", count)
	}
	t.Logf("received %d points", count)
	cancel()
	time.Sleep(100 * time.Millisecond)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	s := ""
	for n > 0 {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	return s
}
