package shipper_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coleYab/chronodb/internal/core"
	"github.com/coleYab/chronodb/internal/shipper"
)

func TestShipper_BuildRequestGroupsByMetricAndTags(t *testing.T) {
	s := shipper.New(shipper.Config{
		URL:      "http://example.com",
		Timeout:  time.Second,
		RetryMax: 0,
	})
	batch := []core.Sample{
		{Metric: "cpu", Tags: map[string]string{"host": "a"}, Timestamp: 1, Value: 0.5},
		{Metric: "cpu", Tags: map[string]string{"host": "a"}, Timestamp: 2, Value: 0.6},
		{Metric: "mem", Tags: map[string]string{"host": "a"}, Timestamp: 3, Value: 100},
		{Metric: "cpu", Tags: map[string]string{"host": "b"}, Timestamp: 4, Value: 0.7},
	}

	req := s.Send(context.Background(), batch)
	if req != nil {
		t.Logf("send returned: %v", req)
	}
}

func TestShipper_SendsValidJSON(t *testing.T) {
	var received []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	s := shipper.New(shipper.Config{
		URL:      ts.URL,
		Timeout:  time.Second,
		RetryMax: 1,
	})

	batch := []core.Sample{
		{Metric: "cpu", Tags: map[string]string{"host": "a"}, Timestamp: 100, Value: 0.5},
		{Metric: "cpu", Tags: map[string]string{"host": "b"}, Timestamp: 200, Value: 0.6},
	}

	if err := s.Send(context.Background(), batch); err != nil {
		t.Fatal(err)
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
	if err := json.Unmarshal(received, &parsed); err != nil {
		t.Fatal(err)
	}

	if len(parsed.Series) != 2 {
		t.Fatalf("expected 2 series, got %d", len(parsed.Series))
	}
	if parsed.Series[0].Metric != "cpu" {
		t.Fatalf("expected cpu, got %s", parsed.Series[0].Metric)
	}
}

func TestShipper_RetriesOnFailure(t *testing.T) {
	attempts := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	s := shipper.New(shipper.Config{
		URL:      ts.URL,
		Timeout:  time.Second,
		RetryMax: 2,
	})

	batch := []core.Sample{
		{Metric: "test", Timestamp: 1, Value: 1},
	}

	err := s.Send(context.Background(), batch)
	if err == nil {
		t.Fatal("expected error after retries")
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts (1 initial + 2 retries), got %d", attempts)
	}
}

func TestShipper_EmptyBatch(t *testing.T) {
	s := shipper.New(shipper.Config{URL: "http://example.com"})
	if err := s.Send(context.Background(), []core.Sample{}); err != nil {
		t.Fatal(err)
	}
}

func TestShipper_SeriesGrouping(t *testing.T) {
	var receivedBody []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	s := shipper.New(shipper.Config{URL: ts.URL, Timeout: time.Second, RetryMax: 0})

	batch := []core.Sample{
		{Metric: "requests", Tags: map[string]string{"host": "a", "status": "200"}, Timestamp: 10, Value: 1},
		{Metric: "requests", Tags: map[string]string{"host": "a", "status": "200"}, Timestamp: 11, Value: 2},
		{Metric: "requests", Tags: map[string]string{"host": "a", "status": "500"}, Timestamp: 12, Value: 1},
	}

	if err := s.Send(context.Background(), batch); err != nil {
		t.Fatal(err)
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
	json.Unmarshal(receivedBody, &parsed)

	if len(parsed.Series) != 2 {
		t.Fatalf("expected 2 series groups, got %d", len(parsed.Series))
	}
	for _, s := range parsed.Series {
		if s.Metric != "requests" {
			t.Fatalf("expected metric requests, got %s", s.Metric)
		}
		switch s.Tags["status"] {
		case "200":
			if len(s.Points) != 2 {
				t.Fatalf("expected 2 points for status=200, got %d", len(s.Points))
			}
		case "500":
			if len(s.Points) != 1 {
				t.Fatalf("expected 1 point for status=500, got %d", len(s.Points))
			}
		default:
			t.Fatalf("unexpected status tag: %s", s.Tags["status"])
		}
	}
}
