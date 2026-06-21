package collector_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/coleYab/chronodb/internal/collector"
	"github.com/coleYab/chronodb/internal/core"
	"github.com/coleYab/chronodb/internal/transform"
)

func TestFileTailer_ReadsNewLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.log")

	if err := os.WriteFile(path, []byte("existing line\n"), 0644); err != nil {
		t.Fatal(err)
	}

	parser := transform.NewJSONParser(transform.JSONParserConfig{
		MetricField: "metric",
		ValueField:  "value",
	})

	ft := collector.NewFileTailer(collector.FileTailerConfig{
		Path:         path,
		Parser:       parser,
		PollInterval: 50 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sampleCh := make(chan core.Sample, 10)
	go ft.Run(ctx, sampleCh)

	time.Sleep(200 * time.Millisecond)

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"metric":"cpu","value":0.5}` + "\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	select {
	case s := <-sampleCh:
		if s.Metric != "cpu" {
			t.Fatalf("expected cpu, got %s", s.Metric)
		}
		if s.Value != 0.5 {
			t.Fatalf("expected 0.5, got %f", s.Value)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for sample")
	}
}

func TestFileTailer_HandlesLogRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.log")

	if err := os.WriteFile(path, []byte("old line\n"), 0644); err != nil {
		t.Fatal(err)
	}

	parser := transform.NewJSONParser(transform.JSONParserConfig{
		MetricField: "metric",
		ValueField:  "value",
	})

	ft := collector.NewFileTailer(collector.FileTailerConfig{
		Path:         path,
		Parser:       parser,
		PollInterval: 50 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sampleCh := make(chan core.Sample, 10)
	go ft.Run(ctx, sampleCh)

	time.Sleep(200 * time.Millisecond)

	// Simulate log rotation: rename old file, create new one with same name
	os.Rename(path, path+".1")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"metric":"cpu","value":0.9}` + "\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	select {
	case s := <-sampleCh:
		if s.Value != 0.9 {
			t.Fatalf("expected 0.9 after rotation, got %f", s.Value)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for sample after rotation")
	}
}

func TestFileTailer_IgnoresExistingContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.log")

	if err := os.WriteFile(path, []byte(`{"metric":"old","value":1}`+"\n"), 0644); err != nil {
		t.Fatal(err)
	}

	// Wait a bit so the file mtime is clearly in the past
	time.Sleep(50 * time.Millisecond)

	parser := transform.NewJSONParser(transform.JSONParserConfig{
		MetricField: "metric",
		ValueField:  "value",
	})

	ft := collector.NewFileTailer(collector.FileTailerConfig{
		Path:         path,
		Parser:       parser,
		PollInterval: 50 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sampleCh := make(chan core.Sample, 10)
	done := make(chan struct{})

	go func() {
		ft.Run(ctx, sampleCh)
		close(done)
	}()

	// Write a new line after tailer has started
	time.Sleep(200 * time.Millisecond)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"metric":"new","value":2}` + "\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	select {
	case s := <-sampleCh:
		if s.Metric != "new" {
			t.Fatalf("expected new metric, got %s", s.Metric)
		}
		// Ensure we didn't get the old line
		select {
		case s2 := <-sampleCh:
			if s2.Metric == "old" {
				t.Fatal("should not have read existing content")
			}
		default:
			// OK - no extra samples
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for new sample")
	}
}
