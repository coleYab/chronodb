package collector_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/coleYab/chronodb/internal/collector"
	"github.com/coleYab/chronodb/internal/core"
)

func TestSysCollector_EmitsCpuUsage(t *testing.T) {
	if _, err := os.Stat("/proc/stat"); err != nil {
		t.Skip("/proc/stat not available")
	}

	sc := collector.NewSysCollector(collector.SysCollectorConfig{
		Interval:    50 * time.Millisecond,
		Enabled:     []string{"cpu"},
		DefaultTags: map[string]string{"host": "test"},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	sampleCh := make(chan core.Sample, 10)
	go sc.Run(ctx, sampleCh)

	// Wait for at least one sample (after the first/noop poll)
	var found bool
	deadline := time.After(2 * time.Second)
	for {
		select {
		case s := <-sampleCh:
			if s.Metric == "system.cpu.usage" {
				found = true
				if s.Value < 0 || s.Value > 100 {
					t.Fatalf("cpu usage out of range: %f", s.Value)
				}
				if s.Tags["host"] != "test" {
					t.Fatalf("expected host=test tag")
				}
			}
		case <-deadline:
			if !found {
				t.Fatal("no cpu usage sample received")
			}
			return
		}
	}
}

func TestSysCollector_EmitsMemory(t *testing.T) {
	if _, err := os.Stat("/proc/meminfo"); err != nil {
		t.Skip("/proc/meminfo not available")
	}

	sc := collector.NewSysCollector(collector.SysCollectorConfig{
		Interval:    50 * time.Millisecond,
		Enabled:     []string{"memory"},
		DefaultTags: map[string]string{"host": "test"},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	sampleCh := make(chan core.Sample, 10)
	go sc.Run(ctx, sampleCh)

	metrics := map[string]bool{
		"system.memory.total":          false,
		"system.memory.available":      false,
		"system.memory.used":           false,
		"system.memory.used_percent":   false,
	}

	deadline := time.After(2 * time.Second)
	for {
		select {
		case s := <-sampleCh:
			if _, ok := metrics[s.Metric]; ok {
				metrics[s.Metric] = true
				if s.Value <= 0 {
					t.Fatalf("%s should be positive, got %f", s.Metric, s.Value)
				}
			}
		case <-deadline:
			all := true
			for m, found := range metrics {
				if !found {
					all = false
					t.Logf("missing: %s", m)
				}
			}
			if !all {
				t.Fatal("not all memory metrics received")
			}
			return
		}
	}
}

func TestSysCollector_EmitsDisk(t *testing.T) {
	if _, err := os.Stat("/proc/diskstats"); err != nil {
		t.Skip("/proc/diskstats not available")
	}

	sc := collector.NewSysCollector(collector.SysCollectorConfig{
		Interval:    50 * time.Millisecond,
		Enabled:     []string{"disk"},
		DefaultTags: map[string]string{"host": "test"},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	sampleCh := make(chan core.Sample, 10)
	go sc.Run(ctx, sampleCh)

	var found bool
	deadline := time.After(2 * time.Second)
	for {
		select {
		case s := <-sampleCh:
			if s.Metric == "system.disk.reads" || s.Metric == "system.disk.writes" {
				found = true
				if s.Tags["device"] == "" {
					t.Fatal("disk sample missing device tag")
				}
			}
		case <-deadline:
			if !found {
				t.Fatal("no disk samples received")
			}
			return
		}
	}
}

func TestSysCollector_EmitsNet(t *testing.T) {
	if _, err := os.Stat("/proc/net/dev"); err != nil {
		t.Skip("/proc/net/dev not available")
	}

	sc := collector.NewSysCollector(collector.SysCollectorConfig{
		Interval:    50 * time.Millisecond,
		Enabled:     []string{"net"},
		DefaultTags: map[string]string{"host": "test"},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	sampleCh := make(chan core.Sample, 10)
	go sc.Run(ctx, sampleCh)

	var found bool
	deadline := time.After(2 * time.Second)
	for {
		select {
		case s := <-sampleCh:
			if s.Metric == "system.net.rx_bytes" || s.Metric == "system.net.tx_bytes" {
				found = true
				if s.Tags["interface"] == "" {
					t.Fatal("net sample missing interface tag")
				}
			}
		case <-deadline:
			if !found {
				t.Fatal("no net samples received")
			}
			return
		}
	}
}
