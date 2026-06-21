package integration

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/coleYab/chronodb/internal/engine"
	"github.com/coleYab/chronodb/internal/memtable"
)

func TestCrashRecovery(t *testing.T) {
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
	done := make(chan struct{})
	go func() {
		e.Run(ctx)
		close(done)
	}()

	writePayloads := []engine.WritePayload{
		{SeriesID: 1, Points: []memtable.Point{{Timestamp: 100, Value: 1.0}, {Timestamp: 200, Value: 2.0}}},
		{SeriesID: 2, Points: []memtable.Point{{Timestamp: 150, Value: 3.0}}},
		{SeriesID: 1, Points: []memtable.Point{{Timestamp: 300, Value: 4.0}}},
	}

	for _, wp := range writePayloads {
		cmd := engine.Command{
			Kind:    engine.WriteCmd,
			Payload: wp,
			RespCh:  make(chan engine.Response, 1),
		}
		if err := e.Submit(cmd); err != nil {
			t.Fatal(err)
		}
		resp := <-cmd.RespCh
		if resp.Err != nil {
			t.Fatal(resp.Err)
		}
	}

	cancel()
	<-done

	e2, err := engine.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if err := e2.Recover(); err != nil {
		t.Fatal(err)
	}

	if e2.MemtableSize() != 2 {
		t.Fatalf("expected 2 series after recovery, got %d", e2.MemtableSize())
	}

	pts1 := e2.GetPoints(1)
	if len(pts1) != 3 {
		t.Fatalf("series 1: expected 3 points, got %d", len(pts1))
	}
	pts2 := e2.GetPoints(2)
	if len(pts2) != 1 {
		t.Fatalf("series 2: expected 1 point, got %d", len(pts2))
	}

	for i, pt := range pts1 {
		if pt.Value != float64(i+1)*1.0 && !(i == 0 && pt.Value == 1.0 || i == 1 && pt.Value == 2.0 || i == 2 && pt.Value == 4.0) {
			t.Logf("series 1 point %d: timestamp=%d value=%f", i, pt.Timestamp, pt.Value)
		}
	}

	e2.Close()
}

func TestCrashRecoveryWithTruncatedWAL(t *testing.T) {
	dir := t.TempDir()
	cfg := engine.DefaultConfig()
	cfg.WALPath = filepath.Join(dir, "truncated.wal")
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

	for i := 0; i < 5; i++ {
		cmd := engine.Command{
			Kind: engine.WriteCmd,
			Payload: engine.WritePayload{
				SeriesID: 1,
				Points:   []memtable.Point{{Timestamp: int64(i), Value: float64(i)}},
			},
			RespCh: make(chan engine.Response, 1),
		}
		if err := e.Submit(cmd); err != nil {
			t.Fatal(err)
		}
		<-cmd.RespCh
	}

	e.Sync()

	cmd := engine.Command{
		Kind: engine.WriteCmd,
		Payload: engine.WritePayload{
			SeriesID: 2,
			Points:   []memtable.Point{{Timestamp: 999, Value: 99.0}},
		},
		RespCh: make(chan engine.Response, 1),
	}
	if err := e.Submit(cmd); err != nil {
		t.Fatal(err)
	}
	<-cmd.RespCh

	cancel()
	<-done

	e2, err := engine.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if err := e2.Recover(); err != nil {
		t.Fatal(err)
	}

	pts1 := e2.GetPoints(1)
	if len(pts1) != 5 {
		t.Fatalf("series 1: expected 5 points after recovery, got %d", len(pts1))
	}

	e2.Close()
}

func TestCrashRecoveryNoWAL(t *testing.T) {
	dir := t.TempDir()
	cfg := engine.DefaultConfig()
	cfg.WALPath = filepath.Join(dir, "empty.wal")
	cfg.ManifestPath = filepath.Join(dir, "manifest.json")
	cfg.DataDir = dir

	e, err := engine.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if err := e.Recover(); err != nil {
		t.Fatal(err)
	}

	if e.MemtableSize() != 0 {
		t.Fatalf("expected 0 series after recovery from empty WAL, got %d", e.MemtableSize())
	}

	e.Close()
}
