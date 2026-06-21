package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/coleYab/chronodb/internal/engine"
	"github.com/coleYab/chronodb/internal/manifest"
	"github.com/coleYab/chronodb/internal/memtable"
	"github.com/coleYab/chronodb/internal/segment"
)

func runEngine(t *testing.T, cfg engine.EngineConfig) (*engine.Engine, context.CancelFunc, chan struct{}) {
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
	return e, cancel, done
}

func submitWrite(t *testing.T, e *engine.Engine, seriesID uint64, ts int64, val float64) {
	cmd := engine.Command{
		Kind: engine.WriteCmd,
		Payload: engine.WritePayload{
			SeriesID: seriesID,
			Points:   []memtable.Point{{Timestamp: ts, Value: val}},
		},
		RespCh: make(chan engine.Response, 1),
	}
	if err := e.Submit(cmd); err != nil {
		t.Fatal(err)
	}
	resp := <-cmd.RespCh
	if resp.Err != nil {
		t.Fatal(resp.Err)
	}
}

func TestFlushPipeline(t *testing.T) {
	dir := t.TempDir()
	cfg := engine.DefaultConfig()
	cfg.WALPath = filepath.Join(dir, "test.wal")
	cfg.ManifestPath = filepath.Join(dir, "manifest.json")
	cfg.DataDir = dir
	cfg.MemtableSize = 800
	cfg.FlushInterval = 50 * time.Millisecond

	e, cancel, done := runEngine(t, cfg)

	for i := 0; i < 200; i++ {
		submitWrite(t, e, 1, int64(i), float64(i))
	}

	time.Sleep(500 * time.Millisecond)
	cancel()
	<-done

	entries := e.Manifest().List()
	if len(entries) == 0 {
		t.Fatal("expected at least 1 segment file after flush, got 0")
	}

	totalPoints := 0
	for _, entry := range entries {
		if _, err := os.Stat(entry.Path); os.IsNotExist(err) {
			t.Errorf("segment file %s does not exist on disk", entry.Path)
			continue
		}
		r, err := segment.OpenReader(entry.Path)
		if err != nil {
			t.Errorf("failed to open segment %s: %v", entry.Path, err)
			continue
		}
		pts, err := r.ReadSeries(1)
		r.Close()
		if err != nil {
			t.Errorf("failed to read series 1 from %s: %v", entry.Path, err)
			continue
		}
		totalPoints += len(pts)
	}

	if totalPoints == 0 {
		t.Fatal("expected points in segment files, got 0")
	}
}

func TestFlushPipelineMultipleSeries(t *testing.T) {
	dir := t.TempDir()
	cfg := engine.DefaultConfig()
	cfg.WALPath = filepath.Join(dir, "test.wal")
	cfg.ManifestPath = filepath.Join(dir, "manifest.json")
	cfg.DataDir = dir
	cfg.MemtableSize = 800
	cfg.FlushInterval = 50 * time.Millisecond

	e, cancel, done := runEngine(t, cfg)

	for i := 0; i < 100; i++ {
		submitWrite(t, e, uint64(i%5+1), int64(i), float64(i))
	}

	e.Sync()
	time.Sleep(500 * time.Millisecond)
	cancel()
	<-done

	entries := e.Manifest().List()
	if len(entries) == 0 {
		t.Fatal("expected at least 1 segment file after flush, got 0")
	}

	seen := make(map[uint64]bool)
	for _, entry := range entries {
		r, err := segment.OpenReader(entry.Path)
		if err != nil {
			t.Fatal(err)
		}
		all, err := r.ReadAll()
		r.Close()
		if err != nil {
			t.Fatal(err)
		}
		for id := range all {
			seen[id] = true
		}
	}

	for id := uint64(1); id <= 5; id++ {
		if !seen[id] {
			t.Errorf("series %d not found in any segment", id)
		}
	}
}

func TestFlushPipelineNoDuplicatePoints(t *testing.T) {
	dir := t.TempDir()
	cfg := engine.DefaultConfig()
	cfg.WALPath = filepath.Join(dir, "test.wal")
	cfg.ManifestPath = filepath.Join(dir, "manifest.json")
	cfg.DataDir = dir
	cfg.MemtableSize = 800
	cfg.FlushInterval = 50 * time.Millisecond

	e, cancel, done := runEngine(t, cfg)

	for i := 0; i < 100; i++ {
		submitWrite(t, e, 1, int64(i), float64(i))
	}

	e.Sync()
	time.Sleep(500 * time.Millisecond)
	cancel()
	<-done

	segmentPoints := 0
	for _, entry := range e.Manifest().List() {
		r, err := segment.OpenReader(entry.Path)
		if err != nil {
			continue
		}
		pts, _ := r.ReadSeries(1)
		segmentPoints += len(pts)
		r.Close()
	}

	recovered := recoveryTest(dir, cfg.WALPath, cfg.ManifestPath)
	memtablePoints := len(recovered.GetPoints(1))
	recovered.Close()

	total := segmentPoints + memtablePoints
	if total != 100 {
		t.Fatalf("expected 100 points total (segments + memtable after recovery), got %d (segments: %d, memtable: %d)",
			total, segmentPoints, memtablePoints)
	}
}

func TestFlushPipelineManifestPersists(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "manifest.json")

	cfg := engine.DefaultConfig()
	cfg.WALPath = filepath.Join(dir, "test.wal")
	cfg.ManifestPath = manifestPath
	cfg.DataDir = dir
	cfg.MemtableSize = 400
	cfg.FlushInterval = 50 * time.Millisecond

	e, cancel, done := runEngine(t, cfg)

	for i := 0; i < 50; i++ {
		submitWrite(t, e, 1, int64(i), float64(i))
	}

	time.Sleep(500 * time.Millisecond)
	cancel()
	<-done
	e.Close()

	m, err := manifest.Load(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if m.NumSegments() == 0 {
		t.Fatal("expected segments in persisted manifest")
	}
	if m.GetWALOffset() == 0 {
		t.Error("expected non-zero WAL offset in persisted manifest")
	}
	if _, err := os.Stat(m.List()[0].Path); os.IsNotExist(err) {
		t.Error("segment file referenced in manifest does not exist")
	}
}

func recoveryTest(dir, walPath, manifestPath string) *engine.Engine {
	cfg := engine.DefaultConfig()
	cfg.WALPath = walPath
	cfg.ManifestPath = manifestPath
	cfg.DataDir = dir
	cfg.FlushInterval = 1 * time.Hour

	e, err := engine.New(cfg)
	if err != nil {
		panic(err)
	}
	if err := e.Recover(); err != nil {
		panic(err)
	}
	return e
}
