package integration

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/coleYab/chronodb/internal/engine"
	"github.com/coleYab/chronodb/internal/manifest"
	"github.com/coleYab/chronodb/internal/memtable"
	"github.com/coleYab/chronodb/internal/segment"
)

func TestRetention(t *testing.T) {
	dir := t.TempDir()

	now := time.Now()
	past := now.Add(-30 * 24 * time.Hour)
	future := now.Add(time.Hour)

	oldSeg := filepath.Join(dir, "old.seg")
	newSeg := filepath.Join(dir, "new.seg")

	writePointsToSegment(t, oldSeg, map[uint64][]memtable.Point{
		1: {{Timestamp: 100, Value: 1.0}},
	}, past.UnixMilli(), past.UnixMilli())
	writePointsToSegment(t, newSeg, map[uint64][]memtable.Point{
		1: {{Timestamp: 200, Value: 2.0}},
	}, future.UnixMilli(), future.UnixMilli())

	cfg := engine.DefaultConfig()
	cfg.WALPath = filepath.Join(dir, "wal")
	cfg.ManifestPath = filepath.Join(dir, "manifest.json")
	cfg.DataDir = dir
	cfg.FlushInterval = 1 * time.Hour

	e, cancel, done := runEngine(t, cfg)

	e.Manifest().Add(manifest.SegmentEntry{
		Path:        oldSeg,
		BlockStart:  past.UnixMilli(),
		BlockEnd:    past.UnixMilli(),
		SeriesCount: 1,
	})
	e.Manifest().Add(manifest.SegmentEntry{
		Path:        newSeg,
		BlockStart:  future.UnixMilli(),
		BlockEnd:    future.UnixMilli(),
		SeriesCount: 1,
	})

	respCh := make(chan engine.Response, 1)
	cmd := engine.Command{
		Kind: engine.RetentionCmd,
		Payload: engine.RetentionPayload{
			Cutoff: now,
		},
		RespCh: respCh,
	}
	if err := e.Submit(cmd); err != nil {
		t.Fatal(err)
	}
	resp := <-respCh
	if resp.Err != nil {
		t.Fatal(resp.Err)
	}

	n := resp.Data.(int)
	if n != 1 {
		t.Fatalf("expected 1 expired segment, got %d", n)
	}

	if _, err := os.Stat(oldSeg); !os.IsNotExist(err) {
		t.Error("old segment should be deleted")
	}
	if _, err := os.Stat(newSeg); err != nil {
		t.Error("new segment should still exist")
	}

	if e.Manifest().NumSegments() != 1 {
		t.Fatalf("expected 1 segment in manifest, got %d", e.Manifest().NumSegments())
	}

	cancel()
	<-done
}

func TestRetentionNoExpired(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	seg := filepath.Join(dir, "seg.seg")
	writePointsToSegment(t, seg, map[uint64][]memtable.Point{
		1: {{Timestamp: 100, Value: 1.0}},
	}, now.UnixMilli(), now.UnixMilli())

	cfg := engine.DefaultConfig()
	cfg.WALPath = filepath.Join(dir, "wal")
	cfg.ManifestPath = filepath.Join(dir, "manifest.json")
	cfg.DataDir = dir
	cfg.FlushInterval = 1 * time.Hour

	e, cancel, done := runEngine(t, cfg)

	e.Manifest().Add(manifest.SegmentEntry{
		Path:       seg,
		BlockStart: now.UnixMilli(),
		BlockEnd:   now.UnixMilli(),
	})

	respCh := make(chan engine.Response, 1)
	cmd := engine.Command{
		Kind: engine.RetentionCmd,
		Payload: engine.RetentionPayload{
			Cutoff: now.Add(-365 * 24 * time.Hour),
		},
		RespCh: respCh,
	}
	if err := e.Submit(cmd); err != nil {
		t.Fatal(err)
	}
	resp := <-respCh
	if resp.Err != nil {
		t.Fatal(resp.Err)
	}

	n := resp.Data.(int)
	if n != 0 {
		t.Fatalf("expected 0 expired segments, got %d", n)
	}

	if _, err := os.Stat(seg); err != nil {
		t.Error("segment should not be deleted")
	}

	cancel()
	<-done
}

func writePointsToSegment(t *testing.T, path string, data map[uint64][]memtable.Point, blockStart, blockEnd int64) {
	var list []segment.SeriesPoints
	for id, pts := range data {
		list = append(list, segment.SeriesPoints{SeriesID: id, Points: pts})
	}
	w, err := segment.NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(list, blockStart, blockEnd); err != nil {
		w.Close()
		t.Fatal(err)
	}
	w.Close()
}
