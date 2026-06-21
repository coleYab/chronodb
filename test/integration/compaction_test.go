package integration

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/coleYab/chronodb/internal/engine"
	"github.com/coleYab/chronodb/internal/manifest"
	"github.com/coleYab/chronodb/internal/memtable"
	"github.com/coleYab/chronodb/internal/query"
	"github.com/coleYab/chronodb/internal/segment"
)

func TestCompaction(t *testing.T) {
	dir := t.TempDir()
	cfg := engine.DefaultConfig()
	cfg.WALPath = filepath.Join(dir, "test.wal")
	cfg.ManifestPath = filepath.Join(dir, "manifest.json")
	cfg.DataDir = dir
	cfg.MemtableSize = 200
	cfg.FlushInterval = 50 * time.Millisecond

	e, cancel, done := runEngine(t, cfg)
	sid := uint64(1)

	for i := 0; i < 50; i++ {
		submitWrite(t, e, sid, int64(i), float64(i))
	}

	time.Sleep(1 * time.Second)

	initialSegments := len(e.Manifest().List())

	if initialSegments < 2 {
		t.Skip("need at least 2 segments to compact")
	}

	respCh := make(chan engine.Response, 1)
	cmd := engine.Command{
		Kind:   engine.CompactionCmd,
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
	if n == 0 {
		t.Skip("compaction skipped")
	}

	time.Sleep(500 * time.Millisecond)

	afterSegments := e.Manifest().NumSegments()
	if afterSegments >= initialSegments {
		t.Fatalf("expected fewer segments after compaction: before=%d after=%d",
			initialSegments, afterSegments)
	}

	segPaths := make([]string, 0)
	for _, entry := range e.Manifest().List() {
		segPaths = append(segPaths, entry.Path)
	}

	segPts := query.ReadSegments(segPaths, sid)
	activePts := e.GetPoints(sid)

	merged := query.Merge(segPts, activePts)
	if len(merged) == 0 {
		t.Fatal("expected points after compaction")
	}

	cancel()
	<-done
}

func TestCompactionMergesDataCorrectly(t *testing.T) {
	dir := t.TempDir()

	writeSegment := func(path string, data map[uint64][]memtable.Point, blockStart, blockEnd int64) {
		var list []segment.SeriesPoints
		for id, pts := range data {
			list = append(list, segment.SeriesPoints{SeriesID: id, Points: pts})
		}
		w, _ := segment.NewWriter(path)
		w.Write(list, blockStart, blockEnd)
		w.Close()
	}

	seg1 := filepath.Join(dir, "seg1.seg")
	seg2 := filepath.Join(dir, "seg2.seg")
	writeSegment(seg1, map[uint64][]memtable.Point{
		1: {{Timestamp: 100, Value: 1.0}, {Timestamp: 300, Value: 3.0}},
	}, 100, 300)
	writeSegment(seg2, map[uint64][]memtable.Point{
		1: {{Timestamp: 200, Value: 2.0}, {Timestamp: 400, Value: 4.0}},
		2: {{Timestamp: 150, Value: 1.5}},
	}, 150, 400)

	cfg := engine.DefaultConfig()
	cfg.WALPath = filepath.Join(dir, "wal")
	cfg.ManifestPath = filepath.Join(dir, "manifest.json")
	cfg.DataDir = dir
	cfg.FlushInterval = 1 * time.Hour

	e, cancel, done := runEngine(t, cfg)

	e.Manifest().Add(manifest.SegmentEntry{
		Path: seg1, BlockStart: 100, BlockEnd: 300, SeriesCount: 1,
	})
	e.Manifest().Add(manifest.SegmentEntry{
		Path: seg2, BlockStart: 150, BlockEnd: 400, SeriesCount: 2,
	})

	respCh := make(chan engine.Response, 1)
	cmd := engine.Command{
		Kind:   engine.CompactionCmd,
		RespCh: respCh,
	}
	if err := e.Submit(cmd); err != nil {
		t.Fatal(err)
	}
	resp := <-respCh
	if resp.Err != nil {
		t.Fatal(resp.Err)
	}

	time.Sleep(500 * time.Millisecond)

	segPaths := make([]string, 0)
	for _, entry := range e.Manifest().List() {
		segPaths = append(segPaths, entry.Path)
	}

	pts := query.ReadSegments(segPaths, 1)
	if len(pts) != 4 {
		t.Fatalf("series 1: expected 4 points after compaction, got %d", len(pts))
	}
	for i, pt := range pts {
		expected := float64(i + 1)
		if pt.Value != expected {
			t.Fatalf("point %d: expected %f, got %f", i, expected, pt.Value)
		}
	}

	if _, err := os.Stat(seg1); !os.IsNotExist(err) {
		t.Error("old segment seg1.seg should be deleted")
	}
	if _, err := os.Stat(seg2); !os.IsNotExist(err) {
		t.Error("old segment seg2.seg should be deleted")
	}

	cancel()
	<-done
}
