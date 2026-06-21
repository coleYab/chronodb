package compaction

import (
	"path/filepath"
	"testing"

	"github.com/coleYab/chronodb/internal/manifest"
	"github.com/coleYab/chronodb/internal/memtable"
	"github.com/coleYab/chronodb/internal/segment"
)

func TestSelectCandidates(t *testing.T) {
	t.Run("nil for <2 entries", func(t *testing.T) {
		if r := SelectCandidates(nil, 5); r != nil {
			t.Fatal("expected nil")
		}
		if r := SelectCandidates([]manifest.SegmentEntry{{Path: "a.seg"}}, 5); r != nil {
			t.Fatal("expected nil")
		}
	})

	t.Run("returns smallest N entries", func(t *testing.T) {
		entries := []manifest.SegmentEntry{
			{Path: "big.seg", SeriesCount: 100},
			{Path: "small.seg", SeriesCount: 10},
			{Path: "medium.seg", SeriesCount: 50},
		}
		r := SelectCandidates(entries, 2)
		if len(r) != 2 {
			t.Fatalf("expected 2 candidates, got %d", len(r))
		}
		if r[0].Path != "small.seg" || r[1].Path != "medium.seg" {
			t.Fatal("expected smallest entries selected")
		}
	})

	t.Run("maxCount caps selection", func(t *testing.T) {
		entries := make([]manifest.SegmentEntry, 10)
		for i := range entries {
			entries[i] = manifest.SegmentEntry{Path: "seg", SeriesCount: i}
		}
		r := SelectCandidates(entries, 3)
		if len(r) != 3 {
			t.Fatalf("expected 3, got %d", len(r))
		}
	})
}

func TestCompact(t *testing.T) {
	dir := t.TempDir()

	writeSegment := func(name string, seriesData map[uint64][]memtable.Point) string {
		path := filepath.Join(dir, name)
		var blockStart, blockEnd int64
		first := true
		var list []segment.SeriesPoints
		for id, pts := range seriesData {
			list = append(list, segment.SeriesPoints{SeriesID: id, Points: pts})
			for _, p := range pts {
				if first || p.Timestamp < blockStart {
					blockStart = p.Timestamp
				}
				if first || p.Timestamp > blockEnd {
					blockEnd = p.Timestamp
				}
				first = false
			}
		}
		w, _ := segment.NewWriter(path)
		w.Write(list, blockStart, blockEnd)
		w.Close()
		return path
	}

	writeSegment("seg1.seg", map[uint64][]memtable.Point{
		1: {{Timestamp: 100, Value: 1.0}, {Timestamp: 200, Value: 2.0}},
	})
	writeSegment("seg2.seg", map[uint64][]memtable.Point{
		1: {{Timestamp: 150, Value: 1.5}, {Timestamp: 250, Value: 2.5}},
		2: {{Timestamp: 300, Value: 3.0}},
	})

	candidates := []manifest.SegmentEntry{
		{Path: filepath.Join(dir, "seg1.seg")},
		{Path: filepath.Join(dir, "seg2.seg")},
	}
	outputPath := filepath.Join(dir, "compacted.seg")
	result, err := Compact(candidates, outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("expected result")
	}

	r, err := segment.OpenReader(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	pts, err := r.ReadSeries(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(pts) != 4 {
		t.Fatalf("series 1: expected 4 points, got %d", len(pts))
	}
	if pts[0].Timestamp != 100 || pts[3].Timestamp != 250 {
		t.Fatal("wrong timestamps for series 1")
	}

	pts2, err := r.ReadSeries(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(pts2) != 1 || pts2[0].Value != 3.0 {
		t.Fatal("series 2 mismatch")
	}
}

func TestCompactNoCandidates(t *testing.T) {
	_, err := Compact(nil, "out.seg")
	if err == nil {
		t.Fatal("expected error for empty candidates")
	}
}

func TestCompactHandlesMissingFile(t *testing.T) {
	dir := t.TempDir()
	candidates := []manifest.SegmentEntry{
		{Path: filepath.Join(dir, "nonexistent.seg")},
	}
	_, err := Compact(candidates, filepath.Join(dir, "out.seg"))
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}
