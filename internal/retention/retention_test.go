package retention

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/coleYab/chronodb/internal/manifest"
)

func TestSweep(t *testing.T) {
	now := time.Now()
	entries := []manifest.SegmentEntry{
		{Path: "old.seg", BlockEnd: now.Add(-2 * 24 * time.Hour).UnixMilli()},
		{Path: "new.seg", BlockEnd: now.UnixMilli()},
		{Path: "future.seg", BlockEnd: now.Add(time.Hour).UnixMilli()},
	}

	result := Sweep(entries, now.Add(-24*time.Hour))
	if len(result.Removed) != 1 {
		t.Fatalf("expected 1 removed, got %d", len(result.Removed))
	}
	if result.Removed[0].Path != "old.seg" {
		t.Fatalf("expected old.seg removed, got %s", result.Removed[0].Path)
	}
	if len(result.Kept) != 2 {
		t.Fatalf("expected 2 kept, got %d", len(result.Kept))
	}
}

func TestSweepNoExpired(t *testing.T) {
	now := time.Now()
	entries := []manifest.SegmentEntry{
		{Path: "a.seg", BlockEnd: now.UnixMilli()},
		{Path: "b.seg", BlockEnd: now.Add(time.Hour).UnixMilli()},
	}

	result := Sweep(entries, now.Add(-24*time.Hour))
	if len(result.Removed) != 0 {
		t.Fatalf("expected 0 removed, got %d", len(result.Removed))
	}
	if len(result.Kept) != 2 {
		t.Fatalf("expected 2 kept, got %d", len(result.Kept))
	}
}

func TestDeleteFiles(t *testing.T) {
	dir := t.TempDir()
	f1 := filepath.Join(dir, "seg1.seg")
	f2 := filepath.Join(dir, "seg2.seg")
	os.WriteFile(f1, []byte("data"), 0644)
	os.WriteFile(f2, []byte("data"), 0644)

	entries := []manifest.SegmentEntry{
		{Path: f1},
		{Path: f2},
	}

	errs := DeleteFiles(entries)
	if len(errs) > 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if _, err := os.Stat(f1); !os.IsNotExist(err) {
		t.Fatal("expected f1 to be deleted")
	}
	if _, err := os.Stat(f2); !os.IsNotExist(err) {
		t.Fatal("expected f2 to be deleted")
	}
}

func TestDeleteFilesMissing(t *testing.T) {
	errs := DeleteFiles([]manifest.SegmentEntry{{Path: "/nonexistent/file.seg"}})
	if len(errs) != 0 {
		t.Fatalf("expected no errors for missing file, got %v", errs)
	}
}
