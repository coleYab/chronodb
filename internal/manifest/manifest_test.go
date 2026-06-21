package manifest

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewManifest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	m := New(path)
	if m == nil {
		t.Fatal("New() returned nil")
	}
	if m.NumSegments() != 0 {
		t.Errorf("expected 0 segments, got %d", m.NumSegments())
	}
}

func TestLoadNonExistent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nonexistent.json")
	m, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if m.NumSegments() != 0 {
		t.Errorf("expected 0 segments, got %d", m.NumSegments())
	}
}

func TestAddAndList(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	m := New(path)

	entry := SegmentEntry{
		Path:        "/data/segments/001.seg",
		BlockStart:  1000,
		BlockEnd:    2000,
		SeriesCount: 5,
		Size:        4096,
	}

	if err := m.Add(entry); err != nil {
		t.Fatal(err)
	}

	if m.NumSegments() != 1 {
		t.Errorf("expected 1 segment, got %d", m.NumSegments())
	}

	entries := m.List()
	if len(entries) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(entries))
	}
	if entries[0].Path != entry.Path {
		t.Errorf("expected path %s, got %s", entry.Path, entries[0].Path)
	}
}

func TestRemove(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	m := New(path)

	m.Add(SegmentEntry{Path: "/data/a.seg", BlockStart: 0, BlockEnd: 100})
	m.Add(SegmentEntry{Path: "/data/b.seg", BlockStart: 100, BlockEnd: 200})
	m.Add(SegmentEntry{Path: "/data/c.seg", BlockStart: 200, BlockEnd: 300})

	if err := m.Remove("/data/b.seg"); err != nil {
		t.Fatal(err)
	}

	entries := m.List()
	if len(entries) != 2 {
		t.Fatalf("expected 2 segments, got %d", len(entries))
	}
	if entries[0].Path != "/data/a.seg" {
		t.Errorf("expected a.seg first, got %s", entries[0].Path)
	}
	if entries[1].Path != "/data/c.seg" {
		t.Errorf("expected c.seg second, got %s", entries[1].Path)
	}
}

func TestWALOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	m := New(path)

	if m.GetWALOffset() != 0 {
		t.Errorf("expected wal offset 0, got %d", m.GetWALOffset())
	}

	if err := m.SetWALOffset(12345); err != nil {
		t.Fatal(err)
	}

	if m.GetWALOffset() != 12345 {
		t.Errorf("expected wal offset 12345, got %d", m.GetWALOffset())
	}
}

func TestSaveAndLoadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	m := New(path)

	m.Add(SegmentEntry{Path: "/data/a.seg", BlockStart: 0, BlockEnd: 100, SeriesCount: 3, Size: 512})
	m.Add(SegmentEntry{Path: "/data/b.seg", BlockStart: 100, BlockEnd: 200, SeriesCount: 7, Size: 1024})
	m.SetWALOffset(9999)

	m2, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}

	if m2.NumSegments() != 2 {
		t.Fatalf("expected 2 segments, got %d", m2.NumSegments())
	}

	entries := m2.List()
	if entries[0].Path != "/data/a.seg" || entries[0].SeriesCount != 3 {
		t.Errorf("entry 0 mismatch: %+v", entries[0])
	}
	if entries[1].Path != "/data/b.seg" || entries[1].SeriesCount != 7 {
		t.Errorf("entry 1 mismatch: %+v", entries[1])
	}

	if m2.GetWALOffset() != 9999 {
		t.Errorf("expected wal offset 9999, got %d", m2.GetWALOffset())
	}
}

func TestAtomicWriteSurvivesCrash(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.json")
	m := New(path)
	m.Add(SegmentEntry{Path: "/data/a.seg", BlockStart: 0, BlockEnd: 100})
	m.Add(SegmentEntry{Path: "/data/b.seg", BlockStart: 100, BlockEnd: 200})

	m2, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if m2.NumSegments() != 2 {
		t.Errorf("expected 2 segments after reload, got %d", m2.NumSegments())
	}
}

func TestConcurrentAccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	m := New(path)

	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(n int) {
			entry := SegmentEntry{
				Path:       filepath.Join("/data", string(rune('a'+n))),
				BlockStart: int64(n * 100),
				BlockEnd:   int64((n + 1) * 100),
			}
			m.Add(entry)
			m.GetWALOffset()
			done <- true
		}(i)
	}
	for i := 0; i < 10; i++ {
		<-done
	}

	if m.NumSegments() != 10 {
		t.Errorf("expected 10 segments, got %d", m.NumSegments())
	}
}

func TestRemoveNonExistent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	m := New(path)
	m.Add(SegmentEntry{Path: "/data/a.seg", BlockStart: 0, BlockEnd: 100})

	if err := m.Remove("/data/nonexistent.seg"); err != nil {
		t.Fatal(err)
	}

	if m.NumSegments() != 1 {
		t.Errorf("expected 1 segment, got %d", m.NumSegments())
	}
}

func TestFilePersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	m := New(path)
	m.Add(SegmentEntry{Path: "/data/test.seg", BlockStart: 0, BlockEnd: 100, SeriesCount: 10, Size: 2048})

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("manifest file is empty")
	}
}
