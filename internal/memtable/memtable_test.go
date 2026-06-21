package memtable

import (
	"testing"
)

func TestNewMemtable(t *testing.T) {
	m := New()
	if m == nil {
		t.Fatal("New() returned nil")
	}
	if m.Len() != 0 {
		t.Errorf("new memtable should be empty, got %d", m.Len())
	}
}

func TestInsertAndGet(t *testing.T) {
	m := New()
	pts := []Point{
		{Timestamp: 100, Value: 1.0},
		{Timestamp: 200, Value: 2.0},
	}
	m.Insert(1, pts)

	got := m.Get(1)
	if len(got) != 2 {
		t.Fatalf("expected 2 points, got %d", len(got))
	}
	if got[0].Timestamp != 100 || got[0].Value != 1.0 {
		t.Errorf("unexpected point 0: %+v", got[0])
	}
	if got[1].Timestamp != 200 || got[1].Value != 2.0 {
		t.Errorf("unexpected point 1: %+v", got[1])
	}
}

func TestInsertMaintainsSortOrder(t *testing.T) {
	m := New()
	pts := []Point{
		{Timestamp: 300, Value: 3.0},
		{Timestamp: 100, Value: 1.0},
		{Timestamp: 200, Value: 2.0},
	}
	m.Insert(1, pts)

	got := m.Get(1)
	if len(got) != 3 {
		t.Fatalf("expected 3 points, got %d", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i].Timestamp < got[i-1].Timestamp {
			t.Errorf("not sorted at index %d: %d < %d", i, got[i].Timestamp, got[i-1].Timestamp)
		}
	}
}

func TestInsertAppendPreservesSort(t *testing.T) {
	m := New()
	m.Insert(1, []Point{
		{Timestamp: 100, Value: 1.0},
		{Timestamp: 200, Value: 2.0},
	})
	m.Insert(1, []Point{
		{Timestamp: 300, Value: 3.0},
		{Timestamp: 400, Value: 4.0},
	})

	got := m.Get(1)
	if len(got) != 4 {
		t.Fatalf("expected 4 points, got %d", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i].Timestamp < got[i-1].Timestamp {
			t.Errorf("not sorted at index %d: %d < %d", i, got[i].Timestamp, got[i-1].Timestamp)
		}
	}
}

func TestInsertAppendOutOfOrderTriggersSort(t *testing.T) {
	m := New()
	m.Insert(1, []Point{
		{Timestamp: 100, Value: 1.0},
		{Timestamp: 200, Value: 2.0},
	})
	m.Insert(1, []Point{
		{Timestamp: 50, Value: 0.5},
	})

	got := m.Get(1)
	if len(got) != 3 {
		t.Fatalf("expected 3 points, got %d", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i].Timestamp < got[i-1].Timestamp {
			t.Errorf("not sorted at index %d: %d < %d", i, got[i].Timestamp, got[i-1].Timestamp)
		}
	}
}

func TestGetMissingSeries(t *testing.T) {
	m := New()
	got := m.Get(999)
	if got != nil {
		t.Errorf("expected nil for missing series, got %v", got)
	}
}

func TestSizeBytes(t *testing.T) {
	m := New()
	if m.SizeBytes() != 0 {
		t.Errorf("expected 0, got %d", m.SizeBytes())
	}
	m.Insert(1, []Point{
		{Timestamp: 100, Value: 1.0},
		{Timestamp: 200, Value: 2.0},
	})
	expected := 2 * 16
	if m.SizeBytes() != expected {
		t.Errorf("expected %d, got %d", expected, m.SizeBytes())
	}
	m.Insert(2, []Point{
		{Timestamp: 300, Value: 3.0},
	})
	expected += 1 * 16
	if m.SizeBytes() != expected {
		t.Errorf("expected %d, got %d", expected, m.SizeBytes())
	}
}

func TestFreezePreventsWrites(t *testing.T) {
	m := New()
	m.Insert(1, []Point{{Timestamp: 100, Value: 1.0}})

	m.Freeze()

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on insert to frozen memtable")
		}
	}()
	m.Insert(1, []Point{{Timestamp: 200, Value: 2.0}})
}

func TestFreezeReturnsSnapshotOfData(t *testing.T) {
	m := New()
	m.Insert(1, []Point{{Timestamp: 100, Value: 1.0}})

	frozen := m.Freeze()

	other := New()
	other.Insert(1, []Point{{Timestamp: 200, Value: 2.0}})

	frozenPts := frozen.Get(1)
	if len(frozenPts) != 1 {
		t.Fatalf("frozen snapshot should have 1 point, got %d", len(frozenPts))
	}
	if frozenPts[0].Timestamp != 100 {
		t.Errorf("frozen snapshot should have original point only")
	}

	otherPts := other.Get(1)
	if len(otherPts) != 1 || otherPts[0].Timestamp != 200 {
		t.Errorf("new memtable should have new point only")
	}
}

func TestClear(t *testing.T) {
	m := New()
	m.Insert(1, []Point{{Timestamp: 100, Value: 1.0}})
	m.Insert(2, []Point{{Timestamp: 200, Value: 2.0}})

	m.Clear()
	if m.Len() != 0 {
		t.Errorf("expected 0 after clear, got %d", m.Len())
	}
	if m.SizeBytes() != 0 {
		t.Errorf("expected 0 size after clear, got %d", m.SizeBytes())
	}
	if m.IsFrozen() {
		t.Error("should not be frozen after clear")
	}
}

func TestClearAllowsWrites(t *testing.T) {
	m := New()
	m.Insert(1, []Point{{Timestamp: 100, Value: 1.0}})
	m.Clear()

	m.Insert(1, []Point{{Timestamp: 200, Value: 2.0}})
	if m.Len() != 1 {
		t.Errorf("expected 1 series after clear and insert, got %d", m.Len())
	}
}

func TestGetAll(t *testing.T) {
	m := New()
	m.Insert(1, []Point{{Timestamp: 100, Value: 1.0}})
	m.Insert(2, []Point{{Timestamp: 200, Value: 2.0}})

	all := m.GetAll()
	if len(all) != 2 {
		t.Fatalf("expected 2 series, got %d", len(all))
	}
	if _, ok := all[1]; !ok {
		t.Error("missing series 1")
	}
	if _, ok := all[2]; !ok {
		t.Error("missing series 2")
	}
}

func TestMultipleSeries(t *testing.T) {
	m := New()
	m.Insert(1, []Point{{Timestamp: 100, Value: 1.0}})
	m.Insert(2, []Point{{Timestamp: 200, Value: 2.0}})
	m.Insert(3, []Point{{Timestamp: 300, Value: 3.0}})

	if m.Len() != 3 {
		t.Errorf("expected 3 series, got %d", m.Len())
	}

	pts1 := m.Get(1)
	if len(pts1) != 1 || pts1[0].Value != 1.0 {
		t.Error("series 1 mismatch")
	}
	pts2 := m.Get(2)
	if len(pts2) != 1 || pts2[0].Value != 2.0 {
		t.Error("series 2 mismatch")
	}
}

func BenchmarkInsert(b *testing.B) {
	m := New()
	pts := []Point{{Timestamp: 100, Value: 1.0}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Insert(uint64(i%1000), pts)
	}
}
