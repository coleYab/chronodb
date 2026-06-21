package query

import (
	"path/filepath"
	"testing"

	"github.com/coleYab/chronodb/internal/memtable"
	"github.com/coleYab/chronodb/internal/segment"
)

func pt(ts int64, val float64) memtable.Point {
	return memtable.Point{Timestamp: ts, Value: val}
}

func TestMerge(t *testing.T) {
	t.Run("all empty", func(t *testing.T) {
		if Merge() != nil {
			t.Fatal("expected nil")
		}
	})

	t.Run("single source", func(t *testing.T) {
		pts := []memtable.Point{pt(1, 1), pt(2, 2)}
		r := Merge(pts)
		if len(r) != 2 {
			t.Fatal("expected 2 points")
		}
	})

	t.Run("two disjoint sources", func(t *testing.T) {
		a := []memtable.Point{pt(1, 1), pt(3, 3)}
		b := []memtable.Point{pt(2, 2), pt(4, 4)}
		r := Merge(a, b)
		for i, ts := range []int64{1, 2, 3, 4} {
			if r[i].Timestamp != ts {
				t.Fatalf("at %d: expected ts %d, got %d", i, ts, r[i].Timestamp)
			}
		}
	})

	t.Run("interleaved sources", func(t *testing.T) {
		a := []memtable.Point{pt(1, 1), pt(4, 4), pt(5, 5)}
		b := []memtable.Point{pt(2, 2), pt(3, 3)}
		r := Merge(a, b)
		for i, ts := range []int64{1, 2, 3, 4, 5} {
			if r[i].Timestamp != ts {
				t.Fatalf("at %d: expected ts %d, got %d", i, ts, r[i].Timestamp)
			}
		}
	})

	t.Run("duplicate timestamps", func(t *testing.T) {
		a := []memtable.Point{pt(1, 1), pt(2, 2)}
		b := []memtable.Point{pt(1, 10), pt(2, 20)}
		r := Merge(a, b)
		if len(r) != 4 {
			t.Fatalf("expected 4 points, got %d", len(r))
		}
		if r[0].Timestamp != 1 || r[1].Timestamp != 1 {
			t.Fatal("expected duplicates preserved")
		}
	})

	t.Run("three sources", func(t *testing.T) {
		a := []memtable.Point{pt(1, 1), pt(6, 6)}
		b := []memtable.Point{pt(3, 3), pt(4, 4)}
		c := []memtable.Point{pt(2, 2), pt(5, 5)}
		r := Merge(a, b, c)
		for i, ts := range []int64{1, 2, 3, 4, 5, 6} {
			if r[i].Timestamp != ts {
				t.Fatalf("at %d: expected ts %d, got %d", i, ts, r[i].Timestamp)
			}
		}
	})
}

func TestFilter(t *testing.T) {
	pts := []memtable.Point{pt(1, 1), pt(2, 2), pt(3, 3), pt(4, 4), pt(5, 5)}

	t.Run("empty input", func(t *testing.T) {
		if r := Filter(nil, 0, 10); r != nil {
			t.Fatal("expected nil")
		}
	})

	t.Run("full range", func(t *testing.T) {
		r := Filter(pts, 1, 5)
		if len(r) != 5 {
			t.Fatalf("expected 5, got %d", len(r))
		}
	})

	t.Run("partial range", func(t *testing.T) {
		r := Filter(pts, 2, 4)
		if len(r) != 3 {
			t.Fatalf("expected 3, got %d", len(r))
		}
		if r[0].Timestamp != 2 || r[2].Timestamp != 4 {
			t.Fatal("wrong range")
		}
	})

	t.Run("no overlap", func(t *testing.T) {
		r := Filter(pts, 10, 20)
		if r != nil {
			t.Fatal("expected nil")
		}
	})

	t.Run("single point", func(t *testing.T) {
		r := Filter(pts, 3, 3)
		if len(r) != 1 || r[0].Timestamp != 3 {
			t.Fatal("expected single point at ts=3")
		}
	})
}

func TestAggregate(t *testing.T) {
	t.Run("empty input", func(t *testing.T) {
		r := Aggregate(nil, 10)
		if r != nil {
			t.Fatal("expected nil")
		}
	})

	t.Run("single bucket", func(t *testing.T) {
		pts := []memtable.Point{pt(5, 1), pt(6, 2), pt(7, 3)}
		buckets := Aggregate(pts, 10)
		if len(buckets) != 1 {
			t.Fatalf("expected 1 bucket, got %d", len(buckets))
		}
		if buckets[0].Start != 0 {
			t.Fatalf("expected start 0, got %d", buckets[0].Start)
		}
		if buckets[0].Sum != 6 || buckets[0].Count != 3 {
			t.Fatalf("expected sum=6 count=3, got sum=%f count=%d", buckets[0].Sum, buckets[0].Count)
		}
		if buckets[0].Min != 1 || buckets[0].Max != 3 {
			t.Fatalf("expected min=1 max=3, got min=%f max=%f", buckets[0].Min, buckets[0].Max)
		}
	})

	t.Run("multiple buckets", func(t *testing.T) {
		pts := []memtable.Point{pt(5, 1), pt(15, 2), pt(25, 3)}
		buckets := Aggregate(pts, 10)
		if len(buckets) != 3 {
			t.Fatalf("expected 3 buckets, got %d", len(buckets))
		}
		if buckets[0].Start != 0 || buckets[1].Start != 10 || buckets[2].Start != 20 {
			t.Fatal("wrong bucket starts")
		}
		if buckets[0].Count != 1 || buckets[1].Count != 1 || buckets[2].Count != 1 {
			t.Fatal("expected 1 point per bucket")
		}
	})

	t.Run("empty middle bucket", func(t *testing.T) {
		pts := []memtable.Point{pt(2, 1), pt(22, 2)}
		buckets := Aggregate(pts, 10)
		if len(buckets) != 3 {
			t.Fatalf("expected 3 buckets, got %d", len(buckets))
		}
		if buckets[0].Count != 1 || buckets[1].Count != 0 || buckets[2].Count != 1 {
			t.Fatal("wrong bucket counts")
		}
	})
}

func TestReadSegments(t *testing.T) {
	dir := t.TempDir()

	w, err := segment.NewWriter(filepath.Join(dir, "seg-000.seg"))
	if err != nil {
		t.Fatal(err)
	}
	err = w.Write([]segment.SeriesPoints{
		{SeriesID: 1, Points: []memtable.Point{pt(1, 10), pt(2, 20)}},
		{SeriesID: 2, Points: []memtable.Point{pt(3, 30)}},
	}, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	pts := ReadSegments([]string{filepath.Join(dir, "seg-000.seg")}, 1)
	if len(pts) != 2 {
		t.Fatalf("expected 2 points, got %d", len(pts))
	}
	if pts[0].Value != 10 || pts[1].Value != 20 {
		t.Fatal("wrong values")
	}

	pts = ReadSegments([]string{filepath.Join(dir, "seg-000.seg")}, 3)
	if len(pts) != 0 {
		t.Fatal("expected 0 points for unknown series")
	}

	pts = ReadSegments([]string{filepath.Join(dir, "nonexistent.seg")}, 1)
	if len(pts) != 0 {
		t.Fatal("expected 0 points for missing file")
	}
}

func TestFullQueryFlow(t *testing.T) {
	dir := t.TempDir()

	w, err := segment.NewWriter(filepath.Join(dir, "seg-000.seg"))
	if err != nil {
		t.Fatal(err)
	}
	err = w.Write([]segment.SeriesPoints{
		{SeriesID: 1, Points: []memtable.Point{pt(100, 1.0), pt(300, 3.0)}},
	}, 100, 300)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	segPts := ReadSegments([]string{filepath.Join(dir, "seg-000.seg")}, 1)
	memPts := []memtable.Point{pt(200, 2.0), pt(400, 4.0)}

	merged := Merge(segPts, memPts)
	for i, ts := range []int64{100, 200, 300, 400} {
		if merged[i].Timestamp != ts {
			t.Fatalf("at %d: expected ts %d, got %d", i, ts, merged[i].Timestamp)
		}
	}

	filtered := Filter(merged, 150, 350)
	if len(filtered) != 2 {
		t.Fatalf("expected 2 points in range, got %d", len(filtered))
	}

	buckets := Aggregate(filtered, 100)
	if len(buckets) != 2 {
		t.Fatalf("expected 2 buckets, got %d", len(buckets))
	}
}

func BenchmarkMerge(b *testing.B) {
	src1 := make([]memtable.Point, 5000)
	src2 := make([]memtable.Point, 5000)
	for i := 0; i < 5000; i++ {
		src1[i] = pt(int64(i*2), float64(i))
		src2[i] = pt(int64(i*2+1), float64(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Merge(src1, src2)
	}
}

func BenchmarkAggregate(b *testing.B) {
	pts := make([]memtable.Point, 10000)
	for i := 0; i < 10000; i++ {
		pts[i] = pt(int64(i), float64(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Aggregate(pts, 10)
	}
}
