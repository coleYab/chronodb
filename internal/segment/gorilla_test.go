package segment

import (
	"math"
	"math/rand"
	"testing"

	"github.com/coleYab/chronodb/internal/memtable"
)

func TestGorillaRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		points []memtable.Point
	}{
		{
			name:   "single point",
			points: []memtable.Point{{Timestamp: 1000, Value: 3.14}},
		},
		{
			name: "two points",
			points: []memtable.Point{
				{Timestamp: 1000, Value: 1.0},
				{Timestamp: 2000, Value: 2.0},
			},
		},
		{
			name: "regular interval",
			points: []memtable.Point{
				{Timestamp: 1000, Value: 1.0},
				{Timestamp: 1001, Value: 2.0},
				{Timestamp: 1002, Value: 3.0},
				{Timestamp: 1003, Value: 4.0},
			},
		},
		{
			name: "same values",
			points: []memtable.Point{
				{Timestamp: 1000, Value: 42.0},
				{Timestamp: 2000, Value: 42.0},
				{Timestamp: 3000, Value: 42.0},
			},
		},
		{
			name: "random walk",
			points: func() []memtable.Point {
				rng := rand.New(rand.NewSource(42))
				pts := make([]memtable.Point, 100)
				ts := int64(1000)
				val := 100.0
				for i := 0; i < 100; i++ {
					pts[i] = memtable.Point{Timestamp: ts + int64(i)*1000, Value: val}
					val += rng.Float64()*10 - 5
				}
				return pts
			}(),
		},
		{
			name: "integer values",
			points: []memtable.Point{
				{Timestamp: 1, Value: 1},
				{Timestamp: 2, Value: 2},
				{Timestamp: 3, Value: 3},
				{Timestamp: 5, Value: 5},
				{Timestamp: 8, Value: 8},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := gorillaEncodeSeries(tt.points)
			decoded, err := gorillaDecodeSeries(encoded, len(tt.points))
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}
			if len(decoded) != len(tt.points) {
				t.Fatalf("length mismatch: got %d, want %d", len(decoded), len(tt.points))
			}
			for i := range tt.points {
				if decoded[i].Timestamp != tt.points[i].Timestamp {
					t.Errorf("timestamp[%d]: got %d, want %d", i, decoded[i].Timestamp, tt.points[i].Timestamp)
				}
				if math.Abs(decoded[i].Value-tt.points[i].Value) > 1e-15 {
					t.Errorf("value[%d]: got %v, want %v", i, decoded[i].Value, tt.points[i].Value)
				}
			}
			t.Logf("encoded %d points into %d bytes (%.1f bytes/point)", len(tt.points), len(encoded), float64(len(encoded))/float64(len(tt.points)))
		})
	}
}

func TestGorillaSegmentV2(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/test_v2.seg"

	w, err := NewWriterV2(path)
	if err != nil {
		t.Fatal(err)
	}

	points := make([]memtable.Point, 50)
	for i := range points {
		points[i] = memtable.Point{Timestamp: int64(1000 + i*100), Value: float64(i) * 1.5}
	}

	series := []SeriesPoints{
		{SeriesID: 1, Points: points},
		{SeriesID: 2, Points: points[:10]},
	}

	if err := w.Write(series, 1000, 5900); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if r.version != 2 {
		t.Fatalf("expected version 2, got %d", r.version)
	}

	got, err := r.ReadSeries(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 50 {
		t.Fatalf("expected 50 points, got %d", len(got))
	}
	for i := range got {
		if got[i].Timestamp != points[i].Timestamp || got[i].Value != points[i].Value {
			t.Fatalf("point[%d]: got (%d,%v), want (%d,%v)", i, got[i].Timestamp, got[i].Value, points[i].Timestamp, points[i].Value)
		}
	}

	got2, err := r.ReadSeries(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got2) != 10 {
		t.Fatalf("expected 10 points, got %d", len(got2))
	}

	all, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("expected 2 series, got %d", len(all))
	}
}
