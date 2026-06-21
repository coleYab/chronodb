package series

import (
	"testing"
)

func TestSeriesKeyDeterministic(t *testing.T) {
	tests := []struct {
		name   string
		metric string
		tags   map[string]string
	}{
		{
			name:   "single tag",
			metric: "cpu.usage",
			tags:   map[string]string{"host": "web-01"},
		},
		{
			name:   "multiple tags",
			metric: "cpu.usage",
			tags:   map[string]string{"host": "web-01", "region": "us-east"},
		},
		{
			name:   "empty tags",
			metric: "cpu.usage",
			tags:   map[string]string{},
		},
		{
			name:   "different metric",
			metric: "http.requests",
			tags:   map[string]string{"host": "web-01", "region": "us-east"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id1 := SeriesKey(tt.metric, tt.tags)
			id2 := SeriesKey(tt.metric, tt.tags)
			if id1 != id2 {
				t.Errorf("SeriesKey not deterministic: %d != %d", id1, id2)
			}
		})
	}
}

func TestSeriesKeyTagOrderIndependent(t *testing.T) {
	tags1 := map[string]string{"host": "web-01", "region": "us-east", "zone": "a"}
	tags2 := map[string]string{"zone": "a", "region": "us-east", "host": "web-01"}

	id1 := SeriesKey("cpu.usage", tags1)
	id2 := SeriesKey("cpu.usage", tags2)

	if id1 != id2 {
		t.Errorf("SeriesKey should be order-independent: %d != %d", id1, id2)
	}
}

func TestSeriesKeyDifferentTags(t *testing.T) {
	tests := []struct {
		name   string
		metric string
		tags1  map[string]string
		tags2  map[string]string
	}{
		{
			name:   "different host values",
			metric: "cpu.usage",
			tags1:  map[string]string{"host": "web-01"},
			tags2:  map[string]string{"host": "web-02"},
		},
		{
			name:   "different tag keys",
			metric: "cpu.usage",
			tags1:  map[string]string{"host": "web-01"},
			tags2:  map[string]string{"region": "us-east"},
		},
		{
			name:   "different metrics same tags",
			metric: "cpu.usage",
			tags1:  map[string]string{"host": "web-01"},
			tags2:  map[string]string{"host": "web-01"},
		},
		{
			name:   "empty vs populated tags",
			metric: "cpu.usage",
			tags1:  map[string]string{},
			tags2:  map[string]string{"host": "web-01"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id1 := SeriesKey(tt.metric, tt.tags1)
			id2 := SeriesKey(tt.metric, tt.tags2)

			sameMetric := tt.tags1["host"] == tt.tags2["host"]
			if tt.name == "different metrics same tags" {
				id1 = SeriesKey("cpu.usage", tt.tags1)
				id2 = SeriesKey("http.requests", tt.tags2)
			}

			if id1 == id2 {
				t.Errorf("SeriesKey should differ for %s: %d == %d", tt.name, id1, id2)
			}
			_ = sameMetric
		})
	}
}

func TestSeriesKeyDifferentMetrics(t *testing.T) {
	id1 := SeriesKey("cpu.usage", map[string]string{"host": "web-01"})
	id2 := SeriesKey("http.requests", map[string]string{"host": "web-01"})

	if id1 == id2 {
		t.Errorf("SeriesKey should differ for different metrics: %d == %d", id1, id2)
	}
}

func TestNewRegistry(t *testing.T) {
	r := NewRegistry()
	if r == nil {
		t.Fatal("NewRegistry returned nil")
	}
	if r.Len() != 0 {
		t.Errorf("new registry should be empty, got %d", r.Len())
	}
}

func TestRegistryGetOrCreateNewSeries(t *testing.T) {
	r := NewRegistry()
	id, exists := r.GetOrCreate("cpu.usage", map[string]string{"host": "web-01"})

	if exists {
		t.Error("GetOrCreate should return exists=false for new series")
	}

	if id == 0 {
		t.Error("GetOrCreate should return non-zero SeriesID")
	}
}

func TestRegistryGetOrCreateExisting(t *testing.T) {
	r := NewRegistry()
	id1, _ := r.GetOrCreate("cpu.usage", map[string]string{"host": "web-01"})
	id2, exists := r.GetOrCreate("cpu.usage", map[string]string{"host": "web-01"})

	if !exists {
		t.Error("GetOrCreate should return exists=true for existing series")
	}

	if id1 != id2 {
		t.Errorf("GetOrCreate should return same ID for same series: %d != %d", id1, id2)
	}

	if r.Len() != 1 {
		t.Errorf("registry should have 1 entry, got %d", r.Len())
	}
}

func TestRegistryGetOrCreateTagOrderIndependent(t *testing.T) {
	r := NewRegistry()
	id1, _ := r.GetOrCreate("cpu.usage", map[string]string{"host": "web-01", "region": "us-east"})
	id2, exists := r.GetOrCreate("cpu.usage", map[string]string{"region": "us-east", "host": "web-01"})

	if !exists {
		t.Error("GetOrCreate should recognize same series regardless of tag order")
	}

	if id1 != id2 {
		t.Errorf("IDs should match: %d != %d", id1, id2)
	}

	if r.Len() != 1 {
		t.Errorf("registry should have 1 entry, got %d", r.Len())
	}
}

func TestRegistryGetOrCreateDifferentSeries(t *testing.T) {
	r := NewRegistry()
	id1, _ := r.GetOrCreate("cpu.usage", map[string]string{"host": "web-01"})
	id2, _ := r.GetOrCreate("cpu.usage", map[string]string{"host": "web-02"})

	if id1 == id2 {
		t.Error("different series should have different IDs")
	}

	if r.Len() != 2 {
		t.Errorf("registry should have 2 entries, got %d", r.Len())
	}
}

func TestRegistryGet(t *testing.T) {
	r := NewRegistry()
	id, _ := r.GetOrCreate("cpu.usage", map[string]string{"host": "web-01"})

	meta, ok := r.Get(id)
	if !ok {
		t.Fatal("Get should return ok=true for existing series")
	}

	if meta.Metric != "cpu.usage" {
		t.Errorf("expected metric cpu.usage, got %s", meta.Metric)
	}
	if meta.Tags["host"] != "web-01" {
		t.Errorf("expected tag host=web-01, got %s", meta.Tags["host"])
	}
}

func TestRegistryGetMissing(t *testing.T) {
	r := NewRegistry()
	_, ok := r.Get(99999)
	if ok {
		t.Error("Get should return ok=false for missing series")
	}
}

func TestRegistryListMetrics(t *testing.T) {
	r := NewRegistry()
	r.GetOrCreate("cpu.usage", map[string]string{"host": "web-01"})
	r.GetOrCreate("cpu.usage", map[string]string{"host": "web-02"})
	r.GetOrCreate("http.requests", map[string]string{"host": "web-01"})

	metrics := r.ListMetrics()
	if len(metrics) != 2 {
		t.Errorf("expected 2 metrics, got %d: %v", len(metrics), metrics)
	}
}

func TestRegistryListSeries(t *testing.T) {
	r := NewRegistry()
	r.GetOrCreate("cpu.usage", map[string]string{"host": "web-01"})
	r.GetOrCreate("cpu.usage", map[string]string{"host": "web-02"})
	r.GetOrCreate("http.requests", map[string]string{"host": "web-01"})

	series := r.ListSeries("cpu.usage")
	if len(series) != 2 {
		t.Errorf("expected 2 series for cpu.usage, got %d", len(series))
	}

	series = r.ListSeries("http.requests")
	if len(series) != 1 {
		t.Errorf("expected 1 series for http.requests, got %d", len(series))
	}

	series = r.ListSeries("nonexistent")
	if len(series) != 0 {
		t.Errorf("expected 0 series for nonexistent, got %d", len(series))
	}
}

func TestRegistryConcurrentAccess(t *testing.T) {
	r := NewRegistry()
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(n int) {
			host := "web-0" + string(rune('0'+n%10))
			r.GetOrCreate("cpu.usage", map[string]string{"host": host})
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	if r.Len() != 10 {
		t.Errorf("expected 10 series, got %d", r.Len())
	}
}

func BenchmarkSeriesKey(b *testing.B) {
	tags := map[string]string{
		"host":   "web-01",
		"region": "us-east",
		"zone":   "a",
		"os":     "linux",
	}
	for i := 0; i < b.N; i++ {
		SeriesKey("cpu.usage", tags)
	}
}
