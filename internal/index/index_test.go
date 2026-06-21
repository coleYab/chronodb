package index

import (
	"sort"
	"testing"
)

func TestInsertAndResolve(t *testing.T) {
	idx := New()
	idx.Insert(1, "cpu_usage", map[string]string{"host": "server1", "region": "us-east"})
	idx.Insert(2, "cpu_usage", map[string]string{"host": "server2", "region": "us-east"})
	idx.Insert(3, "cpu_usage", map[string]string{"host": "server1", "region": "us-west"})
	idx.Insert(4, "memory_usage", map[string]string{"host": "server1", "region": "us-east"})

	t.Run("all series for metric", func(t *testing.T) {
		ids := idx.Resolve("cpu_usage", nil)
		if len(ids) != 3 {
			t.Fatalf("expected 3 series, got %d", len(ids))
		}
	})

	t.Run("single tag filter", func(t *testing.T) {
		ids := idx.Resolve("cpu_usage", map[string]string{"host": "server1"})
		if len(ids) != 2 {
			t.Fatalf("expected 2 series, got %d: %v", len(ids), ids)
		}
	})

	t.Run("multiple tag filters intersection", func(t *testing.T) {
		ids := idx.Resolve("cpu_usage", map[string]string{"host": "server1", "region": "us-east"})
		if len(ids) != 1 || ids[0] != 1 {
			t.Fatalf("expected series 1, got %v", ids)
		}
	})

	t.Run("no matching tag value", func(t *testing.T) {
		ids := idx.Resolve("cpu_usage", map[string]string{"host": "server99"})
		if len(ids) != 0 {
			t.Fatalf("expected 0 series, got %d", len(ids))
		}
	})

	t.Run("unknown metric", func(t *testing.T) {
		ids := idx.Resolve("nonexistent", nil)
		if len(ids) != 0 {
			t.Fatalf("expected 0 series, got %d", len(ids))
		}
	})
}

func TestInsertAndResolveTagOrderIndependent(t *testing.T) {
	idx := New()
	idx.Insert(1, "cpu", map[string]string{"a": "1", "b": "2"})
	ids := idx.Resolve("cpu", map[string]string{"b": "2", "a": "1"})
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("expected series 1 regardless of tag order, got %v", ids)
	}
}

func TestListMetrics(t *testing.T) {
	idx := New()
	idx.Insert(1, "cpu", nil)
	idx.Insert(2, "mem", nil)
	idx.Insert(3, "disk", nil)

	metrics := idx.ListMetrics()
	expected := []string{"cpu", "disk", "mem"}
	for i, m := range metrics {
		if m != expected[i] {
			t.Fatalf("expected %s at index %d, got %s", expected[i], i, m)
		}
	}
}

func TestListTags(t *testing.T) {
	idx := New()
	idx.Insert(1, "cpu", map[string]string{"host": "a", "region": "b"})
	idx.Insert(2, "cpu", map[string]string{"host": "a", "region": "c"})

	tags := idx.ListTags("cpu")
	expected := []string{"host=a", "region=b", "region=c"}
	if len(tags) != 3 {
		t.Fatalf("expected 3 tags, got %d: %v", len(tags), tags)
	}
	for i, tag := range tags {
		if tag != expected[i] {
			t.Fatalf("expected %s at index %d, got %s", expected[i], i, tag)
		}
	}

	tags = idx.ListTags("nonexistent")
	if tags != nil {
		t.Fatalf("expected nil for nonexistent metric, got %v", tags)
	}
}

func TestResolveSortedOutput(t *testing.T) {
	idx := New()
	for i := uint64(100); i >= 1; i-- {
		idx.Insert(i, "test", nil)
	}
	ids := idx.Resolve("test", nil)
	if !sort.SliceIsSorted(ids, func(i, j int) bool { return ids[i] < ids[j] }) {
		t.Fatal("expected sorted output")
	}
	if len(ids) != 100 {
		t.Fatalf("expected 100 series, got %d", len(ids))
	}
}

func BenchmarkResolve(b *testing.B) {
	idx := New()
	for i := 0; i < 10000; i++ {
		idx.Insert(uint64(i), "cpu", map[string]string{
			"host":   randomHost(i),
			"region": randomRegion(i),
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ids := idx.Resolve("cpu", map[string]string{"region": "us-east"})
		if len(ids) == 0 {
			b.Fatal("expected results")
		}
	}
}

func randomHost(i int) string {
	hosts := []string{"server1", "server2", "server3", "server4", "server5"}
	return hosts[i%len(hosts)]
}

func randomRegion(i int) string {
	regions := []string{"us-east", "us-west", "eu-west", "eu-central", "ap-southeast"}
	return regions[i%len(regions)]
}

func BenchmarkResolveNoFilters(b *testing.B) {
	idx := New()
	for i := 0; i < 10000; i++ {
		idx.Insert(uint64(i), "cpu", map[string]string{
			"host":   randomHost(i),
			"region": randomRegion(i),
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ids := idx.Resolve("cpu", nil)
		if len(ids) != 10000 {
			b.Fatalf("expected 10000, got %d", len(ids))
		}
	}
}
