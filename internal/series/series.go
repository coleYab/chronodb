package series

import (
	"hash/fnv"
	"sort"
	"sync"
	"time"
)

type SeriesMeta struct {
	SeriesID  uint64
	Metric    string
	Tags      map[string]string
	FirstSeen time.Time
	LastSeen  time.Time
}

func SeriesKey(metric string, tags map[string]string) uint64 {
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := fnv.New64a()
	h.Write([]byte(metric))
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte("="))
		h.Write([]byte(tags[k]))
		h.Write([]byte(","))
	}
	return h.Sum64()
}

type Registry struct {
	mu     sync.RWMutex
	byID   map[uint64]SeriesMeta
	byKey  map[string]uint64
}

func NewRegistry() *Registry {
	return &Registry{
		byID:  make(map[uint64]SeriesMeta),
		byKey: make(map[string]uint64),
	}
}

func (r *Registry) GetOrCreate(metric string, tags map[string]string) (uint64, bool) {
	id := SeriesKey(metric, tags)

	r.mu.RLock()
	_, exists := r.byID[id]
	r.mu.RUnlock()

	if exists {
		return id, true
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.byID[id]; exists {
		return id, true
	}

	now := time.Now()
	meta := SeriesMeta{
		SeriesID:  id,
		Metric:    metric,
		Tags:      tags,
		FirstSeen: now,
		LastSeen:  now,
	}
	r.byID[id] = meta
	return id, false
}

func (r *Registry) Get(id uint64) (SeriesMeta, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	meta, ok := r.byID[id]
	return meta, ok
}

func (r *Registry) ListMetrics() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	seen := make(map[string]struct{})
	for _, meta := range r.byID {
		seen[meta.Metric] = struct{}{}
	}
	metrics := make([]string, 0, len(seen))
	for m := range seen {
		metrics = append(metrics, m)
	}
	return metrics
}

func (r *Registry) ListSeries(metric string) []SeriesMeta {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []SeriesMeta
	for _, meta := range r.byID {
		if meta.Metric == metric {
			result = append(result, meta)
		}
	}
	return result
}

func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.byID)
}
