package memtable

import (
	"sort"
	"sync"
)

type Point struct {
	Timestamp int64
	Value     float64
}

type Memtable struct {
	mu        sync.Mutex
	series    map[uint64][]Point
	sizeBytes int
	frozen    bool
}

func New() *Memtable {
	return &Memtable{
		series: make(map[uint64][]Point),
	}
}

func (m *Memtable) Insert(seriesID uint64, points []Point) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frozen {
		panic("memtable: insert on frozen memtable")
	}
	if len(points) == 0 {
		return
	}
	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})
	existing := m.series[seriesID]
	needSort := false
	if len(existing) > 0 && points[0].Timestamp < existing[len(existing)-1].Timestamp {
		needSort = true
	}
	m.series[seriesID] = append(existing, points...)
	if needSort {
		sort.Slice(m.series[seriesID], func(i, j int) bool {
			return m.series[seriesID][i].Timestamp < m.series[seriesID][j].Timestamp
		})
	}
	m.sizeBytes += len(points) * 16
}

func (m *Memtable) Get(seriesID uint64) []Point {
	m.mu.Lock()
	defer m.mu.Unlock()
	pts, ok := m.series[seriesID]
	if !ok {
		return nil
	}
	out := make([]Point, len(pts))
	copy(out, pts)
	return out
}

func (m *Memtable) GetAll() map[uint64][]Point {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[uint64][]Point, len(m.series))
	for id, pts := range m.series {
		cp := make([]Point, len(pts))
		copy(cp, pts)
		out[id] = cp
	}
	return out
}

func (m *Memtable) SizeBytes() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sizeBytes
}

func (m *Memtable) Freeze() *Memtable {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.frozen = true
	snapshot := &Memtable{
		series:    m.series,
		sizeBytes: m.sizeBytes,
		frozen:    true,
	}
	return snapshot
}

func (m *Memtable) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.series = make(map[uint64][]Point)
	m.sizeBytes = 0
	m.frozen = false
}

func (m *Memtable) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.series)
}

func (m *Memtable) IsFrozen() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.frozen
}
