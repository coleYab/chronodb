package manifest

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

type SegmentEntry struct {
	Path        string `json:"path"`
	BlockStart  int64  `json:"block_start"`
	BlockEnd    int64  `json:"block_end"`
	SeriesCount int    `json:"series_count"`
	Size        int64  `json:"size"`
}

type Manifest struct {
	mu        sync.RWMutex
	path      string
	Segments  []SegmentEntry `json:"segments"`
	WALOffset int64          `json:"wal_offset"`
}

func New(path string) *Manifest {
	return &Manifest{
		path:     path,
		Segments: make([]SegmentEntry, 0),
	}
}

func Load(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return New(path), nil
		}
		return nil, err
	}
	m := &Manifest{path: path}
	if err := json.Unmarshal(data, m); err != nil {
		return nil, err
	}
	if m.Segments == nil {
		m.Segments = make([]SegmentEntry, 0)
	}
	return m, nil
}

func (m *Manifest) Add(entry SegmentEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Segments = append(m.Segments, entry)
	return m.save()
}

func (m *Manifest) Remove(path string) error {
	return m.RemoveBatch([]string{path})
}

func (m *Manifest) RemoveBatch(paths []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	removeSet := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		removeSet[p] = struct{}{}
	}
	updated := make([]SegmentEntry, 0, len(m.Segments))
	for _, s := range m.Segments {
		if _, ok := removeSet[s.Path]; !ok {
			updated = append(updated, s)
		}
	}
	m.Segments = updated
	return m.save()
}

func (m *Manifest) SetWALOffset(offset int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.WALOffset = offset
	return m.save()
}

func (m *Manifest) GetWALOffset() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.WALOffset
}

func (m *Manifest) List() []SegmentEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]SegmentEntry, len(m.Segments))
	copy(out, m.Segments)
	return out
}

func (m *Manifest) NumSegments() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.Segments)
}

func (m *Manifest) save() error {
	dir := filepath.Dir(m.path)
	tmpPath := filepath.Join(dir, ".manifest.tmp")
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, m.path)
}
