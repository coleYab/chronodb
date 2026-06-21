package compaction

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"

	"github.com/coleYab/chronodb/internal/manifest"
	"github.com/coleYab/chronodb/internal/memtable"
	"github.com/coleYab/chronodb/internal/segment"
)

type Compactor struct {
	dataDir string
	segPath string
}

func New(dataDir string) *Compactor {
	return &Compactor{
		dataDir: dataDir,
		segPath: filepath.Join(dataDir, "segments"),
	}
}

func SelectCandidates(entries []manifest.SegmentEntry, maxCount int) []manifest.SegmentEntry {
	if len(entries) <= 1 || maxCount <= 1 {
		return nil
	}

	sorted := make([]manifest.SegmentEntry, len(entries))
	copy(sorted, entries)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].SeriesCount < sorted[j].SeriesCount
	})

	if maxCount > len(sorted) {
		maxCount = len(sorted)
	}
	if maxCount < 2 {
		return nil
	}
	return sorted[:maxCount]
}

func Compact(candidates []manifest.SegmentEntry, outputPath string) (*manifest.SegmentEntry, error) {
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no candidates to compact")
	}

	merged := make(map[uint64][]memtable.Point)
	var blockStart int64 = math.MaxInt64
	var blockEnd int64 = math.MinInt64

	for _, entry := range candidates {
		r, err := segment.OpenReader(entry.Path)
		if err != nil {
			return nil, fmt.Errorf("open %s: %w", entry.Path, err)
		}
		all, err := r.ReadAll()
		r.Close()
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", entry.Path, err)
		}
		for id, pts := range all {
			merged[id] = append(merged[id], pts...)
			for _, p := range pts {
				if p.Timestamp < blockStart {
					blockStart = p.Timestamp
				}
				if p.Timestamp > blockEnd {
					blockEnd = p.Timestamp
				}
			}
		}
	}

	seriesList := make([]segment.SeriesPoints, 0, len(merged))
	for id, pts := range merged {
		sort.Slice(pts, func(i, j int) bool {
			return pts[i].Timestamp < pts[j].Timestamp
		})
		seriesList = append(seriesList, segment.SeriesPoints{SeriesID: id, Points: pts})
	}

	w, err := segment.NewWriter(outputPath)
	if err != nil {
		return nil, err
	}
	if err := w.Write(seriesList, blockStart, blockEnd); err != nil {
		w.Close()
		os.Remove(outputPath)
		return nil, err
	}
	w.Close()

	return &manifest.SegmentEntry{
		Path:        outputPath,
		BlockStart:  blockStart,
		BlockEnd:    blockEnd,
		SeriesCount: len(seriesList),
	}, nil
}

func (c *Compactor) CompactCandidates(entries []manifest.SegmentEntry, maxCount int) (*manifest.SegmentEntry, []manifest.SegmentEntry, error) {
	candidates := SelectCandidates(entries, maxCount)
	if candidates == nil {
		return nil, nil, nil
	}

	outputPath := filepath.Join(c.dataDir, fmt.Sprintf("compacted-%d.seg", len(candidates)))
	entry, err := Compact(candidates, outputPath)
	if err != nil {
		return nil, nil, err
	}
	return entry, candidates, nil
}
