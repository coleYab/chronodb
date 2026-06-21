package retention

import (
	"os"
	"time"

	"github.com/coleYab/chronodb/internal/manifest"
)

type SweepResult struct {
	Removed []manifest.SegmentEntry
	Kept    []manifest.SegmentEntry
}

func Sweep(entries []manifest.SegmentEntry, cutoff time.Time) SweepResult {
	cutoffMillis := cutoff.UnixMilli()
	var removed, kept []manifest.SegmentEntry
	for _, entry := range entries {
		if entry.BlockEnd < cutoffMillis {
			removed = append(removed, entry)
		} else {
			kept = append(kept, entry)
		}
	}
	return SweepResult{Removed: removed, Kept: kept}
}

func DeleteFiles(entries []manifest.SegmentEntry) []error {
	var errs []error
	for _, entry := range entries {
		if err := os.Remove(entry.Path); err != nil && !os.IsNotExist(err) {
			errs = append(errs, err)
		}
	}
	return errs
}
