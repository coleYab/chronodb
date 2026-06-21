package query

import (
	"math"
	"sort"

	"github.com/coleYab/chronodb/internal/memtable"
	"github.com/coleYab/chronodb/internal/segment"
)

type Bucket struct {
	Start int64
	Sum   float64
	Count int
	Min   float64
	Max   float64
}

type Request struct {
	Metric      string
	TagFilters  map[string]string
	StartTime   int64
	EndTime     int64
	Aggregation string
	BucketWidth int64
}

type Result struct {
	SeriesID uint64
	Buckets  []Bucket
	Points   []memtable.Point
	Err      error
}

func Merge(points ...[]memtable.Point) []memtable.Point {
	var total int
	for _, p := range points {
		total += len(p)
	}
	if total == 0 {
		return nil
	}

	active := 0
	for _, p := range points {
		if len(p) > 0 {
			active++
		}
	}
	if active == 0 {
		return nil
	}
	if active == 1 {
		for _, p := range points {
			if len(p) > 0 {
				return p
			}
		}
	}

	idx := make([]int, len(points))
	result := make([]memtable.Point, 0, total)

	for {
		best := -1
		var bestTs int64 = math.MaxInt64
		for i := range points {
			if idx[i] < len(points[i]) && points[i][idx[i]].Timestamp < bestTs {
				bestTs = points[i][idx[i]].Timestamp
				best = i
			}
		}
		if best == -1 {
			break
		}
		result = append(result, points[best][idx[best]])
		idx[best]++
	}
	return result
}

func Filter(points []memtable.Point, start, end int64) []memtable.Point {
	if len(points) == 0 {
		return nil
	}
	first := sort.Search(len(points), func(i int) bool {
		return points[i].Timestamp >= start
	})
	last := sort.Search(len(points), func(i int) bool {
		return points[i].Timestamp > end
	})
	if first >= last {
		return nil
	}
	out := make([]memtable.Point, last-first)
	copy(out, points[first:last])
	return out
}

func Aggregate(points []memtable.Point, bucketWidth int64) []Bucket {
	if len(points) == 0 {
		return nil
	}

	firstBucket := (points[0].Timestamp / bucketWidth) * bucketWidth
	lastBucket := (points[len(points)-1].Timestamp / bucketWidth) * bucketWidth
	numBuckets := int((lastBucket-firstBucket)/bucketWidth) + 1

	buckets := make([]Bucket, numBuckets)
	for i := range buckets {
		buckets[i] = Bucket{
			Start: firstBucket + int64(i)*bucketWidth,
			Min:   math.MaxFloat64,
			Max:   -math.MaxFloat64,
		}
	}

	for _, pt := range points {
		bIdx := int((pt.Timestamp - firstBucket) / bucketWidth)
		b := &buckets[bIdx]
		b.Sum += pt.Value
		b.Count++
		if pt.Value < b.Min {
			b.Min = pt.Value
		}
		if pt.Value > b.Max {
			b.Max = pt.Value
		}
	}

	for i := range buckets {
		if buckets[i].Count == 0 {
			buckets[i].Min = 0
			buckets[i].Max = 0
		}
	}
	return buckets
}

func ReadSegments(paths []string, seriesID uint64) []memtable.Point {
	all := make([][]memtable.Point, 0, len(paths))
	for _, path := range paths {
		r, err := segment.OpenReader(path)
		if err != nil {
			continue
		}
		pts, err := r.ReadSeries(seriesID)
		r.Close()
		if err != nil || len(pts) == 0 {
			continue
		}
		all = append(all, pts)
	}
	return Merge(all...)
}
