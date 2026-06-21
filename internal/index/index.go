package index

import "sort"

type Index struct {
	MetricSeries map[string]map[uint64]struct{}
	TagIndex     map[string]map[string]map[uint64]struct{}
}

func New() *Index {
	return &Index{
		MetricSeries: make(map[string]map[uint64]struct{}),
		TagIndex:     make(map[string]map[string]map[uint64]struct{}),
	}
}

func (idx *Index) Insert(seriesID uint64, metric string, tags map[string]string) {
	if idx.MetricSeries[metric] == nil {
		idx.MetricSeries[metric] = make(map[uint64]struct{})
	}
	idx.MetricSeries[metric][seriesID] = struct{}{}

	for k, v := range tags {
		if idx.TagIndex[metric] == nil {
			idx.TagIndex[metric] = make(map[string]map[uint64]struct{})
		}
		tagKey := k + "=" + v
		if idx.TagIndex[metric][tagKey] == nil {
			idx.TagIndex[metric][tagKey] = make(map[uint64]struct{})
		}
		idx.TagIndex[metric][tagKey][seriesID] = struct{}{}
	}
}

func (idx *Index) Resolve(metric string, tagFilters map[string]string) []uint64 {
	ids, ok := idx.MetricSeries[metric]
	if !ok {
		return nil
	}

	if len(tagFilters) == 0 {
		result := make([]uint64, 0, len(ids))
		for id := range ids {
			result = append(result, id)
		}
		sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
		return result
	}

	var result map[uint64]struct{}
	first := true
	for k, v := range tagFilters {
		tagKey := k + "=" + v
		s, ok := idx.TagIndex[metric][tagKey]
		if !ok {
			return nil
		}
		if first {
			result = make(map[uint64]struct{}, len(s))
			for id := range s {
				result[id] = struct{}{}
			}
			first = false
		} else {
			for id := range result {
				if _, exists := s[id]; !exists {
					delete(result, id)
				}
			}
		}
	}

	if result == nil {
		return nil
	}

	out := make([]uint64, 0, len(result))
	for id := range result {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (idx *Index) ListMetrics() []string {
	metrics := make([]string, 0, len(idx.MetricSeries))
	for m := range idx.MetricSeries {
		metrics = append(metrics, m)
	}
	sort.Strings(metrics)
	return metrics
}

func (idx *Index) ListTags(metric string) []string {
	tags, ok := idx.TagIndex[metric]
	if !ok {
		return nil
	}
	out := make([]string, 0, len(tags))
	for t := range tags {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}
