package core

type Sample struct {
	Metric    string
	Tags      map[string]string
	Timestamp int64
	Value     float64
}
