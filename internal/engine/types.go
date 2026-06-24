package engine

import (
	"time"

	"github.com/coleYab/chronodb/internal/memtable"
)

type CmdKind int

const (
	WriteCmd CmdKind = iota
	QueryCmd
	FlushDoneCmd
	CompactionCmd
	CompactionDoneCmd
	RetentionCmd
	ShutdownCmd
	BatchWriteCmd
)

type Command struct {
	Kind    CmdKind
	Payload interface{}
	RespCh  chan Response
}

type Response struct {
	Data interface{}
	Err  error
}

type WritePayload struct {
	SeriesID uint64
	Points   []memtable.Point
}

type BatchWritePayload []WritePayload

type QueryPayload struct {
	Metric      string
	TagFilters  map[string]string
	StartTime   time.Time
	EndTime     time.Time
	Aggregation string
	BucketWidth time.Duration
}

type FlushDonePayload struct {
	SegmentPath string
	BlockStart  int64
	BlockEnd    int64
	SeriesCount int
	WALOffset   int64
	Err         error
}

type CompactionDonePayload struct {
	OldPaths    []string
	SegmentPath string
	BlockStart  int64
	BlockEnd    int64
	SeriesCount int
	Err         error
}

type RetentionPayload struct {
	Cutoff time.Time
}
