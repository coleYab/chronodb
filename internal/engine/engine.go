package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/coleYab/chronodb/internal/compaction"
	"github.com/coleYab/chronodb/internal/index"
	"github.com/coleYab/chronodb/internal/manifest"
	"github.com/coleYab/chronodb/internal/memtable"
	"github.com/coleYab/chronodb/internal/query"
	"github.com/coleYab/chronodb/internal/retention"
	"github.com/coleYab/chronodb/internal/segment"
	"github.com/coleYab/chronodb/internal/wal"
)

var ErrBackpressure = &Error{"engine busy", true}
var ErrUnknownCommand = &Error{"unknown command", false}

type Error struct {
	msg   string
	retry bool
}

func (e *Error) Error() string { return e.msg }

type Metrics struct {
	PointsWritten  atomic.Int64
	WritesOK       atomic.Int64
	WritesError    atomic.Int64
	FlushesTotal   atomic.Int64
	CompactionsTotal atomic.Int64
	QueriesTotal   atomic.Int64
}

type Engine struct {
	cmdCh            chan Command
	flushTicker      *time.Ticker
	flushDoneCh      chan FlushDonePayload
	compactionDoneCh chan CompactionDonePayload
	active           *memtable.Memtable
	frozen           *memtable.Memtable
	wal              *wal.Writer
	manifest         *manifest.Manifest
	index            *index.Index
	metrics          Metrics
	memtableSize     int
	walPath          string
	dataDir          string
	flushCount       int
	lastActivity     atomic.Int64
}

type EngineConfig struct {
	WALPath       string
	DataDir       string
	ManifestPath  string
	MemtableSize  int
	FlushInterval time.Duration
}

func DefaultConfig() EngineConfig {
	return EngineConfig{
		MemtableSize:  128 << 20,
		FlushInterval: 10 * time.Second,
	}
}

func New(cfg EngineConfig) (*Engine, error) {
	w, err := wal.NewWriter(cfg.WALPath,
		wal.WithFsyncInterval(10*time.Millisecond),
		wal.WithByteThreshold(1<<20),
	)
	if err != nil {
		return nil, err
	}
	m, err := manifest.Load(cfg.ManifestPath)
	if err != nil {
		w.Close()
		return nil, err
	}
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = filepath.Dir(cfg.WALPath)
	}
	e := &Engine{
		cmdCh:            make(chan Command, 10000),
		flushTicker:      time.NewTicker(cfg.FlushInterval),
		flushDoneCh:      make(chan FlushDonePayload, 10),
		compactionDoneCh: make(chan CompactionDonePayload, 10),
		active:           memtable.New(),
		wal:              w,
		manifest:         m,
		index:            index.New(),
		memtableSize:     cfg.MemtableSize,
		walPath:          cfg.WALPath,
		dataDir:          dataDir,
	}
	e.lastActivity.Store(time.Now().Unix())
	return e, nil
}

func (e *Engine) Recover() error {
	offset := e.manifest.GetWALOffset()
	slog.Info("recovering", "wal_offset", offset)
	entries, err := wal.Replay(e.walPath, offset)
	if err != nil {
		slog.Error("recovery failed", "err", err)
		return err
	}
	for _, data := range entries {
		p := decodeWritePayload(data)
		e.active.Insert(p.SeriesID, p.Points)
	}
	slog.Info("recovery complete", "entries_replayed", len(entries))
	return nil
}

func (e *Engine) Submit(cmd Command) error {
	select {
	case e.cmdCh <- cmd:
		return nil
	default:
		return ErrBackpressure
	}
}

func (e *Engine) Run(ctx context.Context) {
	for {
		select {
		case cmd := <-e.cmdCh:
			e.lastActivity.Store(time.Now().Unix())
			e.dispatch(cmd)
		case <-e.flushTicker.C:
			e.lastActivity.Store(time.Now().Unix())
			e.maybeRotateMemtable()
		case res := <-e.flushDoneCh:
			e.lastActivity.Store(time.Now().Unix())
			e.applyFlushResult(res)
		case res := <-e.compactionDoneCh:
			e.lastActivity.Store(time.Now().Unix())
			e.applyCompactionResult(res)
		case <-ctx.Done():
			e.shutdown()
			return
		}
	}
}

func (e *Engine) dispatch(cmd Command) {
	switch cmd.Kind {
	case WriteCmd:
		e.handleWrite(cmd)
	case QueryCmd:
		e.handleQuery(cmd)
	case FlushDoneCmd:
		payload := cmd.Payload.(FlushDonePayload)
		e.flushDoneCh <- payload
		cmd.RespCh <- Response{}
	case CompactionCmd:
		e.handleCompaction(cmd)
	case RetentionCmd:
		e.handleRetention(cmd)
	default:
		cmd.RespCh <- Response{Err: ErrUnknownCommand}
	}
}

func (e *Engine) handleCompaction(cmd Command) {
	entries := e.manifest.List()
	candidates := compaction.SelectCandidates(entries, 5)
	if len(candidates) == 0 {
		cmd.RespCh <- Response{Data: 0}
		return
	}

	slog.Info("compaction starting", "candidates", len(candidates))
	go func(cands []manifest.SegmentEntry, dir string) {
		outputPath := filepath.Join(dir, fmt.Sprintf("compacted-%d.seg", len(cands)))
		result, err := compaction.Compact(cands, outputPath)
		if err != nil {
			e.compactionDoneCh <- CompactionDonePayload{Err: err}
			return
		}
		oldPaths := make([]string, len(cands))
		for i, c := range cands {
			oldPaths[i] = c.Path
		}
		e.compactionDoneCh <- CompactionDonePayload{
			OldPaths:    oldPaths,
			SegmentPath: result.Path,
			BlockStart:  result.BlockStart,
			BlockEnd:    result.BlockEnd,
			SeriesCount: result.SeriesCount,
		}
	}(candidates, e.dataDir)

	cmd.RespCh <- Response{Data: len(candidates)}
}

func (e *Engine) applyCompactionResult(res CompactionDonePayload) {
	if res.Err != nil {
		slog.Error("compaction failed", "err", res.Err)
		return
	}
	e.metrics.CompactionsTotal.Add(1)
	slog.Info("compaction complete", "path", res.SegmentPath, "old_segments", len(res.OldPaths))
	entry := manifest.SegmentEntry{
		Path:        res.SegmentPath,
		BlockStart:  res.BlockStart,
		BlockEnd:    res.BlockEnd,
		SeriesCount: res.SeriesCount,
	}
	if err := e.manifest.Add(entry); err != nil {
		return
	}
	if err := e.manifest.RemoveBatch(res.OldPaths); err != nil {
		return
	}
	for _, path := range res.OldPaths {
		os.Remove(path)
	}
}

func (e *Engine) handleRetention(cmd Command) {
	payload := cmd.Payload.(RetentionPayload)
	entries := e.manifest.List()
	result := retention.Sweep(entries, payload.Cutoff)
	if len(result.Removed) == 0 {
		cmd.RespCh <- Response{Data: 0}
		return
	}
	removePaths := make([]string, len(result.Removed))
	for i, entry := range result.Removed {
		removePaths[i] = entry.Path
	}
	if err := e.manifest.RemoveBatch(removePaths); err != nil {
		cmd.RespCh <- Response{Err: err}
		return
	}
	retention.DeleteFiles(result.Removed)
	cmd.RespCh <- Response{Data: len(result.Removed)}
}

func (e *Engine) handleQuery(cmd Command) {
	payload := cmd.Payload.(QueryPayload)
	startTime := payload.StartTime.UnixMilli()
	endTime := payload.EndTime.UnixMilli()
	bucketWidth := payload.BucketWidth.Milliseconds()

	seriesIDs := e.index.Resolve(payload.Metric, payload.TagFilters)
	if len(seriesIDs) == 0 {
		e.metrics.QueriesTotal.Add(1)
		cmd.RespCh <- Response{Data: []query.Result{}}
		return
	}

	var segPaths []string
	for _, entry := range e.manifest.List() {
		if entry.BlockStart <= endTime && entry.BlockEnd >= startTime {
			segPaths = append(segPaths, entry.Path)
		}
	}

	results := make([]query.Result, 0, len(seriesIDs))
	for _, sid := range seriesIDs {
		activePts := e.active.Get(sid)
		var frozenPts []memtable.Point
		if e.frozen != nil {
			frozenPts = e.frozen.Get(sid)
		}
		segPts := query.ReadSegments(segPaths, sid)
		merged := query.Merge(activePts, frozenPts, segPts)
		merged = query.Filter(merged, startTime, endTime)
		var buckets []query.Bucket
		var points []memtable.Point
		if bucketWidth > 0 {
			buckets = query.Aggregate(merged, bucketWidth)
		} else {
			points = merged
		}
		results = append(results, query.Result{
			SeriesID: sid,
			Buckets:  buckets,
			Points:   points,
		})
	}

	e.metrics.QueriesTotal.Add(1)
	slog.Debug("query complete", "series", len(seriesIDs), "segments", len(segPaths))
	cmd.RespCh <- Response{Data: results}
}

func (e *Engine) SetIndex(idx *index.Index) {
	e.index = idx
}

func (e *Engine) handleWrite(cmd Command) {
	payload := cmd.Payload.(WritePayload)
	data := encodeWritePayload(payload)
	if err := e.wal.Append(data); err != nil {
		e.metrics.WritesError.Add(1)
		slog.Error("write failed", "series_id", payload.SeriesID, "points", len(payload.Points), "err", err)
		cmd.RespCh <- Response{Err: err}
		return
	}
	e.active.Insert(payload.SeriesID, payload.Points)
	e.metrics.PointsWritten.Add(int64(len(payload.Points)))
	e.metrics.WritesOK.Add(1)
	slog.Debug("write ok", "series_id", payload.SeriesID, "points", len(payload.Points))
	cmd.RespCh <- Response{}
}

func (e *Engine) maybeRotateMemtable() {
	if e.active.SizeBytes() < e.memtableSize {
		return
	}
	frozen := e.active.Freeze()
	e.active = memtable.New()
	e.frozen = frozen

	flushCount := e.flushCount
	e.flushCount++
	slog.Info("memtable rotated", "series", frozen.Len(), "size_bytes", e.active.SizeBytes(), "flush_count", flushCount)

	go func(f *memtable.Memtable, dir string, n int, walOff int64) {
		allData := f.GetAll()
		if len(allData) == 0 {
			e.flushDoneCh <- FlushDonePayload{WALOffset: walOff}
			return
		}

		var seriesList []segment.SeriesPoints
		var blockStart, blockEnd int64
		first := true
		for id, pts := range allData {
			seriesList = append(seriesList, segment.SeriesPoints{SeriesID: id, Points: pts})
			if first || pts[0].Timestamp < blockStart {
				blockStart = pts[0].Timestamp
			}
			if first || pts[len(pts)-1].Timestamp > blockEnd {
				blockEnd = pts[len(pts)-1].Timestamp
			}
			first = false
		}

		segPath := filepath.Join(dir, fmt.Sprintf("seg-%06d.seg", n))
		w, err := segment.NewWriter(segPath)
		if err != nil {
			e.flushDoneCh <- FlushDonePayload{Err: err}
			return
		}
		if err := w.Write(seriesList, blockStart, blockEnd); err != nil {
			w.Close()
			e.flushDoneCh <- FlushDonePayload{Err: err}
			return
		}
		w.Close()

		e.flushDoneCh <- FlushDonePayload{
			SegmentPath: segPath,
			BlockStart:  blockStart,
			BlockEnd:    blockEnd,
			SeriesCount: len(seriesList),
			WALOffset:   walOff,
		}
	}(e.frozen, e.dataDir, flushCount, e.wal.Offset())
}

func (e *Engine) applyFlushResult(res FlushDonePayload) {
	if res.Err != nil {
		slog.Error("flush failed", "err", res.Err)
		e.frozen = nil
		return
	}
	e.metrics.FlushesTotal.Add(1)
	if res.SegmentPath != "" {
		slog.Info("flush complete", "path", res.SegmentPath, "series", res.SeriesCount, "wal_offset", res.WALOffset)
		entry := manifest.SegmentEntry{
			Path:        res.SegmentPath,
			BlockStart:  res.BlockStart,
			BlockEnd:    res.BlockEnd,
			SeriesCount: res.SeriesCount,
		}
		_ = e.manifest.Add(entry)
		_ = e.manifest.SetWALOffset(res.WALOffset)
	}
	e.frozen = nil
}

func (e *Engine) Sync() error {
	return e.wal.Sync()
}

func (e *Engine) Close() error {
	e.flushTicker.Stop()
	return e.wal.Close()
}

func (e *Engine) shutdown() {
	slog.Info("engine shutting down")
	e.flushTicker.Stop()
	e.wal.Close()
	slog.Info("engine shut down complete")
}

func (e *Engine) WALOffset() int64 {
	return e.wal.Offset()
}

func (e *Engine) GetPoints(seriesID uint64) []memtable.Point {
	return e.active.Get(seriesID)
}

func (e *Engine) FrozenPoints(seriesID uint64) []memtable.Point {
	if e.frozen != nil {
		return e.frozen.Get(seriesID)
	}
	return nil
}

func (e *Engine) MemtableSize() int {
	return e.active.Len()
}

func (e *Engine) Manifest() *manifest.Manifest {
	return e.manifest
}

func (e *Engine) Metrics() *Metrics {
	return &e.metrics
}

func (e *Engine) Liveness() bool {
	last := e.lastActivity.Load()
	return time.Now().Unix()-last < 5
}

func decodeWritePayload(data []byte) WritePayload {
	seriesID := binary.LittleEndian.Uint64(data[0:8])
	count := binary.LittleEndian.Uint32(data[8:12])
	pts := make([]memtable.Point, count)
	for i := uint32(0); i < count; i++ {
		off := 12 + i*16
		pts[i] = memtable.Point{
			Timestamp: int64(binary.LittleEndian.Uint64(data[off : off+8])),
			Value:     math.Float64frombits(binary.LittleEndian.Uint64(data[off+8 : off+16])),
		}
	}
	return WritePayload{SeriesID: seriesID, Points: pts}
}

func encodeWritePayload(p WritePayload) []byte {
	buf := make([]byte, 8+4+len(p.Points)*16)
	binary.LittleEndian.PutUint64(buf[0:8], p.SeriesID)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(p.Points)))
	for i, pt := range p.Points {
		off := 12 + i*16
		binary.LittleEndian.PutUint64(buf[off:off+8], uint64(pt.Timestamp))
		binary.LittleEndian.PutUint64(buf[off+8:off+16], math.Float64bits(pt.Value))
	}
	return buf
}
