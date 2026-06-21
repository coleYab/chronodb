package batcher

import (
	"context"
	"sync"
	"time"

	"github.com/coleYab/chronodb/internal/core"
)

var defaultFlushInterval = 5 * time.Second

type Batcher struct {
	sampleCh   chan core.Sample
	batchCh    chan []core.Sample
	batchSize  int
	flushEvery time.Duration
	queueSize  int
	wg         sync.WaitGroup
}

type Config struct {
	BatchSize  int
	FlushEvery time.Duration
	QueueSize  int
}

func New(cfg Config) *Batcher {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}
	if cfg.FlushEvery <= 0 {
		cfg.FlushEvery = defaultFlushInterval
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 100000
	}
	return &Batcher{
		sampleCh:   make(chan core.Sample, cfg.QueueSize),
		batchCh:    make(chan []core.Sample, 64),
		batchSize:  cfg.BatchSize,
		flushEvery: cfg.FlushEvery,
		queueSize:  cfg.QueueSize,
	}
}

func (b *Batcher) Submit(s core.Sample) error {
	select {
	case b.sampleCh <- s:
		return nil
	default:
		return ErrBackpressure
	}
}

func (b *Batcher) BatchCh() <-chan []core.Sample {
	return b.batchCh
}

func (b *Batcher) Run(ctx context.Context) {
	b.wg.Add(1)
	go b.loop(ctx)
}

func (b *Batcher) loop(ctx context.Context) {
	defer b.wg.Done()
	buf := make([]core.Sample, 0, b.batchSize)
	flushTick := time.NewTicker(b.flushEvery)
	defer flushTick.Stop()

	flush := func() {
		if len(buf) == 0 {
			return
		}
		batch := make([]core.Sample, len(buf))
		copy(batch, buf)
		buf = buf[:0]
		select {
		case b.batchCh <- batch:
		case <-ctx.Done():
			return
		}
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case <-flushTick.C:
			flush()
		case s := <-b.sampleCh:
			buf = append(buf, s)
			if len(buf) >= b.batchSize {
				flush()
			}
		}
	}
}

func (b *Batcher) Flush() {
	b.sampleCh <- core.Sample{Metric: "__flush__"}
}

func (b *Batcher) Wait() {
	b.wg.Wait()
}

var ErrBackpressure = fmtError("batcher queue full")

type fmtError string

func (e fmtError) Error() string { return string(e) }
