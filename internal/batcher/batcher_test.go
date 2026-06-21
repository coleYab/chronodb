package batcher_test

import (
	"context"
	"testing"
	"time"

	"github.com/coleYab/chronodb/internal/batcher"
	"github.com/coleYab/chronodb/internal/core"
)

func TestBatcher_FlushesOnBatchSize(t *testing.T) {
	b := batcher.New(batcher.Config{
		BatchSize:  10,
		FlushEvery: time.Minute,
		QueueSize:  1000,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	for i := 0; i < 10; i++ {
		if err := b.Submit(core.Sample{Metric: "test", Value: float64(i)}); err != nil {
			t.Fatal(err)
		}
	}

	select {
	case batch := <-b.BatchCh():
		if len(batch) != 10 {
			t.Fatalf("expected 10 samples, got %d", len(batch))
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for flush")
	}
}

func TestBatcher_FlushesOnInterval(t *testing.T) {
	b := batcher.New(batcher.Config{
		BatchSize:  100,
		FlushEvery: 50 * time.Millisecond,
		QueueSize:  1000,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	if err := b.Submit(core.Sample{Metric: "test", Value: 1}); err != nil {
		t.Fatal(err)
	}

	select {
	case batch := <-b.BatchCh():
		if len(batch) != 1 {
			t.Fatalf("expected 1 sample, got %d", len(batch))
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for interval flush")
	}
}

func TestBatcher_Backpressure(t *testing.T) {
	b := batcher.New(batcher.Config{
		BatchSize:  100,
		FlushEvery: time.Minute,
		QueueSize:  1,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	if err := b.Submit(core.Sample{Metric: "a"}); err != nil {
		t.Fatal(err)
	}
	err := b.Submit(core.Sample{Metric: "b"})
	if err == nil {
		t.Fatal("expected backpressure error")
	}
}
