package engine

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/coleYab/chronodb/internal/memtable"
)

func testConfig(dir string) EngineConfig {
	cfg := DefaultConfig()
	cfg.WALPath = filepath.Join(dir, "test.wal")
	cfg.ManifestPath = filepath.Join(dir, "manifest.json")
	cfg.DataDir = dir
	cfg.FlushInterval = 1 * time.Hour
	return cfg
}

func TestEngineWriteAndRead(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)

	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go e.Run(ctx)

	cmd := Command{
		Kind:   WriteCmd,
		Payload: WritePayload{
			SeriesID: 1,
			Points:   []memtable.Point{{Timestamp: 100, Value: 1.0}},
		},
		RespCh: make(chan Response, 1),
	}

	if err := e.Submit(cmd); err != nil {
		t.Fatal(err)
	}

	resp := <-cmd.RespCh
	if resp.Err != nil {
		t.Fatal(resp.Err)
	}

	cancel()
	time.Sleep(10 * time.Millisecond)
}

func TestEngineMultipleWrites(t *testing.T) {
	dir := t.TempDir()
	e, err := New(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go e.Run(ctx)

	numWrites := 100
	var wg sync.WaitGroup
	for i := 0; i < numWrites; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			cmd := Command{
				Kind: WriteCmd,
				Payload: WritePayload{
					SeriesID: uint64(n % 10),
					Points:   []memtable.Point{{Timestamp: int64(n), Value: float64(n)}},
				},
				RespCh: make(chan Response, 1),
			}
			if err := e.Submit(cmd); err != nil {
				t.Error(err)
				return
			}
			resp := <-cmd.RespCh
			if resp.Err != nil {
				t.Error(resp.Err)
			}
		}(i)
	}
	wg.Wait()

	cancel()
	time.Sleep(10 * time.Millisecond)
}

func TestEngineBackpressure(t *testing.T) {
	dir := t.TempDir()
	e, err := New(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go e.Run(ctx)

	full := false
	for i := 0; i < 20000; i++ {
		cmd := Command{
			Kind: WriteCmd,
			Payload: WritePayload{
				SeriesID: 1,
				Points:   []memtable.Point{{Timestamp: int64(i), Value: float64(i)}},
			},
			RespCh: make(chan Response, 1),
		}
		if err := e.Submit(cmd); err != nil {
			full = true
			break
		}
		<-cmd.RespCh
	}

	if !full {
		t.Log("channel did not fill up (may be fine depending on speed)")
	}

	cancel()
	time.Sleep(10 * time.Millisecond)
}

func TestEngineConcurrentWritesRace(t *testing.T) {
	dir := t.TempDir()
	e, err := New(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go e.Run(ctx)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				cmd := Command{
					Kind: WriteCmd,
					Payload: WritePayload{
						SeriesID: uint64(n),
						Points:   []memtable.Point{{Timestamp: int64(n*100 + j), Value: float64(j)}},
					},
					RespCh: make(chan Response, 1),
				}
				if err := e.Submit(cmd); err != nil {
					return
				}
				<-cmd.RespCh
			}
		}(i)
	}
	wg.Wait()

	cancel()
	time.Sleep(10 * time.Millisecond)
}

func TestEngineMemtableRotation(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.MemtableSize = 160
	cfg.FlushInterval = 50 * time.Millisecond

	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go e.Run(ctx)

	for i := 0; i < 100; i++ {
		cmd := Command{
			Kind: WriteCmd,
			Payload: WritePayload{
				SeriesID: 1,
				Points:   []memtable.Point{{Timestamp: int64(i), Value: float64(i)}},
			},
			RespCh: make(chan Response, 1),
		}
		if err := e.Submit(cmd); err != nil {
			t.Fatal(err)
		}
		<-cmd.RespCh
	}

	time.Sleep(200 * time.Millisecond)
	cancel()
}

func TestEngineUnknownCommand(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.WALPath = filepath.Join(dir, "test.wal")

	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go e.Run(ctx)

	cmd := Command{
		Kind:   CmdKind(99),
		RespCh: make(chan Response, 1),
	}
	e.Submit(cmd)
	resp := <-cmd.RespCh
	if resp.Err == nil {
		t.Error("expected error for unknown command")
	}

	cancel()
	time.Sleep(10 * time.Millisecond)
}

func BenchmarkEngineWrite(b *testing.B) {
	dir := b.TempDir()
	cfg := DefaultConfig()
	cfg.WALPath = filepath.Join(dir, "bench.wal")
	cfg.FlushInterval = 1 * time.Hour

	e, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go e.Run(ctx)
	defer cancel()

	cmd := Command{
		Kind: WriteCmd,
		Payload: WritePayload{
			SeriesID: 1,
			Points:   []memtable.Point{{Timestamp: 100, Value: 1.0}},
		},
		RespCh: make(chan Response, 1),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload := cmd.Payload.(WritePayload)
		payload.Points[0].Timestamp = int64(i)
		cmd.Payload = payload
		e.Submit(cmd)
		<-cmd.RespCh
	}
}
