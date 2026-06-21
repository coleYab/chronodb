package wal

import (
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWriterReaderRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := NewWriter(path, WithFsyncInterval(50*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}

	entries := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("foo"),
		[]byte("bar"),
	}

	for _, e := range entries {
		if err := w.Append(e); err != nil {
			t.Fatal(err)
		}
	}

	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	w.Close()

	r, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	var readBack [][]byte
	for {
		data, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		readBack = append(readBack, data)
	}

	if len(readBack) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(readBack))
	}

	for i, e := range entries {
		if string(readBack[i]) != string(e) {
			t.Errorf("entry %d: expected %q, got %q", i, string(e), string(readBack[i]))
		}
	}
}

func TestWriterReaderLargeEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large.wal")
	w, err := NewWriter(path, WithFsyncInterval(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}

	large := make([]byte, 100000)
	for i := range large {
		large[i] = byte(i % 256)
	}

	entries := [][]byte{
		[]byte("small"),
		large,
		[]byte("end"),
	}

	for _, e := range entries {
		if err := w.Append(e); err != nil {
			t.Fatal(err)
		}
	}

	w.Sync()
	w.Close()

	r, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	var readBack [][]byte
	for {
		data, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		readBack = append(readBack, data)
	}

	if len(readBack) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(readBack))
	}

	for i, e := range entries {
		if len(readBack[i]) != len(e) {
			t.Fatalf("entry %d: length mismatch %d vs %d", i, len(readBack[i]), len(e))
		}
		for j := range e {
			if readBack[i][j] != e[j] {
				t.Fatalf("entry %d byte %d: mismatch", i, j)
			}
		}
	}
}

func TestTruncatedRecordIsSkipped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "truncated.wal")
	w, err := NewWriter(path, WithFsyncInterval(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}

	entries := [][]byte{
		[]byte("entry1"),
		[]byte("entry2"),
		[]byte("entry3"),
	}

	for _, e := range entries {
		if err := w.Append(e); err != nil {
			t.Fatal(err)
		}
	}
	w.Sync()

	w.Close()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	truncatedSize := info.Size() - 3
	if err := os.Truncate(path, truncatedSize); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	var readBack [][]byte
	for {
		data, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		readBack = append(readBack, data)
	}

	if len(readBack) != 2 {
		t.Fatalf("expected 2 entries after truncation, got %d", len(readBack))
	}
}

func TestOnlyFsyncedEntriesSurvive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crash.wal")
	w, err := NewWriter(path, WithFsyncInterval(1*time.Hour), WithByteThreshold(1<<30))
	if err != nil {
		t.Fatal(err)
	}

	entries := [][]byte{
		[]byte("before_sync"),
		[]byte("also_before_sync"),
	}

	for _, e := range entries {
		if err := w.Append(e); err != nil {
			t.Fatal(err)
		}
	}

	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}

	synced := w.Offset()

	unsynced := []byte("not_synced")
	if err := w.Append(unsynced); err != nil {
		t.Fatal(err)
	}

	w.mu.Lock()
	w.ticker.Stop()
	w.closed = true
	w.mu.Unlock()
	w.file.Close()

	if err := os.Truncate(path, synced); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	var readBack [][]byte
	for {
		data, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		readBack = append(readBack, data)
	}

	if len(readBack) != 2 {
		t.Fatalf("expected 2 fsynced entries, got %d", len(readBack))
	}
}

func TestReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replay.wal")
	w, err := NewWriter(path, WithFsyncInterval(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}

	entries := [][]byte{
		[]byte("first"),
		[]byte("second"),
		[]byte("third"),
	}

	for _, e := range entries {
		if err := w.Append(e); err != nil {
			t.Fatal(err)
		}
	}
	w.Sync()
	w.Close()

	replayed, err := Replay(path, 0)
	if err != nil {
		t.Fatal(err)
	}

	if len(replayed) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(replayed))
	}

	for i, e := range entries {
		if string(replayed[i]) != string(e) {
			t.Errorf("entry %d: expected %q, got %q", i, string(e), string(replayed[i]))
		}
	}
}

func TestReplayFromOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replay_offset.wal")
	w, err := NewWriter(path, WithFsyncInterval(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}

	entries := [][]byte{
		[]byte("skip_this"),
		[]byte("keep_this"),
		[]byte("also_keep"),
	}

	for _, e := range entries {
		if err := w.Append(e); err != nil {
			t.Fatal(err)
		}
	}
	w.Sync()
	w.Close()

	r, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}

	r.Read()
	offset := r.Offset()
	r.Close()

	replayed, err := Replay(path, offset)
	if err != nil {
		t.Fatal(err)
	}

	if len(replayed) != 2 {
		t.Fatalf("expected 2 entries after offset, got %d", len(replayed))
	}

	if string(replayed[0]) != "keep_this" {
		t.Errorf("expected %q, got %q", "keep_this", string(replayed[0]))
	}
}

func TestOffsetTracking(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offset.wal")
	w, err := NewWriter(path, WithFsyncInterval(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}

	if w.Offset() != 0 {
		t.Fatalf("expected offset 0, got %d", w.Offset())
	}

	if err := w.Append([]byte("test")); err != nil {
		t.Fatal(err)
	}

	recordLen := recordHeaderSize + 4
	if w.Offset() != int64(recordLen) {
		t.Fatalf("expected offset %d, got %d", recordLen, w.Offset())
	}

	w.Sync()
	w.Close()

	r, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	r.Read()
	if r.Offset() != int64(recordLen) {
		t.Fatalf("expected reader offset %d, got %d", recordLen, r.Offset())
	}
	r.Close()
}

func TestMultipleSyncs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "multi_sync.wal")
	w, err := NewWriter(path, WithFsyncInterval(1*time.Hour))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		if err := w.Append([]byte("data")); err != nil {
			t.Fatal(err)
		}
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	r, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	count := 0
	for {
		_, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		count++
	}

	if count != 10 {
		t.Fatalf("expected 10 entries, got %d", count)
	}
}

func TestCRCIntegrity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crc.wal")
	w, err := NewWriter(path, WithFsyncInterval(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}

	if err := w.Append([]byte("valid")); err != nil {
		t.Fatal(err)
	}
	w.Sync()
	w.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	data[10] ^= 0xFF

	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	_, err = r.Read()
	if err != io.EOF {
		t.Fatalf("expected EOF for corrupted data, got %v", err)
	}
}

func TestEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.wal")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	r, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	_, err = r.Read()
	if err != io.EOF {
		t.Fatalf("expected EOF for empty file, got %v", err)
	}
}

func TestCloseIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "close_idem.wal")
	w, err := NewWriter(path, WithFsyncInterval(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("second close should succeed, got %v", err)
	}
}

func TestAppendAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "append_after_close.wal")
	w, err := NewWriter(path, WithFsyncInterval(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}

	w.Close()

	err = w.Append([]byte("should_fail"))
	if err == nil {
		t.Fatal("expected error appending to closed writer")
	}
}

func BenchmarkAppend(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.wal")
	w, err := NewWriter(path, WithFsyncInterval(1*time.Hour), WithByteThreshold(1<<30))
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	data := make([]byte, 256)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.Append(data)
	}
}
