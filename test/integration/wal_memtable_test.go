package integration

import (
	"encoding/binary"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/coleYab/chronodb/internal/engine"
	"github.com/coleYab/chronodb/internal/memtable"
	"github.com/coleYab/chronodb/internal/wal"
)

func encodeWritePayload(p engine.WritePayload) []byte {
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

func decodeWritePayload(data []byte) engine.WritePayload {
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
	return engine.WritePayload{SeriesID: seriesID, Points: pts}
}

func TestWalMemtable(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")

	w, err := wal.NewWriter(walPath, wal.WithFsyncInterval(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}

	mt := memtable.New()

	writePayloads := []engine.WritePayload{
		{SeriesID: 1, Points: []memtable.Point{{Timestamp: 100, Value: 1.0}, {Timestamp: 200, Value: 2.0}}},
		{SeriesID: 2, Points: []memtable.Point{{Timestamp: 150, Value: 3.0}}},
		{SeriesID: 1, Points: []memtable.Point{{Timestamp: 300, Value: 4.0}}},
	}

	for _, p := range writePayloads {
		data := encodeWritePayload(p)
		if err := w.Append(data); err != nil {
			t.Fatal(err)
		}
		mt.Insert(p.SeriesID, p.Points)
	}

	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	w.Close()

	replayed, err := wal.Replay(walPath, 0)
	if err != nil {
		t.Fatal(err)
	}

	recovered := memtable.New()
	for _, data := range replayed {
		p := decodeWritePayload(data)
		recovered.Insert(p.SeriesID, p.Points)
	}

	if recovered.Len() != mt.Len() {
		t.Fatalf("series count mismatch: %d vs %d", recovered.Len(), mt.Len())
	}

	for _, seriesID := range []uint64{1, 2} {
		orig := mt.Get(seriesID)
		recv := recovered.Get(seriesID)
		if len(orig) != len(recv) {
			t.Fatalf("series %d: point count mismatch %d vs %d", seriesID, len(orig), len(recv))
		}
		for i := range orig {
			if orig[i].Timestamp != recv[i].Timestamp || orig[i].Value != recv[i].Value {
				t.Errorf("series %d point %d mismatch: (%d,%f) vs (%d,%f)",
					seriesID, i, orig[i].Timestamp, orig[i].Value, recv[i].Timestamp, recv[i].Value)
			}
		}
	}
}
