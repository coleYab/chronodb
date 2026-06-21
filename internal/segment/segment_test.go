package segment

import (
	"encoding/binary"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/coleYab/chronodb/internal/memtable"
)

func TestWriterHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.seg")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}

	err = w.Write([]SeriesPoints{
		{SeriesID: 1, Points: []memtable.Point{{Timestamp: 1000, Value: 42.5}}},
	}, 1000, 2000)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if string(data[0:4]) != magic {
		t.Errorf("magic: expected %s, got %s", magic, string(data[0:4]))
	}
	if data[4] != version {
		t.Errorf("version: expected %d, got %d", version, data[4])
	}
	blockStart := int64(binary.LittleEndian.Uint64(data[5:13]))
	if blockStart != 1000 {
		t.Errorf("blockStart: expected 1000, got %d", blockStart)
	}
	blockEnd := int64(binary.LittleEndian.Uint64(data[13:21]))
	if blockEnd != 2000 {
		t.Errorf("blockEnd: expected 2000, got %d", blockEnd)
	}
}

func TestWriterSingleSeries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "single.seg")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}

	pts := []memtable.Point{
		{Timestamp: 100, Value: 1.0},
		{Timestamp: 200, Value: 2.0},
		{Timestamp: 300, Value: 3.0},
	}

	err = w.Write([]SeriesPoints{
		{SeriesID: 42, Points: pts},
	}, 100, 300)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	firstTS := int64(binary.LittleEndian.Uint64(data[headerSize : headerSize+8]))
	if firstTS != 100 {
		t.Errorf("firstTimestamp: expected 100, got %d", firstTS)
	}

	deltaOff := headerSize + 8
	delta, n := binary.Varint(data[deltaOff:])
	if n <= 0 {
		t.Fatal("failed to read first delta")
	}
	if delta != 100 {
		t.Errorf("first delta: expected 100, got %d", delta)
	}
	deltaOff += n

	delta2, n2 := binary.Varint(data[deltaOff:])
	if n2 <= 0 {
		t.Fatal("failed to read second delta")
	}
	if delta2 != 100 {
		t.Errorf("second delta: expected 100, got %d", delta2)
	}
	deltaOff += n2

	for i, expected := range []float64{1.0, 2.0, 3.0} {
		bits := binary.LittleEndian.Uint64(data[deltaOff+i*8 : deltaOff+(i+1)*8])
		val := math.Float64frombits(bits)
		if val != expected {
			t.Errorf("value %d: expected %f, got %f", i, expected, val)
		}
	}

	storedIdxOffset := binary.LittleEndian.Uint64(data[len(data)-8:])
	idxOffset := int(storedIdxOffset)

	seriesID := binary.LittleEndian.Uint64(data[idxOffset : idxOffset+8])
	if seriesID != 42 {
		t.Errorf("seriesID in index: expected 42, got %d", seriesID)
	}

	dataOffset := binary.LittleEndian.Uint64(data[idxOffset+8 : idxOffset+16])
	if dataOffset != headerSize {
		t.Errorf("data offset: expected %d, got %d", headerSize, dataOffset)
	}

	count := binary.LittleEndian.Uint32(data[idxOffset+16 : idxOffset+20])
	if count != 3 {
		t.Errorf("point count: expected 3, got %d", count)
	}

	storedCRC := binary.LittleEndian.Uint32(data[len(data)-footerSize : len(data)-8])
	crcData := data[:len(data)-footerSize]
	expectedCRC := crc32.ChecksumIEEE(crcData)
	if storedCRC != expectedCRC {
		t.Errorf("CRC: expected %x, got %x", expectedCRC, storedCRC)
	}
}

func TestWriterMultipleSeries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "multi.seg")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}

	err = w.Write([]SeriesPoints{
		{SeriesID: 2, Points: []memtable.Point{{Timestamp: 100, Value: 1.0}, {Timestamp: 200, Value: 2.0}}},
		{SeriesID: 1, Points: []memtable.Point{{Timestamp: 50, Value: 0.5}}},
	}, 50, 200)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	idxOffset := int(binary.LittleEndian.Uint64(data[len(data)-8:]))

	entry1ID := binary.LittleEndian.Uint64(data[idxOffset : idxOffset+8])
	entry1Offset := binary.LittleEndian.Uint64(data[idxOffset+8 : idxOffset+16])
	entry1Count := binary.LittleEndian.Uint32(data[idxOffset+16 : idxOffset+20])

	entry2ID := binary.LittleEndian.Uint64(data[idxOffset+20 : idxOffset+28])
	entry2Offset := binary.LittleEndian.Uint64(data[idxOffset+28 : idxOffset+36])
	entry2Count := binary.LittleEndian.Uint32(data[idxOffset+36 : idxOffset+40])

	if entry1ID != 1 || entry2ID != 2 {
		t.Errorf("expected series IDs 1,2 sorted, got %d,%d", entry1ID, entry2ID)
	}

	if entry1Count != 1 || entry2Count != 2 {
		t.Errorf("expected counts 1,2, got %d,%d", entry1Count, entry2Count)
	}

	if entry1Offset >= entry2Offset {
		t.Errorf("series 1 should come before series 2 in data, offsets: %d, %d", entry1Offset, entry2Offset)
	}

	storedCRC := binary.LittleEndian.Uint32(data[len(data)-footerSize : len(data)-8])
	crcData := data[:len(data)-footerSize]
	expectedCRC := crc32.ChecksumIEEE(crcData)
	if storedCRC != expectedCRC {
		t.Errorf("CRC mismatch: %x vs %x", storedCRC, expectedCRC)
	}
}

func TestWriterLargeDataSet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large.seg")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}

	pts := make([]memtable.Point, 10000)
	for i := range pts {
		pts[i] = memtable.Point{Timestamp: int64(i * 10), Value: float64(i)}
	}

	err = w.Write([]SeriesPoints{
		{SeriesID: 1, Points: pts},
	}, 0, int64(10000*10))
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if int64(len(data)) != info.Size() {
		t.Errorf("file size mismatch: %d vs %d", len(data), info.Size())
	}

	storedCRC := binary.LittleEndian.Uint32(data[len(data)-footerSize : len(data)-8])
	crcData := data[:len(data)-footerSize]
	expectedCRC := crc32.ChecksumIEEE(crcData)
	if storedCRC != expectedCRC {
		t.Errorf("CRC mismatch: %x vs %x", storedCRC, expectedCRC)
	}

	idxOffset := binary.LittleEndian.Uint64(data[len(data)-8:])
	seriesID := binary.LittleEndian.Uint64(data[idxOffset : idxOffset+8])
	if seriesID != 1 {
		t.Errorf("expected series 1, got %d", seriesID)
	}
	count := binary.LittleEndian.Uint32(data[idxOffset+16 : idxOffset+20])
	if count != 10000 {
		t.Errorf("expected 10000 points, got %d", count)
	}
}

func TestWriterEmptySeriesList(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.seg")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}

	err = w.Write(nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if len(data) != headerSize+footerSize {
		t.Errorf("expected %d bytes for empty segment, got %d", headerSize+footerSize, len(data))
	}

	storedCRC := binary.LittleEndian.Uint32(data[len(data)-footerSize : len(data)-8])
	crcData := data[:len(data)-footerSize]
	expectedCRC := crc32.ChecksumIEEE(crcData)
	if storedCRC != expectedCRC {
		t.Errorf("CRC mismatch: %x vs %x", storedCRC, expectedCRC)
	}
}

func BenchmarkWriter(b *testing.B) {
	pts := make([]memtable.Point, 1000)
	for i := range pts {
		pts[i] = memtable.Point{Timestamp: int64(i * 10), Value: float64(i)}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dir := b.TempDir()
		path := filepath.Join(dir, "bench.seg")
		w, _ := NewWriter(path)
		w.Write([]SeriesPoints{
			{SeriesID: 1, Points: pts},
		}, 0, 10000)
		w.Close()
	}
}

func writeTestSegment(t *testing.T, path string, data []SeriesPoints, start, end int64) {
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Write(data, start, end); err != nil {
		t.Fatal(err)
	}
}

func TestReaderOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reader.seg")
	writeTestSegment(t, path, []SeriesPoints{
		{SeriesID: 1, Points: []memtable.Point{{Timestamp: 100, Value: 1.0}}},
	}, 100, 200)

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	start, end := r.BlockRange()
	if start != 100 || end != 200 {
		t.Errorf("block range: expected (100,200), got (%d,%d)", start, end)
	}

	if r.NumSeries() != 1 {
		t.Errorf("expected 1 series, got %d", r.NumSeries())
	}
}

func TestReaderReadSeries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "read_series.seg")
	writeTestSegment(t, path, []SeriesPoints{
		{SeriesID: 42, Points: []memtable.Point{
			{Timestamp: 100, Value: 1.0},
			{Timestamp: 200, Value: 2.0},
			{Timestamp: 300, Value: 3.0},
		}},
	}, 100, 300)

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	pts, err := r.ReadSeries(42)
	if err != nil {
		t.Fatal(err)
	}

	if len(pts) != 3 {
		t.Fatalf("expected 3 points, got %d", len(pts))
	}
	if pts[0].Timestamp != 100 || pts[0].Value != 1.0 {
		t.Errorf("point 0: expected (100,1.0), got (%d,%f)", pts[0].Timestamp, pts[0].Value)
	}
	if pts[1].Timestamp != 200 || pts[1].Value != 2.0 {
		t.Errorf("point 1: expected (200,2.0), got (%d,%f)", pts[1].Timestamp, pts[1].Value)
	}
	if pts[2].Timestamp != 300 || pts[2].Value != 3.0 {
		t.Errorf("point 2: expected (300,3.0), got (%d,%f)", pts[2].Timestamp, pts[2].Value)
	}
}

func TestReaderMissingSeries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.seg")
	writeTestSegment(t, path, []SeriesPoints{
		{SeriesID: 1, Points: []memtable.Point{{Timestamp: 100, Value: 1.0}}},
	}, 100, 200)

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	pts, err := r.ReadSeries(999)
	if err != nil {
		t.Fatal(err)
	}
	if pts != nil {
		t.Errorf("expected nil for missing series, got %v", pts)
	}
}

func TestReaderMultipleSeries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "multi_read.seg")
	writeTestSegment(t, path, []SeriesPoints{
		{SeriesID: 2, Points: []memtable.Point{
			{Timestamp: 100, Value: 1.0},
			{Timestamp: 200, Value: 2.0},
		}},
		{SeriesID: 1, Points: []memtable.Point{
			{Timestamp: 50, Value: 0.5},
		}},
		{SeriesID: 3, Points: []memtable.Point{
			{Timestamp: 300, Value: 3.0},
			{Timestamp: 400, Value: 4.0},
			{Timestamp: 500, Value: 5.0},
		}},
	}, 50, 500)

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	seriesList := r.ListSeries()
	if len(seriesList) != 3 {
		t.Fatalf("expected 3 series, got %d", len(seriesList))
	}
	if seriesList[0] != 1 || seriesList[1] != 2 || seriesList[2] != 3 {
		t.Errorf("expected sorted [1,2,3], got %v", seriesList)
	}

	pts1, _ := r.ReadSeries(1)
	if len(pts1) != 1 || pts1[0].Value != 0.5 {
		t.Errorf("series 1 mismatch: %v", pts1)
	}
	pts2, _ := r.ReadSeries(2)
	if len(pts2) != 2 || pts2[1].Value != 2.0 {
		t.Errorf("series 2 mismatch: %v", pts2)
	}
	pts3, _ := r.ReadSeries(3)
	if len(pts3) != 3 || pts3[2].Value != 5.0 {
		t.Errorf("series 3 mismatch: %v", pts3)
	}
}

func TestReaderLargeDataSet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large_read.seg")
	pts := make([]memtable.Point, 10000)
	for i := range pts {
		pts[i] = memtable.Point{Timestamp: int64(i * 10), Value: float64(i)}
	}
	writeTestSegment(t, path, []SeriesPoints{
		{SeriesID: 1, Points: pts},
	}, 0, 10000*10)

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	readBack, err := r.ReadSeries(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(readBack) != 10000 {
		t.Fatalf("expected 10000 points, got %d", len(readBack))
	}
	for i := range pts {
		if readBack[i].Timestamp != pts[i].Timestamp || readBack[i].Value != pts[i].Value {
			t.Fatalf("point %d mismatch: (%d,%f) vs (%d,%f)",
				i, readBack[i].Timestamp, readBack[i].Value, pts[i].Timestamp, pts[i].Value)
		}
	}
}

func TestReaderReadAll(t *testing.T) {
	path := filepath.Join(t.TempDir(), "readall.seg")
	writeTestSegment(t, path, []SeriesPoints{
		{SeriesID: 1, Points: []memtable.Point{{Timestamp: 100, Value: 1.0}}},
		{SeriesID: 2, Points: []memtable.Point{{Timestamp: 200, Value: 2.0}}},
	}, 100, 200)

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	all, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("expected 2 series, got %d", len(all))
	}
	if len(all[1]) != 1 || all[1][0].Value != 1.0 {
		t.Errorf("series 1 mismatch")
	}
	if len(all[2]) != 1 || all[2][0].Value != 2.0 {
		t.Errorf("series 2 mismatch")
	}
}

func TestReaderCorruptFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.seg")
	os.WriteFile(path, []byte("not a segment file"), 0644)

	_, err := OpenReader(path)
	if err == nil {
		t.Error("expected error for corrupt file")
	}
}

func TestReaderEmptySegment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.seg")
	w, _ := NewWriter(path)
	w.Write(nil, 0, 0)
	w.Close()

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if r.NumSeries() != 0 {
		t.Errorf("expected 0 series, got %d", r.NumSeries())
	}
}

func BenchmarkReaderRead(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench_read.seg")
	pts := make([]memtable.Point, 1000)
	for i := range pts {
		pts[i] = memtable.Point{Timestamp: int64(i * 10), Value: float64(i)}
	}
	w, _ := NewWriter(path)
	w.Write([]SeriesPoints{{SeriesID: 1, Points: pts}}, 0, 10000)
	w.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ := OpenReader(path)
		r.ReadSeries(1)
		r.Close()
	}
}
