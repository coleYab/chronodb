package segment

import (
	"encoding/binary"
	"hash/crc32"
	"math"
	"os"
	"sort"

	"github.com/coleYab/chronodb/internal/memtable"
)

const (
	magic      = "CHDB"
	version    = 1
	headerSize = 21
	footerSize = 12
)

type SeriesPoints struct {
	SeriesID uint64
	Points   []memtable.Point
}

type Writer struct {
	file    *os.File
	offset  int64
	crc     uint32
	version uint8
}

func NewWriter(path string) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &Writer{file: f, version: 1}, nil
}

func NewWriterV2(path string) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &Writer{file: f, version: 2}, nil
}

func (w *Writer) Write(seriesList []SeriesPoints, blockStart, blockEnd int64) error {
	sort.Slice(seriesList, func(i, j int) bool {
		return seriesList[i].SeriesID < seriesList[j].SeriesID
	})

	if err := w.writeHeader(blockStart, blockEnd); err != nil {
		return err
	}

	type indexEntry struct {
		seriesID  uint64
		offset    uint64
		numPoints uint32
	}
	entries := make([]indexEntry, 0, len(seriesList))
	var dataBuf []byte

	for _, sp := range seriesList {
		if len(sp.Points) == 0 {
			continue
		}
		entry := indexEntry{
			seriesID:  sp.SeriesID,
			offset:    uint64(w.offset),
			numPoints: uint32(len(sp.Points)),
		}
		seriesData := w.encodeSeries(sp.Points)
		dataBuf = append(dataBuf, seriesData...)
		w.offset += int64(len(seriesData))
		w.crc = crc32.Update(w.crc, crc32.IEEETable, seriesData)
		entries = append(entries, entry)
	}

	if _, err := w.file.Write(dataBuf); err != nil {
		return err
	}

	idxOffset := uint64(w.offset)
	for _, e := range entries {
		var buf [20]byte
		binary.LittleEndian.PutUint64(buf[0:8], e.seriesID)
		binary.LittleEndian.PutUint64(buf[8:16], e.offset)
		binary.LittleEndian.PutUint32(buf[16:20], e.numPoints)
		if _, err := w.file.Write(buf[:]); err != nil {
			return err
		}
		w.offset += 20
		w.crc = crc32.Update(w.crc, crc32.IEEETable, buf[:])
	}

	return w.writeFooter(idxOffset)
}

func (w *Writer) writeHeader(blockStart, blockEnd int64) error {
	var buf [headerSize]byte
	copy(buf[0:4], magic)
	buf[4] = w.version
	binary.LittleEndian.PutUint64(buf[5:13], uint64(blockStart))
	binary.LittleEndian.PutUint64(buf[13:21], uint64(blockEnd))
	n, err := w.file.Write(buf[:])
	if err != nil {
		return err
	}
	w.offset += int64(n)
	w.crc = crc32.Update(w.crc, crc32.IEEETable, buf[:])
	return nil
}

func (w *Writer) writeFooter(indexOffset uint64) error {
	var buf [footerSize]byte
	binary.LittleEndian.PutUint32(buf[0:4], w.crc)
	binary.LittleEndian.PutUint64(buf[4:12], indexOffset)
	_, err := w.file.Write(buf[:])
	if err != nil {
		return err
	}
	w.offset += footerSize
	return nil
}

func (w *Writer) encodeSeries(points []memtable.Point) []byte {
	if len(points) == 0 {
		return nil
	}
	if w.version >= 2 {
		return gorillaEncodeSeries(points)
	}
	var buf []byte
	firstTS := make([]byte, 8)
	binary.LittleEndian.PutUint64(firstTS, uint64(points[0].Timestamp))
	buf = append(buf, firstTS...)

	for i := 1; i < len(points); i++ {
		delta := points[i].Timestamp - points[i-1].Timestamp
		var d [binary.MaxVarintLen64]byte
		n := binary.PutVarint(d[:], delta)
		buf = append(buf, d[:n]...)
	}

	for _, p := range points {
		var v [8]byte
		binary.LittleEndian.PutUint64(v[:], math.Float64bits(p.Value))
		buf = append(buf, v[:]...)
	}
	return buf
}

func (w *Writer) Close() error {
	return w.file.Close()
}

type Reader struct {
	file       *os.File
	headerSize int64
	index      []indexEntry
	blockStart int64
	blockEnd   int64
	version    uint8
}

type indexEntry struct {
	seriesID  uint64
	offset    uint64
	numPoints uint32
}

func OpenReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r := &Reader{file: f, headerSize: headerSize}
	if err := r.readFooter(); err != nil {
		f.Close()
		return nil, err
	}
	if err := r.readHeader(); err != nil {
		f.Close()
		return nil, err
	}
	if err := r.readIndex(); err != nil {
		f.Close()
		return nil, err
	}
	return r, nil
}

func (r *Reader) readFooter() error {
	info, err := r.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() < int64(headerSize+footerSize) {
		return errCorrupt
	}
	return nil
}

func (r *Reader) readHeader() error {
	var buf [headerSize]byte
	_, err := r.file.ReadAt(buf[:], 0)
	if err != nil {
		return err
	}
	if string(buf[0:4]) != magic {
		return errCorrupt
	}
	r.version = buf[4]
	r.blockStart = int64(binary.LittleEndian.Uint64(buf[5:13]))
	r.blockEnd = int64(binary.LittleEndian.Uint64(buf[13:21]))
	return nil
}

func (r *Reader) readIndex() error {
	info, err := r.file.Stat()
	if err != nil {
		return err
	}
	footerOff := info.Size() - footerSize
	var idxOffsetBuf [8]byte
	_, err = r.file.ReadAt(idxOffsetBuf[:], footerOff+4)
	if err != nil {
		return err
	}
	idxOffset := int64(binary.LittleEndian.Uint64(idxOffsetBuf[:]))
	idxSize := footerOff - idxOffset
	if idxSize < 0 || idxSize%20 != 0 {
		return errCorrupt
	}
	numEntries := int(idxSize / 20)
	idxBuf := make([]byte, idxSize)
	_, err = r.file.ReadAt(idxBuf, idxOffset)
	if err != nil {
		return err
	}
	r.index = make([]indexEntry, numEntries)
	for i := 0; i < numEntries; i++ {
		base := i * 20
		r.index[i] = indexEntry{
			seriesID:  binary.LittleEndian.Uint64(idxBuf[base : base+8]),
			offset:    binary.LittleEndian.Uint64(idxBuf[base+8 : base+16]),
			numPoints: binary.LittleEndian.Uint32(idxBuf[base+16 : base+20]),
		}
	}
	return nil
}

func (r *Reader) ReadSeries(seriesID uint64) ([]memtable.Point, error) {
	idx := sort.Search(len(r.index), func(i int) bool {
		return r.index[i].seriesID >= seriesID
	})
	if idx >= len(r.index) || r.index[idx].seriesID != seriesID {
		return nil, nil
	}
	entry := r.index[idx]
	pts, err := r.decodeSeriesAt(int64(entry.offset), int(entry.numPoints))
	if err != nil {
		return nil, err
	}
	return pts, nil
}

func (r *Reader) decodeSeriesAt(offset int64, numPoints int) ([]memtable.Point, error) {
	if r.version >= 2 {
		maxSize := numPoints * 16
		data := make([]byte, maxSize)
		n, err := r.file.ReadAt(data, offset)
		if err != nil && err.Error() != "EOF" {
			return nil, err
		}
		return gorillaDecodeSeries(data[:n], numPoints)
	}
	maxSize := 8 + numPoints*8 + numPoints*8
	data := make([]byte, maxSize)
	n, err := r.file.ReadAt(data, offset)
	if err != nil && err.Error() != "EOF" {
		return nil, err
	}
	return decodePoints(data[:n], numPoints)
}

func (r *Reader) ReadAll() (map[uint64][]memtable.Point, error) {
	result := make(map[uint64][]memtable.Point, len(r.index))
	for _, entry := range r.index {
		pts, err := r.ReadSeries(entry.seriesID)
		if err != nil {
			return nil, err
		}
		result[entry.seriesID] = pts
	}
	return result, nil
}

func (r *Reader) BlockRange() (int64, int64) {
	return r.blockStart, r.blockEnd
}

func (r *Reader) NumSeries() int {
	return len(r.index)
}

func (r *Reader) ListSeries() []uint64 {
	ids := make([]uint64, len(r.index))
	for i, e := range r.index {
		ids[i] = e.seriesID
	}
	return ids
}

func (r *Reader) Close() error {
	return r.file.Close()
}

func decodePoints(data []byte, numPoints int) ([]memtable.Point, error) {
	if numPoints == 0 {
		return nil, nil
	}
	pts := make([]memtable.Point, numPoints)
	off := 0
	pts[0].Timestamp = int64(binary.LittleEndian.Uint64(data[off : off+8]))
	off += 8

	for i := 1; i < numPoints; i++ {
		delta, n := binary.Varint(data[off:])
		if n <= 0 {
			return nil, errCorrupt
		}
		pts[i].Timestamp = pts[i-1].Timestamp + delta
		off += n
	}
	for i := 0; i < numPoints; i++ {
		pts[i].Value = math.Float64frombits(binary.LittleEndian.Uint64(data[off : off+8]))
		off += 8
	}
	return pts, nil
}

var errCorrupt = &Error{"corrupt segment file"}

type Error struct {
	msg string
}

func (e *Error) Error() string { return e.msg }
