package wal

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"
)

const (
	recordHeaderSize = 8
	defaultFsyncInterval = 10 * time.Millisecond
	defaultByteThreshold = 1 << 20
)

type Writer struct {
	file         *os.File
	buf          *bufio.Writer
	fsyncInterval time.Duration
	byteThreshold int
	bytesSinceFlush int
	ticker       *time.Ticker
	offset       int64
	mu           sync.Mutex
	closed       bool
}

type Option func(*Writer)

func WithFsyncInterval(d time.Duration) Option {
	return func(w *Writer) {
		if d > 0 {
			w.fsyncInterval = d
		}
	}
}

func WithByteThreshold(n int) Option {
	return func(w *Writer) {
		if n > 0 {
			w.byteThreshold = n
		}
	}
}

func NewWriter(path string, opts ...Option) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	w := &Writer{
		file:         f,
		buf:          bufio.NewWriterSize(f, 1<<20),
		fsyncInterval: defaultFsyncInterval,
		byteThreshold: defaultByteThreshold,
		ticker:       time.NewTicker(defaultFsyncInterval),
		offset:       info.Size(),
	}
	for _, opt := range opts {
		opt(w)
	}
	if w.fsyncInterval != defaultFsyncInterval {
		w.ticker.Reset(w.fsyncInterval)
	}
	go w.fsyncLoop()
	return w, nil
}

func (w *Writer) Append(data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return os.ErrClosed
	}
	record := make([]byte, recordHeaderSize+len(data))
	binary.LittleEndian.PutUint32(record[0:4], uint32(len(data))+4)
	crc := crc32.ChecksumIEEE(data)
	binary.LittleEndian.PutUint32(record[4:8], crc)
	copy(record[8:], data)
	n, err := w.buf.Write(record)
	if err != nil {
		return err
	}
	w.bytesSinceFlush += n
	w.offset += int64(n)
	if w.bytesSinceFlush >= w.byteThreshold {
		return w.flush()
	}
	return nil
}

func (w *Writer) flush() error {
	if err := w.buf.Flush(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	w.bytesSinceFlush = 0
	return nil
}

func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flush()
}

func (w *Writer) Offset() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.offset
}

func (w *Writer) fsyncLoop() {
	for range w.ticker.C {
		w.mu.Lock()
		if w.closed {
			w.mu.Unlock()
			return
		}
		if w.bytesSinceFlush > 0 {
			w.flush()
		}
		w.mu.Unlock()
	}
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	w.ticker.Stop()
	w.flush()
	return w.file.Close()
}

type Reader struct {
	file   *os.File
	reader *bufio.Reader
	offset int64
}

func NewReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &Reader{
		file:   f,
		reader: bufio.NewReader(f),
	}, nil
}

func NewReaderAt(path string, offset int64) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if offset > 0 {
		_, err := f.Seek(offset, io.SeekStart)
		if err != nil {
			f.Close()
			return nil, err
		}
	}
	return &Reader{
		file:   f,
		reader: bufio.NewReader(f),
		offset: offset,
	}, nil
}

func (r *Reader) Read() ([]byte, error) {
	header := make([]byte, recordHeaderSize)
	_, err := io.ReadFull(r.reader, header)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, io.EOF
		}
		return nil, err
	}
	dataLen := binary.LittleEndian.Uint32(header[0:4])
	storedCRC := binary.LittleEndian.Uint32(header[4:8])
	if dataLen < 4 {
		return nil, io.EOF
	}
	data := make([]byte, dataLen-4)
	_, err = io.ReadFull(r.reader, data)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, io.EOF
		}
		return nil, err
	}
	actualCRC := crc32.ChecksumIEEE(data)
	if actualCRC != storedCRC {
		return nil, io.EOF
	}
	r.offset += int64(recordHeaderSize) + int64(dataLen-4)
	return data, nil
}

func (r *Reader) Offset() int64 {
	return r.offset
}

func (r *Reader) Close() error {
	return r.file.Close()
}

func Replay(path string, offset int64) ([][]byte, error) {
	r, err := NewReaderAt(path, offset)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var entries [][]byte
	for {
		data, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, data)
	}
	return entries, nil
}
