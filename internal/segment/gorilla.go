package segment

import (
	"encoding/binary"
	"math"

	"github.com/coleYab/chronodb/internal/memtable"
)

type bitWriter struct {
	buf []byte
	pos int
}

func newBitWriter() *bitWriter {
	return &bitWriter{buf: make([]byte, 0, 64)}
}

func (bw *bitWriter) writeBit(bit uint8) {
	if bw.pos%8 == 0 {
		bw.buf = append(bw.buf, 0)
	}
	if bit != 0 {
		bw.buf[bw.pos/8] |= 1 << (7 - uint(bw.pos%8))
	}
	bw.pos++
}

func (bw *bitWriter) writeBits(val uint64, nbits int) {
	for i := nbits - 1; i >= 0; i-- {
		bw.writeBit(uint8((val >> i) & 1))
	}
}

func (bw *bitWriter) bytes() []byte {
	return bw.buf
}

type bitReader struct {
	data []byte
	pos  int
}

func newBitReader(data []byte) *bitReader {
	return &bitReader{data: data, pos: 0}
}

func (br *bitReader) readBit() uint8 {
	if br.pos >= len(br.data)*8 {
		return 0
	}
	bit := (br.data[br.pos/8] >> (7 - uint(br.pos%8))) & 1
	br.pos++
	return bit
}

func (br *bitReader) readBits(nbits int) uint64 {
	var val uint64
	for i := 0; i < nbits; i++ {
		val = (val << 1) | uint64(br.readBit())
	}
	return val
}

func gorillaEncodeTimestamps(points []memtable.Point) []byte {
	if len(points) == 0 {
		return nil
	}
	if len(points) == 1 {
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], uint64(points[0].Timestamp))
		return buf[:]
	}

	bw := newBitWriter()
	t0 := make([]byte, 8)
	binary.LittleEndian.PutUint64(t0, uint64(points[0].Timestamp))
	bw.buf = append(bw.buf, t0...)
	bw.pos += 64

	prevDelta := points[1].Timestamp - points[0].Timestamp
	var d [binary.MaxVarintLen64]byte
	n := binary.PutVarint(d[:], prevDelta)
	bw.buf = append(bw.buf, d[:n]...)
	bw.pos += n * 8

	for i := 2; i < len(points); i++ {
		delta := points[i].Timestamp - points[i-1].Timestamp
		dod := delta - prevDelta
		prevDelta = delta

		switch {
		case dod == 0:
			bw.writeBit(0)
		case dod >= -63 && dod <= 64:
			bw.writeBits(2, 2)
			bw.writeBits(uint64(dod+63), 7)
		case dod >= -255 && dod <= 256:
			bw.writeBits(6, 3)
			bw.writeBits(uint64(dod+255), 9)
		case dod >= -2047 && dod <= 2048:
			bw.writeBits(14, 4)
			bw.writeBits(uint64(dod+2047), 12)
		default:
			bw.writeBits(15, 4)
			bw.writeBits(0, (8-bw.pos%8)%8)
			var d2 [binary.MaxVarintLen64]byte
			n2 := binary.PutVarint(d2[:], dod)
			for j := 0; j < n2; j++ {
				bw.buf = append(bw.buf, d2[j])
				bw.pos += 8
			}
		}
	}
	return bw.bytes()
}

func gorillaDecodeTimestamps(data []byte, numPoints int) ([]int64, error) {
	if numPoints == 0 {
		return nil, nil
	}
	if numPoints == 1 {
		if len(data) < 8 {
			return nil, errCorrupt
		}
		return []int64{int64(binary.LittleEndian.Uint64(data[0:8]))}, nil
	}

	br := newBitReader(data[8:])
	ts := make([]int64, numPoints)
	ts[0] = int64(binary.LittleEndian.Uint64(data[0:8]))

	delta, n := binary.Varint(data[8:])
	if n <= 0 {
		return nil, errCorrupt
	}
	ts[1] = ts[0] + delta
	br.pos = n * 8

	for i := 2; i < numPoints; i++ {
		var dod int64
		if br.readBit() == 0 {
			dod = 0
		} else if br.readBit() == 0 {
			dod = int64(br.readBits(7)) - 63
		} else if br.readBit() == 0 {
			dod = int64(br.readBits(9)) - 255
		} else if br.readBit() == 0 {
			dod = int64(br.readBits(12)) - 2047
		} else {
			pad := (8 - br.pos%8) % 8
			br.pos += pad
			remaining := len(data) - br.pos/8
			if remaining <= 0 {
				return nil, errCorrupt
			}
			dod64, n2 := binary.Varint(data[br.pos/8:])
			if n2 <= 0 {
				return nil, errCorrupt
			}
			dod = dod64
			br.pos += n2 * 8
		}
		delta += dod
		ts[i] = ts[i-1] + delta
	}
	return ts, nil
}

func gorillaEncodeValues(points []memtable.Point) []byte {
	if len(points) == 0 {
		return nil
	}
	bw := newBitWriter()
	v0 := make([]byte, 8)
	binary.LittleEndian.PutUint64(v0, math.Float64bits(points[0].Value))
	bw.buf = append(bw.buf, v0...)
	bw.pos += 64

	prevVal := math.Float64bits(points[0].Value)
	prevLeading := ^uint8(0)
	prevTrailing := uint8(0)

	for i := 1; i < len(points); i++ {
		curVal := math.Float64bits(points[i].Value)
		xor := prevVal ^ curVal

		if xor == 0 {
			bw.writeBit(0)
			continue
		}

		leading := uint8(0)
		if xor != 0 {
			leading = uint8(leadingZeros64(xor))
		}
		trailing := uint8(trailingZeros64(xor))

		bw.writeBit(1)
		if leading >= prevLeading && trailing >= prevTrailing {
			bw.writeBit(0)
			middleBits := 64 - int(prevLeading) - int(prevTrailing)
			bw.writeBits(xor>>prevTrailing, middleBits)
		} else {
			bw.writeBit(1)
			bw.writeBits(uint64(leading), 5)
			bw.writeBits(uint64(trailing), 6)
			middleBits := 64 - int(leading) - int(trailing)
			bw.writeBits(xor>>trailing, middleBits)
			prevLeading = leading
			prevTrailing = trailing
		}
		prevVal = curVal
	}
	return bw.bytes()
}

func gorillaDecodeValues(data []byte, numPoints int) ([]float64, error) {
	if numPoints == 0 {
		return nil, nil
	}
	if len(data) < 8 {
		return nil, errCorrupt
	}
	vals := make([]float64, numPoints)
	vals[0] = math.Float64frombits(binary.LittleEndian.Uint64(data[0:8]))

	if numPoints == 1 {
		return vals, nil
	}

	br := newBitReader(data[8:])
	prevVal := math.Float64bits(vals[0])
	prevLeading := ^uint8(0)
	prevTrailing := uint8(0)

	for i := 1; i < numPoints; i++ {
		if br.readBit() == 0 {
			vals[i] = math.Float64frombits(prevVal)
			continue
		}
		var leading, trailing uint8
		var xor uint64
		if br.readBit() == 0 {
			middleBits := 64 - int(prevLeading) - int(prevTrailing)
			valBits := br.readBits(middleBits)
			xor = valBits << prevTrailing
			leading = prevLeading
			trailing = prevTrailing
		} else {
			leading = uint8(br.readBits(5))
			trailing = uint8(br.readBits(6))
			middleBits := 64 - int(leading) - int(trailing)
			valBits := br.readBits(middleBits)
			xor = valBits << trailing
			prevLeading = leading
			prevTrailing = trailing
		}
		newVal := prevVal ^ xor
		vals[i] = math.Float64frombits(newVal)
		prevVal = newVal
	}
	return vals, nil
}

func gorillaEncodeSeries(points []memtable.Point) []byte {
	ts := gorillaEncodeTimestamps(points)
	vs := gorillaEncodeValues(points)
	var tsLen [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tsLen[:], uint64(len(ts)))
	result := make([]byte, 0, n+len(ts)+len(vs))
	result = append(result, tsLen[:n]...)
	result = append(result, ts...)
	result = append(result, vs...)
	return result
}

func gorillaDecodeSeries(data []byte, numPoints int) ([]memtable.Point, error) {
	tsLen, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, errCorrupt
	}
	tsEnd := n + int(tsLen)
	if tsEnd > len(data) {
		return nil, errCorrupt
	}
	ts, err := gorillaDecodeTimestamps(data[n:tsEnd], numPoints)
	if err != nil {
		return nil, err
	}
	vs, err := gorillaDecodeValues(data[tsEnd:], numPoints)
	if err != nil {
		return nil, err
	}
	pts := make([]memtable.Point, numPoints)
	for i := 0; i < numPoints; i++ {
		pts[i] = memtable.Point{Timestamp: ts[i], Value: vs[i]}
	}
	return pts, nil
}

func leadingZeros64(x uint64) int {
	if x == 0 {
		return 64
	}
	n := 0
	for x&0x8000000000000000 == 0 {
		n++
		x <<= 1
	}
	return n
}

func trailingZeros64(x uint64) int {
	if x == 0 {
		return 64
	}
	n := 0
	for x&1 == 0 {
		n++
		x >>= 1
	}
	return n
}
