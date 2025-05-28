package main

import (
	"fmt"
	"io"
	"unsafe"
)

// ByteSequence is a generator of not-crypto-strong random bytes.
type ByteSequence struct {
	size   int64
	offset int64
	next   uint64
}

const (
	a                = 1664525
	c                = 1013904223
	patternBlockSize = 65536
)

// NewByteSequence creates a new generator of given size.
func NewByteSequence(size int64) *ByteSequence {
	return &ByteSequence{
		size:   size,
		offset: 0,
		next:   0x490c734ad1ccf6e9, // Initialize with a couple rounds of the generator
	}
}

// Read implements the io.Reader interface. It fills buf
// until the sequence's size is exhausted and then returns io.EOF.
func (seq *ByteSequence) Read(buf []byte) (int, error) {
	if seq.offset >= seq.size {
		return 0, io.EOF
	}
	remaining := seq.size - seq.offset
	n := int64(len(buf))
	if n > remaining {
		n = remaining
	}
	seq.fill(buf[:n])
	seq.offset += n
	return int(n), nil
}

// Fill fills the entire buf with pseudo-random data.
func (seq *ByteSequence) Fill(buf []byte) {
	if len(buf) == 0 {
		return
	}
	seq.fill(buf)
}

// fill is the core loop that writes pseudo‑random bytes into buf.
// It writes 8 bytes at a time using unsafe pointer arithmetic for speed.
func (seq *ByteSequence) fill(buf []byte) {
	next := seq.next
	i := 0
	n := len(buf)

	// Get the base pointer of the buffer.
	basePtr := unsafe.Pointer(&buf[0])

	// Process full 8‑byte chunks.
	for ; i+8 <= n; i += 8 {
		next = next*a + c
		// Directly write the 64‑bit value to buf at offset i.
		*(*uint64)(unsafe.Pointer(uintptr(basePtr) + uintptr(i))) = next
	}

	// Process any remaining bytes one at a time.
	for ; i < n; i++ {
		next = next*a + c
		buf[i] = byte(next)
	}

	seq.next = next
}

var patternBlock []byte

// PatternFill fills buffer with a certain amount of compressibility,
// ranging from 0 (not compressible) to 100 (completely compressible).
func (seq *ByteSequence) PatternFill(buf []byte, compressibility int) {
	if len(buf) == 0 {
		return
	}

	if compressibility == 0 {
		seq.Fill(buf)
		return
	}

	if patternBlock == nil {
		patternBlock = make([]byte, patternBlockSize)
		for i := range patternBlock {
			patternBlock[i] = byte('A')
		}
	}

	blocks := len(buf) / patternBlockSize
	leftover := len(buf) % patternBlockSize
	patternBlocks := int(float32(blocks) * float32(compressibility) / float32(100))
	randomBlocks := blocks - patternBlocks
	rand := NewNumberSequence()
	rand.Seed(int64(seq.next))

	// Do all the full blocks we've got
	for i := 0; i < blocks; i++ {
		if randomBlocks == 0 {
			// Only pattern blocks remaining
			copy(buf[i*patternBlockSize:], patternBlock)
			patternBlocks--
		} else if patternBlocks == 0 {
			// Only random blocks remaining
			seq.fill(buf[i*patternBlockSize : (i+1)*patternBlockSize])
			randomBlocks--
		} else if rand.Next() > 0 {
			copy(buf[i*patternBlockSize:], patternBlock)
			patternBlocks--
		} else {
			seq.fill(buf[i*patternBlockSize : (i+1)*patternBlockSize])
			randomBlocks--
		}
	}

	// Fill in leftover
	if leftover == 0 {
		// Done
	} else if rand.Next() > 0 {
		copy(buf[blocks*patternBlockSize:], patternBlock[:leftover])
	} else {
		seq.fill(buf[blocks*patternBlockSize:])
	}
}

// Seed sets the sequence's seed to a given value.
func (seq *ByteSequence) Seed(seed uint64) {
	seq.next = seed
}

// Seek lets you rewind (or otherwise change position) when using this
// sequence as a stream. Note, reading from a sequence, seeking to
// zero, and rewinding will result in different data being read. Seek
// only changes the position within the stream, not the random seed.
func (seq *ByteSequence) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64

	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = seq.offset + offset
	case io.SeekEnd:
		newOffset = seq.size + offset
	}

	if newOffset < 0 {
		return 0, fmt.Errorf("Cannot seek to negative offset %d", newOffset)
	}

	if newOffset > seq.size {
		return 0, fmt.Errorf("Cannot seek past end of sequence to offset %d (size %d)", newOffset, seq.size)
	}

	seq.offset = newOffset
	return seq.offset, nil
}

// Write just drops everything on the floor. Provided for compatibility
// with io.ReadWriter.
func (seq *ByteSequence) Write(buf []byte) (int, error) {
	return len(buf), nil
}

// Close sets any remaining sequence size to zero. Provided for compatibility
// with io.Closer.
func (seq *ByteSequence) Close() error {
	seq.size = 0
	return nil
}
