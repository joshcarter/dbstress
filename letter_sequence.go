package main

import (
	"fmt"
	"io"
)

// LetterSequence is a generator of not-crypto-strong random letters.
type LetterSequence struct {
	size   int64
	offset int64
	next   uint64
}

// Create new letter sequence of specified size, after which reading will
// return EOF.
func NewLetterSequence(size int64) *LetterSequence {
	return &LetterSequence{
		size:   size,
		offset: 0,
		next:   0x490c734ad1ccf6e9, // Initialize with a couple rounds of the generator
	}
}

// Read fills the buffer until the sequence's size is exhausted, after which
// it returns io.EOF. Useful with io.Copy and other code that wants to treat
// the sequence like an io.Reader.
func (seq *LetterSequence) Read(buf []byte) (int, error) {
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

// Fill fills the buffer without paying attention to the sequence size.
func (seq *LetterSequence) Fill(buf []byte) {
	if len(buf) == 0 {
		return
	}
	seq.fill(buf)
}

// fill is the core loop that writes pseudo‑random bytes into buf.
// It writes 8 bytes at a time using unsafe pointer arithmetic for speed.
func (seq *LetterSequence) fill(buf []byte) {
	next := seq.next
	i := 0
	n := len(buf)

	// Fill in buf 12 letters at a time. Since we're only using
	// lowercase letters, but generating 64 bits of randomness at a
	// time, we can use 5 bits for each letter. We can't do this at
	// the end of the buffer, but we'll finish that off below.
	// Algorithm and constants borrowed from Numerical Recipes in
	// C (2nd ed), section 7.1.
	for ; i+12 <= n; i += 12 {
		next = next*a + c
		buf[i+0] = byte((next>>0)%26 + 'a')
		buf[i+1] = byte((next>>5)%26 + 'a')
		buf[i+2] = byte((next>>10)%26 + 'a')
		buf[i+3] = byte((next>>15)%26 + 'a')
		buf[i+4] = byte((next>>20)%26 + 'a')
		buf[i+5] = byte((next>>25)%26 + 'a')
		buf[i+6] = byte((next>>30)%26 + 'a')
		buf[i+7] = byte((next>>35)%26 + 'a')
		buf[i+8] = byte((next>>40)%26 + 'a')
		buf[i+9] = byte((next>>45)%26 + 'a')
		buf[i+10] = byte((next>>50)%26 + 'a')
		buf[i+11] = byte((next>>55)%26 + 'a')
	}

	// Process any remaining bytes one at a time.
	for ; i < n; i++ {
		next = next*a + c
		buf[i] = byte((next % 26) + 'a')
	}

	seq.next = next
}

// Letters returns random-ish letters as a string of the given lengh.
func (seq *LetterSequence) Letters(stringLen int) string {
	if stringLen == 0 {
		return ""
	}

	buf := make([]byte, stringLen)
	seq.Fill(buf)
	return string(buf)
}

// Seed sets the sequence's seed to a given value.
func (seq *LetterSequence) Seed(seed uint64) {
	seq.next = seed
}

// Seek lets you rewind (or otherwise change position) when using this
// sequence as a stream. Note, reading from a sequence, seeking to
// zero, and rewinding will result in different data being read. Seek
// only changes the position within the stream, not the random seed.
func (seq *LetterSequence) Seek(offset int64, whence int) (int64, error) {
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
func (seq *LetterSequence) Write(buf []byte) (int, error) {
	return 0, nil
}

// Close sets any remaining sequence size to zero. Provided for compatibility
// with io.Closer.
func (seq *LetterSequence) Close() error {
	seq.size = 0
	return nil
}
