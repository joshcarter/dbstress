package main

// NumberSequence is a generator of not-crypto-strong random numbers.
type NumberSequence struct {
	next int64
}

// Create new number sequence.
func NewNumberSequence() *NumberSequence {
	return &NumberSequence{
		// Initialize with a couple rounds of the generator
		next: 0x490c734ad1ccf6e9,
	}
}

// Next returns the next number in the sequence. Algorithm and constants
// borrowed from Numerical Recipes in C (2nd ed), section 7.1.
func (seq *NumberSequence) Next() int64 {
	a := int64(1664525)
	c := int64(1013904223)

	seq.next = seq.next*a + c

	if seq.next >= 0 {
		return seq.next
	} else {
		return seq.next * -1
	}

}

// Seed reseeds the sequence to the current value.
func (seq *NumberSequence) Seed(seed int64) {
	seq.next = seed
}
