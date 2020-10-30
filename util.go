package amt

import (
	"math"
	"math/bits"
)

// Exponentiation by squaring, saturating at max uint64.
//
// This saturates at max uint64 because:
// 1. It's "correct" in that that's the maximum number of nodes we can have.
// 2. It produces the correct results.
func nodesForHeight(width, height int) uint64 {
	a := uint64(width)
	b := uint64(height)

	// Base case
	if a == 0 {
		return 0
	}

	// If the base is a power of two, we can do this with bit twiddling.
	// This is much faster than exponentiation by squaring.
	if (a & (a - 1)) == 0 {
		shift := uint64(bits.TrailingZeros64(a)) * b
		if shift >= 64 {
			return math.MaxUint64
		}
		return uint64(1) << shift
	}

	// Ok, exponentiation by squaring.
	// https://en.wikipedia.org/wiki/Exponentiation_by_squaring
	c := uint64(1)
	for b > 1 {
		if b%2 != 0 {
			// This produces a 128 bit number split into hi and lo.
			// If we have any hi bits, we "saturate" and return
			// early.
			hi, lo := bits.Mul64(c, a)
			if hi != 0 {
				return math.MaxUint64
			}
			c = lo
			b -= 1
		}

		// Saturate again.
		hi, lo := bits.Mul64(a, a)
		if hi != 0 {
			return math.MaxUint64
		}
		a = lo
		b /= 2
	}
	// And saturate again.
	hi, lo := bits.Mul64(a, c)
	if hi != 0 {
		return math.MaxUint64
	}
	return lo
}
