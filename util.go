package amt

import (
	"bytes"
	"math"
	"sync"

	cbg "github.com/whyrusleeping/cbor-gen"
)

// Given height 'height', how many nodes in a maximally full tree can we
// build? (bitWidth^2)^height = width^height. If we pass in height+1 we can work
// out how many elements a maximally full tree can hold, width^(height+1).
func nodesForHeight(bitWidth uint, height int) uint64 {
	heightLogTwo := uint64(bitWidth) * uint64(height)
	if heightLogTwo >= 64 {
		// The max depth layer may not be full.
		return math.MaxUint64
	}
	return 1 << heightLogTwo
}

var bufferPool sync.Pool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(nil)
	},
}

func cborToBytes(val cbg.CBORMarshaler) ([]byte, error) {
	// Temporary location to put values. We'll copy them to an exact-sized buffer when done.
	valueBuf := bufferPool.Get().(*bytes.Buffer)
	defer func() {
		valueBuf.Reset()
		bufferPool.Put(valueBuf)
	}()

	if err := val.MarshalCBOR(valueBuf); err != nil {
		return nil, err
	}

	// Copy to shrink the allocation.
	buf := valueBuf.Bytes()
	cpy := make([]byte, len(buf))
	copy(cpy, buf)

	return cpy, nil
}
