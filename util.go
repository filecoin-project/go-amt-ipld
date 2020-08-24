package amt

import "github.com/filecoin-project/go-amt-ipld/v2/internal"

func nodesForHeight(height int) uint64 {
	heightLogTwo := uint64(internal.WidthBits * height)
	if heightLogTwo >= 64 {
		// Should never happen. Max height is checked at all entry points.
		panic("height overflow")
	}
	return 1 << heightLogTwo
}
