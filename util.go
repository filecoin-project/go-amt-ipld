package amt

import "math"

func nodesForHeight(bitWidth uint, height int) uint64 {
	heightLogTwo := uint64(bitWidth) * uint64(height)
	if heightLogTwo >= 64 {
		// The max depth layer may not be full.
		return math.MaxUint64
	}
	return 1 << heightLogTwo
}
