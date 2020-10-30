package amt

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

var base = 2

func BenchmarkNodesForHeight(b *testing.B) {
	base = 9
	for i := 0; i < b.N; i++ {
		nodesForHeight(base, i%15)
	}
}

func TestNodesForHeight(t *testing.T) {
	require.Equal(t, uint64(1), nodesForHeight(1, 0))
	require.Equal(t, uint64(2), nodesForHeight(2, 1))
	require.Equal(t, uint64(16), nodesForHeight(4, 2))
	require.Equal(t, uint64(64), nodesForHeight(4, 3))
	require.Equal(t, uint64(125), nodesForHeight(5, 3))
	require.Equal(t, uint64(math.MaxUint64), nodesForHeight(128, 256))
	require.Equal(t, uint64(math.MaxUint64), nodesForHeight(129, 256))
}
