package amt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var width uint = 2

func BenchmarkNodesForHeight(b *testing.B) {
	width = 9
	for i := 0; i < b.N; i++ {
		nodesForHeight(width, i%15)
	}
}

func TestNodesForHeight(t *testing.T) {
	require.Equal(t, uint64(1), nodesForHeight(1, 0))
	require.Equal(t, uint64(4), nodesForHeight(2, 1))
	require.Equal(t, uint64(64), nodesForHeight(3, 2))
	require.Equal(t, uint64(4096), nodesForHeight(4, 3))
}
