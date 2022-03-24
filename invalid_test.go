package amt

import (
	"context"
	"testing"

	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/stretchr/testify/require"
)

func TestInvalidHeightEmpty(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		bs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()
		a, err := NewAMT(bs, opts...)
		require.NoError(t, err)
		a.height = 1
		c, err := a.Flush(ctx)
		require.NoError(t, err)
		_, err = LoadAMT(ctx, bs, c, opts...)
		require.Error(t, err)
	})
}

func TestInvalidHeightSingle(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		bs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()
		a, err := NewAMT(bs, opts...)
		require.NoError(t, err)
		err = a.Set(ctx, 0, cborstr(""))
		require.NoError(t, err)

		a.height = 1
		c, err := a.Flush(ctx)
		require.NoError(t, err)
		_, err = LoadAMT(ctx, bs, c, opts...)
		require.Error(t, err)
	})
}

func TestInvalidHeightTall(t *testing.T) {
	// test only valid for widths less than 16 (2^4)
	runTestWithBitWidths(t, bitWidths2to3, func(t *testing.T, opts ...Option) {
		bs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()
		a, err := NewAMT(bs, opts...)
		require.NoError(t, err)
		err = a.Set(ctx, 15, cborstr(""))
		require.NoError(t, err)

		a.height = 2
		c, err := a.Flush(ctx)
		require.NoError(t, err)
		after, err := LoadAMT(ctx, bs, c, opts...)
		require.NoError(t, err)

		var out CborByteArray
		found, err := after.Get(ctx, 31, &out)
		require.NoError(t, err)
		require.False(t, found)
	})
}
