package amt

import (
	"context"
	"testing"

	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/stretchr/testify/require"
)

func TestInvalidHeightEmpty(t *testing.T) {
	bs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()
	a, err := NewAMT(bs)
	require.NoError(t, err)
	a.height = 1
	c, err := a.Flush(ctx)
	require.NoError(t, err)
	_, err = LoadAMT(ctx, bs, c)
	require.Error(t, err)
}

func TestInvalidHeightSingle(t *testing.T) {
	bs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()
	a, err := NewAMT(bs)
	require.NoError(t, err)
	err = a.Set(ctx, 0, 0)
	require.NoError(t, err)

	a.height = 1
	c, err := a.Flush(ctx)
	require.NoError(t, err)
	_, err = LoadAMT(ctx, bs, c)
	require.Error(t, err)
}

func TestInvalidHeightTall(t *testing.T) {
	if bmapBytes(uint(defaultBitWidth)) >= 2 {
		t.Skip("test only valid for widths less than 16")
	}
	bs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()
	a, err := NewAMT(bs)
	require.NoError(t, err)
	err = a.Set(ctx, 15, 0)
	require.NoError(t, err)

	a.height = 2
	c, err := a.Flush(ctx)
	require.NoError(t, err)
	after, err := LoadAMT(ctx, bs, c)
	require.NoError(t, err)

	var out int
	err = after.Get(ctx, 31, &out)
	require.Error(t, err)
}
