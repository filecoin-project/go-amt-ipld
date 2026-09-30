package amt

import (
	"bytes"
	"context"
	"testing"

	block "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/stretchr/testify/require"
	cbg "github.com/whyrusleeping/cbor-gen"
)

// storeRoot writes r out as-is rather than going through Flush, so a node can
// be given a bitmap that does not agree with the Links or Values it carries.
func storeRoot(t *testing.T, mb *mockBlocks, r *Root) cid.Cid {
	t.Helper()
	buf := new(bytes.Buffer)
	require.NoError(t, r.MarshalCBOR(buf))
	blk := block.NewBlock(buf.Bytes())
	require.NoError(t, mb.Put(blk))
	return blk.Cid()
}

func loadStoredRoot(t *testing.T, r *Root) (*Root, context.Context) {
	t.Helper()
	mb := newMockBlocks()
	c := storeRoot(t, mb, r)
	ctx := context.Background()
	loaded, err := LoadAMT(ctx, cbor.NewCborStore(mb), c)
	require.NoError(t, err)
	return loaded, ctx
}

// A leaf whose bitmap claims one entry while Values is empty.
func bitmapExceedsValues() *Root {
	return &Root{Count: 1, Node: Node{Bmap: [...]byte{0x01}}}
}

// The same one level up, where the set bit is read as a link.
func bitmapExceedsLinks() *Root {
	return &Root{Height: 1, Count: 1, Node: Node{Bmap: [...]byte{0x01}}}
}

func TestInvalidBitmapValuesForEach(t *testing.T) {
	a, ctx := loadStoredRoot(t, bitmapExceedsValues())
	require.Error(t, a.ForEach(ctx, func(uint64, *cbg.Deferred) error { return nil }))
}

func TestInvalidBitmapValuesGet(t *testing.T) {
	a, ctx := loadStoredRoot(t, bitmapExceedsValues())
	var out cbg.Deferred
	require.Error(t, a.Get(ctx, 0, &out))
}

func TestInvalidBitmapValuesFirstSetIndex(t *testing.T) {
	a, ctx := loadStoredRoot(t, bitmapExceedsValues())
	_, err := a.FirstSetIndex(ctx)
	require.Error(t, err)
}

func TestInvalidBitmapValuesDelete(t *testing.T) {
	a, ctx := loadStoredRoot(t, bitmapExceedsValues())
	require.Error(t, a.Delete(ctx, 0))
}

func TestInvalidBitmapValuesSet(t *testing.T) {
	a, ctx := loadStoredRoot(t, bitmapExceedsValues())
	require.Error(t, a.Set(ctx, 0, "foo"))
}

func TestInvalidBitmapLinksForEach(t *testing.T) {
	a, ctx := loadStoredRoot(t, bitmapExceedsLinks())
	require.Error(t, a.ForEach(ctx, func(uint64, *cbg.Deferred) error { return nil }))
}

func TestInvalidBitmapLinksGet(t *testing.T) {
	a, ctx := loadStoredRoot(t, bitmapExceedsLinks())
	var out cbg.Deferred
	require.Error(t, a.Get(ctx, 0, &out))
}

func TestInvalidBitmapLinksFirstSetIndex(t *testing.T) {
	a, ctx := loadStoredRoot(t, bitmapExceedsLinks())
	_, err := a.FirstSetIndex(ctx)
	require.Error(t, err)
}
