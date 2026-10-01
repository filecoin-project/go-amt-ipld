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

func malformedRoots() map[string]*Root {
	two := []*cbg.Deferred{{Raw: []byte{0x01}}, {Raw: []byte{0x02}}}
	link := block.NewBlock([]byte{0x80}).Cid()
	return map[string]*Root{
		// a leaf whose bitmap claims one entry while Values is empty
		"bitmap exceeds values": {Count: 1, Node: Node{Bmap: [...]byte{0x01}}},
		// a leaf with more Values than bits set
		"values exceed bitmap": {Count: 1, Node: Node{Bmap: [...]byte{0x01}, Values: two}},
		// the same one level up, where set bits are read as links
		"bitmap exceeds links": {Height: 1, Count: 1, Node: Node{Bmap: [...]byte{0x01}}},
		"links exceed bitmap":  {Height: 1, Count: 1, Node: Node{Bmap: [...]byte{0x01}, Links: []cid.Cid{link, link}}},
	}
}

var invalidOps = map[string]func(context.Context, *Root) error{
	"ForEach": func(ctx context.Context, a *Root) error {
		return a.ForEach(ctx, func(uint64, *cbg.Deferred) error { return nil })
	},
	"Get": func(ctx context.Context, a *Root) error {
		var out cbg.Deferred
		return a.Get(ctx, 0, &out)
	},
	"FirstSetIndex": func(ctx context.Context, a *Root) error {
		_, err := a.FirstSetIndex(ctx)
		return err
	},
	"Delete": func(ctx context.Context, a *Root) error {
		return a.Delete(ctx, 0)
	},
	"Set": func(ctx context.Context, a *Root) error {
		return a.Set(ctx, 0, "foo")
	},
}

// Each operation must fail, and keep failing on the same loaded root.
func TestInvalidBitmap(t *testing.T) {
	for rname, r := range malformedRoots() {
		for oname, op := range invalidOps {
			t.Run(rname+"/"+oname, func(t *testing.T) {
				a, ctx := loadStoredRoot(t, r)
				require.Error(t, op(ctx, a))
				require.Error(t, op(ctx, a))
			})
		}
	}
}
