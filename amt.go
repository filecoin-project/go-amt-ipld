package amt

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	logging "github.com/ipfs/go-log"
	cbg "github.com/whyrusleeping/cbor-gen"

	"github.com/filecoin-project/go-amt-ipld/v2/internal"
)

var log = logging.Logger("amt")

// MaxIndex is the maximum index for elements in the AMT. This is currently 1^63
// (max int) because the width is 8. That means every "level" consumes 3 bits
// from the index, and 63/3 is a nice even 21
const MaxIndex = uint64(1<<internal.MaxIndexBits) - 1

type Root struct {
	height int
	count  uint64

	node *node

	store cbor.IpldStore
}

func NewAMT(bs cbor.IpldStore) *Root {
	return &Root{
		store: bs,
		node:  new(node),
	}
}

func LoadAMT(ctx context.Context, bs cbor.IpldStore, c cid.Cid) (*Root, error) {
	var r internal.Root
	if err := bs.Get(ctx, c, &r); err != nil {
		return nil, err
	}
	if r.Height > internal.MaxHeight {
		return nil, fmt.Errorf("failed to load AMT: height out of bounds: %d > %d", r.Height, internal.MaxHeight)
	}
	if nodesForHeight(int(r.Height+1)) < r.Count {
		return nil, fmt.Errorf(
			"failed to load AMT: not tall enough (%d) for count (%d)", r.Height, r.Count,
		)
	}

	nd, err := newNode(r.Node, r.Height == 0, r.Height == 0)
	if err != nil {
		return nil, err
	}

	return &Root{
		height: int(r.Height),
		count:  r.Count,
		node:   nd,
		store:  bs,
	}, nil
}

func FromArray(ctx context.Context, bs cbor.IpldStore, vals []cbg.CBORMarshaler) (cid.Cid, error) {
	r := NewAMT(bs)
	if err := r.BatchSet(ctx, vals); err != nil {
		return cid.Undef, err
	}

	return r.Flush(ctx)
}

func (r *Root) Set(ctx context.Context, i uint64, val interface{}) error {
	if i > MaxIndex {
		return fmt.Errorf("index %d is out of range for the amt", i)
	}

	var b []byte
	if m, ok := val.(cbg.CBORMarshaler); ok {
		buf := new(bytes.Buffer)
		if err := m.MarshalCBOR(buf); err != nil {
			return err
		}
		b = buf.Bytes()
	} else {
		var err error
		b, err = cbor.DumpObject(val)
		if err != nil {
			return err
		}
	}

	for i >= nodesForHeight(r.height+1) {
		if !r.node.empty() {
			nd := r.node
			r.node = &node{
				links: [internal.Width]*link{
					0: {
						dirty:  true,
						cached: nd,
					},
				},
			}
		}
		r.height++
	}

	addVal, err := r.node.set(ctx, r.store, int(r.height), i, &cbg.Deferred{Raw: b})
	if err != nil {
		return err
	}

	if addVal {
		// Something is wrong, so we'll just do our best to not overflow.
		if r.count >= (MaxIndex - 1) {
			return errInvalidCount
		}
		r.count++
	}

	return nil
}

func (r *Root) BatchSet(ctx context.Context, vals []cbg.CBORMarshaler) error {
	// TODO: there are more optimized ways of doing this method
	for i, v := range vals {
		if err := r.Set(ctx, uint64(i), v); err != nil {
			return err
		}
	}
	return nil
}

func (r *Root) Get(ctx context.Context, i uint64, out interface{}) error {
	if i > MaxIndex {
		return fmt.Errorf("index %d is out of range for the amt", i)
	}

	if i >= nodesForHeight(int(r.height+1)) {
		return &ErrNotFound{Index: i}
	}
	if found, err := r.node.get(ctx, r.store, int(r.height), i, out); err != nil {
		return err
	} else if !found {
		return &ErrNotFound{Index: i}
	}
	return nil
}

func (r *Root) BatchDelete(ctx context.Context, indices []uint64) error {
	// TODO: theres a faster way of doing this, but this works for now

	// Sort by index so we can safely implement these optimizations in the future.
	less := func(i, j int) bool { return indices[i] < indices[j] }
	if !sort.SliceIsSorted(indices, less) {
		// Copy first so we don't modify our inputs.
		indices = append(indices[0:0:0], indices...)
		sort.Slice(indices, less)
	}

	for _, i := range indices {
		if err := r.Delete(ctx, i); err != nil {
			return err
		}
	}

	return nil
}

func (r *Root) Delete(ctx context.Context, i uint64) error {
	if i > MaxIndex {
		return fmt.Errorf("index %d is out of range for the amt", i)
	}
	if i >= nodesForHeight(int(r.height+1)) {
		return &ErrNotFound{i}
	}

	found, err := r.node.delete(ctx, r.store, int(r.height), i)
	if err != nil {
		return err
	} else if !found {
		return &ErrNotFound{i}
	}

	newHeight, err := r.node.collapse(ctx, r.store, r.height)
	if err != nil {
		return err
	}
	r.height = newHeight

	// Something is very wrong but there's not much we can do. So we perform
	// the operation and then tell the user that something is wrong.
	if r.count == 0 {
		return errInvalidCount
	}

	r.count--
	return nil
}

// Subtract removes all elements of 'or' from 'r'
func (r *Root) Subtract(ctx context.Context, or *Root) error {
	// TODO: as with other methods, there should be an optimized way of doing this
	return or.ForEach(ctx, func(i uint64, _ *cbg.Deferred) error {
		return r.Delete(ctx, i)
	})
}

func (r *Root) ForEach(ctx context.Context, cb func(uint64, *cbg.Deferred) error) error {
	return r.node.forEachAt(ctx, r.store, r.height, 0, 0, cb)
}

func (r *Root) ForEachAt(ctx context.Context, start uint64, cb func(uint64, *cbg.Deferred) error) error {
	return r.node.forEachAt(ctx, r.store, r.height, start, 0, cb)
}

func (r *Root) FirstSetIndex(ctx context.Context) (uint64, error) {
	return r.node.firstSetIndex(ctx, r.store, r.height)
}

func (r *Root) Flush(ctx context.Context) (cid.Cid, error) {
	nd, err := r.node.flush(ctx, r.store, r.height)
	if err != nil {
		return cid.Undef, err
	}
	root := internal.Root{
		Height: uint64(r.height),
		Count:  r.count,
		Node:   *nd,
	}
	return r.store.Put(ctx, &root)
}

func (r *Root) Len() uint64 {
	return r.count
}

type ErrNotFound struct {
	Index uint64
}

func (e ErrNotFound) Error() string {
	return fmt.Sprintf("Index %d not found in AMT", e.Index)
}

func (e ErrNotFound) NotFound() bool {
	return true
}
