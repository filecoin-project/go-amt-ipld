package amt

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/filecoin-project/go-amt-ipld/v3/internal"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	cbg "github.com/whyrusleeping/cbor-gen"
)

type node struct {
	// these may both be nil if the node is empty (a root node)
	links  []*link
	values []*cbg.Deferred
}

var (
	errEmptyNode      = errors.New("unexpected empty amt node")
	errUndefinedCID   = errors.New("amt node has undefined CID")
	errLinksAndValues = errors.New("amt node has both links and values")
	errLeafUnexpected = errors.New("amt leaf not expected at height")
	errLeafExpected   = errors.New("amt expected at height")
	errInvalidCount   = errors.New("amt count does not match number of elements")
)

func bmapBytes(width int) int {
	return ((width + 7) / 8)
}

func makeBmap(width int) []byte {
	return make([]byte, bmapBytes(width))
}

func checkBmap(bf []byte, width int) error {
	expLen := bmapBytes(width)
	if len(bf) != expLen {
		return fmt.Errorf(
			"expected bitfield to be %d bytes long, found bitfield with %d bytes",
			expLen, len(bf),
		)
	}
	rem := width % 8
	if rem == 0 {
		return nil
	}
	expUnset := 8 - rem
	if bf[len(bf)-1]&^(uint8(0xff)>>uint(expUnset)) > 0 {
		return fmt.Errorf("expected top %d bits of bitfield to be unset (width %d): %#b", expUnset, width, bf[len(bf)-1])
	}
	return nil
}

func newNode(nd internal.Node, width int, allowEmpty, expectLeaf bool) (*node, error) {
	if len(nd.Links) > 0 && len(nd.Values) > 0 {
		return nil, errLinksAndValues
	}

	if err := checkBmap(nd.Bmap, width); err != nil {
		return nil, err
	}

	i := 0
	n := new(node)
	if len(nd.Values) > 0 {
		if !expectLeaf {
			return nil, errLeafUnexpected
		}
		n.values = make([]*cbg.Deferred, width)
		for x := uint(0); x < uint(width); x++ {
			if nd.Bmap[x/8]&(1<<(x%8)) > 0 {
				if i >= len(nd.Values) {
					return nil, fmt.Errorf("expected at least %d values, found %d", i+1, len(nd.Values))
				}
				n.values[x] = nd.Values[i]
				i++
			}
		}
		if i != len(nd.Values) {
			return nil, fmt.Errorf("expected %d values, got %d", i, len(nd.Values))
		}
	} else if len(nd.Links) > 0 {
		if expectLeaf {
			return nil, errLeafExpected
		}

		n.links = make([]*link, width)
		for x := uint(0); x < uint(width); x++ {
			if nd.Bmap[x/8]&(1<<(x%8)) > 0 {
				if i >= len(nd.Links) {
					return nil, fmt.Errorf("expected at least %d links, found %d", i+1, len(nd.Links))
				}
				c := nd.Links[i]
				if !c.Defined() {
					return nil, errUndefinedCID
				}
				// TODO: check link hash function.
				prefix := c.Prefix()
				if prefix.Codec != cid.DagCBOR {
					return nil, fmt.Errorf("internal amt nodes must be cbor, found %d", prefix.Codec)
				}
				n.links[x] = &link{cid: c}
				i++
			}
		}
		if i != len(nd.Links) {
			return nil, fmt.Errorf("expected %d links, got %d", i, len(nd.Links))
		}
	} else if !allowEmpty {
		return nil, errEmptyNode
	}
	return n, nil
}

func (nd *node) collapse(ctx context.Context, bs cbor.IpldStore, width, height int) (int, error) {
	// No links at all?
	if nd.links == nil {
		return 0, nil
	}

	// If we have any links going "to the right", we can't collapse any
	// more.
	for _, l := range nd.links[1:] {
		if l != nil {
			return height, nil
		}
	}

	// If we have _no_ links, we've collapsed everything.
	if nd.links[0] == nil {
		return 0, nil
	}

	// only one child, collapse it.

	subn, err := nd.links[0].load(ctx, bs, width, height-1)
	if err != nil {
		return 0, err
	}

	// Collapse recursively.
	newHeight, err := subn.collapse(ctx, bs, width, height-1)
	if err != nil {
		return 0, err
	}

	*nd = *subn

	return newHeight, nil
}

func (nd *node) empty() bool {
	for _, l := range nd.links {
		if l != nil {
			return false
		}
	}
	for _, v := range nd.values {
		if v != nil {
			return false
		}
	}
	return true
}

func (n *node) get(ctx context.Context, bs cbor.IpldStore, width, height int, i uint64, out interface{}) (bool, error) {
	if height == 0 {
		d := n.getValue(i)
		if d == nil {
			return false, nil
		}
		if um, ok := out.(cbg.CBORUnmarshaler); ok {
			return true, um.UnmarshalCBOR(bytes.NewReader(d.Raw))
		}
		return true, cbor.DecodeInto(d.Raw, out)
	}
	nfh := nodesForHeight(width, height)
	ln := n.getLink(i / nfh)
	if ln == nil {
		return false, nil
	}
	subn, err := ln.load(ctx, bs, width, height-1)
	if err != nil {
		return false, err
	}

	return subn.get(ctx, bs, width, height-1, i%nfh, out)
}

func (n *node) delete(ctx context.Context, bs cbor.IpldStore, width, height int, i uint64) (bool, error) {
	if height == 0 {
		if n.getValue(i) == nil {
			return false, nil
		}

		n.setValue(width, i, nil)
		return true, nil
	}

	nfh := nodesForHeight(width, height)
	subi := i / nfh

	ln := n.getLink(subi)
	if ln == nil {
		return false, nil
	}
	subn, err := ln.load(ctx, bs, width, height-1)
	if err != nil {
		return false, err
	}

	if deleted, err := subn.delete(ctx, bs, width, height-1, i%nfh); err != nil {
		return false, err
	} else if !deleted {
		return false, nil
	}

	if subn.empty() {
		n.setLink(width, subi, nil)
	} else {
		ln.dirty = true
	}

	return true, nil
}

func (n *node) forEachAt(ctx context.Context, bs cbor.IpldStore, width, height int, start, offset uint64, cb func(uint64, *cbg.Deferred) error) error {
	if height == 0 {
		for i, v := range n.values {
			if v != nil {
				ix := offset + uint64(i)
				if ix < start {
					continue
				}

				if err := cb(offset+uint64(i), v); err != nil {
					return err
				}
			}
		}

		return nil
	}

	subCount := nodesForHeight(width, height)
	for i, ln := range n.links {
		if ln == nil {
			continue
		}

		offs := offset + (uint64(i) * subCount)
		nextOffs := offs + subCount
		if start >= nextOffs {
			continue
		}

		subn, err := ln.load(ctx, bs, width, height-1)
		if err != nil {
			return err
		}

		if err := subn.forEachAt(ctx, bs, width, height-1, start, offs, cb); err != nil {
			return err
		}
	}
	return nil

}

var errNoVals = fmt.Errorf("no values")

func (n *node) firstSetIndex(ctx context.Context, bs cbor.IpldStore, width, height int) (uint64, error) {
	if height == 0 {
		for i, v := range n.values {
			if v != nil {
				return uint64(i), nil
			}
		}
		// Empty array.
		return 0, errNoVals
	}

	for i, ln := range n.links {
		if ln == nil {
			// nothing here.
			continue
		}
		subn, err := ln.load(ctx, bs, width, height-1)
		if err != nil {
			return 0, err
		}
		ix, err := subn.firstSetIndex(ctx, bs, width, height-1)
		if err != nil {
			return 0, err
		}

		subCount := nodesForHeight(width, height)
		return ix + (uint64(i) * subCount), nil
	}

	return 0, errNoVals
}

func (n *node) set(ctx context.Context, bs cbor.IpldStore, width, height int, i uint64, val *cbg.Deferred) (bool, error) {
	if height == 0 {
		alreadySet := n.getValue(i) != nil
		n.setValue(width, i, val)
		return !alreadySet, nil
	}

	nfh := nodesForHeight(width, height)

	// Load but don't mark dirty or actually link in any _new_ intermediate
	// nodes. We'll do that on return if nothing goes wrong.
	ln := n.getLink(i / nfh)
	if ln == nil {
		ln = &link{cached: new(node)}
	}
	subn, err := ln.load(ctx, bs, width, height-1)
	if err != nil {
		return false, err
	}

	nodeAdded, err := subn.set(ctx, bs, width, height-1, i%nfh, val)
	if err != nil {
		return false, err
	}

	// Make all modifications on the way back up if there was no error.
	ln.dirty = true // only mark dirty on success.
	n.setLink(width, i/nfh, ln)

	return nodeAdded, nil
}

func (n *node) flush(ctx context.Context, bs cbor.IpldStore, width, height int) (*internal.Node, error) {
	nd := new(internal.Node)
	nd.Bmap = makeBmap(width)

	if height == 0 {
		for i, val := range n.values {
			if val == nil {
				continue
			}
			nd.Values = append(nd.Values, val)
			nd.Bmap[i/8] |= 1 << (uint(i) % 8)
		}
		return nd, nil
	}

	for i, ln := range n.links {
		if ln == nil {
			continue
		}
		if ln.dirty {
			if ln.cached == nil {
				return nil, fmt.Errorf("expected dirty node to be cached")
			}
			subn, err := ln.cached.flush(ctx, bs, width, height-1)
			if err != nil {
				return nil, err
			}
			cid, err := bs.Put(ctx, subn)
			if err != nil {
				return nil, err
			}

			ln.cid = cid
			ln.dirty = false
		}
		nd.Links = append(nd.Links, ln.cid)
		nd.Bmap[i/8] |= 1 << (uint(i) % 8)
	}

	return nd, nil
}

func (n *node) setLink(width int, i uint64, l *link) {
	if n.links == nil {
		if l == nil {
			return
		}
		n.links = make([]*link, width)
	}
	n.links[i] = l
}

func (n *node) getLink(i uint64) *link {
	if n.links == nil {
		return nil
	}
	return n.links[i]
}

func (n *node) setValue(width int, i uint64, v *cbg.Deferred) {
	if n.values == nil {
		if v == nil {
			return
		}
		n.values = make([]*cbg.Deferred, width)
	}
	n.values[i] = v
}

func (n *node) getValue(i uint64) *cbg.Deferred {
	if n.values == nil {
		return nil
	}
	return n.values[i]
}
