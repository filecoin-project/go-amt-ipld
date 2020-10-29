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
	links  [internal.Width]*link
	values [internal.Width]*cbg.Deferred
}

var (
	errEmptyNode      = errors.New("unexpected empty amt node")
	errUndefinedCID   = errors.New("amt node has undefined CID")
	errLinksAndValues = errors.New("amt node has both links and values")
	errLeafUnexpected = errors.New("amt leaf not expected at height")
	errLeafExpected   = errors.New("amt expected at height")
	errInvalidCount   = errors.New("amt count does not match number of elements")
)

func newNode(nd internal.Node, allowEmpty, expectLeaf bool) (*node, error) {
	if len(nd.Links) > 0 && len(nd.Values) > 0 {
		return nil, errLinksAndValues
	}

	i := 0
	n := new(node)
	if len(nd.Values) > 0 {
		if !expectLeaf {
			return nil, errLeafUnexpected
		}
		for x := uint(0); x < internal.Width; x++ {
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

		for x := uint(0); x < internal.Width; x++ {
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

func (nd *node) collapse(ctx context.Context, bs cbor.IpldStore, height int) (int, error) {
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

	subn, err := nd.links[0].load(ctx, bs, height-1)
	if err != nil {
		return 0, err
	}

	// Collapse recursively.
	newHeight, err := subn.collapse(ctx, bs, height-1)
	if err != nil {
		return 0, err
	}

	*nd = *subn

	return newHeight, nil
}

func (nd *node) empty() bool {
	return nd.links == [len(nd.links)]*link{} && nd.values == [len(nd.links)]*cbg.Deferred{}
}

func (n *node) get(ctx context.Context, bs cbor.IpldStore, height int, i uint64, out interface{}) (bool, error) {
	if height == 0 {
		d := n.values[i]
		if d == nil {
			return false, nil
		}
		if um, ok := out.(cbg.CBORUnmarshaler); ok {
			return true, um.UnmarshalCBOR(bytes.NewReader(d.Raw))
		}
		return true, cbor.DecodeInto(d.Raw, out)
	}
	nfh := nodesForHeight(height)
	ln := n.links[i/nfh]
	if ln == nil {
		return false, nil
	}
	subn, err := ln.load(ctx, bs, height-1)
	if err != nil {
		return false, err
	}

	return subn.get(ctx, bs, height-1, i%nfh, out)
}

func (n *node) delete(ctx context.Context, bs cbor.IpldStore, height int, i uint64) (bool, error) {
	if height == 0 {
		if n.values[i] == nil {
			return false, nil
		}

		n.values[i] = nil
		return true, nil
	}

	nfh := nodesForHeight(height)
	subi := i / nfh

	ln := n.links[subi]
	if ln == nil {
		return false, nil
	}
	subn, err := ln.load(ctx, bs, height-1)
	if err != nil {
		return false, err
	}

	if deleted, err := subn.delete(ctx, bs, height-1, i%nfh); err != nil {
		return false, err
	} else if !deleted {
		return false, nil
	}

	if subn.empty() {
		n.links[subi] = nil
	} else {
		ln.dirty = true
	}

	return true, nil
}

func (n *node) forEachAt(ctx context.Context, bs cbor.IpldStore, height int, start, offset uint64, cb func(uint64, *cbg.Deferred) error) error {
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

	subCount := nodesForHeight(height)
	for i, ln := range n.links {
		if ln == nil {
			continue
		}
		subn, err := ln.load(ctx, bs, height-1)
		if err != nil {
			return err
		}

		offs := offset + (uint64(i) * subCount)
		nextOffs := offs + subCount
		if start >= nextOffs {
			continue
		}

		if err := subn.forEachAt(ctx, bs, height-1, start, offs, cb); err != nil {
			return err
		}
	}
	return nil

}

var errNoVals = fmt.Errorf("no values")

func (n *node) firstSetIndex(ctx context.Context, bs cbor.IpldStore, height int) (uint64, error) {
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
		subn, err := ln.load(ctx, bs, height-1)
		if err != nil {
			return 0, err
		}
		ix, err := subn.firstSetIndex(ctx, bs, height-1)
		if err != nil {
			return 0, err
		}

		subCount := nodesForHeight(height)
		return ix + (uint64(i) * subCount), nil
	}

	return 0, errNoVals
}

func (n *node) set(ctx context.Context, bs cbor.IpldStore, height int, i uint64, val *cbg.Deferred) (bool, error) {
	if height == 0 {
		alreadySet := n.values[i] != nil
		n.values[i] = val
		return !alreadySet, nil
	}

	nfh := nodesForHeight(height)

	// Load but don't mark dirty or actually link in any _new_ intermediate
	// nodes. We'll do that on return if nothing goes wrong.
	ln := n.links[i/nfh]
	if ln == nil {
		ln = &link{cached: new(node)}
	}
	subn, err := ln.load(ctx, bs, height-1)
	if err != nil {
		return false, err
	}

	nodeAdded, err := subn.set(ctx, bs, height-1, i%nfh, val)
	if err != nil {
		return false, err
	}

	// Make all modifications on the way back up if there was no error.
	ln.dirty = true // only mark dirty on success.
	n.links[i/nfh] = ln

	return nodeAdded, nil
}

func (n *node) flush(ctx context.Context, bs cbor.IpldStore, height int) (*internal.Node, error) {
	var nd internal.Node
	if height == 0 {
		for i, val := range n.values {
			if val == nil {
				continue
			}
			nd.Values = append(nd.Values, val)
			nd.Bmap[i/8] |= 1 << (uint(i) % 8)
		}
		return &nd, nil
	}

	for i, ln := range n.links {
		if ln == nil {
			continue
		}
		if ln.dirty {
			if ln.cached == nil {
				return nil, fmt.Errorf("expected dirty node to be cached")
			}
			subn, err := ln.cached.flush(ctx, bs, height-1)
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

	return &nd, nil
}
