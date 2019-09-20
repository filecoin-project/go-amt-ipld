package amt

import (
	"bytes"
	"fmt"
	"math"
	"math/bits"

	blocks "github.com/ipfs/go-block-format"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	cbor "github.com/ipfs/go-ipld-cbor"
	mh "github.com/multiformats/go-multihash"
	cbg "github.com/whyrusleeping/cbor-gen"

	cid "github.com/ipfs/go-cid"
)

const width = 8

type Root struct {
	Height uint64
	Count  uint64
	Node   Node

	bs Blocks
}

type Node struct {
	Bmap   []byte
	Links  []cid.Cid
	Values []*cbg.Deferred

	expLinks []cid.Cid
	expVals  []*cbg.Deferred
	cache    []*Node
}

type Blocks interface {
	Get(cid.Cid, interface{}) error
	Put(interface{}) (cid.Cid, error)
}

type bstoreWrapper struct {
	bs blockstore.Blockstore
}

func WrapBlockstore(bs blockstore.Blockstore) Blocks {
	return &bstoreWrapper{bs}
}

func (bw *bstoreWrapper) Get(c cid.Cid, out interface{}) error {
	b, err := bw.bs.Get(c)
	if err != nil {
		return err
	}

	um, ok := out.(cbg.CBORUnmarshaler)
	if !ok {
		return fmt.Errorf("object was not a CBORUnmarshaler")
	}
	if err := um.UnmarshalCBOR(bytes.NewReader(b.RawData())); err != nil {
		return err
	}

	return nil
}

func (bw *bstoreWrapper) Put(obj interface{}) (cid.Cid, error) {
	cbm, ok := obj.(cbg.CBORMarshaler)
	if !ok {
		return cid.Undef, fmt.Errorf("object was not a CBORMarshaler")
	}

	buf := new(bytes.Buffer)
	if err := cbm.MarshalCBOR(buf); err != nil {
		return cid.Undef, err
	}

	pref := cid.NewPrefixV1(cid.DagCBOR, mh.BLAKE2B_MIN+31)
	c, err := pref.Sum(buf.Bytes())
	if err != nil {
		return cid.Undef, err
	}

	blk, err := blocks.NewBlockWithCid(buf.Bytes(), c)
	if err != nil {
		return cid.Undef, err
	}

	if err := bw.bs.Put(blk); err != nil {
		return cid.Undef, err
	}

	return blk.Cid(), nil
}

func NewAMT(bs Blocks) *Root {
	return &Root{
		bs: bs,
	}
}

func LoadAMT(bs Blocks, c cid.Cid) (*Root, error) {
	var r Root
	if err := bs.Get(c, &r); err != nil {
		return nil, err
	}

	r.bs = bs

	return &r, nil
}

func (r *Root) Set(i uint64, val interface{}) error {

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

	for i >= nodesForHeight(width, int(r.Height)+1) {
		if !r.Node.empty() {
			if err := r.Node.Flush(r.bs, int(r.Height)); err != nil {
				return err
			}

			c, err := r.bs.Put(&r.Node)
			if err != nil {
				return err
			}

			r.Node = Node{
				Bmap:  []byte{0x01},
				Links: []cid.Cid{c},
			}
		}
		r.Height++
	}

	addVal, err := r.Node.set(r.bs, int(r.Height), i, &cbg.Deferred{Raw: b})
	if err != nil {
		return err
	}

	if addVal {
		r.Count++
	}

	return nil
}

func FromArray(bs Blocks, vals []cbg.CBORMarshaler) (cid.Cid, error) {
	r := NewAMT(bs)
	if err := r.BatchSet(vals); err != nil {
		return cid.Undef, err
	}

	return r.Flush()
}

func (r *Root) BatchSet(vals []cbg.CBORMarshaler) error {
	// TODO: there are more optimized ways of doing this method
	for i, v := range vals {
		if err := r.Set(uint64(i), v); err != nil {
			return err
		}
	}
	return nil
}

func (r *Root) Get(i uint64, out interface{}) error {
	if i >= nodesForHeight(width, int(r.Height+1)) {
		return &ErrNotFound{Index: i}
	}
	return r.Node.get(r.bs, int(r.Height), i, out)
}

func (n *Node) get(bs Blocks, height int, i uint64, out interface{}) error {
	subi := i / nodesForHeight(width, height)
	set, _ := n.getBit(subi)
	if !set {
		return &ErrNotFound{i}
	}
	if height == 0 {
		n.expandValues()

		d := n.expVals[i]

		if um, ok := out.(cbg.CBORUnmarshaler); ok {
			return um.UnmarshalCBOR(bytes.NewReader(d.Raw))
		} else {
			return cbor.DecodeInto(d.Raw, out)
		}
	}

	subn, err := n.loadNode(bs, subi, false)
	if err != nil {
		return err
	}

	return subn.get(bs, height-1, i%nodesForHeight(width, height), out)
}

func (r *Root) BatchDelete(indices []uint64) error {
	// TODO: theres a faster way of doing this, but this works for now
	for _, i := range indices {
		if err := r.Delete(i); err != nil {
			return err
		}
	}

	return nil
}

func (r *Root) Delete(i uint64) error {
	if err := r.Node.delete(r.bs, int(r.Height), i); err != nil {
		return err
	}
	r.Count--

	for r.Node.Bmap[0] == 1 && r.Height > 0 {
		sub, err := r.Node.loadNode(r.bs, 0, false)
		if err != nil {
			return err
		}

		r.Node = *sub
		r.Height--
	}

	return nil
}

func (n *Node) delete(bs Blocks, height int, i uint64) error {
	subi := i / nodesForHeight(width, height)
	set, _ := n.getBit(subi)
	if !set {
		return &ErrNotFound{i}
	}
	if height == 0 {
		n.expandValues()

		n.expVals[i] = nil
		n.clearBit(i)

		return nil
	}

	subn, err := n.loadNode(bs, subi, false)
	if err != nil {
		return err
	}

	if err := subn.delete(bs, height-1, i%nodesForHeight(width, height)); err != nil {
		return err
	}

	if subn.empty() {
		n.clearBit(subi)
		n.cache[subi] = nil
	}

	return nil
}

// Subtract removes all elements of 'or' from 'r'
func (r *Root) Subtract(or *Root) error {
	// TODO: as with other methods, there should be an optimized way of doing this
	return or.ForEach(func(i uint64, _ *cbg.Deferred) error {
		return r.Delete(i)
	})
}

func (r *Root) ForEach(cb func(uint64, *cbg.Deferred) error) error {
	return r.Node.forEach(r.bs, int(r.Height), 0, cb)
}

func (n *Node) forEach(bs Blocks, height int, offset uint64, cb func(uint64, *cbg.Deferred) error) error {
	if height == 0 {
		n.expandValues()

		for i, v := range n.expVals {
			if v != nil {
				if err := cb(offset+uint64(i), v); err != nil {
					return err
				}
			}
		}

		return nil
	}

	if n.cache == nil {
		n.expandLinks()
	}

	subCount := nodesForHeight(width, height)
	for i, v := range n.expLinks {
		if v != cid.Undef {
			var sub Node
			if err := bs.Get(v, &sub); err != nil {
				return err
			}

			offs := offset + (uint64(i) * subCount)
			if err := sub.forEach(bs, height-1, offs, cb); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *Node) expandValues() {
	if len(n.expVals) == 0 {
		n.expVals = make([]*cbg.Deferred, width)
		for x := uint64(0); x < width; x++ {
			set, ix := n.getBit(x)
			if set {
				n.expVals[x] = n.Values[ix]
			}
		}
	}
}

func (n *Node) set(bs Blocks, height int, i uint64, val *cbg.Deferred) (bool, error) {
	if height == 0 {
		n.expandValues()
		alreadySet, _ := n.getBit(i)
		n.expVals[i] = val
		n.setBit(i)

		return !alreadySet, nil
	}

	nfh := nodesForHeight(width, height)

	subn, err := n.loadNode(bs, i/nfh, true)
	if err != nil {
		return false, err
	}

	return subn.set(bs, height-1, i%nfh, val)
}

func (n *Node) getBit(i uint64) (bool, int) {
	if i > 7 {
		panic("cant deal with wider arrays yet")
	}

	if len(n.Bmap) == 0 {
		return false, 0
	}

	if n.Bmap[0]&byte(1<<i) == 0 {
		return false, 0
	}

	mask := byte((1 << i) - 1)
	return true, bits.OnesCount8(n.Bmap[0] & mask)
}

func (n *Node) setBit(i uint64) {
	if i > 7 {
		panic("cant deal with wider arrays yet")
	}

	if len(n.Bmap) == 0 {
		n.Bmap = []byte{0}
	}

	n.Bmap[0] = n.Bmap[0] | byte(1<<i)
}

func (n *Node) clearBit(i uint64) {
	if i > 7 {
		panic("cant deal with wider arrays yet")
	}

	if len(n.Bmap) == 0 {
		panic("invariant violated: called clear bit on empty node")
	}

	mask := byte(0xff - (1 << i))

	n.Bmap[0] = n.Bmap[0] & mask
}

func (n *Node) expandLinks() {
	n.cache = make([]*Node, width)
	n.expLinks = make([]cid.Cid, width)
	for x := uint64(0); x < width; x++ {
		set, ix := n.getBit(x)
		if set {
			n.expLinks[x] = n.Links[ix]
		}
	}
}

func (n *Node) loadNode(bs Blocks, i uint64, create bool) (*Node, error) {
	if n.cache == nil {
		n.expandLinks()
	} else {
		if n := n.cache[i]; n != nil {
			return n, nil
		}
	}

	set, _ := n.getBit(i)

	var subn *Node
	if set {
		var sn Node
		if err := bs.Get(n.expLinks[i], &sn); err != nil {
			return nil, err
		}

		subn = &sn
	} else {
		if create {
			subn = &Node{}
			n.setBit(i)
		} else {
			return nil, fmt.Errorf("no node found at (sub)index %d", i)
		}
	}
	n.cache[i] = subn

	return subn, nil
}

func nodesForHeight(width, height int) uint64 {
	return uint64(math.Pow(float64(width), float64(height)))
}

func (r *Root) Flush() (cid.Cid, error) {
	if err := r.Node.Flush(r.bs, int(r.Height)); err != nil {
		return cid.Undef, err
	}

	return r.bs.Put(r)
}

func (n *Node) empty() bool {
	return len(n.Bmap) == 0 || n.Bmap[0] == 0
}

func (n *Node) Flush(bs Blocks, depth int) error {
	if depth == 0 {
		if len(n.expVals) == 0 {
			return nil
		}
		n.Bmap = []byte{0}
		n.Values = nil
		for i := uint64(0); i < width; i++ {
			v := n.expVals[i]
			if v != nil {
				n.Values = append(n.Values, v)
				n.setBit(i)
			}
		}
		return nil
	}

	if len(n.expLinks) == 0 {
		// nothing to do!
		return nil
	}

	n.Bmap = []byte{0}
	n.Links = nil

	for i := uint64(0); i < width; i++ {
		subn := n.cache[i]
		if subn != nil {
			if err := subn.Flush(bs, depth-1); err != nil {
				return err
			}

			c, err := bs.Put(subn)
			if err != nil {
				return err
			}
			n.expLinks[i] = c
		}

		l := n.expLinks[i]
		if l != cid.Undef {
			n.Links = append(n.Links, l)
			n.setBit(i)
		}
	}

	return nil
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
