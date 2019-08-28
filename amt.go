package amt

import (
	"bytes"
	"fmt"
	"math"
	"math/bits"

	blocks "github.com/ipfs/go-block-format"
	cbor "github.com/ipfs/go-ipld-cbor"
	mh "github.com/multiformats/go-multihash"
	cbg "github.com/whyrusleeping/cbor-gen"

	cid "github.com/ipfs/go-cid"
)

const width = 8

type Root struct {
	Height uint64
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
	Get(cid.Cid) (blocks.Block, error)
	Put(blocks.Block) error
}

func NewAMT(bs Blocks) *Root {
	return &Root{
		bs: bs,
	}
}

func LoadAMT(bs Blocks, c cid.Cid) (*Root, error) {
	b, err := bs.Get(c)
	if err != nil {
		return nil, err
	}

	var r Root
	if err := r.UnmarshalCBOR(bytes.NewReader(b.RawData())); err != nil {
		return nil, err
	}
	r.bs = bs

	return &r, nil
}

func (r *Root) Set(i uint, val interface{}) error {

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

			c, err := putObjectCbor(r.bs, &r.Node)
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

	return r.Node.set(r.bs, int(r.Height), i, &cbg.Deferred{Raw: b})
}

func (r *Root) Get(i uint, out interface{}) error {
	return r.Node.get(r.bs, int(r.Height), i, out)
}

func (n *Node) get(bs Blocks, height int, i uint, out interface{}) error {
	subi := i / nodesForHeight(width, height)
	set, _ := n.getBit(subi)
	if !set {
		return fmt.Errorf("no item found at index")
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

	subn, err := n.loadNode(bs, subi)
	if err != nil {
		return err
	}

	return subn.get(bs, height-1, i%nodesForHeight(width, height), out)
}

func (n *Node) expandValues() {
	if len(n.expVals) == 0 {
		n.expVals = make([]*cbg.Deferred, width)
		for x := uint(0); x < width; x++ {
			set, ix := n.getBit(x)
			if set {
				n.expVals[x] = n.Values[ix]
			}
		}
	}
}

func (n *Node) set(bs Blocks, height int, i uint, val *cbg.Deferred) error {
	if height == 0 {
		n.expandValues()
		n.expVals[i] = val
		n.setBit(i)
		return nil
	}

	nfh := nodesForHeight(width, height)

	subn, err := n.loadNode(bs, i/nfh)
	if err != nil {
		return err
	}

	return subn.set(bs, height-1, i%nfh, val)
}

func (n *Node) getBit(i uint) (bool, int) {
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

func (n *Node) setBit(i uint) {
	if i > 7 {
		panic("cant deal with wider arrays yet")
	}

	if len(n.Bmap) == 0 {
		n.Bmap = []byte{0}
	}

	n.Bmap[0] = n.Bmap[0] | byte(1<<i)
}

func (n *Node) loadNode(bs Blocks, i uint) (*Node, error) {
	if n.cache == nil {
		n.cache = make([]*Node, width)
		n.expLinks = make([]cid.Cid, width)
		for x := uint(0); x < width; x++ {
			set, ix := n.getBit(x)
			if set {
				n.expLinks[x] = n.Links[ix]
			}
		}
	} else {
		if n := n.cache[i]; n != nil {
			return n, nil
		}
	}

	set, _ := n.getBit(i)

	var subn *Node
	if set {
		blk, err := bs.Get(n.expLinks[i])
		if err != nil {
			return nil, err
		}

		var sn Node
		if err := sn.UnmarshalCBOR(bytes.NewReader(blk.RawData())); err != nil {
			return nil, err
		}
		subn = &sn
	} else {
		subn = &Node{}
		n.setBit(i)
	}
	n.cache[i] = subn

	return subn, nil
}

func nodesForHeight(width, height int) uint {
	return uint(math.Pow(float64(width), float64(height)))
}

func (r *Root) Flush() (cid.Cid, error) {
	if err := r.Node.Flush(r.bs, int(r.Height)); err != nil {
		return cid.Undef, err
	}

	return putObjectCbor(r.bs, r)
}

func (n *Node) empty() bool {
	// TODO: probably a simpler way to do this check but i'm kinda tired right now
	return len(n.expLinks) == 0 && len(n.cache) == 0 && len(n.Links) == 0 && len(n.Values) == 0 && len(n.expVals) == 0
}

func (n *Node) Flush(bs Blocks, depth int) error {
	if depth == 0 {
		if len(n.expVals) == 0 {
			return nil
		}
		n.Bmap = []byte{0}
		n.Values = nil
		for i := uint(0); i < width; i++ {
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

	for i := uint(0); i < width; i++ {
		subn := n.cache[i]
		if subn != nil {
			if err := subn.Flush(bs, depth-1); err != nil {
				return err
			}

			c, err := putObjectCbor(bs, subn)
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

func putObjectCbor(bs Blocks, obj cbg.CBORMarshaler) (cid.Cid, error) {
	buf := new(bytes.Buffer)
	if err := obj.MarshalCBOR(buf); err != nil {
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

	if err := bs.Put(blk); err != nil {
		return cid.Undef, err
	}

	return blk.Cid(), nil

}
