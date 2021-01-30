package internal

import (
	cid "github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
)

const (
	// Width must be a power of 2. We set this to 8.
	MaxIndexBits = 63
	WidthBits    = 3
	Width        = 1 << WidthBits             // 8
	BitfieldSize = 1                          // ((width - 1) >> 3) + 1
	MaxHeight    = MaxIndexBits/WidthBits - 1 // 20 (because the root is at height 0).
)

func init() {
	if BitfieldSize != ((Width-1)>>3)+1 {
		panic("bitfield size must match width")
	}
}

type Node struct {
	Bmap   [BitfieldSize]byte
	Links  []cid.Cid
	Values []*cbg.Deferred
}

type Root struct {
	Height uint64
	Count  uint64
	Node   Node
}
