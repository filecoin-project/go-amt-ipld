package internal

import (
	cid "github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
)

type Node struct {
	Bmap   []byte
	Links  []cid.Cid
	Values []*cbg.Deferred
}

type Root struct {
	Height uint64
	Count  uint64
	Node   Node
}
