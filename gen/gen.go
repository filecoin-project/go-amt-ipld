package main

import (
	cbg "github.com/whyrusleeping/cbor-gen"

	"github.com/filecoin-project/go-amt-ipld/v2"
)

func main() {
	if err := cbg.WriteTupleEncodersToFile("cbor_gen.go", "amt", amt.Root{}, amt.Node{}); err != nil {
		panic(err)
	}
}
