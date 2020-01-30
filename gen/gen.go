package main

import (
	cbg "github.com/whyrusleeping/cbor-gen"

	"github.com/filecoin-project/go-amt-ipld/v2"
)

func main() {
	// FIXME this will not generate the correct code, leave the cbor_gen.go file untouched.
	if err := cbg.WriteMapEncodersToFile("cbor_gen.go", "amt", amt.Root{}, amt.Node{}); err != nil {
		panic(err)
	}
}
