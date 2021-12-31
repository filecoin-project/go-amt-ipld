package main

import (
	cbg "github.com/whyrusleeping/cbor-gen"

	"github.com/filecoin-project/go-amt-ipld/v4/internal"
)

func main() {
	if err := cbg.WriteTupleEncodersToFile("internal/cbor_gen.go", "internal", internal.Root{}, internal.Node{}); err != nil {
		panic(err)
	}
}
