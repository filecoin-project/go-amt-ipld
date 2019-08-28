package main

import (
	"os"

	"github.com/filecoin-project/go-amt-ipld"
	cbg "github.com/whyrusleeping/cbor-gen"
)

func main() {
	fi, err := os.Create("cbor_gen.go")
	if err != nil {
		panic(err)
	}
	defer fi.Close()

	if err := cbg.PrintHeaderAndUtilityMethods(fi, "amt"); err != nil {
		panic(err)
	}
	if err := cbg.GenTupleEncodersForType(amt.Root{}, fi); err != nil {
		panic(err)
	}
	if err := cbg.GenTupleEncodersForType(amt.Node{}, fi); err != nil {
		panic(err)
	}
}
