package main

import (
	"os"

	fuzzer "github.com/filecoin-project/go-amt-ipld/fuzz"
)

func init() {
	fuzzer.Debug = true
}

func main() {
	fname := os.Args[1]
	data, err := os.ReadFile(fname)
	if err != nil {
		panic(err)
	}
	fuzzer.Fuzz(data)
}
