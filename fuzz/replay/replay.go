package main

import (
	"io/ioutil"
	"os"

	fuzzer "github.com/filecoin-project/go-amt-ipld/fuzz"
)

func init() {
	fuzzer.Debug = true
}

func main() {
	fname := os.Args[1]
	data, err := ioutil.ReadFile(fname)
	if err != nil {
		panic(err)
	}
	fuzzer.Fuzz(data)
}
