//go:build tools
// +build tools

package fuzzer

import (
	_ "github.com/dvyukov/go-fuzz/go-fuzz-build"
	_ "github.com/dvyukov/go-fuzz/go-fuzz-defs"
	// go-fuzz doesn't do go modules.
	_ "github.com/elazarl/go-bindata-assetfs"
	_ "github.com/stephens2424/writerset"
)
