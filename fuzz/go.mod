module github.com/filecoin-project/go-amt-ipld/fuzz

go 1.15

// go 1.15 fix
replace github.com/dvyukov/go-fuzz => github.com/oraluben/go-fuzz v0.0.0-20200824133829-649feeb79006

replace github.com/filecoin-project/go-amt-ipld/v2 => ../

require (
	github.com/dvyukov/go-fuzz v0.0.0-00010101000000-000000000000
	github.com/elazarl/go-bindata-assetfs v1.0.1 // indirect
	github.com/filecoin-project/go-amt-ipld/v2 v2.1.0
	github.com/ipfs/go-block-format v0.0.2
	github.com/ipfs/go-cid v0.0.7
	github.com/ipfs/go-ipld-cbor v0.0.4
	github.com/stephens2424/writerset v1.0.2 // indirect
	github.com/whyrusleeping/cbor-gen v0.0.0-20200814232421-c568d328ad9d
)
