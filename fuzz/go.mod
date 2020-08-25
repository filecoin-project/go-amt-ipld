module github.com/filecoin-project/go-amt-ipld/fuzz

go 1.15

replace github.com/filecoin-project/go-amt-ipld/v4 => ../

require (
	github.com/dvyukov/go-fuzz v0.0.0-20220726122315-1d375ef9f9f6
	github.com/elazarl/go-bindata-assetfs v1.0.1
	github.com/filecoin-project/go-amt-ipld/v4 v4.1.0
	github.com/ipfs/go-block-format v0.1.2
	github.com/ipfs/go-cid v0.4.0
	github.com/ipfs/go-ipld-cbor v0.0.6
	github.com/stephens2424/writerset v1.0.2
	github.com/whyrusleeping/cbor-gen v0.0.0-20230126041949-52956bd4c9aa
)
