module github.com/filecoin-project/go-amt-ipld/fuzz

go 1.23.10

replace github.com/filecoin-project/go-amt-ipld/v4 => ../

require (
	github.com/dvyukov/go-fuzz v0.0.0-20240924070022-e577bee5275c
	github.com/elazarl/go-bindata-assetfs v1.0.1
	github.com/filecoin-project/go-amt-ipld/v4 v4.4.0
	github.com/ipfs/go-block-format v0.2.2
	github.com/ipfs/go-cid v0.5.0
	github.com/ipfs/go-ipld-cbor v0.2.0
	github.com/stephens2424/writerset v1.0.2
	github.com/whyrusleeping/cbor-gen v0.3.1
)

require (
	github.com/ipfs/boxo v0.32.0 // indirect
	github.com/ipfs/go-ipld-format v0.6.1 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/minio/sha256-simd v1.0.1 // indirect
	github.com/mr-tron/base58 v1.2.0 // indirect
	github.com/multiformats/go-base32 v0.1.0 // indirect
	github.com/multiformats/go-base36 v0.2.0 // indirect
	github.com/multiformats/go-multibase v0.2.0 // indirect
	github.com/multiformats/go-multihash v0.2.3 // indirect
	github.com/multiformats/go-varint v0.0.7 // indirect
	github.com/polydawn/refmt v0.89.0 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	golang.org/x/crypto v0.39.0 // indirect
	golang.org/x/mod v0.25.0 // indirect
	golang.org/x/sync v0.15.0 // indirect
	golang.org/x/sys v0.33.0 // indirect
	golang.org/x/tools v0.34.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	lukechampine.com/blake3 v1.4.1 // indirect
)
