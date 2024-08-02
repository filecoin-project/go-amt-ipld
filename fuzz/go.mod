module github.com/filecoin-project/go-amt-ipld/fuzz

go 1.21

replace github.com/filecoin-project/go-amt-ipld/v4 => ../

require (
	github.com/dvyukov/go-fuzz v0.0.0-20240203152606-b1ce7bc07150
	github.com/elazarl/go-bindata-assetfs v1.0.1
	github.com/filecoin-project/go-amt-ipld/v4 v4.3.0
	github.com/ipfs/go-block-format v0.2.0
	github.com/ipfs/go-cid v0.4.1
	github.com/ipfs/go-ipld-cbor v0.1.0
	github.com/stephens2424/writerset v1.0.2
	github.com/whyrusleeping/cbor-gen v0.1.2
)

require (
	github.com/ipfs/go-ipfs-util v0.0.3 // indirect
	github.com/ipfs/go-ipld-format v0.6.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.8 // indirect
	github.com/minio/sha256-simd v1.0.1 // indirect
	github.com/mr-tron/base58 v1.2.0 // indirect
	github.com/multiformats/go-base32 v0.1.0 // indirect
	github.com/multiformats/go-base36 v0.2.0 // indirect
	github.com/multiformats/go-multibase v0.2.0 // indirect
	github.com/multiformats/go-multihash v0.2.3 // indirect
	github.com/multiformats/go-varint v0.0.7 // indirect
	github.com/polydawn/refmt v0.89.0 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	golang.org/x/crypto v0.25.0 // indirect
	golang.org/x/sync v0.7.0 // indirect
	golang.org/x/sys v0.22.0 // indirect
	golang.org/x/tools v0.0.0-20190328211700-ab21143f2384 // indirect
	golang.org/x/xerrors v0.0.0-20240716161551-93cc26a95ae9 // indirect
	lukechampine.com/blake3 v1.3.0 // indirect
)
