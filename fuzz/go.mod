module github.com/filecoin-project/go-amt-ipld/fuzz

go 1.20

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

require (
	github.com/ipfs/go-ipfs-util v0.0.2 // indirect
	github.com/ipfs/go-ipld-format v0.0.1 // indirect
	github.com/klauspost/cpuid/v2 v2.0.4 // indirect
	github.com/minio/blake2b-simd v0.0.0-20160723061019-3f5f724cb5b1 // indirect
	github.com/minio/sha256-simd v1.0.0 // indirect
	github.com/mr-tron/base58 v1.2.0 // indirect
	github.com/multiformats/go-base32 v0.0.3 // indirect
	github.com/multiformats/go-base36 v0.1.0 // indirect
	github.com/multiformats/go-multibase v0.0.3 // indirect
	github.com/multiformats/go-multihash v0.0.15 // indirect
	github.com/multiformats/go-varint v0.0.6 // indirect
	github.com/polydawn/refmt v0.0.0-20190221155625-df39d6c2d992 // indirect
	golang.org/x/crypto v0.17.0 // indirect
	golang.org/x/mod v0.6.0-dev.0.20220419223038-86c51ed26bb4 // indirect
	golang.org/x/sync v0.0.0-20220722155255-886fb9371eb4 // indirect
	golang.org/x/sys v0.15.0 // indirect
	golang.org/x/tools v0.1.12 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
)
