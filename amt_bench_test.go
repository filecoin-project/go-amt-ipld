package amt

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/stretchr/testify/require"
	cbg "github.com/whyrusleeping/cbor-gen"
)

type rander struct {
	r *rand.Rand
}

func (r *rander) randKey(keyRange uint64) uint64 {
	return r.r.Uint64() % keyRange
}

func (r *rander) randValue(datasize int) []byte {
	buf := make([]byte, datasize)
	rand.Read(buf)
	return buf
}

func (r *rander) selectKey(keys []uint64) uint64 {
	i := rand.Int() % len(keys)
	return keys[i]
}

type amtParams struct {
	id       string
	count    int
	datasize int
	keyrange int
}

type benchCase struct {
	id       string
	count    int
	bitwidth int
	datasize int
	keyrange int
}

var caseTable []benchCase

func init() {

	bitwidths := []int{
		1,
		2,
		3,
		4,
		5,
		6,
		7,
		8,
	}

	amts := []amtParams{
		amtParams{
			id:       "example.Full",
			count:    5000,
			datasize: 4,
			keyrange: 5000,
		},
		amtParams{
			id:       "example.Sparse",
			count:    5000,
			datasize: 4,
			keyrange: 5000000,
		},
		amtParams{
			id:       "example.AlmostFull",
			count:    5000,
			datasize: 4,
			keyrange: 10000,
		},
	}

	for _, a := range amts {
		for _, bw := range bitwidths {
			caseTable = append(caseTable,
				benchCase{
					id:       fmt.Sprintf("%s -- bw=%d", a.id, bw),
					count:    a.count,
					bitwidth: bw,
					datasize: a.datasize,
					keyrange: a.keyrange,
				})
		}
	}
}

func fillContinuous(ctx context.Context, b *testing.B, a *Root, count uint64, dataSize int, r rander) []uint64 {
	keys := make([]uint64, 0)
	for i := uint64(0); i < count; i++ {
		require.NoError(b, a.Set(ctx, i, r.randValue(dataSize)))
		keys = append(keys, i)
	}
	return keys
}

func fillSparse(ctx context.Context, b *testing.B, a *Root, count int, keyrange int, dataSize int, r rander) []uint64 {
	keys := make(map[uint64]struct{})
	keysSlice := make([]uint64, 0)
	for j := 0; j < count; j++ {
		for {
			key := r.randKey(uint64(keyrange))
			_, dup := keys[key]
			if !dup {
				require.NoError(b, a.Set(ctx, key, r.randValue(dataSize)))
				keys[key] = struct{}{}
				keysSlice = append(keysSlice, key)
				break
			}
		}
	}
	return keysSlice
}

func fill(ctx context.Context, b *testing.B, a *Root, count int, dataSize int, keyrange int, r rander) []uint64 {
	if count >= keyrange {
		return fillContinuous(ctx, b, a, uint64(count), dataSize, r)
	} else {
		return fillSparse(ctx, b, a, count, keyrange, dataSize, r)
	}
}

// Note this is only intended for use measuring size as timing and memory usage
// may not be optimal to handle no duplicate writes.
func BenchmarkFill(b *testing.B) {
	ctx := context.Background()
	for _, t := range caseTable {
		b.Run(fmt.Sprintf("%s", t.id), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				r := rander{rand.New(rand.NewSource(int64(i)))}
				mock := newMockBlocks()
				cst := cbor.NewCborStore(mock)
				a, err := NewAMT(cst, UseTreeBitWidth(t.bitwidth))
				require.NoError(b, err)

				// Fill the tree
				fill(ctx, b, a, t.count, t.datasize, t.keyrange, r)
				_, err = a.Flush(ctx)
				require.NoError(b, err)
				b.StopTimer()
				b.ReportMetric(float64(len(mock.data))/float64(t.count), "blocks")
				b.ReportMetric(float64(mock.totalBlockSizes())/float64(t.count), "bytes(blockstoreSize)/entry")
				binarySize, err := a.node.checkSize(ctx, cst, uint(t.bitwidth), a.height)
				require.NoError(b, err)
				b.ReportMetric(float64(binarySize), "binarySize")
				b.ReportMetric(float64(binarySize)/float64(t.count), "bytes(amtSize)/entry")
				b.StartTimer()
			}
		})
	}
}

// 0. Fill AMT with t.count keys selected between 0 and t.keyrange.
// 1. Perform 1000 sets on a random key from t.keyrange on the base AMT
// 2. Report average over sets
func BenchmarkSetIndividual(b *testing.B) {
	ctx := context.Background()
	for _, t := range caseTable {
		b.Run(fmt.Sprintf("%s", t.id), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				r := rander{rand.New(rand.NewSource(int64(i)))}
				mock := newMockBlocks()
				cst := cbor.NewCborStore(mock)
				a, err := NewAMT(cst, UseTreeBitWidth(t.bitwidth))
				require.NoError(b, err)

				// Initial fill
				fill(ctx, b, a, t.count, t.datasize, t.keyrange, r)
				aCid, err := a.Flush(ctx)
				require.NoError(b, err)

				mock.stats = blockstoreStats{}
				b.ReportAllocs()
				b.StartTimer()
				for j := 0; j < 1000; j++ {
					// Load AMT, perform a set at random within key range, flush
					a, err = LoadAMT(ctx, cst, aCid, UseTreeBitWidth(t.bitwidth))
					require.NoError(b, err)

					key := r.randKey(uint64(t.keyrange))
					require.NoError(b, a.Set(ctx, key, r.randValue(t.datasize)))
					_, err = a.Flush(ctx)
					require.NoError(b, err)
				}
				b.StopTimer()
				b.ReportMetric(float64(mock.stats.evtcntGet)/1000, "getEvts")
				b.ReportMetric(float64(mock.stats.evtcntPut)/1000, "putEvts")
				b.ReportMetric(float64(mock.stats.bytesPut)/1000, "bytesPut")
			}
		})
	}
}

func BenchmarkGetIndividual(b *testing.B) {
	ctx := context.Background()

	for _, t := range caseTable {
		b.Run(fmt.Sprintf("%s", t.id), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				r := rander{rand.New(rand.NewSource(int64(i)))}
				mock := newMockBlocks()
				cst := cbor.NewCborStore(mock)
				a, err := NewAMT(cst, UseTreeBitWidth(t.bitwidth))
				require.NoError(b, err)

				// Initial fill
				amtKeys := fill(ctx, b, a, t.count, t.datasize, t.keyrange, r)
				aCid, err := a.Flush(ctx)
				require.NoError(b, err)

				mock.stats = blockstoreStats{}
				b.ReportAllocs()
				b.StartTimer()
				var d cbg.Deferred
				for j := 0; j < 1000; j++ {
					// Load AMT, perform a set on a random existing key
					a, err = LoadAMT(ctx, cst, aCid, UseTreeBitWidth(t.bitwidth))
					require.NoError(b, err)
					require.NoError(b, a.Get(ctx, r.selectKey(amtKeys), &d))
				}
				b.StopTimer()
				b.ReportMetric(float64(mock.stats.evtcntGet)/float64(1000), "getEvts")
				b.ReportMetric(float64(mock.stats.evtcntPut)/float64(1000), "putEvts")
			}
		})
	}
}
