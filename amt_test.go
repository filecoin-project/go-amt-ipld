package amt

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	block "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	assert "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cbg "github.com/whyrusleeping/cbor-gen"

	"github.com/filecoin-project/go-amt-ipld/v4/internal"
)

var numbers []cbg.CBORMarshaler

func init() {
	numbers = make([]cbg.CBORMarshaler, 10)
	for i := range numbers {
		val := cbg.CborInt(i)
		numbers[i] = &val
	}
}

var (
	bitWidths2to18 = []uint{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}
	bitWidths2to3  = []uint{2, 3}
)

func runTestWithBitWidths(t *testing.T, bitwidths []uint, fn func(*testing.T, ...Option)) {
	t.Helper()
	if testing.Short() {
		t.Run(fmt.Sprintf("bitwidth=%d", defaultBitWidth), func(t *testing.T) { fn(t, UseTreeBitWidth(defaultBitWidth)) })
		return
	}
	for _, bw := range bitwidths {
		t.Run(fmt.Sprintf("bitwidth=%d", bw), func(t *testing.T) { fn(t, UseTreeBitWidth(bw)) })
	}
}

func runBenchmarkWithBitWidths(b *testing.B, bitwidths []uint, fn func(*testing.B, ...Option)) {
	b.Helper()
	for _, bw := range bitwidths {
		b.Run(fmt.Sprintf("bitwidth=%d", bw), func(b *testing.B) { fn(b, UseTreeBitWidth(bw)) })
	}
}

type mockBlocks struct {
	data               map[cid.Cid]block.Block
	getCount, putCount int
}

func newMockBlocks() *mockBlocks {
	return &mockBlocks{make(map[cid.Cid]block.Block), 0, 0}
}

func (mb *mockBlocks) Get(c cid.Cid) (block.Block, error) {
	d, ok := mb.data[c]
	mb.getCount++
	if ok {
		return d, nil
	}
	return nil, fmt.Errorf("Not Found")
}

func (mb *mockBlocks) Put(b block.Block) error {
	mb.putCount++
	mb.data[b.Cid()] = b
	return nil
}

func (mb *mockBlocks) report(b *testing.B) {
	b.ReportMetric(float64(mb.getCount)/float64(b.N), "gets/op")
	b.ReportMetric(float64(mb.putCount)/float64(b.N), "puts/op")
}

func TestNew(t *testing.T) {
	bs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()

	t.Run("default config", func(t *testing.T) {
		a, err := NewAMT(bs)
		require.NoError(t, err)
		assert.Equal(t, defaultBitWidth, a.bitWidth)

		c, err := FromArray(ctx, bs, numbers)
		require.NoError(t, err)
		as, err := LoadAMT(ctx, bs, c)
		require.NoError(t, err)
		assert.Equal(t, defaultBitWidth, as.bitWidth)
	})

	t.Run("explicit bitwidth", func(t *testing.T) {
		a, err := NewAMT(bs, UseTreeBitWidth(4))
		require.NoError(t, err)
		assert.Equal(t, uint(4), a.bitWidth)

		c, err := FromArray(ctx, bs, numbers, UseTreeBitWidth(4))
		require.NoError(t, err)
		if defaultBitWidth != uint(4) {
			_, err = LoadAMT(ctx, bs, c) // Fails to load with implicit default bitwidth
			assert.Error(t, err)
		}
		as, err := LoadAMT(ctx, bs, c, UseTreeBitWidth(4))
		require.NoError(t, err)
		assert.Equal(t, uint(4), as.bitWidth)
	})
}

func TestBasicSetGet(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		bs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()
		a, err := NewAMT(bs, opts...)
		require.NoError(t, err)

		assertSet(t, a, 2, "foo")
		assertGet(ctx, t, a, 2, "foo")
		assertCount(t, a, 1)

		c, err := a.Flush(ctx)
		if err != nil {
			t.Fatal(err)
		}

		clean, err := LoadAMT(ctx, bs, c, opts...)
		if err != nil {
			t.Fatal(err)
		}

		assertGet(ctx, t, clean, 2, "foo")

		assertCount(t, clean, 1)
	})
}

func TestRoundTrip(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		bs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()
		a, err := NewAMT(bs, opts...)
		require.NoError(t, err)
		emptyCid, err := a.Flush(ctx)
		require.NoError(t, err)

		k := uint64(100000)
		assertSet(t, a, k, "foo")
		assertDelete(t, a, k)

		c, err := a.Flush(ctx)
		require.NoError(t, err)

		require.Equal(t, emptyCid, c)
	})
}

func TestMaxRange(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		ctx := context.Background()
		bs := cbor.NewCborStore(newMockBlocks())

		a, err := NewAMT(bs, opts...)
		require.NoError(t, err)

		key := uint64(MaxIndex)
		expected := cborstr("what is up 1")
		err = a.Set(ctx, key, expected)
		require.NoError(t, err)

		var actual CborByteArray
		found, err := a.Get(ctx, key, &actual)
		require.NoError(t, err)
		require.True(t, found)
		require.EqualValues(t, actual, *expected)

		found = false
		require.NoError(t, a.ForEach(ctx, func(i uint64, v *cbg.Deferred) error {
			require.Equal(t, uint64(i), key)
			found = true
			return nil
		}))
		require.True(t, found)

		err = a.Set(ctx, MaxIndex+1, cborstr("what is up 2"))
		require.Error(t, err)

		found, err = a.Get(ctx, MaxIndex+1, &actual)
		require.Error(t, err)
		require.False(t, found)
	})
}

func TestMaxRange11(t *testing.T) {
	ctx := context.Background()
	bs := cbor.NewCborStore(newMockBlocks())

	a, err := NewAMT(bs, UseTreeBitWidth(11))
	require.NoError(t, err)

	err = a.Set(ctx, MaxIndex, cborstr("what is up 1"))
	require.NoError(t, err)

	err = a.Set(ctx, MaxIndex+1, cborstr("what is up 2"))
	require.Error(t, err)
}

func assertDelete(t *testing.T, r *Root, i uint64) {
	ctx := context.Background()

	t.Helper()
	found, err := r.Delete(ctx, i)
	require.NoError(t, err)
	require.True(t, found)

	found, err = r.Get(ctx, i, nil)
	require.NoError(t, err)
	require.False(t, found)
}

func assertSet(t *testing.T, r *Root, i uint64, val string) {
	ctx := context.Background()

	t.Helper()
	if err := r.Set(ctx, i, cborstr(val)); err != nil {
		t.Fatal(err)
	}
}

func assertCount(t testing.TB, r *Root, c uint64) {
	t.Helper()
	require.Equal(t, c, r.count)
}

func assertGet(ctx context.Context, t testing.TB, r *Root, i uint64, val string) {
	t.Helper()
	found, err := r.Get(ctx, i, nil)
	require.NoError(t, err)
	require.True(t, found)

	var out CborByteArray
	found, err = r.Get(ctx, i, &out)
	require.NoError(t, err)
	require.True(t, found)

	if !bytes.Equal(out, *cborstr(val)) {
		t.Fatal("value we got out didnt match expectation")
	}
}

func TestExpand(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		bs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()
		a, err := NewAMT(bs, opts...)
		require.NoError(t, err)

		assertSet(t, a, 2, "foo")
		assertSet(t, a, 11, "bar")
		assertSet(t, a, 79, "baz")

		assertGet(ctx, t, a, 2, "foo")
		assertGet(ctx, t, a, 11, "bar")
		assertGet(ctx, t, a, 79, "baz")

		c, err := a.Flush(ctx)
		if err != nil {
			t.Fatal(err)
		}

		na, err := LoadAMT(ctx, bs, c, opts...)
		if err != nil {
			t.Fatal(err)
		}

		assertGet(ctx, t, na, 2, "foo")
		assertGet(ctx, t, na, 11, "bar")
		assertGet(ctx, t, na, 79, "baz")
	})
}

func TestInsertABunch(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		bs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()
		a, err := NewAMT(bs, opts...)
		require.NoError(t, err)

		num := uint64(5000)

		for i := uint64(0); i < num; i++ {
			assertSet(t, a, i, "foo foo bar")
		}

		for i := uint64(0); i < num; i++ {
			assertGet(ctx, t, a, i, "foo foo bar")
		}

		c, err := a.Flush(ctx)
		if err != nil {
			t.Fatal(err)
		}

		na, err := LoadAMT(ctx, bs, c, opts...)
		if err != nil {
			t.Fatal(err)
		}

		for i := uint64(0); i < num; i++ {
			assertGet(ctx, t, na, i, "foo foo bar")
		}

		assertCount(t, na, num)
	})
}

func TestForEachWithoutFlush(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		bs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()

		for _, vals := range [][]uint64{
			{0, 1, 2, 3, 4, 5, 6, 7},
			{8},
			{8, 9, 64},
			{64, 8, 9},
		} {
			amt, err := NewAMT(bs, opts...)
			require.NoError(t, err)
			set1 := make(map[uint64]struct{})
			set2 := make(map[uint64]struct{})
			for _, val := range vals {
				err := amt.Set(ctx, val, cborstr(""))
				require.NoError(t, err)

				set1[val] = struct{}{}
				set2[val] = struct{}{}
			}

			amt.ForEach(ctx, func(u uint64, deferred *cbg.Deferred) error {
				delete(set1, u)
				return nil
			})
			assert.Equal(t, make(map[uint64]struct{}), set1)

			// ensure it still works after flush
			_, err = amt.Flush(ctx)
			require.NoError(t, err)

			amt.ForEach(ctx, func(u uint64, deferred *cbg.Deferred) error {
				delete(set2, u)
				return nil
			})
			assert.Equal(t, make(map[uint64]struct{}), set2)
		}
	})
}

type op struct {
	del  bool
	idxs []uint64
}

func TestChaos(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		bs := cbor.NewCborStore(newMockBlocks())
		seed := time.Now().UnixNano()
		ctx := context.Background()
		// seed = 1579200312848358622 // FIXED
		// seed = 1579202116615474412
		// seed = 1579202774458659521
		// all above are with ops=100,maxIndx=2000
		r := rand.New(rand.NewSource(seed))
		t.Logf("seed: %d", seed)

		a, err := NewAMT(bs, opts...)
		require.NoError(t, err)
		c, err := a.Flush(ctx)
		assert.NoError(t, err)

		ops := make([]op, 1000)
		maxPerOp := 10
		maxIndx := 20000
		for i := range ops {
			o := &ops[i]
			o.del = r.Intn(10) < 4
			o.idxs = make([]uint64, r.Intn(maxPerOp))
			for j := range o.idxs {
				o.idxs[j] = uint64(r.Intn(maxIndx))
			}
		}

		testMap := make(map[uint64]struct{})

		for i, o := range ops {
			a, err = LoadAMT(ctx, bs, c, opts...)
			assert.NoError(t, err)

			for _, index := range o.idxs {
				if !o.del {
					err := a.Set(ctx, index, cborstr("test"))
					testMap[index] = struct{}{}
					assert.NoError(t, err)
				} else {
					_, err := a.Delete(ctx, index)
					delete(testMap, index)
					require.NoError(t, err)
				}
			}

			fail := false
			correctLen := uint64(len(testMap))
			if correctLen != a.Len() {
				t.Errorf("bad length before flush, correct: %d, Count: %d, i: %d", correctLen, a.Len(), i)
				fail = true
			}

			c, err = a.Flush(ctx)
			assert.NoError(t, err)

			a, err = LoadAMT(ctx, bs, c, opts...)
			assert.NoError(t, err)
			if correctLen != a.Len() {
				t.Errorf("bad length after flush, correct: %d, Count: %d, i: %d", correctLen, a.Len(), i)
				fail = true
			}

			var feCount uint64
			a.ForEach(ctx, func(_ uint64, _ *cbg.Deferred) error {
				feCount++
				return nil
			})
			if correctLen != feCount {
				t.Errorf("bad fe length after flush, correct: %d, Count: %d, i: %d", correctLen, feCount, i)
				fail = true
			}
			if fail {
				t.Logf("%+v", o)
				t.FailNow()
			}
		}
	})
}

func TestInsertABunchWithDelete(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		bs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()
		a, err := NewAMT(bs, opts...)
		require.NoError(t, err)

		num := 12000
		originSet := make(map[uint64]bool, num)
		removeSet := make(map[uint64]bool, num)
		seed := time.Now().UnixNano()
		r := rand.New(rand.NewSource(seed))
		t.Logf("seed: %d", seed)

		for i := 0; i < num; i++ {
			originSet[uint64(r.Intn(num))] = true
		}

		for i := 0; i < 660; i++ {
			k := uint64(r.Intn(num))
			if originSet[k] {
				removeSet[k] = true
			}
		}

		for i := uint64(0); i < uint64(num); i++ {
			if originSet[i] {
				assertSet(t, a, i, "foo foo bar")
			}
		}

		for i := uint64(0); i < uint64(num); i++ {
			if originSet[i] {
				assertGet(ctx, t, a, i, "foo foo bar")
			}
		}

		c, err := a.Flush(ctx)
		if err != nil {
			t.Fatal(err)
		}

		na, err := LoadAMT(ctx, bs, c, opts...)
		if err != nil {
			t.Fatal(err)
		}

		for i := uint64(0); i < uint64(num); i++ {
			if removeSet[i] {
				assertDelete(t, na, i)
			}
		}

		c, err = na.Flush(ctx)
		if err != nil {
			t.Fatal(err)
		}
		n2a, err := LoadAMT(ctx, bs, c, opts...)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("originSN: %d, removeSN: %d; expected: %d, actual len(n2a): %d",
			len(originSet), len(removeSet), len(originSet)-len(removeSet), n2a.Len())
		assertCount(t, n2a, uint64(len(originSet)-len(removeSet)))

		for i := uint64(0); i < uint64(num); i++ {
			if originSet[i] && !removeSet[i] {
				assertGet(ctx, t, n2a, i, "foo foo bar")
			}
		}
	})
}

func TestDeleteFirstEntry(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		bs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()
		a, err := NewAMT(bs, opts...)
		require.NoError(t, err)

		assertSet(t, a, 0, "cat")
		assertSet(t, a, 27, "cat")

		assertDelete(t, a, 27)

		c, err := a.Flush(ctx)
		if err != nil {
			t.Fatal(err)
		}

		na, err := LoadAMT(ctx, bs, c, opts...)
		if err != nil {
			t.Fatal(err)
		}

		assertCount(t, na, 1)
	})
}

func TestDelete(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		bs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()
		a, err := NewAMT(bs, opts...)
		require.NoError(t, err)

		// Check that deleting out of range of the current AMT returns not found
		found, err := a.Delete(ctx, 200)
		require.NoError(t, err)
		require.False(t, found)

		assertSet(t, a, 0, "cat")
		assertSet(t, a, 1, "cat")
		assertSet(t, a, 2, "cat")
		assertSet(t, a, 3, "cat")

		assertDelete(t, a, 1)

		assertGet(ctx, t, a, 0, "cat")
		assertGet(ctx, t, a, 2, "cat")
		assertGet(ctx, t, a, 3, "cat")

		assertDelete(t, a, 0)
		assertDelete(t, a, 2)
		assertDelete(t, a, 3)

		assertCount(t, a, 0)
		t.Logf("trying deeper operations now")

		assertSet(t, a, 23, "dog")
		assertSet(t, a, 24, "dog")

		assertDelete(t, a, 23)

		assertCount(t, a, 1)

		c, err := a.Flush(ctx)
		if err != nil {
			t.Fatal(err)
		}

		na, err := LoadAMT(ctx, bs, c, opts...)
		if err != nil {
			t.Fatal(err)
		}

		assertCount(t, na, 1)

		a2, err := NewAMT(bs, opts...)
		require.NoError(t, err)
		assertSet(t, a2, 24, "dog")

		a2c, err := a2.Flush(ctx)
		if err != nil {
			t.Fatal(err)
		}

		if c != a2c {
			t.Logf("%#v\n", a)
			t.Logf("%#v\n", na)
			t.Logf("%#v\n", a2)
			t.Fatal("unexpected cid", c, a2c)
		}
	})
}

func TestDeleteReduceHeight(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		bs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()
		a, err := NewAMT(bs, opts...)
		require.NoError(t, err)

		assertSet(t, a, 1, "thing")

		c1, err := a.Flush(ctx)
		if err != nil {
			t.Fatal(err)
		}

		assertSet(t, a, 37, "other")

		c2, err := a.Flush(ctx)
		if err != nil {
			t.Fatal(err)
		}

		a2, err := LoadAMT(ctx, bs, c2, opts...)
		if err != nil {
			t.Fatal(err)
		}

		assertDelete(t, a2, 37)
		assertCount(t, a2, 1)

		c3, err := a2.Flush(ctx)
		if err != nil {
			t.Fatal(err)
		}

		if c1 != c3 {
			t.Fatal("structures did not match after insert/delete")
		}
	})
}

func BenchmarkAMTInsertBulk(b *testing.B) {
	runBenchmarkWithBitWidths(b, bitWidths2to18, func(b *testing.B, opts ...Option) {
		mock := newMockBlocks()
		defer mock.report(b)

		bs := cbor.NewCborStore(mock)
		ctx := context.Background()

		for i := 0; i < b.N; i++ {
			a, err := NewAMT(bs, opts...)
			require.NoError(b, err)

			num := uint64(5000)

			for i := uint64(0); i < num; i++ {
				if err := a.Set(ctx, i, cborstr("foo foo bar")); err != nil {
					b.Fatal(err)
				}
			}

			for i := uint64(0); i < num; i++ {
				assertGet(ctx, b, a, i, "foo foo bar")
			}

			c, err := a.Flush(ctx)
			if err != nil {
				b.Fatal(err)
			}

			na, err := LoadAMT(ctx, bs, c, opts...)
			if err != nil {
				b.Fatal(err)
			}

			for i := uint64(0); i < num; i++ {
				assertGet(ctx, b, na, i, "foo foo bar")
			}

			assertCount(b, na, num)
		}
	})
}

func BenchmarkAMTLoadAndInsert(b *testing.B) {
	runBenchmarkWithBitWidths(b, bitWidths2to18, func(b *testing.B, opts ...Option) {
		mock := newMockBlocks()
		defer mock.report(b)

		bs := cbor.NewCborStore(mock)
		ctx := context.Background()
		a, err := NewAMT(bs, opts...)
		require.NoError(b, err)

		c, err := a.Flush(ctx)
		if err != nil {
			b.Fatal(err)
		}

		for i := uint64(b.N); i > 0; i-- {
			na, err := LoadAMT(ctx, bs, c, opts...)
			if err != nil {
				b.Fatal(err)
			}

			if err := na.Set(ctx, i, cborstr("some value")); err != nil {
				b.Fatal(err)
			}
			c, err = na.Flush(ctx)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestForEach(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		bs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()
		a, err := NewAMT(bs, opts...)
		require.NoError(t, err)

		r := rand.New(rand.NewSource(101))

		var indexes []uint64
		for i := 0; i < 10000; i++ {
			if r.Intn(2) == 0 {
				indexes = append(indexes, uint64(i))
			}
		}

		for _, i := range indexes {
			if err := a.Set(ctx, i, cborstr("value")); err != nil {
				t.Fatal(err)
			}
		}

		for _, i := range indexes {
			assertGet(ctx, t, a, i, "value")
		}

		assertCount(t, a, uint64(len(indexes)))

		c, err := a.Flush(ctx)
		if err != nil {
			t.Fatal(err)
		}

		na, err := LoadAMT(ctx, bs, c, opts...)
		if err != nil {
			t.Fatal(err)
		}

		assertCount(t, na, uint64(len(indexes)))

		var x int
		err = na.ForEach(ctx, func(i uint64, v *cbg.Deferred) error {
			if i != indexes[x] {
				t.Fatal("got wrong index", i, indexes[x], x)
			}
			x++
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if x != len(indexes) {
			t.Fatal("didnt see enough values")
		}
	})
}

func TestForEachAt(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		bs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()
		a, err := NewAMT(bs, opts...)
		require.NoError(t, err)

		r := rand.New(rand.NewSource(101))

		var indexes []uint64
		for i := 0; i < 10000; i++ {
			if r.Intn(2) == 0 {
				indexes = append(indexes, uint64(i))
			}
		}

		for _, i := range indexes {
			if err := a.Set(ctx, i, cborstr(fmt.Sprint(i))); err != nil {
				t.Fatal(err)
			}
		}

		for _, i := range indexes {
			assertGet(ctx, t, a, i, fmt.Sprint(i))
		}

		assertCount(t, a, uint64(len(indexes)))

		c, err := a.Flush(ctx)
		if err != nil {
			t.Fatal(err)
		}

		na, err := LoadAMT(ctx, bs, c, opts...)
		if err != nil {
			t.Fatal(err)
		}

		assertCount(t, na, uint64(len(indexes)))

		for try := 0; try < 10; try++ {
			start := uint64(r.Intn(10000))

			var x int
			for ; indexes[x] < start; x++ {
			}

			err = na.ForEachAt(ctx, start, func(i uint64, v *cbg.Deferred) error {
				if i != indexes[x] {
					t.Fatal("got wrong index", i, indexes[x], x)
				}
				x++
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if x != len(indexes) {
				t.Fatal("didnt see enough values")
			}
		}
	})
}

func TestFirstSetIndex(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		bs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()

		vals := []uint64{0, 1, 5, 1 << uint64(defaultBitWidth), 1<<uint64(defaultBitWidth) + 1, 276, 1234, 62881923}
		for i, v := range vals {
			t.Log(i, v)
			a, err := NewAMT(bs, opts...)
			require.NoError(t, err)
			if err := a.Set(ctx, v, cborstr(fmt.Sprint(v))); err != nil {
				t.Fatal(err)
			}

			fsi, err := a.FirstSetIndex(ctx)
			if err != nil {
				t.Fatal(err)
			}

			if fsi != v {
				t.Fatal("got wrong index out", fsi, v)
			}

			rc, err := a.Flush(ctx)
			if err != nil {
				t.Fatal(err)
			}

			after, err := LoadAMT(ctx, bs, rc, opts...)
			if err != nil {
				t.Fatal(err)
			}

			fsi, err = after.FirstSetIndex(ctx)
			if err != nil {
				t.Fatal(err)
			}

			if fsi != v {
				t.Fatal("got wrong index out after serialization")
			}
			found, err := after.Delete(ctx, v)
			require.NoError(t, err)
			require.True(t, found)
			_, err = after.FirstSetIndex(ctx)
			require.Error(t, err)
		}
	})
}

func TestEmptyCIDStability(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		bs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()
		a, err := NewAMT(bs, opts...)
		require.NoError(t, err)

		c1, err := a.Flush(ctx)
		require.NoError(t, err)

		// iterating array should not affect its cid
		a.ForEach(ctx, func(k uint64, val *cbg.Deferred) error {
			return nil
		})

		c2, err := a.Flush(ctx)
		require.NoError(t, err)
		assert.Equal(t, c1, c2)

		// adding and removing an item should not affect its cid
		a.Set(ctx, 0, cborstr(""))
		a.Delete(ctx, 0)

		c3, err := a.Flush(ctx)
		require.NoError(t, err)
		assert.Equal(t, c1, c3)
	})
}

func TestBadBitfield(t *testing.T) {
	bs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()

	subnode, err := bs.Put(ctx, new(internal.Node))
	require.NoError(t, err)

	var root internal.Root
	root.Node.Bmap = []byte{0xff}
	root.Node.Links = append(root.Node.Links, subnode)
	root.Height = 10
	root.Count = 10
	c, err := bs.Put(ctx, &root)
	require.NoError(t, err)

	_, err = LoadAMT(ctx, bs, c)
	require.Error(t, err)
}

func TestFromArray(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		bs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()

		c, err := FromArray(ctx, bs, numbers, opts...)
		require.NoError(t, err)
		a, err := LoadAMT(ctx, bs, c, opts...)
		require.NoError(t, err)
		assertEquals(ctx, t, a, numbers)
		assertCount(t, a, 10)
	})
}

func TestForEachSkip(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		bs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()

		a, err := NewAMT(bs, opts...)
		require.NoError(t, err)
		require.NoError(t, a.Set(ctx, 0, cborstr("")))
		require.NoError(t, a.Set(ctx, 199, cborstr("")))
		require.NoError(t, a.Set(ctx, 201, cborstr("")))
		require.NoError(t, a.Set(ctx, 10000, cborstr("")))
		require.NoError(t, a.Set(ctx, 10001, cborstr("")))
		require.NoError(t, a.Set(ctx, 11001, cborstr("")))
		var keys []uint64
		require.NoError(t, a.ForEachAt(ctx, 200, func(i uint64, _ *cbg.Deferred) error {
			keys = append(keys, i)
			return nil
		}))
		require.Equal(t, []uint64{201, 10000, 10001, 11001}, keys)
	})
}

func TestBatch(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		bs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()
		a, err := NewAMT(bs, opts...)
		require.NoError(t, err)

		require.NoError(t, a.BatchSet(ctx, numbers))
		assertEquals(ctx, t, a, numbers)

		c, err := a.Flush(ctx)
		require.NoError(t, err)

		t.Run("with strict, error if a key is missing", func(t *testing.T) {
			clean, err := LoadAMT(ctx, bs, c, opts...)
			require.NoError(t, err)
			assertEquals(ctx, t, clean, numbers)

			_, err = clean.BatchDelete(ctx, []uint64{100}, true)
			require.Error(t, err)
		})
		t.Run("with strict, delete no keys ok", func(t *testing.T) {
			clean, err := LoadAMT(ctx, bs, c, opts...)
			require.NoError(t, err)
			assertEquals(ctx, t, clean, numbers)

			mod, err := clean.BatchDelete(ctx, []uint64{}, true)
			require.NoError(t, err)
			assert.False(t, mod)
			assertEquals(ctx, t, clean, numbers)
		})
		t.Run("with strict, delete some but not all keys ok", func(t *testing.T) {
			clean, err := LoadAMT(ctx, bs, c, opts...)
			require.NoError(t, err)
			assertEquals(ctx, t, clean, numbers)

			mod, err := clean.BatchDelete(ctx, []uint64{0, 1, 2, 3}, true)
			require.NoError(t, err)
			assert.True(t, mod)
			assertEquals(ctx, t, clean, numbers[4:])
		})
		t.Run("with strict, error to delete some keys with one missing", func(t *testing.T) {
			clean, err := LoadAMT(ctx, bs, c, opts...)
			require.NoError(t, err)
			assertEquals(ctx, t, clean, numbers)

			_, err = clean.BatchDelete(ctx, []uint64{0, 1, 2, 3, 100}, true)
			require.Error(t, err)
		})
		t.Run("with strict, delete all keys ok", func(t *testing.T) {
			clean, err := LoadAMT(ctx, bs, c, opts...)
			require.NoError(t, err)
			assertEquals(ctx, t, clean, numbers)

			mod, err := clean.BatchDelete(ctx, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, true)
			require.NoError(t, err)
			assert.True(t, mod)
			assertEquals(ctx, t, clean, []cbg.CBORMarshaler{})
		})
		t.Run("without strict, delete only absent keys ok", func(t *testing.T) {
			clean, err := LoadAMT(ctx, bs, c, opts...)
			require.NoError(t, err)
			assertEquals(ctx, t, clean, numbers)

			mod, err := clean.BatchDelete(ctx, []uint64{100, 101}, false)
			require.NoError(t, err)
			assert.False(t, mod)
			assertEquals(ctx, t, clean, numbers)
		})
		t.Run("without strict, delete some keys ok", func(t *testing.T) {
			clean, err := LoadAMT(ctx, bs, c, opts...)
			require.NoError(t, err)
			assertEquals(ctx, t, clean, numbers)

			mod, err := clean.BatchDelete(ctx, []uint64{0, 1, 2, 3, 100, 101}, false)
			require.NoError(t, err)
			assert.True(t, mod)
			assertEquals(ctx, t, clean, numbers[4:])
		})
		t.Run("without strict, delete all keys ok", func(t *testing.T) {
			clean, err := LoadAMT(ctx, bs, c, opts...)
			require.NoError(t, err)
			assertEquals(ctx, t, clean, numbers)

			mod, err := clean.BatchDelete(ctx, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, false)
			require.NoError(t, err)
			assert.True(t, mod)
			assertEquals(ctx, t, clean, []cbg.CBORMarshaler{})
		})
	})
}

func TestSetOrderIndependent(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		bs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()
		a, err := NewAMT(bs, opts...)
		require.NoError(t, err)

		nKeys := 8 * 8 * 8

		for i := 0; i < nKeys; i++ {
			assertSet(t, a, uint64(i), "foo")
		}

		c, err := a.Flush(ctx)
		require.NoError(t, err)

		vals := make([]int, 100)

		for i := range vals {
			vals[i] = rand.Intn(nKeys)
		}

		res := map[cid.Cid]struct{}{}
		for i := 0; i < 20; i++ {
			a, err := LoadAMT(ctx, bs, c, opts...)
			require.NoError(t, err)

			rand.Shuffle(len(vals), func(i, j int) {
				vals[i], vals[j] = vals[j], vals[i]
			})

			for _, k := range vals {
				assertSet(t, a, uint64(k), "foo2")
			}

			c, err := a.Flush(ctx)
			require.NoError(t, err)
			res[c] = struct{}{}
		}

		require.Len(t, res, 1)
	})
}

func TestDeleteOrderIndependent(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		bs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()
		a, err := NewAMT(bs, opts...)
		require.NoError(t, err)

		nKeys := 8 * 8 * 8 * 8

		for i := 0; i < nKeys; i++ {
			assertSet(t, a, uint64(i), "foo")
		}

		c, err := a.Flush(ctx)
		require.NoError(t, err)

		mvals := map[int]struct{}{}
		for i := 0; i < 100; i++ {
			mvals[rand.Intn(nKeys)] = struct{}{}
		}
		vals := make([]int, 0, len(mvals))
		for v := range mvals {
			vals = append(vals, v)
		}

		res := map[cid.Cid]struct{}{}
		for i := 0; i < 20; i++ {
			a, err := LoadAMT(ctx, bs, c, opts...)
			require.NoError(t, err)

			rand.Shuffle(len(vals), func(i, j int) {
				vals[i], vals[j] = vals[j], vals[i]
			})

			for _, k := range vals {
				assertDelete(t, a, uint64(k))
			}

			c, err := a.Flush(ctx)
			require.NoError(t, err)
			res[c] = struct{}{}
		}

		require.Len(t, res, 1)
	})
}

func assertEquals(ctx context.Context, t testing.TB, a *Root, expected []cbg.CBORMarshaler) {
	// Note: the AMT is not necessarily indexed from zero, so indexes may not align.
	expIndex := 0
	require.NoError(t, a.ForEach(ctx, func(amtIdx uint64, val *cbg.Deferred) error {
		var buf bytes.Buffer
		require.NoError(t, expected[expIndex].MarshalCBOR(&buf))
		require.Equal(t, val.Raw, buf.Bytes(), "AMT index %d, expectation index %d", amtIdx, expIndex)
		expIndex++
		return nil
	}))
	assertCount(t, a, uint64(len(expected)))
}
