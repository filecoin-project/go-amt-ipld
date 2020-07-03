package amt

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	block "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	assert "github.com/stretchr/testify/assert"
	cbg "github.com/whyrusleeping/cbor-gen"
)

type mockBlocks struct {
	data map[cid.Cid]block.Block
}

func newMockBlocks() *mockBlocks {
	return &mockBlocks{make(map[cid.Cid]block.Block)}
}

func (mb *mockBlocks) Get(c cid.Cid) (block.Block, error) {
	d, ok := mb.data[c]
	if ok {
		return d, nil
	}
	return nil, fmt.Errorf("Not Found")
}

func (mb *mockBlocks) Put(b block.Block) error {
	mb.data[b.Cid()] = b
	return nil
}

func TestBasicSetGet(t *testing.T) {
	bs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()
	a := NewAMT(bs)

	assertSet(t, a, 2, "foo")
	assertGet(ctx, t, a, 2, "foo")
	assertCount(t, a, 1)

	c, err := a.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}

	clean, err := LoadAMT(ctx, bs, c)
	if err != nil {
		t.Fatal(err)
	}

	assertGet(ctx, t, clean, 2, "foo")

	assertCount(t, clean, 1)

}

func TestOutOfRange(t *testing.T) {
	ctx := context.Background()
	bs := cbor.NewCborStore(newMockBlocks())

	a := NewAMT(bs)

	err := a.Set(ctx, 1<<50, "what is up")
	if err == nil {
		t.Fatal("should have failed to set value out of range")
	}

	err = a.Set(ctx, MaxIndex, "what is up")
	if err == nil {
		t.Fatal("should have failed to set value out of range")
	}

	err = a.Set(ctx, MaxIndex-1, "what is up")
	if err != nil {
		t.Fatal(err)
	}
}

func assertDelete(t *testing.T, r *Root, i uint64) {
	ctx := context.Background()

	t.Helper()
	if err := r.Delete(ctx, i); err != nil {
		t.Fatal(err)
	}

	err := r.Get(ctx, i, nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	enf, ok := err.(*ErrNotFound)
	if !ok {
		t.Fatal("got wrong error: ", err)
	}

	_ = enf
	/* TODO: do the errors better so this passes
	if enf.Index != i {
		t.Fatal("got error not found with wrong index?", enf)
	}
	*/
}

func assertSet(t *testing.T, r *Root, i uint64, val string) {
	ctx := context.Background()

	t.Helper()
	if err := r.Set(ctx, i, val); err != nil {
		t.Fatal(err)
	}
}

func assertCount(t testing.TB, r *Root, c uint64) {
	t.Helper()
	if r.Count != c {
		t.Fatal("count is wrong")
	}
}

func assertGet(ctx context.Context, t testing.TB, r *Root, i uint64, val string) {

	t.Helper()

	var out string
	if err := r.Get(ctx, i, &out); err != nil {
		t.Fatal(err)
	}

	if out != val {
		t.Fatal("value we got out didnt match expectation")
	}
}

func TestExpand(t *testing.T) {
	bs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()
	a := NewAMT(bs)

	if err := a.Set(ctx, 2, "foo"); err != nil {
		t.Fatal(err)
	}

	if err := a.Set(ctx, 11, "bar"); err != nil {
		t.Fatal(err)
	}

	if err := a.Set(ctx, 79, "baz"); err != nil {
		t.Fatal(err)
	}

	assertGet(ctx, t, a, 2, "foo")
	assertGet(ctx, t, a, 11, "bar")
	assertGet(ctx, t, a, 79, "baz")

	c, err := a.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}

	na, err := LoadAMT(ctx, bs, c)
	if err != nil {
		t.Fatal(err)
	}

	assertGet(ctx, t, na, 2, "foo")
	assertGet(ctx, t, na, 11, "bar")
	assertGet(ctx, t, na, 79, "baz")
}

func TestInsertABunch(t *testing.T) {
	bs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()
	a := NewAMT(bs)

	num := uint64(5000)

	for i := uint64(0); i < num; i++ {
		if err := a.Set(ctx, i, "foo foo bar"); err != nil {
			t.Fatal(err)
		}
	}

	for i := uint64(0); i < num; i++ {
		assertGet(ctx, t, a, i, "foo foo bar")
	}

	c, err := a.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}

	na, err := LoadAMT(ctx, bs, c)
	if err != nil {
		t.Fatal(err)
	}

	for i := uint64(0); i < num; i++ {
		assertGet(ctx, t, na, i, "foo foo bar")
	}

	assertCount(t, na, num)
}

type op struct {
	del  bool
	idxs []uint64
}

func TestChaos(t *testing.T) {
	bs := cbor.NewCborStore(newMockBlocks())
	seed := time.Now().UnixNano()
	ctx := context.Background()
	//seed = 1579200312848358622 // FIXED
	//seed = 1579202116615474412
	//seed = 1579202774458659521
	// all above are with ops=100,maxIndx=2000
	r := rand.New(rand.NewSource(seed))
	t.Logf("seed: %d", seed)

	a := NewAMT(bs)
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
		a, err = LoadAMT(ctx, bs, c)
		assert.NoError(t, err)

		for _, index := range o.idxs {
			if !o.del {
				err := a.Set(ctx, index, "test")
				testMap[index] = struct{}{}
				assert.NoError(t, err)
			} else {
				err := a.Delete(ctx, index)
				delete(testMap, index)
				if err != nil {
					if _, ok := err.(*ErrNotFound); !ok {
						assert.NoError(t, err)
					}
				}
			}

		}

		fail := false
		correctLen := uint64(len(testMap))
		if correctLen != a.Count {
			t.Errorf("bad length before flush, correct: %d, Count: %d, i: %d", correctLen, a.Count, i)
			fail = true
		}

		c, err = a.Flush(ctx)
		assert.NoError(t, err)

		a, err = LoadAMT(ctx, bs, c)
		assert.NoError(t, err)
		if correctLen != a.Count {
			t.Errorf("bad length after flush, correct: %d, Count: %d, i: %d", correctLen, a.Count, i)
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
}

func TestInsertABunchWithDelete(t *testing.T) {
	bs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()
	a := NewAMT(bs)

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
			if err := a.Set(ctx, i, "foo foo bar"); err != nil {
				t.Fatal(err)
			}
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

	na, err := LoadAMT(ctx, bs, c)
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
	n2a, err := LoadAMT(ctx, bs, c)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("originSN: %d, removeSN: %d; expected: %d, actual len(n2a): %d",
		len(originSet), len(removeSet), len(originSet)-len(removeSet), n2a.Count)
	assertCount(t, n2a, uint64(len(originSet)-len(removeSet)))

	for i := uint64(0); i < uint64(num); i++ {
		if originSet[i] && !removeSet[i] {
			assertGet(ctx, t, n2a, i, "foo foo bar")
		}
	}
}

func TestDeleteFirstEntry(t *testing.T) {
	bs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()
	a := NewAMT(bs)

	assertSet(t, a, 0, "cat")
	assertSet(t, a, 27, "cat")

	assertDelete(t, a, 27)

	c, err := a.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}

	na, err := LoadAMT(ctx, bs, c)
	if err != nil {
		t.Fatal(err)
	}

	assertCount(t, na, 1)
}

func TestDelete(t *testing.T) {
	bs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()
	a := NewAMT(bs)

	// Check that deleting out of range of the current AMT fails as we expect it to
	err := a.Delete(ctx, 200)
	assert.EqualValues(t, &ErrNotFound{200}, err)

	assertSet(t, a, 0, "cat")
	assertSet(t, a, 1, "cat")
	assertSet(t, a, 2, "cat")
	assertSet(t, a, 3, "cat")

	assertDelete(t, a, 1)

	assertGet(ctx, t, a, 0, "cat")
	assertGet(ctx, t, a, 2, "cat")
	assertGet(ctx, t, a, 3, "cat")

	assertDelete(t, a, 0)
	fmt.Printf("%b\n", a.Node.Bmap[0])
	assertDelete(t, a, 2)
	fmt.Printf("%b\n", a.Node.Bmap[0])
	assertDelete(t, a, 3)
	fmt.Printf("%b\n", a.Node.Bmap[0])

	assertCount(t, a, 0)
	fmt.Println("trying deeper operations now")

	assertSet(t, a, 23, "dog")
	fmt.Printf("%b\n", a.Node.Bmap[0])
	assertSet(t, a, 24, "dog")
	fmt.Printf("%b\n", a.Node.Bmap[0])

	fmt.Println("FAILURE NEXT")
	assertDelete(t, a, 23)
	fmt.Printf("%b\n", a.Node.Bmap[0])

	assertCount(t, a, 1)

	c, err := a.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}

	na, err := LoadAMT(ctx, bs, c)
	if err != nil {
		t.Fatal(err)
	}

	assertCount(t, na, 1)

	a2 := NewAMT(bs)
	assertSet(t, a2, 24, "dog")

	a2c, err := a2.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if c != a2c {
		fmt.Printf("%#v\n", a)
		fmt.Printf("%#v\n", na)
		fmt.Printf("%#v\n", a2)
		t.Fatal("unexpected cid", c, a2c)
	}
}

func TestDeleteReduceHeight(t *testing.T) {
	bs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()
	a := NewAMT(bs)

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

	a2, err := LoadAMT(ctx, bs, c2)
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
}

func BenchmarkAMTInsertBulk(b *testing.B) {
	bs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()
	a := NewAMT(bs)

	for i := uint64(b.N); i > 0; i-- {
		if err := a.Set(ctx, i, "some value"); err != nil {
			b.Fatal(err)
		}
	}

	assertCount(b, a, uint64(b.N))
}

func BenchmarkAMTLoadAndInsert(b *testing.B) {
	bs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()
	a := NewAMT(bs)

	c, err := a.Flush(ctx)
	if err != nil {
		b.Fatal(err)
	}

	for i := uint64(b.N); i > 0; i-- {
		na, err := LoadAMT(ctx, bs, c)
		if err != nil {
			b.Fatal(err)
		}

		if err := na.Set(ctx, i, "some value"); err != nil {
			b.Fatal(err)
		}
		c, err = na.Flush(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestForEach(t *testing.T) {
	bs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()
	a := NewAMT(bs)

	r := rand.New(rand.NewSource(101))

	var indexes []uint64
	for i := 0; i < 10000; i++ {
		if r.Intn(2) == 0 {
			indexes = append(indexes, uint64(i))
		}
	}

	for _, i := range indexes {
		if err := a.Set(ctx, i, "value"); err != nil {
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

	na, err := LoadAMT(ctx, bs, c)
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
}

func TestForEachAt(t *testing.T) {
	bs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()
	a := NewAMT(bs)

	r := rand.New(rand.NewSource(101))

	var indexes []uint64
	for i := 0; i < 10000; i++ {
		if r.Intn(2) == 0 {
			indexes = append(indexes, uint64(i))
		}
	}

	for _, i := range indexes {
		if err := a.Set(ctx, i, fmt.Sprint(i)); err != nil {
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

	na, err := LoadAMT(ctx, bs, c)
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
}

func TestFirstSetIndex(t *testing.T) {
	bs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()

	vals := []uint64{0, 1, 5, width, width + 1, 276, 1234, 62881923}
	for _, v := range vals {
		a := NewAMT(bs)
		if err := a.Set(ctx, v, fmt.Sprint(v)); err != nil {
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

		after, err := LoadAMT(ctx, bs, rc)
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
	}

}

func TestForParRead(t *testing.T) {
	bs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()
	a := NewAMT(bs)

	r := rand.New(rand.NewSource(101))

	var indexes []uint64
	for i := 0; i < 10000; i++ {
		if r.Intn(2) == 0 {
			indexes = append(indexes, uint64(i))
		}
	}

	for _, i := range indexes {
		if err := a.Set(ctx, i, strconv.Itoa(int(i))); err != nil {
			t.Fatal(err)
		}
	}

	assertCount(t, a, uint64(len(indexes)))

	c, err := a.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}

	na, err := LoadAMT(ctx, bs, c)
	if err != nil {
		t.Fatal(err)
	}

	assertCount(t, na, uint64(len(indexes)))

	err = na.ParForEach(ctx, 1000, func(i uint64, v *cbg.Deferred) error {
		var val string
		if err := cbor.DecodeInto(v.Raw, &val); err != nil {
			return err
		}
		expected := strconv.Itoa(int(i))
		if expected != val {
			t.Fatal(fmt.Sprintf("read value error expected %s but got %s", expected, val))
		}
		time.Sleep(time.Millisecond * 100)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestForParReadErr(t *testing.T) {
	bs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()
	a := NewAMT(bs)

	for i := 10; i < 1000; i++ {
		if err := a.Set(ctx, uint64(i), strconv.Itoa(int(i))); err != nil {
			t.Fatal(err)
		}
	}

	c, err := a.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}

	na, err := LoadAMT(ctx, bs, c)
	if err != nil {
		t.Fatal(err)
	}

	err = na.ParForEach(ctx, 100, func(i uint64, v *cbg.Deferred) error {
		var val string
		if err := cbor.DecodeInto(v.Raw, &val); err != nil {
			return err
		}
		if i == 100 {
			return errors.New("expect erro")
		}
		expected := strconv.Itoa(int(i))
		if expected != val {
			t.Fatal(fmt.Sprintf("read value error expected %s but got %s", expected, val))
		}
		return nil
	})
	if err != nil && err.Error() != "expect erro" {
		log.Fatal("expect specific error but got %s", err)
	}
}
