package fuzzer

import (
	"context"
	"fmt"
	"math/rand"

	cbor "github.com/ipfs/go-ipld-cbor"
	cbg "github.com/whyrusleeping/cbor-gen"

	"github.com/filecoin-project/go-amt-ipld/v2"
)

type checkedAMT struct {
	amt  *amt.Root
	step uint64
	bs   cbor.IpldStore

	array    map[uint64]uint64
	keyCache []uint64
	seen     map[uint64]struct{}
}

func newCheckedAMT() *checkedAMT {
	bs := cbor.NewCborStore(newMockBlocks())
	return &checkedAMT{
		amt:   amt.NewAMT(bs),
		bs:    bs,
		array: make(map[uint64]uint64),
		seen:  make(map[uint64]struct{}),
	}
}

func (c *checkedAMT) randKey(key uint64) uint64 {
	if len(c.keyCache) == 0 {
		return key
	}
	return c.keyCache[key%uint64(len(c.keyCache))]
}

func (c *checkedAMT) cache(key uint64) {
	if _, ok := c.seen[key]; !ok {
		c.seen[key] = struct{}{}
		c.keyCache = append(c.keyCache, key)
	}
}

func (c *checkedAMT) setSeen(key, value uint64) {
	c.set(c.randKey(key), value)
}

func (c *checkedAMT) getSeen(key uint64) {
	c.get(c.randKey(key))
}

func (c *checkedAMT) deleteSeen(key uint64) {
	c.delete(c.randKey(key))
}

func (c *checkedAMT) set(key, value uint64) {
	c.trace("set %d", key)
	c.array[key] = value
	c.checkErr(c.amt.Set(context.Background(), key, value))
	c.cache(key)
}

func (c *checkedAMT) get(key uint64) {
	c.trace("get %d", key)
	expected, hasValue := c.array[key]
	var actual uint64
	err := c.amt.Get(context.Background(), key, &actual)
	if hasValue {
		c.checkErr(err)
		c.checkEq(expected, actual)
	} else {
		c.checkEq(key, err.(*amt.ErrNotFound).Index)
	}
	c.cache(key)
}

func (c *checkedAMT) delete(key uint64) {
	c.trace("delete %d", key)
	_, hasValue := c.array[key]
	delete(c.array, key)
	err := c.amt.Delete(context.Background(), key)
	if hasValue {
		c.checkErr(err)
	} else {
		c.checkEq(key, err.(*amt.ErrNotFound).Index)
	}
	c.cache(key)
}

func (c *checkedAMT) flush() {
	c.trace("flush")
	c1, err := c.amt.Flush(context.Background())
	c.checkErr(err)
	c2, err := c.amt.Flush(context.Background())
	c.checkErr(err)
	if c1 != c2 {
		c.fail("cids don't match %s != %s", c1, c2)
	}
	// Don't check the amt itself here, we'll check that at the end.
}

func (c *checkedAMT) reload() {
	c.trace("reload")
	cid, err := c.amt.Flush(context.Background())
	c.checkErr(err)
	c.amt, err = amt.LoadAMT(context.Background(), c.bs, cid)
	c.checkErr(err)
	// Don't check the amt itself here, we'll check that at the end.
}

func (c *checkedAMT) trace(msg string, args ...interface{}) {
	c.step++
	if Debug {
		fmt.Printf("step %d: "+msg+"\n", append([]interface{}{c.step}, args...)...)
	}
}

func (c *checkedAMT) check() {
	// Check in-memory state
	c.checkByIter(c.amt.Clone())
	c.checkByGet(c.amt.Clone())

	root, err := c.amt.Clone().Flush(context.Background())
	c.checkErr(err)

	// Now try loading
	{
		// Check by iterating
		array, err := amt.LoadAMT(context.Background(), c.bs, root)
		c.checkErr(err)
		c.checkByIter(array)
	}

	{
		// Check by random get
		array, err := amt.LoadAMT(context.Background(), c.bs, root)
		c.checkErr(err)
		c.checkByGet(array)
	}

	{
		// Check by reproducing.
		array := amt.NewAMT(c.bs)
		for i, j := range c.array {
			c.checkErr(array.Set(context.Background(), i, j))
		}
		newCid, err := array.Flush(context.Background())
		c.checkErr(err)
		if newCid != root {
			c.fail("expected to reconstruct identical AMT")
		}
	}

}

func (c *checkedAMT) checkErr(e error) {
	if e != nil {
		c.fail(e.Error())
	}
}

func (c *checkedAMT) checkEq(a, b uint64) {
	if a != b {
		c.fail("expected %d == %d", a, b)
	}
}

func (c *checkedAMT) checkByGet(array *amt.Root) {
	expectedKeys := make([]uint64, 0, len(c.array))
	for k := range c.array {
		expectedKeys = append(expectedKeys, k)
	}
	rand.Shuffle(len(expectedKeys), func(i, j int) {
		expectedKeys[i], expectedKeys[j] = expectedKeys[j], expectedKeys[i]
	})
	for _, k := range expectedKeys {
		var actual uint64
		c.checkErr(array.Get(context.Background(), k, &actual))
		c.checkEq(c.array[k], actual)
	}
}

func (c *checkedAMT) checkByIter(array *amt.Root) {
	toFind := make(map[uint64]uint64, len(c.array))
	for k, v := range c.array {
		toFind[k] = v
	}
	c.checkEq(uint64(len(c.array)), array.Len())
	c.checkErr(array.ForEach(context.Background(), func(k uint64, v *cbg.Deferred) error {
		expected, found := toFind[k]
		if !found {
			c.fail("unexpected key %d", k)
		}
		delete(toFind, k)
		var actual uint64
		c.checkErr(cbor.DecodeInto(v.Raw, &actual))
		c.checkEq(expected, actual)
		return nil
	}))
	if len(toFind) > 0 {
		c.fail("failed to find expected entries in AMT")
	}
}

func (c *checkedAMT) fail(msg string, args ...interface{}) {
	panic(fmt.Sprintf("step %d: "+msg, append([]interface{}{c.step}, args...)...))
}
