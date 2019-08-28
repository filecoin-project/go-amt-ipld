package amt

import (
	"testing"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-ipfs-blockstore"
)

func TestBasicSetGet(t *testing.T) {
	bs := &bstoreWrapper{blockstore.NewBlockstore(ds.NewMapDatastore())}

	a := NewAMT(bs)

	assertSet(t, a, 2, "foo")
	assertGet(t, a, 2, "foo")
	assertCount(t, a, 1)

	c, err := a.Flush()
	if err != nil {
		t.Fatal(err)
	}

	clean, err := LoadAMT(bs, c)
	if err != nil {
		t.Fatal(err)
	}

	assertGet(t, clean, 2, "foo")

	assertCount(t, clean, 1)
}

func assertSet(t *testing.T, r *Root, i uint64, val string) {
	t.Helper()
	if err := r.Set(i, val); err != nil {
		t.Fatal(err)
	}
}

func assertCount(t testing.TB, r *Root, c uint64) {
	t.Helper()
	if r.Count != c {
		t.Fatal("count is wrong")
	}
}

func assertGet(t testing.TB, r *Root, i uint64, val string) {
	t.Helper()

	var out string
	if err := r.Get(i, &out); err != nil {
		t.Fatal(err)
	}

	if out != val {
		t.Fatal("value we got out didnt match expectation")
	}
}

func TestExpand(t *testing.T) {
	bs := &bstoreWrapper{blockstore.NewBlockstore(ds.NewMapDatastore())}
	a := NewAMT(bs)

	if err := a.Set(2, "foo"); err != nil {
		t.Fatal(err)
	}

	if err := a.Set(11, "bar"); err != nil {
		t.Fatal(err)
	}

	if err := a.Set(79, "baz"); err != nil {
		t.Fatal(err)
	}

	assertGet(t, a, 2, "foo")
	assertGet(t, a, 11, "bar")
	assertGet(t, a, 79, "baz")

	c, err := a.Flush()
	if err != nil {
		t.Fatal(err)
	}

	na, err := LoadAMT(bs, c)
	if err != nil {
		t.Fatal(err)
	}

	assertGet(t, na, 2, "foo")
	assertGet(t, na, 11, "bar")
	assertGet(t, na, 79, "baz")
}

func TestInsertABunch(t *testing.T) {
	bs := &bstoreWrapper{blockstore.NewBlockstore(ds.NewMapDatastore())}
	a := NewAMT(bs)

	num := uint64(5000)

	for i := uint64(0); i < num; i++ {
		if err := a.Set(i, "foo foo bar"); err != nil {
			t.Fatal(err)
		}
	}

	for i := uint64(0); i < num; i++ {
		assertGet(t, a, i, "foo foo bar")
	}

	c, err := a.Flush()
	if err != nil {
		t.Fatal(err)
	}

	na, err := LoadAMT(bs, c)
	if err != nil {
		t.Fatal(err)
	}

	for i := uint64(0); i < num; i++ {
		assertGet(t, na, i, "foo foo bar")
	}

	assertCount(t, na, num)
}

func BenchmarkAMTInsertBulk(b *testing.B) {
	bs := &bstoreWrapper{blockstore.NewBlockstore(ds.NewMapDatastore())}
	a := NewAMT(bs)

	for i := uint64(b.N); i > 0; i-- {
		if err := a.Set(i, "some value"); err != nil {
			b.Fatal(err)
		}
	}

	assertCount(b, a, uint64(b.N))
}

func BenchmarkAMTLoadAndInsert(b *testing.B) {
	bs := &bstoreWrapper{blockstore.NewBlockstore(ds.NewMapDatastore())}
	a := NewAMT(bs)
	c, err := a.Flush()
	if err != nil {
		b.Fatal(err)
	}

	for i := uint64(b.N); i > 0; i-- {
		na, err := LoadAMT(bs, c)
		if err != nil {
			b.Fatal(err)
		}

		if err := na.Set(i, "some value"); err != nil {
			b.Fatal(err)
		}
		c, err = na.Flush()
		if err != nil {
			b.Fatal(err)
		}
	}
}
