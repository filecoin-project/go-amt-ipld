package amt

import (
	"context"
	"sort"
	"strconv"
	"testing"

	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/stretchr/testify/assert"
)

type expectedChange struct {
	Type   ChangeType
	Key    uint64
	Before string
	After  string
}

func (ec expectedChange) assertExpectation(t *testing.T, change *Change) {
	assert.Equal(t, ec.Type, change.Type)
	assert.Equal(t, ec.Key, change.Key)

	switch ec.Type {
	case Add:
		assert.Nilf(t, change.Before, "before val should be nil for Add")
		assert.NotNilf(t, change.After, "after val shouldn't be nil for Add")
		var afterVal CborByteArray
		cbor.DecodeInto(change.After.Raw, &afterVal)
		assert.Equal(t, cborstr(ec.After), &afterVal)
	case Remove:
		assert.NotNilf(t, change.Before, "before val shouldn't be nil for Remove")
		assert.Nilf(t, change.After, "after val should be nil for Remove")
		var beforeVal CborByteArray
		cbor.DecodeInto(change.Before.Raw, &beforeVal)
		assert.Equal(t, cborstr(ec.Before), &beforeVal)
	case Modify:
		assert.NotNilf(t, change.Before, "before val shouldn't be nil for Modify")
		assert.NotNilf(t, change.After, "after val shouldn't be nil for Modify")

		var beforeVal CborByteArray
		cbor.DecodeInto(change.Before.Raw, &beforeVal)
		assert.Equal(t, cborstr(ec.Before), &beforeVal)

		var afterVal CborByteArray
		cbor.DecodeInto(change.After.Raw, &afterVal)
		assert.Equal(t, cborstr(ec.After), &afterVal)
	}
}

func TestSimpleEquals(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		prevBs := cbor.NewCborStore(newMockBlocks())
		curBs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()

		a, err := NewAMT(prevBs, opts...)
		assert.NoError(t, err)

		b, err := NewAMT(curBs, opts...)
		assert.NoError(t, err)

		_ = diffAndAssertLength(ctx, t, prevBs, curBs, a, b, 0, opts)

		assertSet(t, a, 2, "foo")
		assertSet(t, b, 2, "foo")

		_ = diffAndAssertLength(ctx, t, prevBs, curBs, a, b, 0, opts)
	})
}

func TestSimpleAdd(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		prevBs := cbor.NewCborStore(newMockBlocks())
		curBs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()

		a, err := NewAMT(prevBs, opts...)
		assert.NoError(t, err)

		b, err := NewAMT(curBs, opts...)
		assert.NoError(t, err)

		assertSet(t, a, 2, "foo")
		assertGet(ctx, t, a, 2, "foo")
		assertCount(t, a, 1)

		assertSet(t, b, 2, "foo")
		assertSet(t, b, 5, "bar")

		assertGet(ctx, t, b, 2, "foo")
		assertGet(ctx, t, b, 5, "bar")
		assertCount(t, b, 2)

		cs := diffAndAssertLength(ctx, t, prevBs, curBs, a, b, 1, opts)

		ec := expectedChange{
			Type:   Add,
			Key:    5,
			Before: "",
			After:  "bar",
		}

		ec.assertExpectation(t, cs[0])
	})
}

func TestDiffEmptyStateWithNonEmptyState(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		t.Run("Removed values", func(t *testing.T) {
			prevBs := cbor.NewCborStore(newMockBlocks())
			curBs := cbor.NewCborStore(newMockBlocks())
			ctx := context.Background()

			prev, err := NewAMT(prevBs, opts...)
			assert.NoError(t, err)

			cur, err := NewAMT(curBs, opts...)
			assert.NoError(t, err)
			assertCount(t, cur, 0)

			assertSet(t, prev, 2, "foo")
			assertGet(ctx, t, prev, 2, "foo")
			assertCount(t, prev, 1)

			cs := diffAndAssertLength(ctx, t, prevBs, curBs, prev, cur, 1, opts)

			ec := expectedChange{
				Type:   Remove,
				Key:    2,
				Before: "foo",
				After:  "",
			}

			ec.assertExpectation(t, cs[0])
		})

		t.Run("Added values", func(t *testing.T) {
			prevBs := cbor.NewCborStore(newMockBlocks())
			curBs := cbor.NewCborStore(newMockBlocks())
			ctx := context.Background()

			prev, err := NewAMT(prevBs, opts...)
			assert.NoError(t, err)
			assertCount(t, prev, 0)

			cur, err := NewAMT(curBs, opts...)
			assert.NoError(t, err)

			assertSet(t, cur, 2, "foo")
			assertGet(ctx, t, cur, 2, "foo")
			assertCount(t, cur, 1)

			cs := diffAndAssertLength(ctx, t, prevBs, curBs, prev, cur, 1, opts)

			ec := expectedChange{
				Type:   Add,
				Key:    2,
				Before: "",
				After:  "foo",
			}

			ec.assertExpectation(t, cs[0])
		})
	})
}

func TestSimpleRemove(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		prevBs := cbor.NewCborStore(newMockBlocks())
		curBs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()

		a, err := NewAMT(prevBs, opts...)
		assert.NoError(t, err)

		b, err := NewAMT(curBs, opts...)
		assert.NoError(t, err)

		assertSet(t, a, 2, "foo")
		assertSet(t, a, 5, "bar")

		assertGet(ctx, t, a, 2, "foo")
		assertGet(ctx, t, a, 5, "bar")
		assertCount(t, a, 2)

		assertSet(t, b, 2, "foo")
		assertGet(ctx, t, b, 2, "foo")

		cs := diffAndAssertLength(ctx, t, prevBs, curBs, a, b, 1, opts)

		ec := expectedChange{
			Type:   Remove,
			Key:    5,
			Before: "bar",
			After:  "",
		}

		ec.assertExpectation(t, cs[0])
	})
}

func TestSimpleModify(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		prevBs := cbor.NewCborStore(newMockBlocks())
		curBs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()

		a, err := NewAMT(prevBs, opts...)
		assert.NoError(t, err)

		b, err := NewAMT(curBs, opts...)
		assert.NoError(t, err)

		assertSet(t, a, 2, "foo")
		assertSet(t, b, 2, "bar")

		cs := diffAndAssertLength(ctx, t, prevBs, curBs, a, b, 1, opts)

		ec := expectedChange{
			Type:   Modify,
			Key:    2,
			Before: "foo",
			After:  "bar",
		}

		ec.assertExpectation(t, cs[0])
	})
}

func TestLargeModify(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		prevBs := cbor.NewCborStore(newMockBlocks())
		curBs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()

		a, err := NewAMT(prevBs, opts...)
		assert.NoError(t, err)

		b, err := NewAMT(curBs, opts...)
		assert.NoError(t, err)

		for i := 0; i < 100; i++ {
			assertSet(t, a, uint64(i), "foo"+strconv.Itoa(i))
		}

		ecs := make([]expectedChange, 0)

		// modify every other element, 50 modifies + 50 removes
		for i := 0; i < 100; i += 2 {
			assertSet(t, b, uint64(i), "bar"+strconv.Itoa(i))

			ecs = append(ecs, expectedChange{
				Type:   Modify,
				Key:    uint64(i),
				Before: "foo" + strconv.Itoa(i),
				After:  "bar" + strconv.Itoa(i),
			})

			ecs = append(ecs, expectedChange{
				Type:   Remove,
				Key:    uint64(i + 1),
				Before: "foo" + strconv.Itoa(i+1),
				After:  "",
			})
		}

		cs := diffAndAssertLength(ctx, t, prevBs, curBs, a, b, 100, opts)

		sort.Slice(cs, func(i, j int) bool {
			return cs[i].Key < cs[j].Key
		})

		for i := range cs {
			ecs[i].assertExpectation(t, cs[i])
		}
	})
}

func TestLargeAdditions(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		prevBs := cbor.NewCborStore(newMockBlocks())
		curBs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()

		a, err := NewAMT(prevBs, opts...)
		assert.NoError(t, err)

		b, err := NewAMT(curBs, opts...)
		assert.NoError(t, err)

		for i := 0; i < 100; i++ {
			assertSet(t, a, uint64(i), "foo"+strconv.Itoa(i))
			assertSet(t, b, uint64(i), "foo"+strconv.Itoa(i))
		}

		ecs := make([]expectedChange, 0)

		// new additions, 500 additions
		for i := 2000; i < 2500; i++ {
			assertSet(t, b, uint64(i), "bar"+strconv.Itoa(i))

			ecs = append(ecs, expectedChange{
				Type:   Add,
				Key:    uint64(i),
				Before: "",
				After:  "bar" + strconv.Itoa(i),
			})
		}

		cs := diffAndAssertLength(ctx, t, prevBs, curBs, a, b, 500, opts)

		sort.Slice(cs, func(i, j int) bool {
			return cs[i].Key < cs[j].Key
		})

		for i := range cs {
			ecs[i].assertExpectation(t, cs[i])
		}
	})
}

func TestBigDiff(t *testing.T) {
	runTestWithBitWidths(t, bitWidths2to18, func(t *testing.T, opts ...Option) {
		prevBs := cbor.NewCborStore(newMockBlocks())
		curBs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()

		a, err := NewAMT(prevBs, opts...)
		assert.NoError(t, err)

		b, err := NewAMT(curBs, opts...)
		assert.NoError(t, err)

		for i := 0; i < 100; i++ {
			assertSet(t, a, uint64(i), "foo"+strconv.Itoa(i))
		}

		ecs := make([]expectedChange, 0)

		// modify every other element, 50 modifies + 50 removes
		for i := 0; i < 100; i += 2 {
			assertSet(t, b, uint64(i), "bar"+strconv.Itoa(i))

			ecs = append(ecs, expectedChange{
				Type:   Modify,
				Key:    uint64(i),
				Before: "foo" + strconv.Itoa(i),
				After:  "bar" + strconv.Itoa(i),
			})

			ecs = append(ecs, expectedChange{
				Type:   Remove,
				Key:    uint64(i + 1),
				Before: "foo" + strconv.Itoa(i+1),
				After:  "",
			})
		}

		// modify every element between 1000 and 1500, 500 modifies
		for i := 1000; i < 1500; i++ {
			assertSet(t, a, uint64(i), "foo"+strconv.Itoa(i))
			assertSet(t, b, uint64(i), "bar"+strconv.Itoa(i))

			ecs = append(ecs, expectedChange{
				Type:   Modify,
				Key:    uint64(i),
				Before: "foo" + strconv.Itoa(i),
				After:  "bar" + strconv.Itoa(i),
			})
		}

		// new additions, 500 additions
		for i := 2000; i < 2500; i++ {
			assertSet(t, b, uint64(i), "bar"+strconv.Itoa(i))

			ecs = append(ecs, expectedChange{
				Type:   Add,
				Key:    uint64(i),
				Before: "",
				After:  "bar" + strconv.Itoa(i),
			})
		}

		// 10000-10249 is removed, 250 removals
		for i := 10000; i < 10250; i++ {
			assertSet(t, a, uint64(i), "foo"+strconv.Itoa(i))

			ecs = append(ecs, expectedChange{
				Type:   Remove,
				Key:    uint64(i),
				Before: "foo" + strconv.Itoa(i),
				After:  "",
			})
		}

		// 10250-10500 is modified, 250 modifies
		for i := 10250; i < 10500; i++ {
			assertSet(t, a, uint64(i), "foo"+strconv.Itoa(i))
			assertSet(t, b, uint64(i), "bar"+strconv.Itoa(i))

			ecs = append(ecs, expectedChange{
				Type:   Modify,
				Key:    uint64(i),
				Before: "foo" + strconv.Itoa(i),
				After:  "bar" + strconv.Itoa(i),
			})
		}

		cs := diffAndAssertLength(ctx, t, prevBs, curBs, a, b, 1600, opts)

		sort.Slice(cs, func(i, j int) bool {
			return cs[i].Key < cs[j].Key
		})

		for i := range cs {
			ecs[i].assertExpectation(t, cs[i])
		}
	})
}

func diffAndAssertLength(ctx context.Context, t *testing.T, prevBs, curBs cbor.IpldStore, a *Root, b *Root, expectedLength int, opts []Option) []*Change {
	aCid, err := a.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}

	bCid, err := b.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}

	cs, err := Diff(ctx, prevBs, curBs, aCid, bCid, opts...)
	if err != nil {
		t.Fatalf("unexpected error from diff: %v", err)
	}

	if len(cs) != expectedLength {
		t.Fatalf("got %d changes, wanted %d", len(cs), expectedLength)
	}

	return cs
}
