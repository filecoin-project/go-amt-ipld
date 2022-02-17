package amt

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"testing"
	"time"

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
	prevBs := cbor.NewCborStore(newMockBlocks())
	curBs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()

	a, err := NewAMT(prevBs)
	assert.NoError(t, err)

	b, err := NewAMT(curBs)
	assert.NoError(t, err)

	diffAndAssertLength(ctx, t, prevBs, curBs, a, b, 0)

	assertSet(t, a, 2, "foo")
	assertSet(t, b, 2, "foo")

	diffAndAssertLength(ctx, t, prevBs, curBs, a, b, 0)
}

func TestSimpleAdd(t *testing.T) {
	prevBs := cbor.NewCborStore(newMockBlocks())
	curBs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()

	a, err := NewAMT(prevBs)
	assert.NoError(t, err)

	b, err := NewAMT(curBs)
	assert.NoError(t, err)

	assertSet(t, a, 2, "foo")
	assertGet(ctx, t, a, 2, "foo")
	assertCount(t, a, 1)

	assertSet(t, b, 2, "foo")
	assertSet(t, b, 5, "bar")

	assertGet(ctx, t, b, 2, "foo")
	assertGet(ctx, t, b, 5, "bar")
	assertCount(t, b, 2)

	ec := expectedChange{
		Type:   Add,
		Key:    5,
		Before: "",
		After:  "bar",
	}

	diffAndAssertLength(ctx, t, prevBs, curBs, a, b, 1, ec)
}

func TestDiffEmptyStateWithNonEmptyState(t *testing.T) {
	t.Run("Removed values", func(t *testing.T) {
		prevBs := cbor.NewCborStore(newMockBlocks())
		curBs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()

		prev, err := NewAMT(prevBs)
		assert.NoError(t, err)

		cur, err := NewAMT(curBs)
		assert.NoError(t, err)
		assertCount(t, cur, 0)

		assertSet(t, prev, 2, "foo")
		assertGet(ctx, t, prev, 2, "foo")
		assertCount(t, prev, 1)

		ec := expectedChange{
			Type:   Remove,
			Key:    2,
			Before: "foo",
			After:  "",
		}

		diffAndAssertLength(ctx, t, prevBs, curBs, prev, cur, 1, ec)
	})

	t.Run("Added values", func(t *testing.T) {
		prevBs := cbor.NewCborStore(newMockBlocks())
		curBs := cbor.NewCborStore(newMockBlocks())
		ctx := context.Background()

		prev, err := NewAMT(prevBs)
		assert.NoError(t, err)
		assertCount(t, prev, 0)

		cur, err := NewAMT(curBs)
		assert.NoError(t, err)

		assertSet(t, cur, 2, "foo")
		assertGet(ctx, t, cur, 2, "foo")
		assertCount(t, cur, 1)

		ec := expectedChange{
			Type:   Add,
			Key:    2,
			Before: "",
			After:  "foo",
		}

		diffAndAssertLength(ctx, t, prevBs, curBs, prev, cur, 1, ec)
	})
}

func TestSimpleRemove(t *testing.T) {
	prevBs := cbor.NewCborStore(newMockBlocks())
	curBs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()

	a, err := NewAMT(prevBs)
	assert.NoError(t, err)

	b, err := NewAMT(curBs)
	assert.NoError(t, err)

	assertSet(t, a, 2, "foo")
	assertSet(t, a, 5, "bar")

	assertGet(ctx, t, a, 2, "foo")
	assertGet(ctx, t, a, 5, "bar")
	assertCount(t, a, 2)

	assertSet(t, b, 2, "foo")
	assertGet(ctx, t, b, 2, "foo")

	ec := expectedChange{
		Type:   Remove,
		Key:    5,
		Before: "bar",
		After:  "",
	}

	diffAndAssertLength(ctx, t, prevBs, curBs, a, b, 1, ec)
}

func TestSimpleModify(t *testing.T) {
	prevBs := cbor.NewCborStore(newMockBlocks())
	curBs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()

	a, err := NewAMT(prevBs)
	assert.NoError(t, err)

	b, err := NewAMT(curBs)
	assert.NoError(t, err)

	assertSet(t, a, 2, "foo")
	assertSet(t, b, 2, "bar")

	ec := expectedChange{
		Type:   Modify,
		Key:    2,
		Before: "foo",
		After:  "bar",
	}

	diffAndAssertLength(ctx, t, prevBs, curBs, a, b, 1, ec)
}

func TestLargeModify(t *testing.T) {
	prevBs := cbor.NewCborStore(newMockBlocks())
	curBs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()

	a, err := NewAMT(prevBs)
	assert.NoError(t, err)

	b, err := NewAMT(curBs)
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

	diffAndAssertLength(ctx, t, prevBs, curBs, a, b, 100, ecs...)
}

func TestLargeAdditions(t *testing.T) {
	prevBs := cbor.NewCborStore(newMockBlocks())
	curBs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()

	a, err := NewAMT(prevBs)
	assert.NoError(t, err)

	b, err := NewAMT(curBs)
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

	diffAndAssertLength(ctx, t, prevBs, curBs, a, b, 500, ecs...)
}

func TestBigDiff(t *testing.T) {
	for multiplier := 1; multiplier < 7; multiplier++ {
		t.Run(fmt.Sprintf("Multiplier %d", multiplier), func(t *testing.T) {

			prevBs := cbor.NewCborStore(newMockBlocks(WithGetDelay(time.Millisecond * 1)))
			curBs := cbor.NewCborStore(newMockBlocks(WithGetDelay(time.Millisecond * 1)))
			ctx := context.Background()

			a, err := NewAMT(prevBs)
			assert.NoError(t, err)

			b, err := NewAMT(curBs)
			assert.NoError(t, err)

			for i := 0; i < 100*multiplier; i++ {
				assertSet(t, a, uint64(i), "foo"+strconv.Itoa(i))
			}

			ecs := make([]expectedChange, 0)

			// modify every other element, 50 modifies + 50 removes
			for i := 0; i < 100*multiplier; i += 2 {
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
			for i := 1000 * multiplier; i < 1500*multiplier; i++ {
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
			for i := 2000 * multiplier; i < 2500*multiplier; i++ {
				assertSet(t, b, uint64(i), "bar"+strconv.Itoa(i))

				ecs = append(ecs, expectedChange{
					Type:   Add,
					Key:    uint64(i),
					Before: "",
					After:  "bar" + strconv.Itoa(i),
				})
			}

			// 10000-10249 is removed, 250 removals
			for i := 10000 * multiplier; i < 10250*multiplier; i++ {
				assertSet(t, a, uint64(i), "foo"+strconv.Itoa(i))

				ecs = append(ecs, expectedChange{
					Type:   Remove,
					Key:    uint64(i),
					Before: "foo" + strconv.Itoa(i),
					After:  "",
				})
			}

			// 10250-10500 is modified, 250 modifies
			for i := 10250 * multiplier; i < 10500*multiplier; i++ {
				assertSet(t, a, uint64(i), "foo"+strconv.Itoa(i))
				assertSet(t, b, uint64(i), "bar"+strconv.Itoa(i))

				ecs = append(ecs, expectedChange{
					Type:   Modify,
					Key:    uint64(i),
					Before: "foo" + strconv.Itoa(i),
					After:  "bar" + strconv.Itoa(i),
				})
			}

			diffAndAssertLength(ctx, t, prevBs, curBs, a, b, 1600*multiplier, ecs...)
		})
	}
}

func diffAndAssertLength(ctx context.Context, t *testing.T, prevBs, curBs cbor.IpldStore, a *Root, b *Root, expectedLength int, ecs ...expectedChange) {
	aCid, err := a.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}

	bCid, err := b.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var serial time.Duration
	var parallel time.Duration
	t.Run("assert serial diff", func(t *testing.T) {
		start := time.Now()
		cs, err := Diff(ctx, prevBs, curBs, aCid, bCid)
		if err != nil {
			t.Fatalf("unexpected error from diff: %v", err)
		}
		serial = time.Since(start)

		if len(cs) != expectedLength {
			t.Fatalf("got %d changes, wanted %d", len(cs), expectedLength)
		}

		sort.Slice(cs, func(i, j int) bool {
			return cs[i].Key < cs[j].Key
		})

		for i := range cs {
			ecs[i].assertExpectation(t, cs[i])
		}
	})

	t.Run("assert parallel diff", func(t *testing.T) {
		start := time.Now()
		cs, err := ParallelDiff(ctx, prevBs, curBs, aCid, bCid)
		if err != nil {
			t.Fatalf("unexpected error from diff: %v", err)
		}
		parallel = time.Since(start)

		if len(cs) != expectedLength {
			t.Fatalf("got %d changes, wanted %d", len(cs), expectedLength)
		}

		sort.Slice(cs, func(i, j int) bool {
			return cs[i].Key < cs[j].Key
		})

		for i := range cs {
			ecs[i].assertExpectation(t, cs[i])
		}

	})
	t.Logf("Serial Diff took   %s\tsize\t%d\tbitwidth %d", serial.Round(time.Millisecond), expectedLength, a.bitWidth)
	t.Logf("Parallel Diff took %s\tsize\t%d\tbitwidth %d", parallel.Round(time.Millisecond), expectedLength, a.bitWidth)
}
