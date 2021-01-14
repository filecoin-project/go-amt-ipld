package amt

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"golang.org/x/xerrors"

	"github.com/ipfs/go-cid"

	cbor "github.com/ipfs/go-ipld-cbor"
	typegen "github.com/whyrusleeping/cbor-gen"
)

// ChangeType denotes type of change in Change
type ChangeType int

// These constants define the changes that can be applied to a DAG.
const (
	Add ChangeType = iota
	Remove
	Modify
)

// Change represents a change to a DAG and contains a reference to the old and
// new CIDs.
type Change struct {
	Type   ChangeType
	Key    uint64
	Before *typegen.Deferred
	After  *typegen.Deferred
}

func (ch Change) String() string {
	b, _ := json.Marshal(ch)
	return string(b)
}

// Diff returns a set of changes that transform node 'a' into node 'b'.
func Diff(ctx context.Context, prevBs, curBs cbor.IpldStore, prev, cur cid.Cid) ([]*Change, error) {
	prevAmt, err := LoadAMT(ctx, prevBs, prev)
	if err != nil {
		return nil, xerrors.Errorf("loading previous root: %w", err)
	}

	curAmt, err := LoadAMT(ctx, curBs, cur)
	if err != nil {
		return nil, xerrors.Errorf("loading current root: %w", err)
	}

	return diffNode(ctx, prevBs, curBs, &prevAmt.Node, &curAmt.Node, int(prevAmt.Height), int(curAmt.Height), 0)
}

func diffNode(ctx context.Context, prevBs, curBs cbor.IpldStore, prev, cur *Node, prevHeight, curHeight int, offset uint64) ([]*Change, error) {

	if prev == nil && cur == nil {
		return nil, nil
	}

	if prev == nil {
		return addAll(ctx, curBs, cur, curHeight, offset)
	}

	if cur == nil {
		return removeAll(ctx, prevBs, prev, prevHeight, offset)
	}

	if prevHeight == 0 && curHeight == 0 {
		return diffLeaves(prev, cur, offset)
	}

	changes := make([]*Change, 0)

	if curHeight > prevHeight {
		if cur.cache == nil {
			if err := cur.expandLinks(); err != nil {
				return nil, err
			}
		}

		subCount := nodesForHeight(curHeight)
		for i, v := range cur.expLinks {
			var sub Node
			if cur.cache[i] != nil {
				sub = *cur.cache[i]
			} else if v != cid.Undef {
				if err := curBs.Get(ctx, v, &sub); err != nil {
					return nil, err
				}
			} else {
				continue
			}

			offs := offset + (uint64(i) * subCount)

			if i == 0 {
				cs, err := diffNode(ctx, prevBs, curBs, prev, &sub, 0, curHeight-1, offs)
				if err != nil {
					return nil, err
				}

				changes = append(changes, cs...)
			} else {
				cs, err := addAll(ctx, curBs, &sub, curHeight-1, offs)
				if err != nil {
					return nil, err
				}

				changes = append(changes, cs...)
			}
		}

		return changes, nil
	}

	if prevHeight > curHeight {
		if prev.cache == nil {
			if err := prev.expandLinks(); err != nil {
				return nil, err
			}
		}

		subCount := nodesForHeight(prevHeight)
		for i, v := range prev.expLinks {
			var sub Node
			if prev.cache[i] != nil {
				sub = *prev.cache[i]
			} else if v != cid.Undef {
				if err := prevBs.Get(ctx, v, &sub); err != nil {
					return nil, err
				}
			} else {
				continue
			}

			offs := offset + (uint64(i) * subCount)

			if i == 0 {
				cs, err := diffNode(ctx, prevBs, curBs, &sub, cur, prevHeight-1, curHeight, offs)
				if err != nil {
					return nil, err
				}

				changes = append(changes, cs...)
			} else {
				cs, err := removeAll(ctx, prevBs, &sub, prevHeight-1, offs)
				if err != nil {
					return nil, err
				}

				changes = append(changes, cs...)
			}
		}

		return changes, nil
	}

	// sanity check
	if prevHeight != curHeight {
		return nil, fmt.Errorf("comparing non-leaf nodes of unequal heights (%d, %d)", prevHeight, curHeight)
	}

	if cur.cache == nil {
		if err := cur.expandLinks(); err != nil {
			return nil, err
		}
	}

	if prev.cache == nil {
		if err := prev.expandLinks(); err != nil {
			return nil, err
		}
	}

	subCount := nodesForHeight(prevHeight)
	for i := range prev.expLinks {
		var prevSub Node
		if prev.cache[i] != nil {
			prevSub = *prev.cache[i]
		} else if prev.expLinks[i] != cid.Undef {
			if err := prevBs.Get(ctx, prev.expLinks[i], &prevSub); err != nil {
				return nil, err
			}
		}

		var curSub Node
		if cur.cache[i] != nil {
			curSub = *cur.cache[i]
		} else if cur.expLinks[i] != cid.Undef {
			if err := curBs.Get(ctx, cur.expLinks[i], &curSub); err != nil {
				return nil, err
			}
		}

		offs := offset + (uint64(i) * subCount)

		cs, err := diffNode(ctx, prevBs, curBs, &prevSub, &curSub, prevHeight-1, curHeight-1, offs)
		if err != nil {
			return nil, err
		}

		changes = append(changes, cs...)
	}

	return changes, nil
}

func addAll(ctx context.Context, bs cbor.IpldStore, node *Node, height int, offset uint64) ([]*Change, error) {
	changes := make([]*Change, 0)
	err := node.forEachAt(ctx, bs, height, 0, offset, func(index uint64, deferred *typegen.Deferred) error {
		changes = append(changes, &Change{
			Type:   Add,
			Key:    index,
			Before: nil,
			After:  deferred,
		})

		return nil
	})

	if err != nil {
		return nil, err
	}

	return changes, nil
}

func removeAll(ctx context.Context, bs cbor.IpldStore, node *Node, height int, offset uint64) ([]*Change, error) {
	changes := make([]*Change, 0)

	err := node.forEachAt(ctx, bs, height, 0, offset, func(index uint64, deferred *typegen.Deferred) error {
		changes = append(changes, &Change{
			Type:   Remove,
			Key:    index,
			Before: deferred,
			After:  nil,
		})

		return nil
	})

	if err != nil {
		return nil, err
	}

	return changes, nil
}

func diffLeaves(prev, cur *Node, offset uint64) ([]*Change, error) {
	if err := prev.expandValues(); err != nil {
		return nil, err
	}

	if err := cur.expandValues(); err != nil {
		return nil, err
	}

	if len(prev.expVals) != len(cur.expVals) {
		return nil, fmt.Errorf("unexpected length of values %d and %d", len(prev.expVals), len(cur.expVals))
	}

	changes := make([]*Change, 0)
	for i, prevVal := range prev.expVals {
		index := offset + uint64(i)

		curVal := cur.expVals[i]
		if prevVal == nil && curVal == nil {
			continue
		}

		if prevVal == nil && curVal != nil {
			changes = append(changes, &Change{
				Type:   Add,
				Key:    index,
				Before: nil,
				After:  curVal,
			})

			continue
		}

		if prevVal != nil && curVal == nil {
			changes = append(changes, &Change{
				Type:   Remove,
				Key:    index,
				Before: prevVal,
				After:  nil,
			})

			continue
		}

		if !bytes.Equal(prevVal.Raw, curVal.Raw) {
			changes = append(changes, &Change{
				Type:   Modify,
				Key:    index,
				Before: prevVal,
				After:  curVal,
			})
		}
	}

	return changes, nil
}
