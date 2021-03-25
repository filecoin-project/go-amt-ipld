package amt

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"golang.org/x/xerrors"

	"github.com/ipfs/go-cid"

	cbor "github.com/ipfs/go-ipld-cbor"
	cbg "github.com/whyrusleeping/cbor-gen"
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
	Before *cbg.Deferred
	After  *cbg.Deferred
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

	return diffNode(ctx, prevBs, curBs, prevAmt.node, curAmt.node, int(prevAmt.height), int(curAmt.height), prevAmt.bitWidth, curAmt.bitWidth, 0)
}

func diffNode(ctx context.Context, prevBs, curBs cbor.IpldStore, prev, cur *node, prevHeight, curHeight int, prevBitWidth, curBitWidth uint, offset uint64) ([]*Change, error) {
	if prev == nil && cur == nil {
		return nil, nil
	}

	if prev == nil {
		return addAll(ctx, curBs, cur, curBitWidth, curHeight, offset)
	}

	if cur == nil {
		return removeAll(ctx, prevBs, prev, prevBitWidth, prevHeight, offset)
	}

	if prevHeight == 0 && curHeight == 0 {
		return diffLeaves(prev, cur, offset)
	}

	changes := make([]*Change, 0)

	if curHeight > prevHeight {
		subCount := nodesForHeight(curBitWidth, curHeight)
		for i, ln := range cur.links {
			if ln == nil || ln.cid == cid.Undef {
				continue
			}

			subn, err := ln.load(ctx, curBs, curBitWidth, curHeight-1)
			if err != nil {
				return nil, err
			}

			offs := offset + (uint64(i) * subCount)
			if i == 0 {
				cs, err := diffNode(ctx, prevBs, curBs, prev, subn, prevHeight, curHeight-1, prevBitWidth, curBitWidth, offs)
				if err != nil {
					return nil, err
				}

				changes = append(changes, cs...)
			} else {
				cs, err := addAll(ctx, curBs, subn, curBitWidth, curHeight-1, offs)
				if err != nil {
					return nil, err
				}

				changes = append(changes, cs...)
			}
		}

		return changes, nil
	}

	if prevHeight > curHeight {
		subCount := nodesForHeight(prevBitWidth, prevHeight)
		for i, ln := range prev.links {
			if ln == nil || ln.cid == cid.Undef {
				continue
			}

			subn, err := ln.load(ctx, prevBs, prevBitWidth, prevHeight-1)
			if err != nil {
				return nil, err
			}

			offs := offset + (uint64(i) * subCount)

			if i == 0 {
				cs, err := diffNode(ctx, prevBs, curBs, subn, cur, prevHeight-1, curHeight, prevBitWidth, curBitWidth, offs)
				if err != nil {
					return nil, err
				}

				changes = append(changes, cs...)
			} else {
				cs, err := removeAll(ctx, prevBs, subn, prevBitWidth, prevHeight-1, offs)
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

	if len(prev.links) != len(cur.links) {
		return nil, fmt.Errorf("nodes have different numbers of links (prev=%d, cur=%d)", len(prev.links), len(cur.links))
	}

	if prev.links == nil || cur.links == nil {
		return nil, fmt.Errorf("nodes have no links")
	}

	subCount := nodesForHeight(prevBitWidth, prevHeight)
	for i := range prev.links {
		// Neither previous or current links are in use
		if prev.links[i] == nil && cur.links[i] == nil {
			continue
		}

		// Previous had link, current did not
		if prev.links[i] != nil && cur.links[i] == nil {
			if prev.links[i].cid == cid.Undef {
				continue
			}

			subn, err := prev.links[i].load(ctx, prevBs, prevBitWidth, prevHeight-1)
			if err != nil {
				return nil, err
			}

			offs := offset + (uint64(i) * subCount)
			cs, err := removeAll(ctx, prevBs, subn, prevBitWidth, prevHeight-1, offs)
			if err != nil {
				return nil, err
			}

			changes = append(changes, cs...)

			continue
		}

		// Current has link, previous did not
		if prev.links[i] == nil && cur.links[i] != nil {
			if cur.links[i].cid == cid.Undef {
				continue
			}
			subn, err := cur.links[i].load(ctx, curBs, curBitWidth, curHeight-1)
			if err != nil {
				return nil, err
			}

			offs := offset + (uint64(i) * subCount)
			cs, err := addAll(ctx, curBs, subn, curBitWidth, curHeight-1, offs)
			if err != nil {
				return nil, err
			}

			changes = append(changes, cs...)

			continue
		}

		// Both previous and current have links to diff

		prevSubn, err := prev.links[i].load(ctx, prevBs, prevBitWidth, prevHeight-1)
		if err != nil {
			return nil, err
		}

		curSubn, err := cur.links[i].load(ctx, curBs, curBitWidth, curHeight-1)
		if err != nil {
			return nil, err
		}

		offs := offset + (uint64(i) * subCount)

		cs, err := diffNode(ctx, prevBs, curBs, prevSubn, curSubn, prevHeight-1, curHeight-1, prevBitWidth, curBitWidth, offs)
		if err != nil {
			return nil, err
		}

		changes = append(changes, cs...)
	}

	return changes, nil
}

func addAll(ctx context.Context, bs cbor.IpldStore, node *node, bitWidth uint, height int, offset uint64) ([]*Change, error) {
	changes := make([]*Change, 0)
	err := node.forEachAt(ctx, bs, bitWidth, height, 0, offset, func(index uint64, deferred *cbg.Deferred) error {
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

func removeAll(ctx context.Context, bs cbor.IpldStore, node *node, bitWidth uint, height int, offset uint64) ([]*Change, error) {
	changes := make([]*Change, 0)

	err := node.forEachAt(ctx, bs, bitWidth, height, 0, offset, func(index uint64, deferred *cbg.Deferred) error {
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

func diffLeaves(prev, cur *node, offset uint64) ([]*Change, error) {
	if len(prev.values) != len(cur.values) {
		return nil, fmt.Errorf("node leaves have different numbers of values (prev=%d, cur=%d)", len(prev.values), len(cur.values))
	}

	changes := make([]*Change, 0)
	for i, prevVal := range prev.values {
		index := offset + uint64(i)

		curVal := cur.values[i]
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
