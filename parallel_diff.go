package amt

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	logging "github.com/ipfs/go-log/v2"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

var log = logging.Logger("amt")

func ParallelDiff(ctx context.Context, prevBs, curBs cbor.IpldStore, prev, cur cid.Cid, opts ...Option) ([]*Change, error) {
	start := time.Now()
	prevAmt, err := LoadAMT(ctx, prevBs, prev, opts...)
	if err != nil {
		return nil, xerrors.Errorf("loading previous root: %w", err)
	}

	prevCtx := &nodeContext{
		bs:       prevBs,
		bitWidth: prevAmt.bitWidth,
		height:   prevAmt.height,
	}

	curAmt, err := LoadAMT(ctx, curBs, cur, opts...)
	if err != nil {
		return nil, xerrors.Errorf("loading current root: %w", err)
	}

	// TODO: remove when https://github.com/filecoin-project/go-amt-ipld/issues/54 is closed.
	if curAmt.bitWidth != prevAmt.bitWidth {
		return nil, xerrors.Errorf("diffing AMTs with differing bitWidths not supported (prev=%d, cur=%d)", prevAmt.bitWidth, curAmt.bitWidth)
	}

	curCtx := &nodeContext{
		bs:       curBs,
		bitWidth: curAmt.bitWidth,
		height:   curAmt.height,
	}

	// edge case of diffing an empty AMT against non-empty
	if prevAmt.count == 0 && curAmt.count != 0 {
		return addAll(ctx, curCtx, curAmt.node, 0)
	}
	if prevAmt.count != 0 && curAmt.count == 0 {
		return removeAll(ctx, prevCtx, prevAmt.node, 0)
	}
	grp, ctx := errgroup.WithContext(ctx)
	out := make(chan *Change)
	gets := int64(0) // updated atomically
	parallelDiffNode(ctx, prevCtx, curCtx, prevAmt.node, curAmt.node, 0, grp, out, &gets)

	var changes []*Change
	done := make(chan struct{}, 1)
	go func() {
		for change := range out {
			changes = append(changes, change)
		}
		done <- struct{}{}
	}()

	if err := grp.Wait(); err != nil {
		close(out)
		return nil, err
	}
	close(out)
	<-done
	log.Infow("parallel diff", "duration", time.Since(start), "gets", gets)

	return changes, nil
}

func parallelDiffNode(ctx context.Context, prevCtx, curCtx *nodeContext, prev, cur *node, offset uint64, grp *errgroup.Group, outCh chan *Change, gets *int64) {
	grp.Go(func() error {
		if prev == nil && cur == nil {
			return nil
		}

		if prev == nil {
			return parallelAddAll(ctx, curCtx, cur, offset, outCh)
		}

		if cur == nil {
			return parallelRemoveAll(ctx, prevCtx, prev, offset, outCh)
		}

		if prevCtx.height == 0 && curCtx.height == 0 {
			return parallelDiffLeaves(prev, cur, offset, outCh)
		}

		if curCtx.height > prevCtx.height {
			subCount := curCtx.nodesAtHeight()
			for i, ln := range cur.links {
				if ln == nil || ln.cid == cid.Undef {
					continue
				}

				subCtx := &nodeContext{
					bs:       curCtx.bs,
					bitWidth: curCtx.bitWidth,
					height:   curCtx.height - 1,
				}

				atomic.AddInt64(gets, 1)
				subn, err := ln.load(ctx, subCtx.bs, subCtx.bitWidth, subCtx.height)
				if err != nil {
					return err
				}

				offs := offset + (uint64(i) * subCount)
				if i == 0 {
					parallelDiffNode(ctx, prevCtx, subCtx, prev, subn, offs, grp, outCh, gets)
				} else {
					err := parallelAddAll(ctx, subCtx, subn, offs, outCh)
					if err != nil {
						return err
					}
				}
			}

			return nil
		}

		if prevCtx.height > curCtx.height {
			subCount := prevCtx.nodesAtHeight()
			for i, ln := range prev.links {
				if ln == nil || ln.cid == cid.Undef {
					continue
				}

				subCtx := &nodeContext{
					bs:       prevCtx.bs,
					bitWidth: prevCtx.bitWidth,
					height:   prevCtx.height - 1,
				}

				atomic.AddInt64(gets, 1)
				subn, err := ln.load(ctx, subCtx.bs, subCtx.bitWidth, subCtx.height)
				if err != nil {
					return err
				}

				offs := offset + (uint64(i) * subCount)

				if i == 0 {
					parallelDiffNode(ctx, subCtx, curCtx, subn, cur, offs, grp, outCh, gets)
				} else {
					err := parallelRemoveAll(ctx, subCtx, subn, offs, outCh)
					if err != nil {
						return err
					}
				}
			}

			return nil
		}

		// sanity check
		if prevCtx.height != curCtx.height {
			return fmt.Errorf("comparing non-leaf nodes of unequal heights (%d, %d)", prevCtx.height, curCtx.height)
		}

		if len(prev.links) != len(cur.links) {
			return fmt.Errorf("nodes have different numbers of links (prev=%d, cur=%d)", len(prev.links), len(cur.links))
		}

		if prev.links == nil || cur.links == nil {
			return fmt.Errorf("nodes have no links")
		}

		subCount := prevCtx.nodesAtHeight()
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

				subCtx := &nodeContext{
					bs:       prevCtx.bs,
					bitWidth: prevCtx.bitWidth,
					height:   prevCtx.height - 1,
				}

				atomic.AddInt64(gets, 1)
				subn, err := prev.links[i].load(ctx, subCtx.bs, subCtx.bitWidth, subCtx.height)
				if err != nil {
					return err
				}

				offs := offset + (uint64(i) * subCount)
				err = parallelRemoveAll(ctx, subCtx, subn, offs, outCh)
				if err != nil {
					return err
				}
				continue
			}

			// Current has link, previous did not
			if prev.links[i] == nil && cur.links[i] != nil {
				if cur.links[i].cid == cid.Undef {
					continue
				}

				subCtx := &nodeContext{
					bs:       curCtx.bs,
					bitWidth: curCtx.bitWidth,
					height:   curCtx.height - 1,
				}

				atomic.AddInt64(gets, 1)
				subn, err := cur.links[i].load(ctx, subCtx.bs, subCtx.bitWidth, subCtx.height)
				if err != nil {
					return err
				}

				offs := offset + (uint64(i) * subCount)
				err = parallelAddAll(ctx, subCtx, subn, offs, outCh)
				if err != nil {
					return err
				}
				continue
			}

			// Both previous and current have links to diff

			prevSubCtx := &nodeContext{
				bs:       prevCtx.bs,
				bitWidth: prevCtx.bitWidth,
				height:   prevCtx.height - 1,
			}

			atomic.AddInt64(gets, 1)
			prevSubn, err := prev.links[i].load(ctx, prevSubCtx.bs, prevSubCtx.bitWidth, prevSubCtx.height)
			if err != nil {
				return err
			}

			curSubCtx := &nodeContext{
				bs:       curCtx.bs,
				bitWidth: curCtx.bitWidth,
				height:   curCtx.height - 1,
			}

			atomic.AddInt64(gets, 1)
			curSubn, err := cur.links[i].load(ctx, curSubCtx.bs, curSubCtx.bitWidth, curSubCtx.height)
			if err != nil {
				return err
			}

			offs := offset + (uint64(i) * subCount)

			parallelDiffNode(ctx, prevSubCtx, curSubCtx, prevSubn, curSubn, offs, grp, outCh, gets)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func parallelAddAll(ctx context.Context, nc *nodeContext, node *node, offset uint64, out chan *Change) error {
	err := node.forEachAt(ctx, nc.bs, nc.bitWidth, nc.height, 0, offset, func(index uint64, deferred *cbg.Deferred) error {
		out <- &Change{
			Type:   Add,
			Key:    index,
			Before: nil,
			After:  deferred,
		}
		return nil
	})
	return err
}
func parallelRemoveAll(ctx context.Context, nc *nodeContext, node *node, offset uint64, out chan *Change) error {
	err := node.forEachAt(ctx, nc.bs, nc.bitWidth, nc.height, 0, offset, func(index uint64, deferred *cbg.Deferred) error {
		out <- &Change{
			Type:   Remove,
			Key:    index,
			Before: deferred,
			After:  nil,
		}
		return nil
	})
	return err
}

func parallelDiffLeaves(prev, cur *node, offset uint64, out chan *Change) error {
	if len(prev.values) != len(cur.values) {
		return fmt.Errorf("node leaves have different numbers of values (prev=%d, cur=%d)", len(prev.values), len(cur.values))
	}

	for i, prevVal := range prev.values {
		index := offset + uint64(i)

		curVal := cur.values[i]
		if prevVal == nil && curVal == nil {
			continue
		}

		if prevVal == nil && curVal != nil {
			out <- &Change{
				Type:   Add,
				Key:    index,
				Before: nil,
				After:  curVal,
			}
			continue
		}

		if prevVal != nil && curVal == nil {
			out <- &Change{
				Type:   Remove,
				Key:    index,
				Before: prevVal,
				After:  nil,
			}
			continue
		}

		if !bytes.Equal(prevVal.Raw, curVal.Raw) {
			out <- &Change{
				Type:   Modify,
				Key:    index,
				Before: prevVal,
				After:  curVal,
			}
		}

	}
	return nil
}
