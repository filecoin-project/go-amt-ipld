package amt

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

func ParallelDiff(ctx context.Context, prevBs, curBs cbor.IpldStore, prev, cur cid.Cid, workers int64, opts ...Option) ([]*Change, error) {
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
	out := make(chan *Change)
	differ, ctx := newDiffScheduler(ctx, workers, &task{
		prevCtx: prevCtx,
		curCtx:  curCtx,
		prev:    prevAmt.node,
		cur:     curAmt.node,
		offset:  0,
	})
	differ.startScheduler(ctx)
	differ.startWorkers(ctx, out)

	var changes []*Change
	done := make(chan struct{})
	go func() {
		for change := range out {
			changes = append(changes, change)
		}
		close(done)
	}()

	err = differ.grp.Wait()
	close(out)
	<-done

	return changes, err
}

func parallelAddAll(ctx context.Context, nc *nodeContext, node *node, offset uint64, out chan *Change) error {
	return node.forEachAt(ctx, nc.bs, nc.bitWidth, nc.height, 0, offset, func(index uint64, deferred *cbg.Deferred) error {
		out <- &Change{
			Type:   Add,
			Key:    index,
			Before: nil,
			After:  deferred,
		}
		return nil
	})
}
func parallelRemoveAll(ctx context.Context, nc *nodeContext, node *node, offset uint64, out chan *Change) error {
	return node.forEachAt(ctx, nc.bs, nc.bitWidth, nc.height, 0, offset, func(index uint64, deferred *cbg.Deferred) error {
		out <- &Change{
			Type:   Remove,
			Key:    index,
			Before: deferred,
			After:  nil,
		}
		return nil
	})
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

type task struct {
	prevCtx, curCtx *nodeContext
	prev, cur       *node
	offset          uint64
}

func newDiffScheduler(ctx context.Context, numWorkers int64, rootTasks ...*task) (*diffScheduler, context.Context) {
	grp, ctx := errgroup.WithContext(ctx)
	s := &diffScheduler{
		numWorkers: numWorkers,
		stack:      rootTasks,
		in:         make(chan *task, numWorkers),
		out:        make(chan *task, numWorkers),
		grp:        grp,
	}
	s.taskWg.Add(len(rootTasks))
	return s, ctx
}

type diffScheduler struct {
	// number of worker routine to spawn
	numWorkers int64
	// buffer holds tasks until they are processed
	stack []*task
	// inbound and outbound tasks
	in, out chan *task
	// tracks number of inflight tasks
	taskWg sync.WaitGroup
	// launches workers and collects errors if any occur
	grp *errgroup.Group
}

func (s *diffScheduler) enqueueTask(task *task) {
	s.taskWg.Add(1)
	s.in <- task
}

func (s *diffScheduler) startScheduler(ctx context.Context) {
	s.grp.Go(func() error {
		defer func() {
			close(s.out)
			// Because the workers may have exited early (due to the context being canceled).
			for range s.out {
				s.taskWg.Done()
			}
			// Because the workers may have enqueued additional tasks.
			for range s.in {
				s.taskWg.Done()
			}
			// now, the waitgroup should be at 0, and the goroutine that was _waiting_ on it should have exited.
		}()
		go func() {
			s.taskWg.Wait()
			close(s.in)
		}()
		for {
			if n := len(s.stack) - 1; n >= 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case newJob, ok := <-s.in:
					if !ok {
						return nil
					}
					s.stack = append(s.stack, newJob)
				case s.out <- s.stack[n]:
					s.stack[n] = nil
					s.stack = s.stack[:n]
				}
			} else {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case newJob, ok := <-s.in:
					if !ok {
						return nil
					}
					s.stack = append(s.stack, newJob)
				}
			}
		}
	})
}

func (s *diffScheduler) startWorkers(ctx context.Context, out chan *Change) {
	for i := int64(0); i < s.numWorkers; i++ {
		s.grp.Go(func() error {
			for task := range s.out {
				if err := s.work(ctx, task, out); err != nil {
					return err
				}
			}
			return nil
		})
	}
}

func (s *diffScheduler) work(ctx context.Context, todo *task, results chan *Change) error {
	defer s.taskWg.Done()

	prev := todo.prev
	prevCtx := todo.prevCtx
	cur := todo.cur
	curCtx := todo.curCtx
	offset := todo.offset

	if prev == nil && cur == nil {
		return nil
	}

	if prev == nil {
		return parallelAddAll(ctx, curCtx, cur, offset, results)
	}

	if cur == nil {
		return parallelRemoveAll(ctx, prevCtx, prev, offset, results)
	}

	if prevCtx.height == 0 && curCtx.height == 0 {
		return parallelDiffLeaves(prev, cur, offset, results)
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

			subn, err := ln.load(ctx, subCtx.bs, subCtx.bitWidth, subCtx.height)
			if err != nil {
				return err
			}

			offs := offset + (uint64(i) * subCount)
			if i == 0 {
				s.enqueueTask(&task{
					prevCtx: prevCtx,
					curCtx:  subCtx,
					prev:    prev,
					cur:     subn,
					offset:  offs,
				})
			} else {
				err := parallelAddAll(ctx, subCtx, subn, offs, results)
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

			subn, err := ln.load(ctx, subCtx.bs, subCtx.bitWidth, subCtx.height)
			if err != nil {
				return err
			}

			offs := offset + (uint64(i) * subCount)

			if i == 0 {
				s.enqueueTask(&task{
					prevCtx: subCtx,
					curCtx:  curCtx,
					prev:    subn,
					cur:     cur,
					offset:  offs,
				})
			} else {
				err := parallelRemoveAll(ctx, subCtx, subn, offs, results)
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

			subn, err := prev.links[i].load(ctx, subCtx.bs, subCtx.bitWidth, subCtx.height)
			if err != nil {
				return err
			}

			offs := offset + (uint64(i) * subCount)
			err = parallelRemoveAll(ctx, subCtx, subn, offs, results)
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

			subn, err := cur.links[i].load(ctx, subCtx.bs, subCtx.bitWidth, subCtx.height)
			if err != nil {
				return err
			}

			offs := offset + (uint64(i) * subCount)
			err = parallelAddAll(ctx, subCtx, subn, offs, results)
			if err != nil {
				return err
			}
			continue
		}

		// Both previous and current have links to diff
		if prev.links[i].cid.Equals(cur.links[i].cid) {
			continue
		}

		prevSubCtx := &nodeContext{
			bs:       prevCtx.bs,
			bitWidth: prevCtx.bitWidth,
			height:   prevCtx.height - 1,
		}

		prevSubn, err := prev.links[i].load(ctx, prevSubCtx.bs, prevSubCtx.bitWidth, prevSubCtx.height)
		if err != nil {
			return err
		}

		curSubCtx := &nodeContext{
			bs:       curCtx.bs,
			bitWidth: curCtx.bitWidth,
			height:   curCtx.height - 1,
		}

		curSubn, err := cur.links[i].load(ctx, curSubCtx.bs, curSubCtx.bitWidth, curSubCtx.height)
		if err != nil {
			return err
		}

		offs := offset + (uint64(i) * subCount)

		s.enqueueTask(&task{
			prevCtx: prevSubCtx,
			curCtx:  curSubCtx,
			prev:    prevSubn,
			cur:     curSubn,
			offset:  offs,
		})
		if err != nil {
			return err
		}
	}

	return nil
}
