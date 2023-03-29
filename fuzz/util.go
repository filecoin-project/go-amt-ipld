package fuzzer

import (
	"context"
	"fmt"

	block "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
)

type mockBlocks struct {
	data map[cid.Cid]block.Block
}

func newMockBlocks() *mockBlocks {
	return &mockBlocks{make(map[cid.Cid]block.Block)}
}

func (mb *mockBlocks) Get(_ context.Context, c cid.Cid) (block.Block, error) {
	d, ok := mb.data[c]
	if ok {
		return d, nil
	}
	return nil, fmt.Errorf("not found")
}

func (mb *mockBlocks) Put(_ context.Context, b block.Block) error {
	mb.data[b.Cid()] = b
	return nil
}
