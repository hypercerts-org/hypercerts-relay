package mst

import (
	"context"
	"fmt"
	"testing"

	bf "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
)

type testBlockSource map[cid.Cid]bf.Block

func (s testBlockSource) Get(ctx context.Context, c cid.Cid) (bf.Block, error) {
	blk, ok := s[c]
	if !ok {
		return nil, fmt.Errorf("block not found: %s", c)
	}
	return blk, nil
}

func (s testBlockSource) put(t *testing.T, nd NodeData) cid.Cid {
	b, c, err := nd.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	blk, err := bf.NewBlockWithCid(b, *c)
	if err != nil {
		t.Fatal(err)
	}
	s[*c] = blk
	return *c
}

// Removing a key whose left neighbour is an empty child node must return an error, not panic.
func TestRemoveAfterEmptyLeftChild(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	val := testEntryCid(t)

	bs := testBlockSource{}
	empty := bs.put(t, NodeData{Entries: []EntryData{}})
	right := bs.put(t, NodeData{
		Entries: []EntryData{{
			KeySuffix: []byte("com.example.record/3jqfcqzm3fo2z"),
			Value:     val,
		}},
	})
	root := bs.put(t, NodeData{
		Left: &empty,
		Entries: []EntryData{{
			KeySuffix: []byte("com.example.record/3jqfcqzm3fo2j"),
			Value:     val,
			Right:     &right,
		}},
	})

	tree, err := LoadTreeFromStore(ctx, bs, root)
	if err != nil {
		t.Fatal(err)
	}

	assert.NotPanics(func() {
		_, err = tree.Remove([]byte("com.example.record/3jqfcqzm3fo2j"))
	})
	assert.ErrorIs(err, ErrInvalidTree)
}

// Removing a key whose right neighbour is an empty child node must return an error, not panic.
func TestRemoveBeforeEmptyRightChild(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	val := testEntryCid(t)

	bs := testBlockSource{}
	empty := bs.put(t, NodeData{Entries: []EntryData{}})
	left := bs.put(t, NodeData{
		Entries: []EntryData{{
			KeySuffix: []byte("com.example.record/3jqfcqzm3fo2a"),
			Value:     val,
		}},
	})
	root := bs.put(t, NodeData{
		Left: &left,
		Entries: []EntryData{{
			KeySuffix: []byte("com.example.record/3jqfcqzm3fo2j"),
			Value:     val,
			Right:     &empty,
		}},
	})

	tree, err := LoadTreeFromStore(ctx, bs, root)
	if err != nil {
		t.Fatal(err)
	}

	assert.NotPanics(func() {
		_, err = tree.Remove([]byte("com.example.record/3jqfcqzm3fo2j"))
	})
	assert.ErrorIs(err, ErrInvalidTree)
}
