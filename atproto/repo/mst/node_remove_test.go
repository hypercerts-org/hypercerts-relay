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

// Removing a key between two empty child nodes (invalid, but accepted when decoding) must return an error, not panic.
func TestRemoveBetweenEmptyChildren(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	val := testEntryCid(t)

	bs := testBlockSource{}
	empty := bs.put(t, NodeData{Entries: []EntryData{}})
	root := bs.put(t, NodeData{
		Left: &empty,
		Entries: []EntryData{{
			KeySuffix: []byte("com.example.record/3jqfcqzm3fo2j"),
			Value:     val,
			Right:     &empty,
		}},
	})

	tree, err := LoadTreeFromStore(ctx, bs, root)
	if err != nil {
		// Both empty children share one CID, so the loader (which refuses
		// repeated node references) rejects the tree at load time — stronger
		// than the mergeNodes guard this test was written for.
		assert.ErrorIs(err, ErrInvalidTree)
		return
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
	assert.NoError(err)

	assert.NotPanics(func() {
		_, err = tree.Remove([]byte("com.example.record/3jqfcqzm3fo2j"))
	})
	assert.ErrorIs(err, ErrInvalidTree)
}
