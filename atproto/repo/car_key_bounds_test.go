package repo

// hypercerts: keys in a child node must sort below the next key of every
// node above it, or the CAR is rejected when loaded.

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/bluesky-social/indigo/atproto/repo/mst"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
)

// Returns ascending keys whose MST heights follow `heights`, in order.
func ascendingKeysWithHeights(t *testing.T, heights ...int) [][]byte {
	t.Helper()
	out := make([][]byte, 0, len(heights))
	for i := 0; len(out) < len(heights); i++ {
		if i > 1_000_000 {
			t.Fatal("could not find keys for the requested heights")
		}
		key := []byte(fmt.Sprintf("com.example.record/%013d", i))
		if mst.HeightForKey(key) == heights[len(out)] {
			out = append(out, key)
		}
	}
	return out
}

func testKeyNode(t *testing.T, val cid.Cid, right *cid.Cid, keys ...[]byte) testBlock {
	t.Helper()
	nd := mst.NodeData{}
	for _, k := range keys {
		nd.Entries = append(nd.Entries, mst.EntryData{KeySuffix: k, Value: val})
	}
	if right != nil {
		nd.Entries[0].Right = right
	}
	return testNodeBlock(t, nd)
}

func loadTestTree(t *testing.T, root testBlock, nodes ...testBlock) error {
	t.Helper()
	commitBlk := testCommitBlock(t, root.cid)
	blks := []testCARBlock{commitBlk}
	for _, n := range append([]testBlock{root}, nodes...) {
		blks = append(blks, testCARBlock{c: n.cid, data: n.data})
	}
	_, _, err := LoadRepoFromCAR(context.Background(), bytes.NewReader(buildTestCAR(t, commitBlk.c, blks...)))
	return err
}

// A child whose key sorts after the next key in its parent.
func TestLoadCARRejectsChildKeyPastNextParentKey(t *testing.T) {
	assert := assert.New(t)
	rec := testRecordBlock(t)
	keys := ascendingKeysWithHeights(t, 1, 0, 1, 0)
	first, inside, second, outside := keys[0], keys[1], keys[2], keys[3]

	child := testKeyNode(t, rec.cid, nil, inside)
	root := testKeyNode(t, rec.cid, &child.cid, first, second)
	assert.NoError(loadTestTree(t, root, child))
	// the child may be omitted, as in a diff CAR
	assert.NoError(loadTestTree(t, root))

	child = testKeyNode(t, rec.cid, nil, outside)
	root = testKeyNode(t, rec.cid, &child.cid, first, second)
	assert.ErrorIs(loadTestTree(t, root, child), mst.ErrInvalidTree)
	assert.NoError(loadTestTree(t, root))
}

// A trailing child two levels down is bounded by the next key of its grandparent.
func TestLoadCARRejectsChildKeyPastInheritedBound(t *testing.T) {
	assert := assert.New(t)
	rec := testRecordBlock(t)
	keys := ascendingKeysWithHeights(t, 2, 1, 0, 2, 0)
	first, mid, inside, second, outside := keys[0], keys[1], keys[2], keys[3], keys[4]

	leaf := testKeyNode(t, rec.cid, nil, inside)
	middle := testKeyNode(t, rec.cid, &leaf.cid, mid)
	root := testKeyNode(t, rec.cid, &middle.cid, first, second)
	assert.NoError(loadTestTree(t, root, middle, leaf))

	leaf = testKeyNode(t, rec.cid, nil, outside)
	middle = testKeyNode(t, rec.cid, &leaf.cid, mid)
	root = testKeyNode(t, rec.cid, &middle.cid, first, second)
	assert.ErrorIs(loadTestTree(t, root, middle, leaf), mst.ErrInvalidTree)
	assert.NoError(loadTestTree(t, root, middle))
}
