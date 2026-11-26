package mst

// hypercerts: trees produced by the insert path still load and pass
// structural verification.

import (
	"context"
	"testing"

	blockstore "github.com/ipfs/boxo/blockstore" // hypercerts: replace incompatible legacy go-ipfs-blockstore dependency
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/stretchr/testify/assert"
)

// Build a tree through the normal insert path (including records whose keys
// force deeper layers), write it out, reload it, and verify it.
func TestLoadRoundTripValidTree(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	bs := blockstore.NewBlockstore(datastore.NewMapDatastore())
	tree := NewEmptyTree()

	// 64 records with varied keys; some hash prefixes produce layered trees.
	for i := 0; i < 64; i++ {
		key := []byte(string(rune('a'+i%26)) + "-key-" + string(rune('0'+i%10)) + "-" + itoa(i))
		c, err := cid.NewPrefixV1(cid.DagCBOR, 0x12).Sum([]byte{byte(i)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tree.Insert(key, c); err != nil {
			t.Fatal(err)
		}
	}

	assert.NoError(tree.Verify())

	rootCID, err := tree.WriteDiffBlocks(ctx, bs)
	if err != nil {
		t.Fatal(err)
	}

	loaded, err := LoadTreeFromStore(ctx, bs, *rootCID)
	if err != nil {
		t.Fatalf("valid tree failed to load: %v", err)
	}
	assert.NoError(loaded.Verify())

	// the loaded tree serializes back to the same root; every node is
	// re-encoded, as RootCID would return the CID stored at load time
	again, err := loaded.Root.writeBlocks(ctx, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(*rootCID, *again)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
