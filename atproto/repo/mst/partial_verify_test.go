package mst

import (
	"context"
	"fmt"
	"testing"

	blockstore "github.com/ipfs/boxo/blockstore" // hypercerts: replace incompatible legacy go-ipfs-blockstore dependency
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	ipld "github.com/ipfs/go-ipld-format"
)

// withheldSource hides some blocks, as a diff CAR does.
type withheldSource struct {
	bs       blockstore.Blockstore
	withheld map[string]bool
}

func (s withheldSource) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	if s.withheld[c.KeyString()] {
		return nil, &ipld.ErrNotFound{Cid: c}
	}
	return s.bs.Get(ctx, c)
}

func childNodeCIDs(n *Node, out *[]cid.Cid) {
	for _, e := range n.Entries {
		if e.Child != nil {
			*out = append(*out, *e.Child.CID)
			childNodeCIDs(e.Child, out)
		}
	}
}

// hypercerts: a partial tree, as carried by a #commit diff CAR, must pass
// structural verification whichever nodes below the root are absent.
func TestVerifyAcceptsPartialTrees(t *testing.T) {
	ctx := context.Background()
	bs := blockstore.NewBlockstore(datastore.NewMapDatastore())
	tree := NewEmptyTree()
	maxHeight := 0
	for i := 0; i < 4000; i++ {
		key := []byte(fmt.Sprintf("com.example.record/%013d", i))
		maxHeight = max(maxHeight, HeightForKey(key))
		c, err := cid.NewPrefixV1(cid.DagCBOR, 0x12).Sum(key)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tree.Insert(key, c); err != nil {
			t.Fatal(err)
		}
	}
	if maxHeight < 3 {
		t.Fatalf("fixture tree is too shallow: height %d", maxHeight)
	}
	root, err := tree.WriteDiffBlocks(ctx, bs)
	if err != nil {
		t.Fatal(err)
	}
	var nodes []cid.Cid
	childNodeCIDs(tree.Root, &nodes)

	check := func(withheld map[string]bool) {
		t.Helper()
		loaded, err := LoadTreeFromStore(ctx, withheldSource{bs: bs, withheld: withheld}, *root)
		if err != nil {
			t.Fatal(err)
		}
		if !loaded.IsPartial() {
			t.Fatal("expected a partial tree")
		}
		if err := loaded.Verify(); err != nil {
			t.Fatalf("partial tree failed verification: %v", err)
		}
	}
	all := map[string]bool{}
	for _, c := range nodes {
		check(map[string]bool{c.KeyString(): true})
		all[c.KeyString()] = true
	}
	check(all)
	t.Logf("height %d, %d nodes below the root", maxHeight, len(nodes))
}
