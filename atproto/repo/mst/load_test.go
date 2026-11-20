package mst

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multihash"
)

// hypercerts: regression tests for loading MSTs from untrusted blocks.

type mapBlockSource map[cid.Cid]blocks.Block

func (m mapBlockSource) Get(_ context.Context, c cid.Cid) (blocks.Block, error) {
	if b, ok := m[c]; ok {
		return b, nil
	}
	return nil, &ipld.ErrNotFound{Cid: c}
}

// putNode encodes nd as a sha2-256 dag-cbor block (as a well-behaved PDS would) and returns its CID.
func (m mapBlockSource) putNode(t *testing.T, nd NodeData) cid.Cid {
	b, c, err := nd.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	blk, err := blocks.NewBlockWithCid(b, *c)
	if err != nil {
		t.Fatal(err)
	}
	m[*c] = blk
	return *c
}

// A node referenced more than once must be rejected.
func TestLoadTreeRejectsSharedSubtree(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	bs := mapBlockSource{}

	child := bs.putNode(t, NodeData{Entries: []EntryData{{KeySuffix: []byte("a"), Value: testEntryCid(t)}}})
	prev := bs.putNode(t, NodeData{
		Left:    &child,
		Entries: []EntryData{{KeySuffix: []byte("a"), Value: testEntryCid(t), Right: &child}},
	})

	_, err := LoadTreeFromStore(ctx, bs, prev)
	assert.ErrorIs(err, ErrInvalidTree)
}

// Chains of nodes deeper than any well-formed tree are rejected; a chain at the limit still loads.
func TestLoadTreeDepthLimit(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	build := func(levels int) (mapBlockSource, cid.Cid) {
		bs := mapBlockSource{}
		prev := bs.putNode(t, NodeData{Entries: []EntryData{{KeySuffix: []byte("a"), Value: testEntryCid(t)}}})
		for i := 1; i < levels; i++ {
			child := prev
			prev = bs.putNode(t, NodeData{Left: &child, Entries: []EntryData{}})
		}
		return bs, prev
	}

	bs, root := build(maxTreeLevels)
	_, err := LoadTreeFromStore(ctx, bs, root)
	assert.NoError(err)

	bs, root = build(maxTreeLevels + 1)
	_, err = LoadTreeFromStore(ctx, bs, root)
	assert.ErrorIs(err, ErrInvalidTree)
}

// Links using a weak or truncated hash must be rejected, not treated as missing ("partial") subtrees.
func TestLoadTreeRejectsWeakLinkCID(t *testing.T) {
	ctx := context.Background()

	// a CID whose sha2-256 digest is not 32 bytes
	emptyDigest, err := cid.Cast([]byte{0x01, 0x71, 0x12, 0x00})
	if err != nil {
		t.Fatal(err)
	}
	md5Link, err := cid.NewPrefixV1(cid.DagCBOR, multihash.MD5).Sum([]byte("subtree"))
	if err != nil {
		t.Fatal(err)
	}

	for _, weak := range []cid.Cid{emptyDigest, md5Link} {
		cases := map[string]NodeData{
			"left":  {Left: &weak, Entries: []EntryData{{KeySuffix: []byte("a"), Value: testEntryCid(t)}}},
			"right": {Entries: []EntryData{{KeySuffix: []byte("a"), Value: testEntryCid(t), Right: &weak}}},
			"value": {Entries: []EntryData{{KeySuffix: []byte("a"), Value: weak}}},
		}
		for name, nd := range cases {
			bs := mapBlockSource{}
			root := bs.putNode(t, nd)
			_, err := LoadTreeFromStore(ctx, bs, root)
			assert.ErrorIs(t, err, ErrInvalidTree, "%s link %s", name, weak)

			buf := new(bytes.Buffer)
			if err := nd.MarshalCBOR(buf); err != nil {
				t.Fatal(err)
			}
			_, err = NodeDataFromCBOR(buf)
			assert.ErrorIs(t, err, ErrInvalidTree, "%s link %s", name, weak)
		}
	}
}
