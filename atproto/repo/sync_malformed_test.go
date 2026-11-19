package repo

// hypercerts: end-to-end regression tests for malformed MST shapes inside an
// upstream #commit CAR (TECH-932 F2/F3). LoadRepoFromCAR must reject
// structurally invalid trees (tree.Verify) instead of panicking later when the
// inverted tree is re-encoded.

import (
	"bytes"
	"testing"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/repo/mst"
	lexutil "github.com/bluesky-social/indigo/lex/util"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
	carutil "github.com/ipld/go-car/util"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
)

type testBlock struct {
	cid  cid.Cid
	data []byte
}

func testNodeBlock(t *testing.T, nd mst.NodeData) testBlock {
	b, c, err := nd.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	return testBlock{cid: *c, data: b}
}

func testRecordBlock(t *testing.T) testBlock {
	data := []byte{0xa0} // empty DAG-CBOR map
	c, err := cid.NewPrefixV1(cid.DagCBOR, multihash.SHA2_256).Sum(data)
	if err != nil {
		t.Fatal(err)
	}
	return testBlock{cid: c, data: data}
}

// builds a #commit message for a single "create" of `path`, with the given MST blocks and `root` as the MST root
func testMalformedCommitMessage(t *testing.T, path string, rec testBlock, root cid.Cid, mstBlocks ...testBlock) *comatproto.SyncSubscribeRepos_Commit {
	did := "did:plc:abcdefghijklmnopqrstuvwx"
	rev := "3jqfcqzm3fo2j"

	commit := Commit{
		DID:     did,
		Version: ATPROTO_REPO_VERSION,
		Data:    root,
		Rev:     rev,
		Sig:     []byte{1},
	}
	buf := new(bytes.Buffer)
	if err := commit.MarshalCBOR(buf); err != nil {
		t.Fatal(err)
	}
	commitCID, err := cid.NewPrefixV1(cid.DagCBOR, multihash.SHA2_256).Sum(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	carBuf := new(bytes.Buffer)
	if err := car.WriteHeader(&car.CarHeader{Roots: []cid.Cid{commitCID}, Version: 1}, carBuf); err != nil {
		t.Fatal(err)
	}
	blocks := append([]testBlock{{cid: commitCID, data: buf.Bytes()}, rec}, mstBlocks...)
	for _, blk := range blocks {
		if err := carutil.LdWrite(carBuf, blk.cid.Bytes(), blk.data); err != nil {
			t.Fatal(err)
		}
	}

	recCID := lexutil.LexLink(rec.cid)
	return &comatproto.SyncSubscribeRepos_Commit{
		Blocks: lexutil.LexBytes(carBuf.Bytes()),
		Commit: lexutil.LexLink(commitCID),
		Ops: []*comatproto.SyncSubscribeRepos_RepoOp{{
			Action: "create",
			Cid:    &recCID,
			Path:   path,
		}},
		Repo: did,
		Rev:  rev,
		Time: "2024-01-01T00:00:00Z",
	}
}

// MST root with an empty-key entry followed by a child pointer must be
// rejected, not panic when the inverted tree is encoded.
func TestCommitMessageEmptyKeyEntry(t *testing.T) {
	assert := assert.New(t)
	path := "com.example.record/3jqfcqzm3fo2j"
	rec := testRecordBlock(t)

	missing := testNodeBlock(t, mst.NodeData{Entries: []mst.EntryData{{KeySuffix: []byte("missing"), Value: rec.cid}}})
	root := testNodeBlock(t, mst.NodeData{
		Entries: []mst.EntryData{
			{PrefixLen: 0, KeySuffix: []byte{}, Value: rec.cid, Right: &missing.cid},
			{PrefixLen: 0, KeySuffix: []byte(path), Value: rec.cid},
		},
	})

	msg := testMalformedCommitMessage(t, path, rec, root.cid, root, missing)
	_, err := VerifyCommitMessage(t.Context(), msg)
	assert.ErrorIs(err, mst.ErrInvalidTree)
}

// MST root whose key sits between two empty child nodes must be rejected, not
// panic when the "create" op is inverted. (F3, through the full path.)
func TestCommitMessageEmptyChildNodes(t *testing.T) {
	assert := assert.New(t)
	path := "com.example.record/3jqfcqzm3fo2j"
	rec := testRecordBlock(t)

	empty := testNodeBlock(t, mst.NodeData{Entries: []mst.EntryData{}})
	root := testNodeBlock(t, mst.NodeData{
		Left: &empty.cid,
		Entries: []mst.EntryData{
			{PrefixLen: 0, KeySuffix: []byte(path), Value: rec.cid, Right: &empty.cid},
		},
	})

	msg := testMalformedCommitMessage(t, path, rec, root.cid, root, empty)
	_, err := VerifyCommitMessage(t.Context(), msg)
	assert.ErrorIs(err, mst.ErrInvalidTree)
}
