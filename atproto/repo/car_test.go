package repo

import (
	"bytes"
	"context"
	"testing"

	"github.com/bluesky-social/indigo/atproto/repo/mst"
	"github.com/bluesky-social/indigo/atproto/syntax"

	"github.com/stretchr/testify/assert"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
	carutil "github.com/ipld/go-car/util"
	"github.com/multiformats/go-multihash"
)

// hypercerts: regression tests for CAR loading with weak or truncated CID hashes.

type testCARBlock struct {
	c    cid.Cid
	data []byte
}

func buildTestCAR(t *testing.T, root cid.Cid, blks ...testCARBlock) []byte {
	buf := new(bytes.Buffer)
	if err := car.WriteHeader(&car.CarHeader{Roots: []cid.Cid{root}, Version: 1}, buf); err != nil {
		t.Fatal(err)
	}
	for _, b := range blks {
		if err := carutil.LdWrite(buf, b.c.Bytes(), b.data); err != nil {
			t.Fatal(err)
		}
	}
	return buf.Bytes()
}

// CIDv1 dag-cbor sha2-256 with a zero-length digest: go-car accepts any block data under it.
func emptyDigestCID(t *testing.T) cid.Cid {
	c, err := cid.Cast([]byte{0x01, 0x71, 0x12, 0x00})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func testCommitBlock(t *testing.T, data cid.Cid) testCARBlock {
	commit := Commit{
		DID:     "did:plc:abc123",
		Version: ATPROTO_REPO_VERSION,
		Data:    data,
		Rev:     syntax.NewTIDNow(0).String(),
		Sig:     []byte("signature"),
	}
	buf := new(bytes.Buffer)
	if err := commit.MarshalCBOR(buf); err != nil {
		t.Fatal(err)
	}
	c, err := cid.NewPrefixV1(cid.DagCBOR, multihash.SHA2_256).Sum(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	return testCARBlock{c: c, data: buf.Bytes()}
}

// The finding's exploit: commit.data points at a zero-length-digest CID whose block is a self-referencing MST node.
func TestLoadCARRejectsEmptyDigestMSTRoot(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	w := emptyDigestCID(t)
	nd := mst.NodeData{Left: &w, Entries: []mst.EntryData{}}
	ndBuf := new(bytes.Buffer)
	if err := nd.MarshalCBOR(ndBuf); err != nil {
		t.Fatal(err)
	}
	commitBlk := testCommitBlock(t, w)
	carBytes := buildTestCAR(t, commitBlk.c, commitBlk, testCARBlock{c: w, data: ndBuf.Bytes()})

	_, _, err := LoadRepoFromCAR(ctx, bytes.NewReader(carBytes))
	assert.ErrorContains(err, "unsupported CID hash")

	_, _, err = LoadCommitFromCAR(ctx, bytes.NewReader(carBytes))
	assert.ErrorContains(err, "unsupported CID hash")
}

// A weak-hash root (which would match any commit block) is rejected by both loaders.
func TestLoadCARRejectsWeakRoot(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	w := emptyDigestCID(t)
	commitBlk := testCommitBlock(t, testCommitBlock(t, w).c) // data value is irrelevant here
	carBytes := buildTestCAR(t, w, testCARBlock{c: w, data: commitBlk.data})

	_, _, err := LoadRepoFromCAR(ctx, bytes.NewReader(carBytes))
	assert.ErrorContains(err, "unsupported CID hash")

	_, _, err = LoadCommitFromCAR(ctx, bytes.NewReader(carBytes))
	assert.ErrorContains(err, "unsupported CID hash")
}

func TestCommitVerifyStructureRejectsWeakData(t *testing.T) {
	assert := assert.New(t)

	md5Data, err := cid.NewPrefixV1(cid.DagCBOR, multihash.MD5).Sum([]byte("mst"))
	if err != nil {
		t.Fatal(err)
	}
	strong, err := cid.NewPrefixV1(cid.DagCBOR, multihash.SHA2_256).Sum([]byte("mst"))
	if err != nil {
		t.Fatal(err)
	}
	commit := Commit{
		DID:     "did:plc:abc123",
		Version: ATPROTO_REPO_VERSION,
		Data:    strong,
		Rev:     syntax.NewTIDNow(0).String(),
		Sig:     []byte("signature"),
	}
	assert.NoError(commit.VerifyStructure())

	commit.Data = md5Data
	assert.Error(commit.VerifyStructure())

	commit.Data = strong
	commit.Prev = &md5Data
	assert.Error(commit.VerifyStructure())
}
