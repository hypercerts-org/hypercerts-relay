package live

import (
	"context"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/mst"
	atmosrepo "github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"io"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/streaming"
	"github.com/stretchr/testify/require"
)

func TestHypercertsMixedAndFilteredOnlyProgress(t *testing.T) {
	st := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "segments")
	policy, err := selection.Open(st, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	var delivered []segment.Event
	cfg := Config{SegmentsDir: dir, SeqKey: SteadySeqKey, CursorKey: CursorKey, Store: st, RelayURL: "https://example.invalid", Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Verifier: newTestVerifier(t), CollectionPolicy: policy, OnEvent: func(e *segment.Event) { delivered = append(delivered, *e) }}
	c, err := Open(cfg)
	require.NoError(t, err)
	evt, _ := buildCommit(t, "did:plc:mixed", "3l3qo2vutsw2b", struct{ Coll, Rkey string }{"app.bsky.feed.post", "selected"}, struct{ Coll, Rkey string }{"app.bsky.feed.like", "excluded"})
	evt.Seq = 41
	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{evt}))
	filtered, _ := buildCommit(t, "did:plc:filtered", "3l3qo2vutsw2b", struct{ Coll, Rkey string }{"app.bsky.feed.like", "only-excluded"})
	filtered.Seq = 42
	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{filtered}))
	require.Equal(t, int64(42), c.LastUpstreamSeq())
	require.Len(t, delivered, 1)
	require.Equal(t, "app.bsky.feed.post", delivered[0].Collection)
	require.NoError(t, c.writer.DrainDurability(t.Context()))
	persisted, err := LoadUpstreamCursor(st, CursorKey)
	require.NoError(t, err)
	require.Equal(t, int64(42), persisted, "progress must persist before graceful shutdown")
	require.NoError(t, c.Close())
	c, err = Open(cfg)
	require.NoError(t, err)
	cursor, err := LoadUpstreamCursor(st, CursorKey)
	require.NoError(t, err)
	require.Equal(t, int64(42), cursor, "filtered-only source position survives restart")
	require.NoError(t, c.Close())
	rows := readAllSegmentEvents(t, dir)
	require.Len(t, rows, 1)
	require.Equal(t, "selected", rows[0].Rkey)
}

func TestHypercertsExcludedOnlyTimerCheckpoint(t *testing.T) {
	did := atmos.DID("did:plc:checkpoint")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	blocks := mst.NewMemBlockStore()
	repo := &atmosrepo.Repo{DID: did, Clock: atmos.NewTIDClock(0), Store: blocks, Tree: mst.NewTree(blocks)}
	prev, err := repo.Tree.WriteBlocks(blocks)
	require.NoError(t, err)
	commit, _ := buildSyntheticChainedCommit(t, repo, key, prev, struct{ Coll, Rkey string }{"app.bsky.feed.post", "excluded"})
	commit.Seq = 1
	body, err := commit.MarshalCBOR()
	require.NoError(t, err)
	source := &fakeFirehose{t: t, frames: [][]byte{encodeFrame(t, "#commit", body)}}
	srv := httptest.NewServer(source.handler())
	defer srv.Close()
	resolver := &stubResolver{docs: map[atmos.DID]*identity.DIDDocument{did: buildDIDDoc(did, key.PublicKey())}}
	verifier, err := atmossync.NewVerifier(atmossync.VerifierOptions{Directory: &identity.Directory{Resolver: resolver}, StateStore: atmossync.NewMemStateStore(), Policy: gt.Some(atmossync.PolicyError), SyncClient: gt.Some(atmossync.NewClient(atmossync.Options{Client: &xrpc.Client{Host: srv.URL}}))})
	require.NoError(t, err)
	defer verifier.Close()
	db := newTestStore(t)
	policy, err := selection.Open(db, nil)
	require.NoError(t, err)
	c, err := Open(Config{SegmentsDir: filepath.Join(t.TempDir(), "segments"), Store: db, SeqKey: SteadySeqKey, CursorKey: CursorKey, RelayURL: srv.URL, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Verifier: verifier, CollectionPolicy: policy})
	require.NoError(t, err)
	defer c.Close()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	defer func() { cancel(); <-done }()
	require.Eventually(t, func() bool { cursor, err := LoadUpstreamCursor(db, CursorKey); return err == nil && cursor == 1 }, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, uint64(1), c.Writer().NextSeq(), "excluded record never received an archive sequence")
}
