package backfill

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/policygate"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/mst"
	atmosrepo "github.com/jcalabro/atmos/repo"
	"github.com/stretchr/testify/require"
)

func ptrSeq(v uint64) *uint64 { return &v }
func ptrID(v string) *string  { return &v }

func TestHandleRepoResyncPolicyAdmissionCoversProjection(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(filepath.Join(dir, "db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	manager, err := selection.Open(db, []string{"app.bsky.feed.post", "app.bsky.feed.like"})
	require.NoError(t, err)
	started, release := make(chan struct{}), make(chan struct{})
	history, err := attribution.Open(db, "selected-stream", "selected-stream")
	require.NoError(t, err)
	initial := attribution.Policy{Revision: 1, Collections: []string{"app.bsky.feed.like", "app.bsky.feed.post"}}
	require.NoError(t, history.EnsureInitialPolicyHistory(initial, nil))
	writer, err := ingest.Open(ingest.Config{SegmentsDir: filepath.Join(dir, "segments"), Store: db, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), CollectionPolicy: manager, Attribution: history, AttributionDomain: "selected-stream", MaxEventsPerBlock: 4, MaxSegmentBytes: 1 << 30, ReconcileSnapshot: func(_ context.Context, snapshot ingest.Snapshot) error {
		if snapshot.AppendMetadata == nil {
			return nil
		}
		if snapshot.DID == "did:plc:retryprojection" {
			close(started)
			<-release
		}
		require.Len(t, snapshot.Records, 2)
		return nil
	}})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, writer.Close()) })
	require.NoError(t, writer.ReconcileSnapshot(t.Context(), ingest.Snapshot{DID: "did:plc:prior", Rev: "3l3qo2vutsw2b", Collections: initial.Collections, AppendMetadata: &ingest.AttributionMetadata{StreamID: "selected-stream", Policy: initial, Provenance: attribution.ProvenanceReconciliation, Boundary: attribution.Boundary{Seq: new(uint64), SnapshotID: new("did:plc:prior/3l3qo2vutsw2b")}}}))
	gate := policygate.New(manager, writer, "selected-stream")
	handler := NewSegmentHandler(writer, nil, nil)
	handler.policyGate, handler.attribution, handler.streamID = gate, history, "selected-stream"
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	did := atmos.DID("did:plc:retryprojection")
	blockStore := mst.NewMemBlockStore()
	repository := &atmosrepo.Repo{DID: did, Clock: atmos.NewTIDClock(0), Store: blockStore, Tree: mst.NewTree(blockStore)}
	require.NoError(t, repository.Create("app.bsky.feed.post", "selected", map[string]any{"text": "selected"}))
	require.NoError(t, repository.Create("app.bsky.feed.like", "preserved", map[string]any{"subject": "other"}))
	commit, err := repository.Commit(key)
	require.NoError(t, err)
	retryDone := make(chan error, 1)
	go func() { retryDone <- handler.HandleRepoResync(t.Context(), did, repository, commit) }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("actual retry did not reach snapshot reconciliation")
	}
	adoptDone := make(chan error, 1)
	go func() {
		adoptDone <- gate.Adopt(t.Context(), "selected-stream", func(*uint64) error {
			_, updateErr := manager.Update(1, []string{"app.certified.actor.profile"}, nil)
			return updateErr
		})
	}()
	select {
	case err := <-adoptDone:
		t.Fatalf("policy adoption crossed active retry projection: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-retryDone)
	require.NoError(t, <-adoptDone)
}
