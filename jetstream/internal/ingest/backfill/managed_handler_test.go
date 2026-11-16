package backfill

import (
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/policygate"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
	"github.com/stretchr/testify/require"
)

func TestManagedHandlerCapturesSharedBoundaryAndSelectedProjection(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(filepath.Join(dir, "db"), nil)
	require.NoError(t, err)
	defer db.Close()
	history, err := attribution.Open(db, "selected-stream", "selected-stream")
	require.NoError(t, err)
	policy, err := selection.Open(db, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	require.NoError(t, history.EnsureInitialPolicyHistory(attribution.Policy{Revision: 1, Collections: []string{"app.bsky.feed.post"}}, nil))
	w, err := ingest.Open(ingest.Config{SegmentsDir: filepath.Join(dir, "segments"), Store: db, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), CollectionPolicy: policy, AttributionDomain: "selected-stream", Attribution: history})
	require.NoError(t, err)
	defer w.Close()
	h := NewSegmentHandler(w, nil, nil)
	h.attribution, h.policyGate, h.streamID = history, policygate.New(policy, w, "selected-stream"), "selected-stream"
	did := atmos.DID("did:plc:managed")
	r, commit := buildMultiRecordRepo(t, did, "app.bsky.feed.post", 1025)
	require.NoError(t, r.Create("app.bsky.feed.like", "excluded", map[string]any{"subject": "excluded"}))
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	commit, err = r.Commit(key)
	require.NoError(t, err)
	require.NoError(t, h.HandleRepo(t.Context(), did, r, commit))
	require.NoError(t, w.DrainDurability(t.Context()))
	events := collectActiveEvents(t, filepath.Join(dir, "segments", ingest.SegmentFilename(w.ActiveIndex())))
	require.Len(t, events, 1025)
	first := uint64(0)
	for _, event := range events {
		require.Equal(t, "app.bsky.feed.post", event.Collection)
		if first == 0 || event.Seq < first {
			first = event.Seq
		}
		exists, err := history.Lookup(event.Seq)
		require.NoError(t, err)
		require.Equal(t, "selected-stream", exists.StreamID)
		require.Equal(t, uint64(1), *exists.PolicyRev)
	}
	require.Equal(t, uint64(1), first)
	for _, event := range events {
		metadata, err := history.Lookup(event.Seq)
		require.NoError(t, err)
		require.Equal(t, first, *metadata.Boundary.Seq)
	}
}
