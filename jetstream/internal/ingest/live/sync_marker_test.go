package live

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/policygate"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/streaming"
	"github.com/stretchr/testify/require"
)

func TestProcessBatchManagedSyncReconcilesVerifiedOperationsBeforeMarker(t *testing.T) {
	st := newTestStore(t)
	stream := "selected-stream"
	history, err := attribution.Open(st, stream, "live-sync-marker-replacement")
	require.NoError(t, err)
	policy, err := selection.Open(st, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	initial := attribution.Policy{Revision: 1, Collections: []string{"app.bsky.feed.post"}}
	activation := uint64(1)
	require.NoError(t, history.EnsureInitialPolicyHistory(initial, &activation))
	gate := policygate.New(policy, nil, "live-sync-marker-replacement")
	var c *Consumer
	c, err = Open(Config{
		SegmentsDir: filepath.Join(t.TempDir(), "segments"), Store: st, SeqKey: SteadySeqKey, CursorKey: CursorKey,
		RelayURL: "https://example.invalid", Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Verifier: newTestVerifier(t),
		Attribution: history, AttributionDomain: "live-sync-marker-replacement", AttributionStreamID: stream,
		CollectionPolicy: policy, PolicyGate: gate, ReadLogRetentionBytes: 1 << 20,
		ReconcileSnapshot: func(context.Context, ingest.Snapshot) error { return nil },
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	did := "did:plc:syncmarker"
	prior, priorPayloads := buildCommit(t, did, "3mmoojp7vgo2f", struct{ Coll, Rkey string }{"app.bsky.feed.post", "stale"})
	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{{Seq: 41, Commit: prior.Commit}}))
	require.NoError(t, c.writer.Flush(t.Context()))
	_, payloads := buildCommit(t, did, "3mmoojp7vgo2g", struct{ Coll, Rkey string }{"app.bsky.feed.post", "fresh"})
	require.Len(t, priorPayloads, 1)
	require.Len(t, payloads, 1)
	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{{Seq: 42, Sync: &comatproto.SyncSubscribeRepos_Sync{DID: did, Rev: "3mmoojp7vgo2g", Time: "2026-05-21T00:00:00Z"}}}))
	require.NoError(t, c.writer.Flush(t.Context()))
	events, _, ok, _ := c.writer.ReadLog().ReadFrom(1, 10)
	require.True(t, ok)
	var markers int
	for _, entry := range events {
		if entry.Event().Kind == segment.KindSync {
			markers++
		}
	}
	require.Equal(t, 1, markers)
	require.Equal(t, int64(42), c.LastUpstreamSeq())
}

func TestProcessBatchManagedSyncNoopStillPersistsMarkerAndCursor(t *testing.T) {
	st := newTestStore(t)
	stream := "selected-stream"
	history, err := attribution.Open(st, stream, "live-sync-marker-noop")
	require.NoError(t, err)
	policy, err := selection.Open(st, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	initial := attribution.Policy{Revision: 1, Collections: []string{"app.bsky.feed.post"}}
	activation := uint64(1)
	require.NoError(t, history.EnsureInitialPolicyHistory(initial, &activation))
	gate := policygate.New(policy, nil, "live-sync-marker-noop")
	var c *Consumer
	c, err = Open(Config{SegmentsDir: filepath.Join(t.TempDir(), "segments"), Store: st, SeqKey: SteadySeqKey, CursorKey: CursorKey, RelayURL: "https://example.invalid", Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Verifier: newTestVerifier(t), Attribution: history, AttributionDomain: "live-sync-marker-noop", AttributionStreamID: stream, CollectionPolicy: policy, PolicyGate: gate, ReadLogRetentionBytes: 1 << 20, ReconcileSnapshot: func(ctx context.Context, snapshot ingest.Snapshot) error {
		return c.writer.ReconcileSnapshot(ctx, snapshot)
	}})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	initialEvent, payloads := buildCommit(t, "did:plc:syncmarker", "3mmoojp7vgo2g", struct{ Coll, Rkey string }{"app.bsky.feed.post", "existing"})
	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{{Seq: 42, Commit: initialEvent.Commit}}))
	require.NoError(t, c.writer.Flush(t.Context()))
	beforeRows, _, ok, _ := c.writer.ReadLog().ReadFrom(1, 10)
	require.True(t, ok)
	require.Len(t, beforeRows, 1)
	require.Equal(t, payloads[0], beforeRows[0].Event().Payload)
	before := c.writer.NextSeq()
	_, _ = buildCommit(t, "did:plc:syncmarker", "3mmoojp7vgo2h", struct{ Coll, Rkey string }{"app.bsky.feed.post", "existing"})
	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{{Seq: 43, Sync: &comatproto.SyncSubscribeRepos_Sync{DID: "did:plc:syncmarker", Rev: "3mmoojp7vgo2g", Time: "2026-05-21T00:00:00Z"}}}))
	require.NoError(t, c.writer.Flush(t.Context()))
	events, _, ok, _ := c.writer.ReadLog().ReadFrom(1, 10)
	require.True(t, ok)
	require.Equal(t, before+1, c.writer.NextSeq(), "no-op sync appends exactly one marker")
	var markers int
	for _, entry := range events {
		if entry.Event().Kind == segment.KindSync {
			markers++
		}
	}
	require.Equal(t, 1, markers)
	require.Equal(t, int64(43), c.LastUpstreamSeq())
}

func TestProcessBatchImportedLiveSyncCannotBypassSnapshotBoundary(t *testing.T) {
	st := newTestStore(t)
	stream := "selected-stream"
	history, err := attribution.Open(st, stream, "imported-live-sync-test-"+t.Name())
	require.NoError(t, err)
	policy, err := selection.Open(st, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	initial := attribution.Policy{Revision: 1, Collections: []string{"app.bsky.feed.post"}}
	require.NoError(t, history.EnsureInitialPolicyHistory(initial, nil))
	gate := policygate.New(policy, nil, "imported-live-sync-test-"+t.Name())
	var c *Consumer
	c, err = Open(Config{
		SegmentsDir: filepath.Join(t.TempDir(), "segments"), Store: st, SeqKey: SteadySeqKey, CursorKey: CursorKey,
		RelayURL: "https://example.invalid", Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Verifier: newTestVerifier(t),
		Attribution: history, AttributionDomain: "imported-live-sync-test-" + t.Name(), AttributionStreamID: stream,
		CollectionPolicy: policy, PolicyGate: gate,
		ReconcileSnapshot: func(context.Context, ingest.Snapshot) error { return nil },
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()
	did := "did:plc:importedsync"
	boundarySeq := uint64(999)
	snapshotID := "fixture-snapshot"
	boundary := ingest.Snapshot{DID: did, Rev: "3mmoojp7vgo2h", Collections: []string{"app.bsky.feed.post"}, Records: []segment.Event{{Kind: segment.KindCreateResync, DID: did, Rev: "3mmoojp7vgo2h", Collection: "app.bsky.feed.post", Rkey: "fresh", Payload: []byte(`{"fresh":true}`)}}, AppendMetadata: &ingest.AttributionMetadata{StreamID: stream, Policy: initial, Provenance: attribution.ProvenanceSnapshot, Boundary: attribution.Boundary{Seq: &boundarySeq, SnapshotID: &snapshotID}}}
	require.NoError(t, c.writer.ReconcileSnapshot(t.Context(), boundary))

	// Reconciliation has advanced the durable local sequence. The protection
	// under test is liveSync's rejection of imported source evidence across the
	// snapshot boundary, not processBatch's behavior for a local relay event.
	event := &segment.Event{Kind: segment.KindSync, DID: did, Rev: "3mmoojp7vgo2i"}
	sourceRev := uint64(1)
	source := attribution.Event{StreamID: "upstream-stream", Seq: 42, PolicyRev: &sourceRev, Provenance: attribution.ProvenanceLive, Payload: []byte(`{"event":"sync"}`), Source: &attribution.ImportedSource{Domain: "upstream", StreamID: "upstream-stream", Seq: 42, PolicyRev: &sourceRev, Provenance: attribution.ProvenanceLive, Policy: &initial, WitnessedAt: 1, IndexedAt: 1, SegmentFingerprint: attribution.FingerprintSegment(*event)}}
	destination := ingest.AttributionMetadata{StreamID: stream, Policy: initial, Provenance: attribution.ProvenanceLive}
	before := c.writer.NextSeq()
	err = c.writer.AppendImported(t.Context(), event, source, "upstream", destination, initial)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "invalid imported attribution", "fixture must satisfy source validation so this covers the snapshot guard")
	require.Equal(t, before, c.writer.NextSeq(), "rejected imported sync must not allocate a local sequence")

	// Local live-sync replacement and no-op-boundary cases remain valid.
	for _, blocks := range [][]byte{{0x01, 0x02, 0x03}, nil} {
		require.NoError(t, c.processBatch(t.Context(), []streaming.Event{{Seq: 43, Sync: &comatproto.SyncSubscribeRepos_Sync{DID: did, Rev: "3mmoojp7vgo2i", Time: "2026-05-21T00:00:00Z", Blocks: blocks}}}))
	}
}

func TestProcessBatchManagedSyncFailureDoesNotAppendMarker(t *testing.T) {
	st := newTestStore(t)
	stream := "selected-stream"
	history, err := attribution.Open(st, stream, "live-sync-marker-failure")
	require.NoError(t, err)
	policy, err := selection.Open(st, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	initial := attribution.Policy{Revision: 1, Collections: []string{"app.bsky.feed.post"}}
	activation := uint64(1)
	require.NoError(t, history.EnsureInitialPolicyHistory(initial, &activation))
	gate := policygate.New(policy, nil, "live-sync-marker-failure")
	c, err := Open(Config{
		SegmentsDir: filepath.Join(t.TempDir(), "segments"), Store: st, SeqKey: SteadySeqKey, CursorKey: CursorKey,
		RelayURL: "https://example.invalid", Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Verifier: newTestVerifier(t),
		Attribution: history, AttributionDomain: "live-sync-marker-failure", AttributionStreamID: stream,
		CollectionPolicy: policy, PolicyGate: gate,
		ReconcileSnapshot: func(context.Context, ingest.Snapshot) error { return errors.New("reconciliation failed") },
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	err = c.processBatch(t.Context(), []streaming.Event{{Seq: 42, Sync: &comatproto.SyncSubscribeRepos_Sync{DID: "did:plc:syncmarker", Rev: "3mmoojp7vgo2g", Time: "2026-05-21T00:00:00Z"}}})
	require.ErrorContains(t, err, "reconciliation failed")
	require.EqualValues(t, 1, c.writer.NextSeq())
	require.Zero(t, c.LastUpstreamSeq())
}
