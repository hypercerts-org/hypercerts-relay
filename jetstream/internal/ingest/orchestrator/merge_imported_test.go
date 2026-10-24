package orchestrator

import (
	"context"
	"encoding/base64"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/policygate"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/live"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func TestMergeRunnerManagedImportPreservesSourceEvidenceAndLegacyUnknown(t *testing.T) {
	fix := newMergeFixture(t, nil, nil)
	const stream = "selected-stream"
	source, err := attribution.Open(fix.store, stream, "merge-source")
	require.NoError(t, err)
	destination, err := attribution.Open(fix.store, stream, "merge-destination")
	require.NoError(t, err)
	policy := attribution.Policy{Revision: 1, Collections: []string{"org.example.record"}}
	require.NoError(t, destination.EnsureInitialPolicyHistory(policy, nil))
	selectionManager, err := selection.Open(fix.store, []string{"org.example.record"})
	require.NoError(t, err)

	sourceDir := filepath.Join(fix.dataDir, "managed-source")
	w, err := ingest.Open(ingest.Config{SegmentsDir: sourceDir, Store: fix.store, SeqKey: live.BootstrapSeqKey, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	legacy := segment.Event{Kind: segment.Kind(1), DID: "did:plc:merge", Collection: "org.example.record", Rkey: "legacy", Rev: "3l3qo2vutsw2b", Payload: []byte(`{"v":1}`), WitnessedAt: 100, IndexedAt: 200}
	require.NoError(t, w.Append(t.Context(), &legacy))
	require.NoError(t, w.SealActiveAndClose())

	// Activation must follow the already-sealed legacy segment. Reopening the
	// same source directory and sequence key proves the writer carries forward
	// both segment index and sequence rather than manufacturing coordinates.
	activationBoundary := uint64(2)
	require.NoError(t, source.EnsureInitialPolicyHistory(policy, nil))
	require.NoError(t, source.EnsureInitialPolicyHistory(policy, &activationBoundary))
	w, err = ingest.Open(ingest.Config{SegmentsDir: sourceDir, Store: fix.store, SeqKey: live.BootstrapSeqKey, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Attribution: source, AttributionDomain: "merge-source"})
	require.NoError(t, err)
	known := segment.Event{Kind: segment.Kind(1), DID: "did:plc:merge", Collection: "org.example.record", Rkey: "known", Rev: "3l3qo2vutsw2c", Payload: []byte(`{"v":2}`), WitnessedAt: 101, IndexedAt: 201}
	boundarySeq, snapshotID := uint64(2), "source-snapshot"
	require.NoError(t, w.AppendAttributed(t.Context(), &known, ingest.AttributionMetadata{StreamID: stream, Policy: policy, Provenance: attribution.ProvenanceSnapshot, Boundary: attribution.Boundary{Seq: &boundarySeq, SnapshotID: &snapshotID}}))
	require.NoError(t, w.SealActiveAndClose())
	_, err = source.Lookup(1)
	require.ErrorIs(t, err, attribution.ErrLegacyUnknown)
	attributed, err := source.Lookup(2)
	require.NoError(t, err)
	require.Equal(t, attribution.ProvenanceSnapshot, attributed.Provenance)

	dst, err := ingest.Open(ingest.Config{SegmentsDir: filepath.Join(fix.dataDir, "segments"), Store: fix.store, SeqKey: live.SteadySeqKey, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Attribution: destination, AttributionDomain: "merge-destination"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dst.Close() })
	runner := newMergeRunner(dst, fix.store, sourceDir, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, nil)
	runner.sourceAttribution, runner.sourceDomain, runner.sourceAttributionStreamID = source, "bootstrap-live", stream
	runner.now = func() time.Time { return time.UnixMicro(9999).UTC() }
	runner.destinationAttribution, runner.destinationStreamID = destination, stream
	runner.collectionPolicy, runner.policyGate = selectionManager, policygate.New(selectionManager, dst, "merge-destination")
	files, err := ingest.SegmentFiles(sourceDir)
	require.NoError(t, err)
	require.Len(t, files, 2)
	for _, file := range files {
		_, err = runner.processSourceSegment(t.Context(), file)
		require.NoError(t, err)
	}
	for seq, expectedTime := range map[uint64]int64{1: 100, 2: 101} {
		got, err := destination.Lookup(seq)
		require.NoError(t, err)
		require.Equal(t, attribution.ProvenanceReconciliation, got.Provenance)
		require.NotNil(t, got.Source)
		require.Equal(t, "bootstrap-live", got.Source.Domain)
		require.Equal(t, expectedTime, got.Source.WitnessedAt)
		if seq == 1 {
			require.Equal(t, attribution.ProvenanceLegacyUnknown, got.Source.Provenance)
			require.Nil(t, got.Source.Policy)
			require.Nil(t, got.Source.PolicyRev)
		} else {
			require.Equal(t, attribution.ProvenanceSnapshot, got.Source.Provenance)
			require.NotNil(t, got.Source.Policy)
		}
	}
}

func TestMergeRunnerSourcePolicyLookupFailurePropagates(t *testing.T) {
	fix := newMergeFixture(t, nil, nil)
	// A managed source without a valid policy-history coordinate must not be
	// silently imported as though its evidence were incomplete-but-valid.
	source, err := attribution.Open(fix.store, "selected-stream", "merge-source-errors")
	require.NoError(t, err)
	destination, err := attribution.Open(fix.store, "selected-stream", "merge-destination-errors")
	require.NoError(t, err)
	policy := attribution.Policy{Revision: 1, Collections: []string{"org.example.record"}}
	require.NoError(t, destination.EnsureInitialPolicyHistory(policy, nil))
	unknownPolicy := attribution.Policy{Revision: 9, Collections: []string{"org.example.record"}}
	sourceDir := filepath.Join(fix.dataDir, "managed-source")
	w, err := ingest.Open(ingest.Config{SegmentsDir: sourceDir, Store: fix.store, SeqKey: live.BootstrapSeqKey, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Attribution: source, AttributionDomain: "merge-source-errors"})
	require.NoError(t, err)
	event := segment.Event{Kind: segment.Kind(1), DID: "did:plc:merge", Collection: "org.example.record", Rkey: "known", Rev: "3l3qo2vutsw2b", Payload: []byte(`{"v":1}`)}
	activationBoundary := uint64(1)
	require.NoError(t, source.EnsureInitialPolicyHistory(unknownPolicy, nil))
	require.NoError(t, source.EnsureInitialPolicyHistory(unknownPolicy, &activationBoundary))
	require.NoError(t, w.AppendAttributed(t.Context(), &event, ingest.AttributionMetadata{StreamID: "selected-stream", Policy: unknownPolicy, Provenance: attribution.ProvenanceLive, Boundary: attribution.Boundary{}}))
	require.NoError(t, w.SealActiveAndClose())
	dst, err := ingest.Open(ingest.Config{SegmentsDir: filepath.Join(fix.dataDir, "segments"), Store: fix.store, SeqKey: live.SteadySeqKey, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Attribution: destination, AttributionDomain: "merge-destination-errors"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dst.Close() })
	runner := newMergeRunner(dst, fix.store, sourceDir, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, nil)
	runner.sourceAttribution, runner.sourceDomain, runner.sourceAttributionStreamID = source, "bootstrap-live", "selected-stream"
	runner.destinationAttribution, runner.destinationStreamID = destination, "selected-stream"
	runner.collectionPolicy = nil
	policyKey := []byte("hypercerts/attribution/v1/" + base64.RawURLEncoding.EncodeToString([]byte("merge-source-errors")) + "/policy/0000000000000009")
	require.NoError(t, fix.store.Delete(policyKey, pebble.Sync))
	_, lookupErr := source.Lookup(1)
	require.NoError(t, lookupErr)
	_, policyErr := source.LookupPolicy(9)
	require.Error(t, policyErr)
	files, err := ingest.SegmentFiles(sourceDir)
	require.NoError(t, err)
	_, err = runner.processSourceSegment(context.Background(), files[0])
	require.Error(t, err)
	require.ErrorContains(t, err, "lookup source policy")
	seqs, err := destination.PreparedSeqs()
	require.NoError(t, err)
	require.Empty(t, seqs)
}
