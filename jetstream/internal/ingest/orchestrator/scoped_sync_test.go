package orchestrator

import (
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
	"github.com/bluesky-social/jetstream/internal/tombstone"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/stretchr/testify/require"
)

func TestBootstrapScopedSyncMergeCompaction(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "replacement", true: "empty"}[empty], func(t *testing.T) {
			fixture := newScopedSyncMergeFixture(t)
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			policy := attribution.Policy{Revision: 1, Collections: []string{"org.example.record"}}
			snapshot := ingest.Snapshot{DID: "did:plc:scope", Rev: "rev2", Collections: policy.Collections, AppendMetadata: &ingest.AttributionMetadata{StreamID: "stream", Policy: policy, Provenance: attribution.ProvenanceReconciliation}}
			paths := []string{}
			if !empty {
				snapshot.Records = []segment.Event{{Kind: segment.KindCreateResync, DID: snapshot.DID, Rev: snapshot.Rev, Collection: "org.example.record", Rkey: "latest", Payload: []byte{0xa0}}}
				paths = append(paths, "org.example.record/latest")
			}
			require.NoError(t, fixture.sourceWriter.ReconcileSnapshot(t.Context(), snapshot))
			payload, err := (&comatproto.SyncSubscribeRepos_Sync{DID: snapshot.DID, Rev: snapshot.Rev, Blocks: []byte{}}).MarshalCBOR()
			require.NoError(t, err)
			payload, err = tombstone.EncodeScopedSync(payload, tombstone.SyncScope{Collections: policy.Collections, Paths: paths})
			require.NoError(t, err)
			require.NoError(t, fixture.sourceWriter.AppendAttributed(t.Context(), &segment.Event{Kind: segment.KindSync, DID: snapshot.DID, Rev: snapshot.Rev, Payload: payload}, ingest.AttributionMetadata{StreamID: "stream", Policy: policy, Provenance: attribution.ProvenanceLive}))
			require.NoError(t, fixture.sourceWriter.SealActiveAndClose())
			runner := newMergeRunner(fixture.destinationWriter, fixture.merge.store, fixture.sourceDir, nil, logger, nil, nil)
			runner.sourceAttribution, runner.sourceDomain, runner.sourceAttributionStreamID = fixture.sourceAttribution, "bootstrap-scoped", "stream"
			runner.destinationAttribution, runner.destinationStreamID = fixture.destinationAttribution, "stream"
			runner.collectionPolicy, runner.policyGate = fixture.selectionManager, policygate.New(fixture.selectionManager, fixture.destinationWriter, "destination-scoped")
			files, err := ingest.SegmentFiles(fixture.sourceDir)
			require.NoError(t, err)
			for _, file := range files {
				_, err = runner.processSourceSegment(t.Context(), file)
				require.NoError(t, err)
			}
			require.NoError(t, fixture.destinationWriter.SealActiveAndClose())
			fixture.merge.cfg.CompactionInterval = time.Hour
			o, err := New(fixture.merge.cfg)
			require.NoError(t, err)
			require.NoError(t, o.runDeleteCompaction(t.Context(), compactionMergeTail, nil))
			assertScopedSyncCompactionRows(t, fixture.merge.dataDir, empty)
		})
	}
}

type scopedSyncMergeFixture struct {
	merge                  *mergeFixture
	destinationWriter      *ingest.Writer
	sourceAttribution      *attribution.Manager
	destinationAttribution *attribution.Manager
	selectionManager       *selection.Manager
	sourceDir              string
	sourceWriter           *ingest.Writer
}

// newScopedSyncMergeFixture seeds managed replacement candidates alongside an
// unmanaged full-repository sync, retaining both compaction semantics in one run.
func newScopedSyncMergeFixture(t *testing.T) *scopedSyncMergeFixture {
	t.Helper()
	fix := newMergeFixture(t, nil, nil)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	policy := attribution.Policy{Revision: 1, Collections: []string{"org.example.record"}}
	selectionManager, err := selection.Open(fix.store, policy.Collections)
	require.NoError(t, err)
	source, err := attribution.Open(fix.store, "stream", "bootstrap-scoped")
	require.NoError(t, err)
	destination, err := attribution.Open(fix.store, "stream", "destination-scoped")
	require.NoError(t, err)
	require.NoError(t, source.EnsureInitialPolicyHistory(policy, nil))
	require.NoError(t, destination.EnsureInitialPolicyHistory(policy, nil))
	dst, err := ingest.Open(ingest.Config{SegmentsDir: filepath.Join(fix.dataDir, "segments"), Store: fix.store, Logger: logger})
	require.NoError(t, err)
	for _, row := range []segment.Event{
		{Kind: segment.KindCreate, DID: "did:plc:scope", Rev: "rev1", Collection: "org.example.record", Rkey: "absent", Payload: []byte{0xa0}},
		{Kind: segment.KindCreate, DID: "did:plc:scope", Rev: "rev1", Collection: "org.example.other", Rkey: "unrelated", Payload: []byte{0xa0}},
		{Kind: segment.KindCreate, DID: "did:plc:scope", Rev: "rev3", Collection: "org.example.record", Rkey: "newer", Payload: []byte{0xa0}},
		{Kind: segment.KindCreate, DID: "did:plc:ordinary", Rev: "rev1", Collection: "org.example.record", Rkey: "ordinary-old", Payload: []byte{0xa0}},
	} {
		require.NoError(t, dst.Append(t.Context(), &row))
	}
	ordinary, err := (&comatproto.SyncSubscribeRepos_Sync{DID: "did:plc:ordinary", Rev: "rev2", Blocks: []byte{}}).MarshalCBOR()
	require.NoError(t, err)
	require.NoError(t, dst.Append(t.Context(), &segment.Event{Kind: segment.KindSync, DID: "did:plc:ordinary", Rev: "rev2", Payload: ordinary}))
	require.NoError(t, dst.SealActiveAndClose())
	dst, err = ingest.Open(ingest.Config{SegmentsDir: filepath.Join(fix.dataDir, "segments"), Store: fix.store, Logger: logger, Attribution: destination, AttributionDomain: "destination-scoped", CollectionPolicy: selectionManager})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dst.Close() })
	sourceDir := filepath.Join(fix.dataDir, "bootstrap-scoped")
	src, err := ingest.Open(ingest.Config{SegmentsDir: sourceDir, Store: fix.store, SeqKey: live.BootstrapSeqKey, Logger: logger, Attribution: source, AttributionDomain: "bootstrap-scoped", CollectionPolicy: selectionManager, AsyncFlushWorkers: 2, MaxEventsPerBlock: 1})
	require.NoError(t, err)
	return &scopedSyncMergeFixture{
		merge:                  fix,
		destinationWriter:      dst,
		sourceAttribution:      source,
		destinationAttribution: destination,
		selectionManager:       selectionManager,
		sourceDir:              sourceDir,
		sourceWriter:           src,
	}
}

func assertScopedSyncCompactionRows(t *testing.T, dataDir string, empty bool) {
	t.Helper()
	materialized := map[string]bool{}
	deleted := false
	for _, row := range readDestEvents(t, dataDir) {
		if row.Kind.IsMaterialization() {
			materialized[row.Rkey] = true
		}
		if row.Kind == segment.KindDelete && row.Rkey == "absent" {
			deleted = true
		}
	}
	require.True(t, deleted)
	require.False(t, materialized["absent"])
	require.False(t, materialized["ordinary-old"])
	require.True(t, materialized["unrelated"])
	require.True(t, materialized["newer"])
	require.Equal(t, !empty, materialized["latest"])
}
