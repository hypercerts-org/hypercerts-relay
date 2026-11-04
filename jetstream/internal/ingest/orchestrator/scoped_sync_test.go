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
			defer dst.Close()
			sourceDir := filepath.Join(fix.dataDir, "bootstrap-scoped")
			src, err := ingest.Open(ingest.Config{SegmentsDir: sourceDir, Store: fix.store, SeqKey: live.BootstrapSeqKey, Logger: logger, Attribution: source, AttributionDomain: "bootstrap-scoped", CollectionPolicy: selectionManager, AsyncFlushWorkers: 2, MaxEventsPerBlock: 1})
			require.NoError(t, err)
			snapshot := ingest.Snapshot{DID: "did:plc:scope", Rev: "rev2", Collections: policy.Collections, AppendMetadata: &ingest.AttributionMetadata{StreamID: "stream", Policy: policy, Provenance: attribution.ProvenanceReconciliation}}
			paths := []string{}
			if !empty {
				snapshot.Records = []segment.Event{{Kind: segment.KindCreateResync, DID: snapshot.DID, Rev: snapshot.Rev, Collection: "org.example.record", Rkey: "latest", Payload: []byte{0xa0}}}
				paths = append(paths, "org.example.record/latest")
			}
			require.NoError(t, src.ReconcileSnapshot(t.Context(), snapshot))
			payload, err := (&comatproto.SyncSubscribeRepos_Sync{DID: snapshot.DID, Rev: snapshot.Rev, Blocks: []byte{}}).MarshalCBOR()
			require.NoError(t, err)
			payload, err = tombstone.EncodeScopedSync(payload, tombstone.SyncScope{Collections: policy.Collections, Paths: paths})
			require.NoError(t, err)
			require.NoError(t, src.AppendAttributed(t.Context(), &segment.Event{Kind: segment.KindSync, DID: snapshot.DID, Rev: snapshot.Rev, Payload: payload}, ingest.AttributionMetadata{StreamID: "stream", Policy: policy, Provenance: attribution.ProvenanceLive}))
			require.NoError(t, src.SealActiveAndClose())
			runner := newMergeRunner(dst, fix.store, sourceDir, nil, logger, nil, nil)
			runner.sourceAttribution, runner.sourceDomain, runner.sourceAttributionStreamID = source, "bootstrap-scoped", "stream"
			runner.destinationAttribution, runner.destinationStreamID = destination, "stream"
			runner.collectionPolicy, runner.policyGate = selectionManager, policygate.New(selectionManager, dst, "destination-scoped")
			files, err := ingest.SegmentFiles(sourceDir)
			require.NoError(t, err)
			for _, file := range files {
				_, err = runner.processSourceSegment(t.Context(), file)
				require.NoError(t, err)
			}
			require.NoError(t, dst.SealActiveAndClose())
			fix.cfg.CompactionInterval = time.Hour
			o, err := New(fix.cfg)
			require.NoError(t, err)
			require.NoError(t, o.runDeleteCompaction(t.Context(), compactionMergeTail, nil))
			materialized := map[string]bool{}
			deleted := false
			for _, row := range readDestEvents(t, fix.dataDir) {
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
		})
	}
}
