package ingest

import (
	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRejectedBatchSequencesAndImportedSource(t *testing.T) {
	w := newTestWriter(t, Config{})
	policy, err := selection.Open(w.cfg.Store, []string{"org.example.record"})
	require.NoError(t, err)
	w.cfg.CollectionPolicy = policy
	require.NoError(t, w.persistSnapshotBoundary(Snapshot{DID: "did:plc:test", Rev: "rev2", Collections: []string{"org.example.record"}}))
	manager, err := attribution.Open(w.cfg.Store, "stream", "batch-source")
	require.NoError(t, err)
	w.cfg.Attribution, w.cfg.AttributionDomain = manager, "batch-source"
	metadata := AttributionMetadata{StreamID: "stream", Policy: attribution.Policy{Revision: 1, Collections: []string{"org.example.record"}}, Provenance: attribution.ProvenanceLive}
	var callbacks []*segment.Event
	w.cfg.OnAppend = func(event *segment.Event) error { callbacks = append(callbacks, event); return nil }
	rows := []segment.Event{
		{Seq: 70, IndexedAt: 123, Kind: segment.KindCreate, Collection: "org.example.excluded", DID: "did:plc:test", Rkey: "filtered"},
		{Seq: 71, IndexedAt: 124, Kind: segment.KindCreate, Collection: "org.example.record", DID: "did:plc:test", Rkey: "stale", Rev: "rev1"},
		{Seq: 72, IndexedAt: 125, Kind: segment.KindCreate, Collection: "org.example.record", DID: "did:plc:test", Rkey: "accepted", Rev: "rev3"},
	}
	require.NoError(t, w.AppendBatchAttributed(t.Context(), rows, metadata))
	require.Zero(t, rows[0].Seq)
	require.Zero(t, rows[1].Seq)
	require.EqualValues(t, 123, rows[0].IndexedAt)
	require.EqualValues(t, 124, rows[1].IndexedAt)
	require.EqualValues(t, 1, rows[2].Seq)
	require.Len(t, callbacks, 1)
	require.Same(t, &rows[2], callbacks[0])
	require.Equal(t, 1, w.active.Pending())
	imported := segment.Event{Seq: 90, IndexedAt: 126, Kind: segment.KindCreate, Collection: "org.example.record", DID: "did:plc:test", Rkey: "imported", Rev: "rev4"}
	evidence := attribution.ImportedSource{Domain: "bootstrap-live", StreamID: "source-stream", Seq: 90, Provenance: attribution.ProvenanceLegacyUnknown, IndexedAt: imported.IndexedAt, SegmentFingerprint: attribution.FingerprintSegment(imported)}
	metadata.Source = &evidence
	batch := []segment.Event{imported}
	require.NoError(t, w.AppendBatchAttributed(t.Context(), batch, metadata))
	require.EqualValues(t, 2, batch[0].Seq)
	require.NoError(t, w.Flush(t.Context()))
	got, err := manager.Lookup(2)
	require.NoError(t, err)
	require.Equal(t, attribution.FingerprintSegment(imported), got.Source.SegmentFingerprint)
	batch = []segment.Event{imported}
	batch[0].Seq++
	require.ErrorIs(t, w.AppendBatchAttributed(t.Context(), batch, metadata), attribution.ErrInvalid)
	require.EqualValues(t, 3, w.NextSeq())
}
