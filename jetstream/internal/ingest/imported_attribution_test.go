package ingest

import (
	"context"
	"testing"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func TestAppendImportedRetainsOriginalSourceEvidence(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(map[bool]string{false: "sync", true: "async"}[async], func(t *testing.T) {
			cfg := Config{}
			if async {
				cfg.AsyncFlushWorkers = 2
			}
			w := newTestWriter(t, cfg)
			manager, err := attribution.Open(w.cfg.Store, "destination-stream", "imported-attribution")
			require.NoError(t, err)
			w.cfg.Attribution, w.cfg.AttributionDomain = manager, "imported-attribution"

			sourcePolicy := attribution.Policy{Revision: 1, Collections: []string{"org.example.record"}}
			destinationPolicy := attribution.Policy{Revision: 2, Collections: []string{"org.example.record"}}
			sourceRev := uint64(1)
			source := attribution.Event{StreamID: "source-stream", Seq: 17, PolicyRev: &sourceRev, Provenance: attribution.ProvenanceLive, Payload: []byte(`{"source":true}`)}
			event := &segment.Event{Seq: 17, Kind: segment.Kind(1), Collection: "org.example.record", DID: "did:plc:imported", Rkey: "record", Payload: []byte(`{"record":true}`), WitnessedAt: 100, IndexedAt: 200}
			original := *event
			destination := AttributionMetadata{StreamID: "destination-stream", Policy: destinationPolicy, Provenance: attribution.ProvenanceLive}

			require.NoError(t, w.AppendImported(context.Background(), event, source, "bootstrap-live", destination, sourcePolicy))
			require.EqualValues(t, 1, event.Seq)
			require.NotEqual(t, attribution.FingerprintSegment(original), attribution.FingerprintSegment(*event))
			require.NoError(t, w.Flush(context.Background()))
			got, err := manager.Lookup(event.Seq)
			require.NoError(t, err)
			require.EqualValues(t, 1, got.Seq)
			require.Equal(t, uint64(2), *got.PolicyRev)
			require.NotNil(t, got.Source)
			require.Equal(t, "bootstrap-live", got.Source.Domain)
			require.Equal(t, "source-stream", got.Source.StreamID)
			require.EqualValues(t, 17, got.Source.Seq)
			require.Equal(t, int64(100), got.Source.WitnessedAt)
			require.Equal(t, int64(200), got.Source.IndexedAt)
			require.Equal(t, attribution.FingerprintSegment(original), got.Source.SegmentFingerprint)
			require.NotEqual(t, attribution.FingerprintSegment(*event), got.Source.SegmentFingerprint)

			require.NoError(t, w.Close())
			w2, err := Open(w.cfg)
			require.NoError(t, err)
			defer w2.Close()
			recovered, err := manager.Lookup(event.Seq)
			require.NoError(t, err)
			require.Equal(t, got, recovered)
		})
	}
}

func TestAppendImportedRejectsInvalidSourceWithoutAllocating(t *testing.T) {
	w := newTestWriter(t, Config{})
	manager, err := attribution.Open(w.cfg.Store, "destination-stream", "imported-invalid")
	require.NoError(t, err)
	w.cfg.Attribution, w.cfg.AttributionDomain = manager, "imported-invalid"
	policy := attribution.Policy{Revision: 2, Collections: []string{"org.example.record"}}
	destination := AttributionMetadata{StreamID: "destination-stream", Policy: policy, Provenance: attribution.ProvenanceLive}
	event := &segment.Event{Kind: segment.Kind(1), Collection: "org.example.record", DID: "did:plc:imported", Rkey: "record", Payload: []byte(`{"record":true}`)}
	source := attribution.Event{StreamID: "source-stream", Seq: 17, Provenance: attribution.ProvenanceLegacyUnknown, Payload: []byte(`{"source":true}`)}

	require.Error(t, w.AppendImported(context.Background(), nil, source, "bootstrap-live", destination))
	require.Error(t, w.AppendImported(context.Background(), event, source, "", destination))
	require.Error(t, w.AppendImported(context.Background(), event, source, "bootstrap-live", destination, attribution.Policy{Revision: 1}))
	seqs, err := manager.PreparedSeqs()
	require.NoError(t, err)
	require.Empty(t, seqs)
	require.EqualValues(t, 1, w.nextSeq)
	require.Zero(t, w.active.Pending())

	// Unknown source policy does not imply an unknown source sequence.
	mismatchedEvent := *event
	mismatchedEvent.Seq = 18
	require.Error(t, w.AppendImported(context.Background(), &mismatchedEvent, source, "bootstrap-live", destination))
	seqs, err = manager.PreparedSeqs()
	require.NoError(t, err)
	require.Empty(t, seqs)
	require.EqualValues(t, 1, w.nextSeq)
	require.Zero(t, w.active.Pending())

	// Legacy/unknown source policy is explicitly null and carries no policy argument.
	event.Seq = source.Seq
	require.NoError(t, w.AppendImported(context.Background(), event, source, "bootstrap-live", destination))
	require.NoError(t, w.Flush(context.Background()))
	got, err := manager.Lookup(event.Seq)
	require.NoError(t, err)
	require.NotNil(t, got.Source)
	require.Nil(t, got.Source.Policy)
	require.Nil(t, got.Source.PolicyRev)
}
