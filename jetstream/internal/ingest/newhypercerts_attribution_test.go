package ingest

import (
	"context"
	"testing"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func TestAppendBatchAttributedPreflightsEntireBatchBeforePrepare(t *testing.T) {
	w := newTestWriter(t, Config{})
	manager, err := attribution.Open(w.cfg.Store, "stream", "selected-preflight")
	require.NoError(t, err)
	w.cfg.Attribution, w.cfg.AttributionDomain = manager, "selected-preflight"
	policy := attribution.Policy{Revision: 1, Collections: []string{"org.example.record"}}
	first := segment.Event{Kind: segment.Kind(1), Collection: "org.example.record", DID: "did:plc:test", Rkey: "first", Payload: []byte(`{"ok":true}`)}
	invalid := segment.Event{Kind: segment.Kind(1), Collection: "org.example.record", DID: "did:plc:test", Rkey: "invalid", Payload: []byte(`{`), Rev: string(make([]byte, 256))}
	batch := []segment.Event{first, invalid}
	metadata := AttributionMetadata{StreamID: "stream", Policy: policy, Provenance: attribution.ProvenanceLive}
	require.Error(t, w.AppendBatchAttributed(context.Background(), batch, metadata))
	seqs, err := manager.PreparedSeqs()
	require.NoError(t, err)
	require.Empty(t, seqs)
	require.EqualValues(t, 1, w.nextSeq)
	require.Zero(t, w.active.Pending())

	batch[1] = segment.Event{Kind: segment.Kind(1), Collection: "org.example.record", DID: "did:plc:test", Rkey: "second", Payload: []byte(`{"ok":true}`)}
	require.NoError(t, w.AppendBatchAttributed(context.Background(), batch, metadata))
	require.EqualValues(t, 1, batch[0].Seq)
	require.EqualValues(t, 2, batch[1].Seq)
}

func TestAppendBatchFilteredRowsKeepOriginalIndexAndCallback(t *testing.T) {
	w := newTestWriter(t, Config{})
	policy, err := selection.Open(w.cfg.Store, []string{"org.example.accepted"})
	require.NoError(t, err)
	w.cfg.CollectionPolicy = policy
	var callbacks []*segment.Event
	w.cfg.OnAppend = func(event *segment.Event) error { callbacks = append(callbacks, event); return nil }
	filtered := segment.Event{Kind: segment.Kind(1), Collection: "org.example.rejected", DID: "did:plc:test", Rkey: "filtered", Payload: []byte(`{"ok":true}`)}
	accepted := segment.Event{Kind: segment.Kind(1), Collection: "org.example.accepted", DID: "did:plc:test", Rkey: "accepted", Payload: []byte(`{"ok":true}`)}
	batch := []segment.Event{filtered, accepted}
	require.NoError(t, w.AppendBatch(context.Background(), batch))
	require.Zero(t, batch[0].Seq)
	require.Zero(t, batch[0].IndexedAt)
	require.EqualValues(t, 1, batch[1].Seq)
	require.Len(t, callbacks, 1)
	require.Same(t, &batch[1], callbacks[0])
	require.Zero(t, batch[0].IndexedAt)
}
