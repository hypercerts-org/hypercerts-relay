package ingest

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func TestRecoverAttributionFindsPreparedEventInSealedSegment(t *testing.T) {
	w := newTestWriter(t, Config{})
	event := &segment.Event{Kind: segment.Kind(1), Collection: "org.example.record", DID: "did:plc:test", Rkey: "sealed", Payload: []byte(`{"ok":true}`)}
	require.NoError(t, w.Append(context.Background(), event))
	require.NoError(t, w.Flush(context.Background()))
	require.NoError(t, w.SealActiveAndClose())

	files, err := SegmentFilesFS(nil, w.cfg.SegmentsDir)
	require.NoError(t, err)
	r, err := segment.Open(segment.ReaderConfig{Path: files[0].Path})
	require.NoError(t, err)
	events, err := r.DecodeBlock(0)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	require.Len(t, events, 1)
	durable := events[0]
	manager, err := attribution.Open(w.cfg.Store, "stream", "sealed-recovery")
	require.NoError(t, err)
	policy := attribution.Policy{Revision: 1, Collections: []string{"org.example.record"}}
	_, err = manager.PrepareSegment(durable, "stream", policy, attribution.ProvenanceLive, attribution.Boundary{})
	require.NoError(t, err)

	cfg := w.cfg
	cfg.Attribution = manager
	cfg.AttributionDomain = "sealed-recovery"
	require.NoError(t, recoverAttributions(cfg))
	got, err := manager.Lookup(event.Seq)
	require.NoError(t, err)
	require.EqualValues(t, event.Seq, got.Seq)
	seqs, err := manager.PreparedSeqs()
	require.NoError(t, err)
	require.Empty(t, seqs)
}

func TestRecoverAttributionIgnoresTornTailAndRemovesMissingPreparedRow(t *testing.T) {
	w := newTestWriter(t, Config{})
	event := &segment.Event{Kind: segment.Kind(1), Collection: "org.example.record", DID: "did:plc:test", Rkey: "complete", Payload: []byte(`{"ok":true}`)}
	require.NoError(t, w.Append(context.Background(), event))
	require.NoError(t, w.Flush(context.Background()))

	segmentPath := filepath.Join(w.cfg.SegmentsDir, SegmentFilename(w.activeIdx))
	f, err := os.OpenFile(segmentPath, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.Write([]byte{0x01, 0x02, 0x03, 0x04})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	manager, err := attribution.Open(w.cfg.Store, "stream", "torn-tail-recovery")
	require.NoError(t, err)
	policy := attribution.Policy{Revision: 1, Collections: []string{"org.example.record"}}
	durable := *event
	prepared, err := manager.PrepareSegment(durable, "stream", policy, attribution.ProvenanceLive, attribution.Boundary{})
	require.NoError(t, err)
	require.NoError(t, manager.Commit(prepared, policy, nil))
	missing := segment.Event{Seq: event.Seq + 1, Kind: segment.Kind(1), Collection: "org.example.record", DID: "did:plc:test", Rkey: "missing", Payload: []byte(`{"ok":false}`)}
	_, err = manager.PrepareSegment(missing, "stream", policy, attribution.ProvenanceLive, attribution.Boundary{})
	require.NoError(t, err)

	cfg := w.cfg
	cfg.Attribution = manager
	cfg.AttributionDomain = "torn-tail-recovery"
	require.NoError(t, recoverAttributions(cfg))
	got, err := manager.Lookup(event.Seq)
	require.NoError(t, err)
	require.EqualValues(t, event.Seq, got.Seq)
	seqs, err := manager.PreparedSeqs()
	require.NoError(t, err)
	require.Empty(t, seqs)
}
