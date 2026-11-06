package ingest

import (
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/history"
	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestOpenReplaysCapturedEventWithoutIntentFromJournalMetadata(t *testing.T) {
	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	defer db.Close()
	base, h, attr, _ := newCaptureWriterWithStore(t, db)
	body := []byte(`{"value":1}`)
	event := admittedEvent("3l3qo2vutsw2a", "replay", 123, 456)
	_, err = h.Append(history.Append{Source: captureSource(50, body), Kind: "create", DID: event.DID, Collection: event.Collection, Revision: event.Rev, Original: body, InputEvent: event, Policy: capturePolicy(), Provenance: string(attribution.ProvenanceLive), Disposition: history.Apply, Attribution: history.Attribution{StreamID: h.StreamID(), Policy: history.EffectivePolicy{Revision: 1, Collections: []string{"org.example.record"}}, Provenance: string(attribution.ProvenanceLive)}})
	require.NoError(t, err)
	cfg := base.cfg
	cfg.ReadLogRetentionBytes = 1 << 20
	require.NoError(t, base.Close())
	w, err := Open(cfg)
	require.NoError(t, err)
	applied, err := h.AppliedThrough()
	require.NoError(t, err)
	require.EqualValues(t, 1, applied)
	intent, found, err := h.ProjectionIntent(1)
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, 1, intent.SegmentSequence)
	observed, err := attr.Lookup(1)
	require.NoError(t, err)
	require.Equal(t, attribution.ProvenanceLive, observed.Provenance)
	page, err := h.Read(0, 1, 1)
	require.NoError(t, err)
	require.EqualValues(t, event.WitnessedAt, page.Events[0].InputEvent.WitnessedAt)
	require.EqualValues(t, event.IndexedAt, page.Events[0].InputEvent.IndexedAt)
	require.EqualValues(t, 2, w.ReadLog().TipSeq())
	require.EqualValues(t, 2, w.ReadLog().DurableSeq())
	readEvents, _, ok, _ := w.ReadLog().ReadCommittedFrom(1, 10)
	require.True(t, ok)
	require.Len(t, readEvents, 1)
	require.Equal(t, event.DID, readEvents[0].Event().DID)
	require.Equal(t, event.Rev, readEvents[0].Event().Rev)
	require.Equal(t, event.Payload, readEvents[0].Event().Payload)
	require.EqualValues(t, 1, w.NextSeq()-1)
	w.closed = true
	require.NoError(t, w.active.Close())
}

func TestOpenReplaysIntentWithMissingSegmentBytesOnceAcrossReopens(t *testing.T) {
	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	defer db.Close()
	root := t.TempDir()
	h, err := history.Open(db)
	require.NoError(t, err)
	attr, err := attribution.Open(db, h.StreamID(), "replay-missing")
	require.NoError(t, err)
	policy, err := selection.Open(db, []string{"org.example.record"})
	require.NoError(t, err)
	cfg := Config{SegmentsDir: filepath.Join(root, "segments"), Store: db, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: NewMetrics(prometheus.NewRegistry()), MaxEventsPerBlock: 1, History: h, Attribution: attr, AttributionDomain: "replay-missing", CollectionPolicy: policy}
	w, err := Open(cfg)
	require.NoError(t, err)
	body := []byte(`{"value":2}`)
	event := admittedEvent("3l3qo2vutsw2b", "missing", 222, 333)
	receipt, err := h.Append(history.Append{Source: captureSource(51, body), Kind: "create", DID: event.DID, Collection: event.Collection, Revision: event.Rev, Original: body, InputEvent: event, Policy: capturePolicy(), Provenance: string(attribution.ProvenanceLive), Disposition: history.Apply, Attribution: history.Attribution{StreamID: h.StreamID(), Policy: history.EffectivePolicy{Revision: 1, Collections: []string{"org.example.record"}}, Provenance: string(attribution.ProvenanceLive)}})
	require.NoError(t, err)
	planned := event
	planned.Seq = w.NextSeq()
	intent := history.ProjectionIntent{JournalSequence: receipt.Sequence, SegmentSequence: planned.Seq, Fingerprint: attribution.FingerprintSegment(planned), Disposition: history.Apply, PlannedEvent: &planned}
	require.NoError(t, h.PrepareProjection(intent))
	w.closed = true
	require.NoError(t, w.active.Close())
	w, err = Open(cfg)
	require.NoError(t, err)
	w.closed = true
	require.NoError(t, w.active.Close())
	w, err = Open(cfg)
	require.NoError(t, err)
	defer w.Close()
	applied, err := h.AppliedThrough()
	require.NoError(t, err)
	require.EqualValues(t, 1, applied)
	segmentRow, found, err := lookupProjectionSegment(cfg, 1)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, intent.Fingerprint, segmentRow.Fingerprint)
	require.EqualValues(t, 2, w.NextSeq())
	_, err = attr.Lookup(1)
	require.NoError(t, err)
}
