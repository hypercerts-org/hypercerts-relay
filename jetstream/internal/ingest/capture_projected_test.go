package ingest

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/history"
	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func newCaptureWriter(t *testing.T) (*Writer, *history.Store, *attribution.Manager, *selection.Manager) {
	return newCaptureWriterWithStore(t, newTestStore(t))
}

func newCaptureWriterWithStore(t *testing.T, db *store.Store) (*Writer, *history.Store, *attribution.Manager, *selection.Manager) {
	t.Helper()
	h, err := history.Open(db)
	require.NoError(t, err)
	attr, err := attribution.Open(db, h.StreamID(), "capture-test")
	require.NoError(t, err)
	policy, err := selection.Open(db, []string{"org.example.record"})
	require.NoError(t, err)
	w, err := Open(Config{SegmentsDir: filepath.Join(t.TempDir(), "segments"), Store: db, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: NewMetrics(prometheus.NewRegistry()), MaxEventsPerBlock: 128, History: h, Attribution: attr, AttributionDomain: "capture-test", HistoryProjectionTarget: history.TargetMain, CollectionPolicy: policy})
	require.NoError(t, err)
	writer := w
	t.Cleanup(func() { _ = writer.Close() })
	return w, h, attr, policy
}

func captureMetadata(h *history.Store) AttributionMetadata {
	return AttributionMetadata{StreamID: h.StreamID(), Policy: attribution.Policy{Revision: 1, Collections: []string{"org.example.record"}}, Provenance: attribution.ProvenanceLive}
}

func capturedEvent(rev, rkey string) segment.Event {
	return segment.Event{Kind: segment.KindCreate, DID: "did:plc:capture", Collection: "org.example.record", Rkey: rkey, Rev: rev, Payload: []byte(`{"v":1}`), WitnessedAt: 100, IndexedAt: 101}
}

func captureSnapshotEvent() segment.Event {
	return segment.Event{Kind: segment.KindCreateResync, DID: "did:plc:capture", Collection: "org.example.record", Rkey: "snapshot", Rev: "3l3qo2vutsw2b", Payload: []byte(`{"v":1}`), WitnessedAt: 100, IndexedAt: 101}
}

func admittedEvent(rev, rkey string, observed, indexed int64) segment.Event {
	event := capturedEvent(rev, rkey)
	event.WitnessedAt, event.IndexedAt = observed, indexed
	return event
}

func captureSource(cursor uint64, body []byte) history.Source {
	return history.Source{Domain: "capture-test", Epoch: "epoch", Cursor: cursor, Operation: 0, Path: "org.example.record/r1", ContentHash: history.OriginalHash(body)}
}

func capturePolicy() history.Policy {
	return history.Policy{Revision: 1, Collections: []string{"org.example.record"}}
}

func TestCaptureAndProjectKeepsDelayedOriginalHistoricalOnly(t *testing.T) {
	w, h, _, _ := newCaptureWriter(t)
	meta := captureMetadata(h)
	meta.Boundary = attribution.Boundary{Seq: new(uint64), SnapshotID: new("snapshot-1")}
	*meta.Boundary.Seq = 1
	snapshotBody := []byte("snapshot source")
	snapshotSource := captureSource(1, snapshotBody)
	snapshotReceipt, err := h.Append(history.Append{Source: snapshotSource, Kind: "snapshot", DID: "did:plc:capture", Collection: "org.example.record", Revision: "3l3qo2vutsw2b", Original: snapshotBody, InputEvent: captureSnapshotEvent(), Policy: capturePolicy(), Provenance: string(attribution.ProvenanceSnapshot), Disposition: history.Apply, Attribution: history.Attribution{StreamID: h.StreamID(), Policy: history.EffectivePolicy{Revision: 1, Collections: []string{"org.example.record"}}, Provenance: string(attribution.ProvenanceSnapshot), Boundary: history.AttributionBoundary{Seq: new(uint64), SnapshotID: new("snapshot-1")}}})
	require.NoError(t, err)
	newer := captureSnapshotEvent()
	newer.Seq = 1
	snapshotMeta := meta
	snapshotMeta.Provenance = attribution.ProvenanceSnapshot
	snapshotMeta.Boundary = attribution.Boundary{Seq: new(uint64), SnapshotID: new("snapshot-1"), JournalSequence: new(snapshotReceipt.Sequence)}
	*snapshotMeta.Boundary.Seq = 1
	require.NoError(t, w.AppendProjected(t.Context(), newer, snapshotReceipt.Sequence, snapshotMeta))
	require.NoError(t, w.Flush(t.Context()))
	require.NoError(t, w.ForceRotate(t.Context()))
	boundary := Snapshot{DID: newer.DID, Rev: newer.Rev, Collections: []string{newer.Collection}, AppendMetadata: &snapshotMeta}
	require.NoError(t, w.persistSnapshotBoundary(boundary))
	older := capturedEvent("3l3qo2vutsw2a", "r1")
	body := []byte("original older")
	liveMeta := meta
	liveMeta.Boundary = attribution.Boundary{}
	receipt, err := w.CaptureAndProject(t.Context(), captureSource(10, body), older, body, capturePolicy(), "live", liveMeta)
	require.NoError(t, err)
	require.Equal(t, history.HistoricalOnly, receipt.Disposition)
	require.Zero(t, receipt.SegmentSeq)
	require.EqualValues(t, 2, receipt.JournalSeq)
	require.EqualValues(t, 2, w.nextSeq)
	applied, err := h.AppliedThrough()
	require.NoError(t, err)
	require.EqualValues(t, 2, applied)
	page, err := h.Read(0, 10, 10)
	require.NoError(t, err)
	require.Len(t, page.Events, 2)
	require.Equal(t, history.HistoricalOnly, page.Events[1].Disposition)
	require.Equal(t, snapshotReceipt.Sequence, page.Events[1].Boundary)
	storedBoundary, closer, err := w.cfg.Store.Get(snapshotKey(newer.DID))
	require.NoError(t, err)
	var persisted snapshotBoundary
	require.NoError(t, json.Unmarshal(storedBoundary, &persisted))
	require.NoError(t, closer.Close())
	require.Equal(t, snapshotReceipt.Sequence, persisted.Collections[newer.Collection].JournalSequence)
	files, err := SegmentFiles(w.cfg.SegmentsDir)
	require.NoError(t, err)
	require.Len(t, files, 2)
	reader, err := segment.Open(segment.ReaderConfig{Path: files[0].Path})
	require.NoError(t, err)
	projected, err := reader.DecodeBlock(0)
	require.NoError(t, err)
	require.Len(t, projected, 1)
	require.Equal(t, newer.Rev, projected[0].Rev)
	require.NoError(t, reader.Close())
	require.EqualValues(t, 2, w.nextSeq)
}

func TestDelayedOriginalCreateUpdateDeleteRemainHistoricalOnly(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind segment.Kind
	}{{"create", segment.KindCreate}, {"update", segment.KindUpdate}, {"delete", segment.KindDelete}} {
		t.Run(tc.name, func(t *testing.T) {
			kind := tc.kind
			w, h, _, _ := newCaptureWriter(t)
			meta := captureMetadata(h)
			body := []byte(`{"value":"snapshot"}`)
			snapshot := captureSnapshotEvent()
			snapshot.Rkey = "same-rkey"
			snapshot.Payload = body
			snapshot.Seq = 1
			snapshotReceipt, err := h.Append(history.Append{Source: captureSource(1, []byte("snapshot")), Kind: "snapshot", DID: snapshot.DID, Collection: snapshot.Collection, Revision: snapshot.Rev, Original: []byte("snapshot"), InputEvent: snapshot, Policy: capturePolicy(), Provenance: string(attribution.ProvenanceSnapshot), Disposition: history.Apply, Attribution: history.Attribution{StreamID: h.StreamID(), Policy: history.EffectivePolicy{Revision: 1, Collections: []string{"org.example.record"}}, Provenance: string(attribution.ProvenanceSnapshot), Boundary: history.AttributionBoundary{Seq: new(uint64), SnapshotID: new("snapshot-1")}}})
			require.NoError(t, err)
			snapshotMeta := meta
			snapshotMeta.Provenance = attribution.ProvenanceSnapshot
			snapshotMeta.Boundary = attribution.Boundary{Seq: new(uint64), SnapshotID: new("snapshot-1"), JournalSequence: new(snapshotReceipt.Sequence)}
			*snapshotMeta.Boundary.Seq = 1
			require.NoError(t, w.AppendProjected(t.Context(), snapshot, snapshotReceipt.Sequence, snapshotMeta))
			require.NoError(t, w.Flush(t.Context()))
			require.NoError(t, w.ForceRotate(t.Context()))
			require.NoError(t, w.persistSnapshotBoundary(Snapshot{DID: snapshot.DID, Rev: snapshot.Rev, Collections: []string{snapshot.Collection}, AppendMetadata: &snapshotMeta}))
			older := segment.Event{Kind: kind, DID: snapshot.DID, Collection: snapshot.Collection, Rkey: snapshot.Rkey, Rev: "3l3qo2vutsw2a", Payload: body, WitnessedAt: 200, IndexedAt: 201}
			if kind == segment.KindDelete {
				older.Payload = nil
			}
			original := []byte(tc.name + " original")
			receipt, err := w.CaptureAndProject(t.Context(), captureSource(2, original), older, original, capturePolicy(), "live", meta)
			require.NoError(t, err)
			require.Equal(t, history.HistoricalOnly, receipt.Disposition)
			require.Zero(t, receipt.SegmentSeq)
			page, err := h.Read(0, 10, 10)
			require.NoError(t, err)
			require.Len(t, page.Events, 2)
			require.Equal(t, kind, page.Events[1].InputEvent.Kind)
			if kind != segment.KindDelete {
				require.Equal(t, body, page.Events[1].InputEvent.Payload)
			} else {
				require.Empty(t, page.Events[1].InputEvent.Payload)
			}
			files, err := SegmentFiles(w.cfg.SegmentsDir)
			require.NoError(t, err)
			reader, err := segment.Open(segment.ReaderConfig{Path: files[0].Path})
			require.NoError(t, err)
			rows, err := reader.DecodeBlock(0)
			require.NoError(t, err)
			require.NoError(t, reader.Close())
			require.Len(t, rows, 1)
			require.Equal(t, snapshot.Rev, rows[0].Rev)
		})
	}
}

func TestCaptureAndProjectCloseReopenPreservesSnapshotBoundaryAndHistory(t *testing.T) {
	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	defer db.Close()
	w, h, _, _ := newCaptureWriterWithStore(t, db)
	meta := captureMetadata(h)
	snapshotBody := []byte(`{"value":"snapshot"}`)
	snapshot := captureSnapshotEvent()
	snapshot.Rkey, snapshot.Payload, snapshot.Seq = "durable-rkey", snapshotBody, 1
	snapshotReceipt, err := h.Append(history.Append{Source: captureSource(1, []byte("snapshot")), Kind: "snapshot", DID: snapshot.DID, Collection: snapshot.Collection, Revision: snapshot.Rev, Original: []byte("snapshot"), InputEvent: snapshot, Policy: capturePolicy(), Provenance: string(attribution.ProvenanceSnapshot), Disposition: history.Apply, Attribution: history.Attribution{StreamID: h.StreamID(), Policy: history.EffectivePolicy{Revision: 1, Collections: []string{"org.example.record"}}, Provenance: string(attribution.ProvenanceSnapshot), Boundary: history.AttributionBoundary{Seq: new(uint64), SnapshotID: new("snapshot-1")}}})
	require.NoError(t, err)
	snapshotMeta := meta
	snapshotMeta.Provenance = attribution.ProvenanceSnapshot
	snapshotMeta.Boundary = attribution.Boundary{Seq: new(uint64), SnapshotID: new("snapshot-durable"), JournalSequence: new(snapshotReceipt.Sequence)}
	*snapshotMeta.Boundary.Seq = 1
	require.NoError(t, w.AppendProjected(t.Context(), snapshot, snapshotReceipt.Sequence, snapshotMeta))
	require.NoError(t, w.Flush(t.Context()))
	require.NoError(t, w.ForceRotate(t.Context()))
	require.NoError(t, w.persistSnapshotBoundary(Snapshot{DID: snapshot.DID, Rev: snapshot.Rev, Collections: []string{snapshot.Collection}, AppendMetadata: &snapshotMeta}))
	older := capturedEvent("3l3qo2vutsw2a", "durable-rkey")
	older.Payload = []byte(`{"value":"original"}`)
	original := []byte("delayed-original")
	receipt, err := w.CaptureAndProject(t.Context(), captureSource(2, original), older, original, capturePolicy(), "live", meta)
	require.NoError(t, err)
	require.Equal(t, history.HistoricalOnly, receipt.Disposition)
	cfg := w.cfg
	require.NoError(t, w.Close())
	w = nil
	w, err = Open(cfg)
	require.NoError(t, err)
	defer w.Close()
	reopenedHistory, err := history.Open(db)
	require.NoError(t, err)
	tip, err := reopenedHistory.Tip()
	require.NoError(t, err)
	require.EqualValues(t, 2, tip)
	page, err := reopenedHistory.Read(1, 10, 10)
	require.NoError(t, err)
	require.Len(t, page.Events, 1)
	require.Equal(t, original, page.Events[0].Original)
	stored, closer, err := db.Get(snapshotKey(snapshot.DID))
	require.NoError(t, err)
	var persisted snapshotBoundary
	require.NoError(t, json.Unmarshal(stored, &persisted))
	require.NoError(t, closer.Close())
	require.Equal(t, snapshotReceipt.Sequence, persisted.Collections[snapshot.Collection].JournalSequence)
	later := capturedEvent("3l3qo2vutsw2a", "durable-rkey")
	laterBody := []byte("delayed after reopen")
	laterReceipt, err := w.CaptureAndProject(t.Context(), captureSource(3, laterBody), later, laterBody, capturePolicy(), "live", captureMetadata(reopenedHistory))
	require.NoError(t, err)
	require.Equal(t, history.HistoricalOnly, laterReceipt.Disposition)
	require.Zero(t, laterReceipt.SegmentSeq)
}

func TestCaptureAndProjectRejectsCallerSequenceAndInputSubstitution(t *testing.T) {
	w, h, attr, _ := newCaptureWriter(t)
	body := []byte("frozen source")
	event := capturedEvent("3l3qo2vutsw2a", "r1")
	event.Seq = 999
	source := captureSource(999, body)
	receipt, err := w.CaptureAndProject(t.Context(), source, event, body, capturePolicy(), "live", captureMetadata(h))
	require.NoError(t, err)
	require.EqualValues(t, 1, receipt.JournalSeq)
	require.EqualValues(t, 1, receipt.SegmentSeq)
	require.NoError(t, w.Flush(t.Context()))
	require.NoError(t, w.ForceRotate(t.Context()))
	observed, err := attr.Lookup(receipt.SegmentSeq)
	require.NoError(t, err)
	require.Equal(t, receipt.SegmentSeq, observed.Seq)
	files, err := SegmentFiles(w.cfg.SegmentsDir)
	require.NoError(t, err)
	r, err := segment.Open(segment.ReaderConfig{Path: files[0].Path})
	require.NoError(t, err)
	rows, err := r.DecodeBlock(0)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	require.Len(t, rows, 1)
	require.EqualValues(t, 999, event.Seq)
	require.EqualValues(t, 1, rows[0].Seq)
	require.EqualValues(t, event.WitnessedAt, rows[0].WitnessedAt)
	require.EqualValues(t, event.IndexedAt, rows[0].IndexedAt)
	changed := event
	changed.Payload = []byte(`{"v":2}`)
	_, err = w.CaptureAndProject(t.Context(), source, changed, body, capturePolicy(), "live", captureMetadata(h))
	require.ErrorIs(t, err, history.ErrConflict)
}

func TestCaptureAndProjectDeduplicatesIgnoringLocalTimes(t *testing.T) {
	w, h, attr, _ := newCaptureWriter(t)
	body := []byte("same source content")
	first := admittedEvent("3l3qo2vutsw2a", "r1", 100, 101)
	a, err := w.CaptureAndProject(t.Context(), captureSource(20, body), first, body, capturePolicy(), "live", captureMetadata(h))
	require.NoError(t, err)
	require.NoError(t, w.Flush(t.Context()))
	require.NotZero(t, a.SegmentSeq)
	firstObserved, err := attr.Lookup(a.SegmentSeq)
	require.NoError(t, err)
	require.JSONEq(t, `{"operation":"1"}`, string(firstObserved.Payload))
	redelivery := first
	redelivery.WitnessedAt, redelivery.IndexedAt = 900, 901
	b, err := w.CaptureAndProject(t.Context(), captureSource(20, body), redelivery, body, capturePolicy(), "live", captureMetadata(h))
	require.NoError(t, err)
	require.Equal(t, a.JournalSeq, b.JournalSeq)
	require.Equal(t, a.SegmentSeq, b.SegmentSeq)
	require.Equal(t, a.Disposition, b.Disposition)
	require.False(t, b.NewProjection)
	require.EqualValues(t, 2, w.nextSeq)
	secondObserved, err := attr.Lookup(a.SegmentSeq)
	require.NoError(t, err)
	require.Equal(t, firstObserved, secondObserved)
	page, err := h.Read(0, 10, 10)
	require.NoError(t, err)
	require.Len(t, page.Events, 1)
}

func TestCaptureAndProjectJournalCommitFaultIsRetrySafe(t *testing.T) {
	db, err := store.Open(t.TempDir(), nil, store.WithFaultInjector(&store.KeyPrefixFault{Prefix: []byte("selected-history/v1/event/"), Op: store.WriteOpBatchCommit, Ordinal: 1, Err: errors.New("injected journal commit failure")}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	w, h, _, _ := newCaptureWriterWithStore(t, db)
	body := []byte("faulted source")
	_, err = w.CaptureAndProject(t.Context(), captureSource(30, body), capturedEvent("3l3qo2vutsw2a", "r1"), body, capturePolicy(), "live", captureMetadata(h))
	require.Error(t, err)
	tip, err := h.Tip()
	require.NoError(t, err)
	require.Zero(t, tip)
	applied, err := h.AppliedThrough()
	require.NoError(t, err)
	require.Zero(t, applied)
	require.EqualValues(t, 1, w.nextSeq)
	require.EqualValues(t, 1, w.durableNextSeq)
	_, found, err := h.LookupSource(captureSource(30, body))
	require.NoError(t, err)
	require.False(t, found)
	pending, err := h.PendingProjections()
	require.NoError(t, err)
	require.Empty(t, pending)
	receipt, err := w.CaptureAndProject(t.Context(), captureSource(30, body), capturedEvent("3l3qo2vutsw2a", "r1"), body, capturePolicy(), "live", captureMetadata(h))
	require.NoError(t, err)
	require.EqualValues(t, 1, receipt.JournalSeq)
	require.EqualValues(t, 1, receipt.SegmentSeq)
	require.EqualValues(t, 2, w.nextSeq)
}

func TestCaptureAndProjectRedeliveryUsesFrozenPolicyAfterRemoval(t *testing.T) {
	w, h, _, policy := newCaptureWriter(t)
	body := []byte("accepted under revision one")
	source := captureSource(40, body)
	event := admittedEvent("3l3qo2vutsw2a", "r1", 100, 101)
	first, err := w.CaptureAndProject(t.Context(), source, event, body, capturePolicy(), "live", captureMetadata(h))
	require.NoError(t, err)
	_, err = policy.Update(1, nil, nil)
	require.NoError(t, err)
	redelivery := event
	redelivery.WitnessedAt, redelivery.IndexedAt = 2000, 2001
	receipt, err := w.CaptureAndProject(t.Context(), source, redelivery, body, capturePolicy(), "live", captureMetadata(h))
	require.NoError(t, err)
	require.False(t, receipt.NewProjection)
	require.Equal(t, first.JournalSeq, receipt.JournalSeq)
	require.Equal(t, first.SegmentSeq, receipt.SegmentSeq)
	require.Equal(t, first.Disposition, receipt.Disposition)
	require.EqualValues(t, 2, w.nextSeq)
	page, err := h.Read(0, 10, 10)
	require.NoError(t, err)
	require.Len(t, page.Events, 1)
	require.Equal(t, capturePolicy(), page.Events[0].Policy)
	require.Equal(t, string(attribution.ProvenanceLive), page.Events[0].Attribution.Provenance)
	require.EqualValues(t, event.WitnessedAt, page.Events[0].InputEvent.WitnessedAt)
	require.EqualValues(t, event.IndexedAt, page.Events[0].InputEvent.IndexedAt)
	newBody := []byte("new coordinate after removal")
	_, err = w.CaptureAndProject(t.Context(), captureSource(41, newBody), capturedEvent("3l3qo2vutsw2b", "r2"), newBody, history.Policy{Revision: 2}, "live", AttributionMetadata{StreamID: h.StreamID(), Policy: attribution.Policy{Revision: 2}, Provenance: attribution.ProvenanceLive})
	require.ErrorContains(t, err, "denied")
}
