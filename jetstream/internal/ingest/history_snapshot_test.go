package ingest

import (
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
	"github.com/stretchr/testify/require"
)

func TestReconcileSnapshotHistoryResumesImmutablePlanAfterPartialCommit(t *testing.T) {
	dir := t.TempDir()
	faultErr := errors.New("planned interruption")
	fault := &store.KeyPrefixFault{Prefix: []byte("selected-history/v1/event/"), Op: store.WriteOpBatchCommit, Ordinal: 2, Err: faultErr}
	db, err := store.Open(filepath.Join(dir, "db"), nil, store.WithFaultInjector(fault))
	require.NoError(t, err)
	policy, err := selection.Open(db, []string{"org.example.record"})
	require.NoError(t, err)
	attr, err := attribution.Open(db, "snapshot-resume", "snapshot-resume")
	require.NoError(t, err)
	journal, err := history.Open(db)
	require.NoError(t, err)
	cfg := Config{SegmentsDir: filepath.Join(dir, "segments"), Store: db, CollectionPolicy: policy, Attribution: attr, AttributionDomain: "snapshot-resume", History: journal, HistoryProjectionTarget: history.TargetMain, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	w, err := Open(cfg)
	require.NoError(t, err)
	snapshot := Snapshot{DID: "did:plc:resume", Rev: "3l3qo2vutsw2b", Collections: []string{"org.example.record"}, Records: []segment.Event{
		{Kind: segment.KindCreateResync, DID: "did:plc:resume", Collection: "org.example.record", Rkey: "first", Rev: "3l3qo2vutsw2b", Payload: []byte(`{"n":1}`)},
		{Kind: segment.KindCreateResync, DID: "did:plc:resume", Collection: "org.example.record", Rkey: "second", Rev: "3l3qo2vutsw2b", Payload: []byte(`{"n":2}`)},
	}, AppendMetadata: &AttributionMetadata{StreamID: "snapshot-resume", Policy: attribution.Policy{Revision: 1, Collections: []string{"org.example.record"}}, Provenance: attribution.ProvenanceSnapshot}}
	err = w.ReconcileSnapshot(t.Context(), snapshot)
	require.ErrorIs(t, err, faultErr)
	page, err := journal.Read(0, 0, 10)
	require.NoError(t, err)
	require.Len(t, page.Events, 1)
	require.Equal(t, "first", page.Events[0].InputEvent.Rkey)
	require.NoError(t, w.Close())
	require.NoError(t, db.Close())
	db, err = store.Open(filepath.Join(dir, "db"), nil)
	require.NoError(t, err)
	defer db.Close()
	cfg.Store = db
	cfg.History, err = history.Open(db)
	require.NoError(t, err)
	cfg.Attribution, err = attribution.Open(db, "snapshot-resume", "snapshot-resume")
	require.NoError(t, err)
	w, err = Open(cfg)
	require.NoError(t, err)
	defer w.Close()
	require.NoError(t, w.ReconcileSnapshot(t.Context(), snapshot))
	page, err = cfg.History.Read(0, 0, 10)
	require.NoError(t, err)
	require.Len(t, page.Events, 3)
	require.Equal(t, "first", page.Events[0].InputEvent.Rkey)
	require.Equal(t, "second", page.Events[1].InputEvent.Rkey)
	require.Equal(t, "snapshot-boundary", page.Events[2].Source.Path)
	freshMetadata := &AttributionMetadata{StreamID: "snapshot-resume", Policy: attribution.Policy{Revision: 99, Collections: []string{"org.example.record"}}, Provenance: attribution.ProvenanceSnapshot}
	snapshot.AppendMetadata = freshMetadata
	require.NoError(t, w.ReconcileSnapshot(t.Context(), snapshot))
	require.NotNil(t, freshMetadata.Boundary.JournalSequence)
	require.Equal(t, uint64(4), *freshMetadata.Boundary.JournalSequence)
	require.NotNil(t, freshMetadata.Boundary.Seq)
	require.Equal(t, uint64(3), *freshMetadata.Boundary.Seq)
	require.Equal(t, uint64(99), freshMetadata.Policy.Revision)
}

func TestSnapshotHistorySurvivesSegmentCompactionAndRestart(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(filepath.Join(dir, "db"), nil)
	require.NoError(t, err)
	policy, err := selection.Open(db, []string{"org.example.record"})
	require.NoError(t, err)
	attr, err := attribution.Open(db, "compact-snapshot", "compact-snapshot")
	require.NoError(t, err)
	journal, err := history.Open(db)
	require.NoError(t, err)
	cfg := Config{SegmentsDir: filepath.Join(dir, "segments"), Store: db, CollectionPolicy: policy, Attribution: attr, AttributionDomain: "compact-snapshot", History: journal, HistoryProjectionTarget: history.TargetMain, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	w, err := Open(cfg)
	require.NoError(t, err)
	snapshot := Snapshot{DID: "did:plc:compact", Rev: "3l3qo2vutsw2b", Collections: []string{"org.example.record"}, Records: []segment.Event{{Kind: segment.KindCreateResync, DID: "did:plc:compact", Collection: "org.example.record", Rkey: "replacement", Rev: "3l3qo2vutsw2b", Payload: []byte(`{"n":1}`)}}, AppendMetadata: &AttributionMetadata{StreamID: "compact-snapshot", Policy: attribution.Policy{Revision: 1, Collections: []string{"org.example.record"}}, Provenance: attribution.ProvenanceSnapshot}}
	require.NoError(t, w.ReconcileSnapshot(t.Context(), snapshot))
	other := segment.Event{Kind: segment.KindCreate, DID: "did:plc:other", Collection: "org.example.record", Rkey: "unrelated", Rev: "3l3qo2vutsw2b", Payload: []byte(`{"n":2}`)}
	otherBytes := []byte(`{"n":2}`)
	other.Payload = otherBytes
	_, err = w.CaptureAndProject(t.Context(), history.Source{Domain: "compact-snapshot", Epoch: "live", Cursor: 1, Path: "org.example.record/unrelated", ContentHash: history.OriginalHash(otherBytes)}, other, otherBytes, history.Policy{Revision: 1, Collections: []string{"org.example.record"}}, string(attribution.ProvenanceLive), AttributionMetadata{StreamID: "compact-snapshot", Policy: attribution.Policy{Revision: 1, Collections: []string{"org.example.record"}}, Provenance: attribution.ProvenanceLive})
	require.NoError(t, err)
	require.NoError(t, w.Flush(t.Context()))
	before, err := journal.Read(0, 0, 10)
	require.NoError(t, err)
	require.Equal(t, []history.Disposition{history.Apply, history.Noop, history.Apply}, []history.Disposition{before.Events[0].Disposition, before.Events[1].Disposition, before.Events[2].Disposition})
	require.NoError(t, w.Close())
	require.NoError(t, db.Close())
	db, err = store.Open(filepath.Join(dir, "db"), nil)
	require.NoError(t, err)
	defer db.Close()
	journal, err = history.Open(db)
	require.NoError(t, err)
	page, err := journal.Read(0, 0, 10)
	require.NoError(t, err)
	require.Len(t, page.Events, 3)
	require.Equal(t, history.Noop, page.Events[1].Disposition)
	files, err := SegmentFiles(filepath.Join(dir, "segments"))
	require.NoError(t, err)
	var current []segment.Event
	for _, file := range files {
		require.NoError(t, segment.WalkActive(file.Path, func(events []segment.Event) error { current = append(current, events...); return nil }))
	}
	var replacement, unrelated, syncRows int
	for _, event := range current {
		if event.Kind == segment.KindSync {
			syncRows++
		}
		if event.Rkey == "replacement" {
			replacement++
		}
		if event.Rkey == "unrelated" {
			unrelated++
		}
	}
	require.Zero(t, syncRows)
	require.Equal(t, 1, replacement)
	require.Equal(t, 1, unrelated)
}

func TestReconcileEmptySnapshotBoundaryAndFrozenPolicyRetry(t *testing.T) {
	w, journal, attr, policy := newCaptureWriter(t)
	snapshot := Snapshot{DID: "did:plc:empty-snapshot", Rev: "3l3qo2vutsw2b", Collections: []string{"org.example.record"}, AppendMetadata: &AttributionMetadata{StreamID: journal.StreamID(), Policy: attribution.Policy{Revision: 1, Collections: []string{"org.example.record"}}, Provenance: attribution.ProvenanceSnapshot}}
	require.NoError(t, w.ReconcileSnapshot(t.Context(), snapshot))
	next := w.NextSeq()
	page, err := journal.Read(0, 0, 10)
	require.NoError(t, err)
	require.Len(t, page.Events, 1)
	require.Equal(t, "snapshot-boundary", page.Events[0].Source.Path)
	_, err = policy.Update(1, nil, nil)
	require.NoError(t, err)
	require.NoError(t, w.ReconcileSnapshot(t.Context(), snapshot))
	page, err = journal.Read(0, 0, 10)
	require.NoError(t, err)
	require.Len(t, page.Events, 1)
	require.Equal(t, next, w.NextSeq())
	require.Equal(t, uint64(1), page.Events[0].Policy.Revision)
	require.NotNil(t, attr)
}
