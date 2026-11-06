package ingest

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/internal/tombstone"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

func TestHypercertsScopedSnapshotPreservesConcurrentAndOtherCollections(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(dir, nil)
	require.NoError(t, err)
	defer db.Close()
	policy, err := selection.Open(db, []string{"app.bsky.feed.post", "app.bsky.feed.like"})
	require.NoError(t, err)
	cfg := Config{SegmentsDir: filepath.Join(dir, "segments"), Store: db, CollectionPolicy: policy, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	w, err := Open(cfg)
	require.NoError(t, err)
	record := func(coll, key, rev string, kind segment.Kind) segment.Event {
		return segment.Event{DID: "did:plc:snapshot", Collection: coll, Rkey: key, Rev: rev, Kind: kind}
	}
	for _, r := range []segment.Event{
		record("app.bsky.feed.like", "other", "3l3qo2vutsw2b", segment.KindCreate),
		record("app.bsky.feed.post", "remove", "3l3qo2vutsw2b", segment.KindCreate),
		record("app.bsky.feed.post", "newer", "3l3qo2vutsw2d", segment.KindUpdate),
	} {
		require.NoError(t, w.Append(t.Context(), &r))
	}
	snapshot := Snapshot{DID: "did:plc:snapshot", Rev: "3l3qo2vutsw2c", Collections: []string{"app.bsky.feed.post"}, Records: []segment.Event{record("app.bsky.feed.post", "newer", "3l3qo2vutsw2c", segment.KindCreateResync), record("app.bsky.feed.post", "snapshot", "3l3qo2vutsw2c", segment.KindCreateResync)}}
	require.NoError(t, w.ReconcileSnapshot(t.Context(), snapshot))
	next := w.NextSeq()
	require.NoError(t, w.ReconcileSnapshot(t.Context(), snapshot))
	require.Equal(t, next, w.NextSeq(), "retry must be idempotent")
	stale := record("app.bsky.feed.post", "snapshot", "3l3qo2vutsw2b", segment.KindDelete)
	require.NoError(t, w.Append(t.Context(), &stale))
	require.Zero(t, stale.Seq)
	require.NoError(t, w.Close())
	w, err = Open(cfg)
	require.NoError(t, err)
	defer w.Close()
	require.NoError(t, w.Append(t.Context(), &stale))
	require.Zero(t, stale.Seq, "snapshot boundary survives restart")
	files, err := SegmentFiles(cfg.SegmentsDir)
	require.NoError(t, err)
	var rows []segment.Event
	for _, file := range files {
		require.NoError(t, segment.WalkActive(file.Path, func(events []segment.Event) error { rows = append(rows, events...); return nil }))
	}
	var removed, other, newer int
	for _, r := range rows {
		require.NotEqual(t, segment.KindSync, r.Kind)
		switch r.Rkey {
		case "other":
			other++
			require.Equal(t, segment.KindCreate, r.Kind)
		case "newer":
			newer++
			require.Equal(t, "3l3qo2vutsw2d", r.Rev)
		case "remove":
			if r.Kind == segment.KindDelete {
				removed++
			}
		}
	}
	require.Equal(t, 1, other)
	require.Equal(t, 1, newer)
	require.Equal(t, 1, removed)
}

func TestNonHistoryManagedSnapshotBoundaryUsesBatchStartCoordinate(t *testing.T) {
	w := newTestWriter(t, Config{CollectionPolicy: nil})
	manager, err := attribution.Open(w.cfg.Store, "stream", "non-history-snapshot-boundary")
	require.NoError(t, err)
	w.cfg.Attribution = manager
	w.cfg.AttributionDomain = "non-history-snapshot-boundary"
	w.cfg.CollectionPolicy, err = selection.Open(w.cfg.Store, []string{"org.example.record"})
	require.NoError(t, err)
	policy := attribution.Policy{Revision: 1, Collections: []string{"org.example.record"}}
	start := w.NextSeq()
	boundarySeq, snapshotID := start, "normal-snapshot"
	snapshot := Snapshot{
		DID: "did:plc:normal-snapshot", Rev: "3l3qo2vutsw2b", Collections: policy.Collections,
		Records:        []segment.Event{{Kind: segment.KindCreateResync, DID: "did:plc:normal-snapshot", Rev: "3l3qo2vutsw2b", Collection: "org.example.record", Rkey: "r1", Payload: []byte(`{"value":true}`)}},
		AppendMetadata: &AttributionMetadata{StreamID: "stream", Policy: policy, Provenance: attribution.ProvenanceSnapshot, Boundary: attribution.Boundary{Seq: &boundarySeq, SnapshotID: &snapshotID}},
	}
	require.NoError(t, w.ReconcileSnapshot(t.Context(), snapshot))
	require.Greater(t, w.NextSeq(), start)
}

func TestManagedSnapshotCoordinateValidationDoesNotMutateBoundary(t *testing.T) {
	w, h, _, _ := newCaptureWriter(t)
	policy := attribution.Policy{Revision: 1, Collections: []string{"org.example.record"}}
	valid := func() Snapshot {
		seq, snapshotID, journalSeq := uint64(0), "snapshot-coordinate", uint64(1)
		return Snapshot{DID: "did:plc:snapshot-coordinate", Rev: "3l3qo2vutsw2b", Collections: []string{"org.example.record"}, Records: []segment.Event{{Kind: segment.KindCreateResync, DID: "did:plc:snapshot-coordinate", Collection: "org.example.record", Rkey: "r1", Rev: "3l3qo2vutsw2b", Payload: []byte(`{"value":true}`)}}, AppendMetadata: &AttributionMetadata{StreamID: h.StreamID(), Policy: policy, Provenance: attribution.ProvenanceSnapshot, Boundary: attribution.Boundary{Seq: &seq, SnapshotID: &snapshotID, JournalSequence: &journalSeq}}}
	}
	cases := []struct {
		name   string
		mutate func(*Snapshot)
	}{
		{"invalid sequence coordinate", func(s *Snapshot) { *s.AppendMetadata.Boundary.Seq = 99 }},
		{"missing sequence coordinate", func(s *Snapshot) { s.AppendMetadata.Boundary.Seq = nil }},
		{"missing coordinates", func(s *Snapshot) {
			s.AppendMetadata.Boundary = attribution.Boundary{Seq: new(uint64), SnapshotID: new("")}
		}},
		{"missing snapshot id", func(s *Snapshot) { s.AppendMetadata.Boundary.SnapshotID = nil }},
		{"cross collection", func(s *Snapshot) { s.AppendMetadata.Policy.Collections = []string{"org.other.record"} }},
		{"uncompleted boundary", func(s *Snapshot) { *s.AppendMetadata.Boundary.JournalSequence = 99 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := valid()
			tc.mutate(&snapshot)
			next := w.NextSeq()
			err := w.ReconcileSnapshot(t.Context(), snapshot)
			require.Error(t, err)
			require.Equal(t, next, w.NextSeq())
			stored, closer, lookupErr := w.cfg.Store.Get(snapshotKey(snapshot.DID))
			if lookupErr == nil {
				require.NoError(t, closer.Close())
				require.NotEmpty(t, stored)
			} else {
				require.ErrorIs(t, lookupErr, store.ErrNotFound)
			}
		})
	}
	t.Run("legacy metadata without coordinate is accepted", func(t *testing.T) {
		snapshot := valid()
		snapshot.AppendMetadata.Boundary = attribution.Boundary{}
		require.NoError(t, w.ReconcileSnapshot(t.Context(), snapshot))
		require.Greater(t, w.NextSeq(), uint64(1))
	})
}

func TestHypercertsManagedSnapshotWritesRowsAndAttribution(t *testing.T) {
	w := newTestWriter(t, Config{CollectionPolicy: nil})
	manager, err := attribution.Open(w.cfg.Store, "stream", "snapshot-attribution")
	require.NoError(t, err)
	w.cfg.Attribution = manager
	w.cfg.AttributionDomain = "snapshot-attribution"
	w.cfg.CollectionPolicy, err = selection.Open(w.cfg.Store, []string{"org.example.record"})
	require.NoError(t, err)
	policy := attribution.Policy{Revision: 1, Collections: []string{"org.example.record"}}
	snapshot := Snapshot{
		DID: "did:plc:snapshot-attribution", Rev: "rev2", Collections: []string{"org.example.record"},
		Records:        []segment.Event{{Kind: segment.KindCreateResync, DID: "did:plc:snapshot-attribution", Rev: "rev2", Collection: "org.example.record", Rkey: "present", Payload: []byte(`{"value":true}`)}},
		AppendMetadata: &AttributionMetadata{StreamID: "stream", Policy: policy, Provenance: attribution.ProvenanceSnapshot},
	}
	// Seed prior state so reconciliation must emit an actual generated delete.
	old := segment.Event{Kind: segment.KindCreate, DID: snapshot.DID, Rev: "rev1", Collection: "org.example.record", Rkey: "removed", Payload: []byte(`{"old":true}`)}
	old.Seq = w.nextSeq
	boundarySeq := uint64(1)
	boundaryID := "prior-snapshot"
	preparedOld, err := manager.PrepareSegment(old, "stream", policy, attribution.ProvenanceSnapshot, attribution.Boundary{Seq: &boundarySeq, SnapshotID: &boundaryID})
	require.NoError(t, err)
	old.Seq = preparedOld.Event.Seq
	_, err = w.active.Append(old)
	require.NoError(t, err)
	w.nextSeq++
	w.cfg.Metrics.setNextSeq(w.nextSeq)
	w.readLog.append(&old)
	require.NoError(t, w.Flush(context.Background()))
	before := w.NextSeq()
	require.NoError(t, w.ReconcileSnapshot(context.Background(), snapshot))
	require.Equal(t, before+2, w.NextSeq(), "snapshot replacement and generated delete are appended")
	require.NoError(t, w.Flush(context.Background()))
	files, err := SegmentFiles(w.cfg.SegmentsDir)
	require.NoError(t, err)
	var replacement, generatedDelete *segment.Event
	for _, file := range files {
		require.NoError(t, segment.WalkActive(file.Path, func(events []segment.Event) error {
			for i := range events {
				row := events[i]
				if row.Seq < before {
					continue
				}
				if row.Rkey == "present" && row.Kind == segment.KindUpdate {
					replacement = &row
				}
				if row.Rkey == "removed" && row.Kind == segment.KindDelete {
					generatedDelete = &row
				}
			}
			return nil
		}))
	}
	require.NotNil(t, replacement)
	require.NotNil(t, generatedDelete)
	for _, row := range []*segment.Event{replacement, generatedDelete} {
		got, err := manager.Lookup(row.Seq)
		require.NoError(t, err)
		require.Equal(t, attribution.ProvenanceSnapshot, got.Provenance)
		require.EqualValues(t, 1, *got.PolicyRev)
	}
}

func TestHypercertsMixedSnapshotSyncAndConcurrentAccountDeletion(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(dir, nil)
	require.NoError(t, err)
	defer db.Close()
	policy, err := selection.Open(db, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	w, err := Open(Config{Logger: slog.Default(), SegmentsDir: filepath.Join(dir, "segments"), Store: db, CollectionPolicy: policy})
	require.NoError(t, err)
	defer w.Close()
	did := "did:plc:snapshot"
	post := segment.Event{DID: did, Rev: "3l3qo2vutsw2d", Kind: segment.KindCreateResync, Collection: "app.bsky.feed.post", Rkey: "new"}
	require.NoError(t, w.ReconcileSnapshot(t.Context(), Snapshot{DID: did, Rev: post.Rev, Collections: []string{post.Collection}, Records: []segment.Event{post}}))
	_, err = policy.Update(1, []string{"app.bsky.feed.post", "app.bsky.feed.like"}, nil)
	require.NoError(t, err)
	like := segment.Event{DID: did, Rev: "3l3qo2vutsw2b", Kind: segment.KindCreate, Collection: "app.bsky.feed.like", Rkey: "missing"}
	require.NoError(t, w.Append(t.Context(), &like))
	handled, err := w.ReconcileSync(t.Context(), did, "3l3qo2vutsw2c", nil)
	require.NoError(t, err)
	require.True(t, handled)
	require.NoError(t, w.Flush(t.Context()))
	files, err := SegmentFiles(filepath.Join(dir, "segments"))
	require.NoError(t, err)
	var rows []segment.Event
	for _, f := range files {
		require.NoError(t, segment.WalkActive(f.Path, func(es []segment.Event) error { rows = append(rows, es...); return nil }))
	}
	ts := tombstone.New()
	for i := range rows {
		err := ts.Observe(&rows[i])
		require.NoError(t, err)
	}
	survived := false
	deletedLike := false
	for _, r := range rows {
		require.NotEqual(t, segment.KindSync, r.Kind)
		if r.Rkey == "new" {
			drop, _ := ts.Snapshot().ShouldDrop(&r)
			require.False(t, drop)
			survived = true
		}
		if r.Rkey == "missing" && r.Kind == segment.KindDelete {
			deletedLike = true
		}
	}
	require.True(t, survived)
	require.True(t, deletedLike)
	account := comatproto.SyncSubscribeRepos_Account{DID: did, Active: false, Status: gt.Some("deleted")}
	payload, err := account.MarshalCBOR()
	require.NoError(t, err)
	ev := segment.Event{Kind: segment.KindAccount, DID: did, Payload: payload}
	require.NoError(t, w.Append(t.Context(), &ev))
	next := w.NextSeq()
	post.Rev = "3l3qo2vutsw2e"
	post.Rkey = "resurrect"
	require.ErrorIs(t, w.ReconcileSnapshot(t.Context(), Snapshot{DID: did, Rev: post.Rev, Collections: []string{post.Collection}, Records: []segment.Event{post}}), ErrAccountUnavailable)
	require.Equal(t, next, w.NextSeq())
}

func TestHypercertsFirstSyncAlwaysUsesSnapshotSerialization(t *testing.T) {
	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	defer db.Close()
	policy, err := selection.Open(db, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	called := false
	w, err := Open(Config{Logger: slog.Default(), SegmentsDir: filepath.Join(t.TempDir(), "segments"), Store: db, CollectionPolicy: policy, ReconcileSnapshot: func(_ context.Context, s Snapshot) error {
		called = true
		require.Equal(t, "did:plc:first", s.DID)
		return nil
	}})
	require.NoError(t, err)
	defer w.Close()
	handled, err := w.ReconcileSync(t.Context(), "did:plc:first", "3l3qo2vutsw2c", nil)
	require.NoError(t, err)
	require.True(t, handled)
	require.True(t, called)
}
