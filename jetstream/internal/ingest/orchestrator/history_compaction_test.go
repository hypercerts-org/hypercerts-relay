package orchestrator

import (
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/history"
	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/internal/tombstone"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func TestHistorySurvivesActualSegmentCompactionAndRestart(t *testing.T) {
	dir := t.TempDir()
	segmentsDir := filepath.Join(dir, "segments")
	db, err := store.Open(filepath.Join(dir, "db"), nil)
	require.NoError(t, err)
	journal, err := history.Open(db)
	require.NoError(t, err)
	const domain = "history-compaction"
	policyValue := attribution.Policy{Revision: 1, Collections: []string{"org.example.record", "org.example.unselected"}}
	attr, err := attribution.Open(db, journal.StreamID(), domain)
	require.NoError(t, err)
	require.NoError(t, attr.EnsureInitialPolicyHistory(policyValue, nil))
	policy, err := selection.Open(db, policyValue.Collections)
	require.NoError(t, err)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tombstones := tombstone.New()
	writer, err := ingest.Open(ingest.Config{SegmentsDir: segmentsDir, Store: db, Logger: logger, MaxEventsPerBlock: 64, OnAppend: func(event *segment.Event) error { return tombstones.Observe(event) }, Attribution: attr, AttributionDomain: domain, History: journal, HistoryProjectionTarget: history.TargetMain, CollectionPolicy: policy})
	require.NoError(t, err)
	appendOriginal := func(cursor uint64, event segment.Event, original []byte) {
		event.Payload = append([]byte(nil), original...)
		_, appendErr := writer.CaptureAndProject(t.Context(), history.Source{Domain: domain, Epoch: "relay-epoch", Cursor: cursor, Path: event.Collection + "/" + event.Rkey, ContentHash: history.OriginalHash(original)}, event, original, history.Policy{Revision: 1, Collections: policyValue.Collections}, string(attribution.ProvenanceLive), ingest.AttributionMetadata{StreamID: journal.StreamID(), Policy: policyValue, Provenance: attribution.ProvenanceLive})
		require.NoError(t, appendErr)
	}
	selected := "org.example.record"
	appendOriginal(1, segment.Event{Kind: segment.KindCreate, DID: "did:plc:selected", Collection: selected, Rkey: "same", Rev: "1", WitnessedAt: 1}, []byte(`{"version":1}`))
	appendOriginal(2, segment.Event{Kind: segment.KindUpdate, DID: "did:plc:selected", Collection: selected, Rkey: "same", Rev: "2", WitnessedAt: 2}, []byte(`{"version":2}`))
	appendOriginal(3, segment.Event{Kind: segment.KindDelete, DID: "did:plc:selected", Collection: selected, Rkey: "same", Rev: "3", WitnessedAt: 3}, []byte(`{"delete":true}`))
	appendOriginal(4, segment.Event{Kind: segment.KindCreate, DID: "did:plc:selected", Collection: selected, Rkey: "same", Rev: "4", WitnessedAt: 4}, []byte(`{"version":4}`))
	appendOriginal(5, segment.Event{Kind: segment.KindCreate, DID: "did:plc:selected", Collection: "org.example.unselected", Rkey: "unrelated", Rev: "1", WitnessedAt: 5}, []byte(`{"unselected":true}`))
	snapshot := ingest.Snapshot{DID: "did:plc:selected", Rev: "9zzzzzzzzzzz", Collections: []string{selected}, Records: []segment.Event{{Kind: segment.KindCreateResync, DID: "did:plc:selected", Collection: selected, Rkey: "same", Rev: "9zzzzzzzzzzz", Payload: []byte(`{"version":"snapshot"}`)}}, AppendMetadata: &ingest.AttributionMetadata{StreamID: journal.StreamID(), Policy: policyValue, Provenance: attribution.ProvenanceSnapshot}}
	require.NoError(t, writer.ReconcileSnapshot(t.Context(), snapshot))
	appendOriginal(6, segment.Event{Kind: segment.KindUpdate, DID: "did:plc:selected", Collection: selected, Rkey: "same", Rev: "5", WitnessedAt: 6}, []byte(`{"version":5}`))
	appendOriginal(7, segment.Event{Kind: segment.KindDelete, DID: "did:plc:selected", Collection: selected, Rkey: "same", Rev: "6", WitnessedAt: 7}, []byte(`{"delete":true}`))
	require.NoError(t, writer.SealActiveAndClose())
	before, err := journal.Read(0, 0, 100)
	require.NoError(t, err)
	require.NotEmpty(t, before.Events)
	beforeTip := before.Tip
	beforeEvents := append([]history.Event(nil), before.Events...)
	beforeSegments, err := ingest.SegmentFiles(segmentsDir)
	require.NoError(t, err)
	var beforeCount int
	for _, file := range beforeSegments {
		beforeCount += len(readCompactionSegment(t, file.Path))
	}
	require.Greater(t, beforeCount, 0)

	o := &Orchestrator{cfg: Config{DataDir: dir, Store: db, Logger: logger, Tombstones: tombstones, CompactionInterval: time.Hour}, logger: logger}
	require.NoError(t, o.runDeleteCompaction(t.Context(), compactionMergeTail, nil))
	afterSegments, err := ingest.SegmentFiles(segmentsDir)
	require.NoError(t, err)
	var afterCount int
	var current []segment.Event
	for _, file := range afterSegments {
		events := readCompactionSegment(t, file.Path)
		afterCount += len(events)
		current = append(current, events...)
	}
	require.Less(t, afterCount, beforeCount, "compaction must physically remove superseded projection events")
	var latestSelected, unrelated, didWideTombstone int
	for _, event := range current {
		if event.DID == "did:plc:selected" && event.Collection == selected && event.Rkey == "same" && event.Kind != segment.KindDelete {
			latestSelected++
			require.Equal(t, "9zzzzzzzzzzz", event.Rev, "snapshot must remain current despite delayed originals")
		}
		if event.Rkey == "unrelated" {
			unrelated++
		}
		if event.Kind == segment.KindSync && event.DID == "did:plc:selected" {
			didWideTombstone++
		}
	}
	require.Equal(t, 1, latestSelected)
	require.Equal(t, 1, unrelated)
	require.Zero(t, didWideTombstone)
	after, err := journal.Read(0, 0, 100)
	require.NoError(t, err)
	require.Equal(t, beforeTip, after.Tip)
	require.Equal(t, beforeEvents, after.Events, "compaction must not alter canonical journal event bytes or metadata")
	require.NoError(t, writer.Close())
	require.NoError(t, db.Close())

	db, err = store.Open(filepath.Join(dir, "db"), nil)
	require.NoError(t, err)
	defer db.Close()
	reopened, err := history.Open(db)
	require.NoError(t, err)
	reopenedPage, err := reopened.Read(0, 0, 100)
	require.NoError(t, err)
	require.Equal(t, beforeTip, reopenedPage.Tip)
	require.Equal(t, beforeEvents, reopenedPage.Events)
}
