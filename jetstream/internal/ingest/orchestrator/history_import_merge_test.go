package orchestrator

import (
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/history"
	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/policygate"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/live"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func TestRunMergeImportsHistoryOnceAcrossRetry(t *testing.T) {
	fix := newMergeFixture(t, [][]segment.Event{{ev("did:plc:history-merge", "3l3", segment.KindCreate, 100)}}, nil)
	journal, err := history.Open(fix.store)
	require.NoError(t, err)
	const stream = "merge-history-stream"
	source, err := attribution.Open(fix.store, stream, "merge-history-source")
	require.NoError(t, err)
	destination, err := attribution.Open(fix.store, stream, "merge-history-destination")
	require.NoError(t, err)
	policy := attribution.Policy{Revision: 1, Collections: []string{"app.bsky.feed.post"}}
	require.NoError(t, source.EnsureInitialPolicyHistory(policy, nil))
	require.NoError(t, destination.EnsureInitialPolicyHistory(policy, nil))
	selectionPolicy, err := selection.Open(fix.store, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	bootstrapDir := filepath.Join(fix.dataDir, "backfill", "live_segments")
	bootstrapWriter, err := ingest.Open(ingest.Config{SegmentsDir: bootstrapDir, Store: fix.store, SeqKey: live.BootstrapSeqKey, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Attribution: source, AttributionDomain: "merge-history-source", History: journal, HistoryProjectionTarget: history.TargetBootstrapLive, CollectionPolicy: selectionPolicy})
	require.NoError(t, err)
	original := ev("did:plc:history-merge", "3l4", segment.KindCreate, 200)
	originalBytes := []byte(`{"operation":"create"}`)
	capture, err := bootstrapWriter.CaptureAndProject(t.Context(), history.Source{Domain: "merge-history-source", Epoch: "bootstrap", Cursor: 1, Path: original.Collection + "/" + original.Rkey, ContentHash: history.OriginalHash(originalBytes)}, original, originalBytes, history.Policy{Revision: 1, Collections: policy.Collections}, string(attribution.ProvenanceLive), ingest.AttributionMetadata{StreamID: stream, Policy: policy, Provenance: attribution.ProvenanceLive})
	require.NoError(t, err)
	require.Equal(t, uint64(1), capture.JournalSeq)
	require.NoError(t, bootstrapWriter.SealActiveAndClose())
	fix.cfg.History, fix.cfg.BootstrapAttribution, fix.cfg.Attribution = journal, source, destination
	fix.cfg.BootstrapAttributionDomain, fix.cfg.AttributionDomain = "merge-history-source", "merge-history-destination"
	fix.cfg.AttributionStreamID, fix.cfg.CollectionPolicy = stream, selectionPolicy
	fix.cfg.SkipMergeDiscovery = true
	sentinel := errors.New("after destination fsync before source checkpoint")
	fix.cfg.CrashInjector = pointErrorInjector{point: crashpoint.AfterMergeDstFlushBeforeSourceCommit, err: sentinel}
	o, err := New(fix.cfg)
	require.NoError(t, err)
	firstTime := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	mergeRunner := newMergeRunner(nil, fix.store, filepath.Join(fix.dataDir, "backfill", "live_segments"), nil, fix.cfg.Logger, nil, fix.cfg.CrashInjector)
	mergeRunner.dst, err = o.openMergeDestination(filepath.Join(fix.dataDir, "segments"))
	require.NoError(t, err)
	mergeRunner.sourceAttribution, mergeRunner.sourceDomain = source, "merge-history-source"
	mergeRunner.destinationAttribution, mergeRunner.destinationStreamID = destination, stream
	mergeRunner.sourceAttributionStreamID, mergeRunner.collectionPolicy = stream, selectionPolicy
	mergeRunner.policyGate = policygate.New(selectionPolicy, mergeRunner.dst, "merge-history-destination")
	mergeRunner.now = func() time.Time { return firstTime }
	require.ErrorIs(t, mergeRunner.run(t.Context()), sentinel)
	require.NoError(t, mergeRunner.dst.Close())
	cur, err := loadMergeCursor(fix.store)
	require.NoError(t, err)
	require.Zero(t, cur)
	firstImport, err := destination.Lookup(1)
	require.NoError(t, err)
	require.Equal(t, attribution.ProvenanceReconciliation, firstImport.Provenance)
	require.NotNil(t, firstImport.Source)
	require.Equal(t, int64(100), firstImport.Source.WitnessedAt)
	destinationFiles, err := filepath.Glob(filepath.Join(fix.dataDir, "segments", "seg_*.jss"))
	require.NoError(t, err)
	require.NotEmpty(t, destinationFiles)
	require.NoError(t, fix.store.Close())

	fix.store, err = store.Open(fix.dataDir, nil)
	require.NoError(t, err)
	defer fix.store.Close()
	journal, err = history.Open(fix.store)
	require.NoError(t, err)
	source, err = attribution.Open(fix.store, stream, "merge-history-source")
	require.NoError(t, err)
	destination, err = attribution.Open(fix.store, stream, "merge-history-destination")
	require.NoError(t, err)
	selectionPolicy, err = selection.Open(fix.store, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	fix.cfg.Store, fix.cfg.History = fix.store, journal
	fix.cfg.BootstrapAttribution, fix.cfg.Attribution = source, destination
	fix.cfg.CollectionPolicy = selectionPolicy
	fix.cfg.CrashInjector = nil
	secondTime := time.Date(2026, time.January, 2, 3, 5, 5, 0, time.UTC)
	mergeRunner.dst, err = ingest.Open(ingest.Config{SegmentsDir: filepath.Join(fix.dataDir, "segments"), Store: fix.store, SeqKey: live.SteadySeqKey, Logger: fix.cfg.Logger, Attribution: destination, AttributionDomain: "merge-history-destination", History: journal, HistoryProjectionTarget: history.TargetMain, CollectionPolicy: selectionPolicy})
	require.NoError(t, err)
	mergeRunner.store, mergeRunner.sourceAttribution, mergeRunner.destinationAttribution = fix.store, source, destination
	mergeRunner.cache = newRepoStatusLookup(fix.store, nil)
	mergeRunner.crashInjector = nil
	mergeRunner.now = func() time.Time { return secondTime }
	require.NoError(t, mergeRunner.run(t.Context()))
	require.NoError(t, mergeRunner.dst.SealActiveAndClose())
	beforeNextCapture, err := journal.Tip()
	require.NoError(t, err)
	page, err := journal.Read(0, 0, 20)
	require.NoError(t, err)
	require.Len(t, page.Events, 1)
	require.Equal(t, originalBytes, page.Events[0].Original)
	importedRows := 0
	for _, event := range readDestEvents(t, fix.dataDir) {
		if event.Rkey == original.Rkey {
			importedRows++
		}
	}
	require.Equal(t, 1, importedRows)
	nextWriter, err := ingest.Open(ingest.Config{SegmentsDir: filepath.Join(fix.dataDir, "segments"), Store: fix.store, SeqKey: live.SteadySeqKey, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), History: journal, Attribution: destination, AttributionDomain: "merge-history-destination", HistoryProjectionTarget: history.TargetMain, CollectionPolicy: selectionPolicy})
	require.NoError(t, err)
	followup := segment.Event{Kind: segment.KindCreate, DID: "did:plc:next", Collection: "app.bsky.feed.post", Rkey: "next", Payload: []byte("next")}
	_, err = nextWriter.CaptureAndProject(t.Context(), history.Source{Domain: "post-merge-live", Epoch: "1", Cursor: 1, Path: "app.bsky.feed.post/next", ContentHash: history.OriginalHash([]byte("next"))}, followup, []byte("next"), history.Policy{Revision: 1, Collections: policy.Collections}, "live", ingest.AttributionMetadata{StreamID: fix.cfg.AttributionStreamID, Policy: policy, Provenance: attribution.ProvenanceLive})
	require.NoError(t, err)
	require.NoError(t, nextWriter.DrainDurability(t.Context()))
	followupAttribution, err := destination.Lookup(nextWriter.NextSeq() - 1)
	require.NoError(t, err)
	require.Equal(t, attribution.ProvenanceLive, followupAttribution.Provenance)
	require.NoError(t, nextWriter.Close())
	afterNextCapture, err := journal.Tip()
	require.NoError(t, err)
	require.Equal(t, beforeNextCapture+1, afterNextCapture)
}
