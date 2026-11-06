package orchestrator

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

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

func TestOrchestratorCaptureGenerationAndSnapshotOwnership(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "db"), nil)
	require.NoError(t, err)
	defer st.Close()
	journal, err := history.Open(st)
	require.NoError(t, err)
	domain := sourceDomain("https://example.invalid")
	attr, err := attribution.Open(st, journal.StreamID(), domain)
	require.NoError(t, err)
	policy, err := selection.Open(st, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	effective := attribution.Policy{Revision: 1, Collections: []string{"app.bsky.feed.post"}}
	require.NoError(t, attr.EnsureInitialPolicyHistory(effective, nil))
	o := &Orchestrator{cfg: Config{DataDir: dir, Store: st, RelayURL: "https://example.invalid", Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), History: journal, BootstrapAttribution: attr, Attribution: attr, BootstrapAttributionDomain: domain, AttributionDomain: domain, AttributionStreamID: journal.StreamID(), CollectionPolicy: policy}}
	bootCapture, steadyCapture := bootstrapHistoryCapture(o), steadyHistoryCapture(o)
	require.NotNil(t, bootCapture)
	require.NotNil(t, steadyCapture)
	require.Equal(t, bootCapture.Epoch, steadyCapture.Epoch)
	generation := journal.CaptureGeneration()
	require.Equal(t, generation, bootCapture.Epoch)
	require.NotEqual(t, "upstream-epoch", generation)

	boot, err := ingest.Open(ingest.Config{SegmentsDir: filepath.Join(dir, "backfill", "live_segments"), Store: st, SeqKey: live.BootstrapSeqKey, Logger: o.cfg.Logger, Attribution: attr, AttributionDomain: bootCapture.Domain, History: journal, HistoryProjectionTarget: history.TargetBootstrapLive, CollectionPolicy: policy})
	require.NoError(t, err)
	mainAttr, err := attribution.Open(st, journal.StreamID(), domain+"-main")
	require.NoError(t, err)
	require.NoError(t, mainAttr.EnsureInitialPolicyHistory(effective, nil))
	main, err := ingest.Open(ingest.Config{SegmentsDir: filepath.Join(dir, "segments"), Store: st, SeqKey: live.SteadySeqKey, Logger: o.cfg.Logger, Attribution: mainAttr, AttributionDomain: domain + "-main", History: journal, HistoryProjectionTarget: history.TargetMain, CollectionPolicy: policy})
	require.NoError(t, err)
	defer boot.Close()
	defer main.Close()
	body := []byte(`{"text":"accepted original"}`)
	event := segment.Event{Kind: segment.KindCreate, DID: "did:plc:history-owner", Collection: "app.bsky.feed.post", Rkey: "original", Rev: "3l3qo2vutsw2b", Payload: body}
	source := history.Source{Domain: domain, Epoch: bootCapture.Epoch, Cursor: 77, Operation: 0, Path: event.Collection + "/" + event.Rkey, ContentHash: history.OriginalHash(body)}
	admitted := ingest.AttributionMetadata{StreamID: journal.StreamID(), Policy: effective, Provenance: attribution.ProvenanceLive}
	receipt, err := boot.CaptureAndProject(t.Context(), source, event, body, history.Policy{Revision: 1, Collections: effective.Collections}, string(attribution.ProvenanceLive), admitted)
	require.NoError(t, err)
	require.EqualValues(t, 1, receipt.JournalSeq)
	require.EqualValues(t, 2, boot.NextSeq())

	boundary := ingest.Snapshot{DID: event.DID, Rev: "3l3qoz2vutsw2b", Collections: effective.Collections, Records: []segment.Event{{Kind: segment.KindCreateResync, DID: event.DID, Collection: event.Collection, Rkey: "snapshot", Rev: "3l3qoz2vutsw2b", Payload: []byte(`{"text":"snapshot"}`)}}, AppendMetadata: &ingest.AttributionMetadata{StreamID: journal.StreamID(), Policy: effective, Provenance: attribution.ProvenanceSnapshot}}
	require.NoError(t, main.ReconcileSnapshot(t.Context(), boundary))
	mainSeqAfterSnapshot := main.NextSeq()
	mainApplied, err := journal.AppliedThroughFor(history.TargetMain)
	require.NoError(t, err)
	require.Greater(t, mainApplied, uint64(0))
	require.NoError(t, main.Close())
	main, err = ingest.Open(ingest.Config{SegmentsDir: filepath.Join(dir, "segments"), Store: st, SeqKey: live.SteadySeqKey, Logger: o.cfg.Logger, Attribution: mainAttr, AttributionDomain: domain + "-main", History: journal, HistoryProjectionTarget: history.TargetMain, CollectionPolicy: policy})
	require.NoError(t, err)
	require.Equal(t, mainSeqAfterSnapshot, main.NextSeq())

	// The main source reader sees the same upstream delivery after cutover.
	// It must retain the bootstrap-owned receipt, not create or project it again.
	receipt, err = main.CaptureAndProject(t.Context(), source, event, body, history.Policy{Revision: 1, Collections: effective.Collections}, string(attribution.ProvenanceLive), admitted)
	require.NoError(t, err)
	require.EqualValues(t, 1, receipt.JournalSeq)
	require.Equal(t, mainSeqAfterSnapshot, main.NextSeq())
	original, found, err := journal.LookupSource(source)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, history.TargetBootstrapLive, original.Owner)
	require.Equal(t, body, original.InputEvent.Payload)
	require.Equal(t, uint64(1), original.Sequence)
	require.Equal(t, string(attribution.ProvenanceLive), original.Provenance)

	require.NoError(t, main.Close())
	require.NoError(t, boot.Close())
	require.NoError(t, st.Close())
	st, err = store.Open(filepath.Join(dir, "db"), nil)
	require.NoError(t, err)
	defer st.Close()
	reopened, err := history.Open(st)
	require.NoError(t, err)
	require.Equal(t, generation, reopened.CaptureGeneration())
	reopenedOriginal, found, err := reopened.LookupSource(source)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, history.TargetBootstrapLive, reopenedOriginal.Owner)
	require.Equal(t, body, reopenedOriginal.InputEvent.Payload)
}

func TestBootstrapSnapshotCallbackUsesLiveHistoryWriter(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "db"), nil)
	require.NoError(t, err)
	defer st.Close()
	journal, err := history.Open(st)
	require.NoError(t, err)
	attr, err := attribution.Open(st, "bootstrap-history", "bootstrap-history")
	require.NoError(t, err)
	policy, err := selection.Open(st, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	effective := attribution.Policy{Revision: 1, Collections: []string{"app.bsky.feed.post"}}
	require.NoError(t, attr.EnsureInitialPolicyHistory(effective, nil))
	o := &Orchestrator{cfg: Config{DataDir: dir, Store: st, RelayURL: "https://example.invalid", Verifier: newTestVerifier(t, "https://example.invalid"), Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), History: journal, BootstrapAttribution: attr, BootstrapAttributionDomain: "bootstrap-history", AttributionStreamID: "bootstrap-history", CollectionPolicy: policy}}
	consumer, err := o.openBootstrapLive(filepath.Join(dir, "backfill", "live_segments"))
	require.NoError(t, err)
	defer consumer.Close()
	writer := consumer.Writer()
	snapshot := ingest.Snapshot{AppendMetadata: &ingest.AttributionMetadata{StreamID: "bootstrap-history", Policy: effective, Provenance: attribution.ProvenanceSnapshot}, DID: "did:plc:bootstrap", Rev: "3l3qo2vutsw2b", Collections: effective.Collections, Records: []segment.Event{{Kind: segment.KindCreateResync, DID: "did:plc:bootstrap", Rev: "3l3qo2vutsw2b", Collection: "app.bsky.feed.post", Rkey: "one", Payload: []byte(`{"text":"snapshot"}`)}}}
	cfg := live.Config{SegmentsDir: filepath.Join(dir, "backfill", "live_segments"), Store: st, SeqKey: live.BootstrapSeqKey, CursorKey: live.CursorKey, RelayURL: "https://example.invalid", Verifier: newTestVerifier(t, "https://example.invalid"), Logger: o.cfg.Logger, Attribution: attr, AttributionDomain: "bootstrap-history", AttributionStreamID: "bootstrap-history", History: journal, HistoryProjectionTarget: history.TargetBootstrapLive, CollectionPolicy: policy, InitializePolicyGate: func(w *ingest.Writer) (*policygate.Gate, error) {
		return policygate.New(policy, w, "bootstrap-history"), nil
	}, ReconcileSnapshot: func(ctx context.Context, snapshot ingest.Snapshot) error {
		return writer.ReconcileSnapshot(ctx, snapshot)
	}}
	other, err := live.Open(cfg)
	require.NoError(t, err)
	require.NoError(t, cfg.ReconcileSnapshot(t.Context(), snapshot))
	require.Same(t, writer, consumer.Writer())
	require.NoError(t, other.Close())
	page, err := journal.Read(0, 0, 10)
	require.NoError(t, err)
	require.NotEmpty(t, page.Events)
	for _, event := range page.Events {
		require.Equal(t, history.TargetBootstrapLive, event.Owner)
	}
	before := writer.NextSeq()
	require.NoError(t, cfg.ReconcileSnapshot(t.Context(), snapshot))
	require.Equal(t, before, writer.NextSeq(), "retry must not create another projection row")
}
