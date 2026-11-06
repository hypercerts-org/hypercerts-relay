package live

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/bluesky-social/jetstream/internal/history"
	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/policygate"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/syncstate"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/streaming"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

func TestLifecycleHistoryRetainsDelayedAndDuplicateAccountAndIdentity(t *testing.T) {
	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	streamID, domain := "lifecycle-history-test", "lifecycle-projection-domain"
	sourceDomain := "relay-hash-domain"
	attr, err := attribution.Open(db, streamID, domain)
	require.NoError(t, err)
	policy, err := selection.Open(db, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	initial := attribution.Policy{Revision: 1, Collections: []string{"app.bsky.feed.post"}}
	require.NoError(t, attr.EnsureInitialPolicyHistory(initial, nil))
	gate := policygate.New(policy, nil, domain)
	journal, err := history.Open(db)
	require.NoError(t, err)
	stateStore := syncstate.New(db)
	segmentsDir := filepath.Join(t.TempDir(), "segments")
	var callbacks atomic.Int64
	cfg := Config{SegmentsDir: segmentsDir, Store: db, SeqKey: SteadySeqKey, CursorKey: CursorKey, ReadLogRetentionBytes: 1,
		RelayURL: "https://example.invalid", Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Verifier: newTestVerifier(t),
		Attribution: attr, AttributionDomain: domain, AttributionStreamID: streamID, CollectionPolicy: policy, PolicyGate: gate,
		History: journal, HistoryProjectionTarget: history.TargetMain, SyncStateStore: stateStore,
		LifecycleOrdering: journalLifecycleOrdering{journal: journal},
		HistoryCapture:    &OriginalCapture{Journal: journal, Domain: sourceDomain, Epoch: "lifecycle-epoch"},
		OnEvent:           func(*segment.Event) { callbacks.Add(1) }}
	c, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	account := func(active bool, seq int64) streaming.Event {
		return streaming.Event{Seq: seq, Account: &comatproto.SyncSubscribeRepos_Account{DID: "did:plc:lifecycle", Active: active, Status: gt.Some("active"), Seq: seq, Time: "2026-08-01T00:00:00Z"}}
	}
	identity := func(handle string, seq int64) streaming.Event {
		return streaming.Event{Seq: seq, Identity: &comatproto.SyncSubscribeRepos_Identity{DID: "did:plc:lifecycle", Handle: gt.Some(handle), Seq: seq, Time: "2026-08-01T00:00:00Z"}}
	}
	for _, evt := range []streaming.Event{account(false, 20), identity("new.test", 21), account(true, 22), identity("old.test", 19), account(false, 18)} {
		require.NoError(t, c.processBatch(context.Background(), []streaming.Event{evt}))
	}
	page, err := journal.Read(0, 0, 20)
	require.NoError(t, err)
	require.Len(t, page.Events, 5)
	require.Equal(t, sourceDomain, page.Events[0].Source.Domain)
	require.NotEqual(t, domain, page.Events[0].Source.Domain)
	for _, index := range []int{3} {
		require.Equal(t, history.HistoricalOnly, page.Events[index].Disposition)
		require.NotEmpty(t, page.Events[index].Source.ContentHash)
		require.Equal(t, page.Events[index].InputEvent.WitnessedAt, page.Events[index].Time)
	}
	require.Equal(t, int64(3), callbacks.Load(), "only first-seen, projectable lifecycle records should call back")
	var currentAccount segment.Event
	var currentIdentity segment.Event
	require.NoError(t, c.writer.DrainDurability(context.Background()))
	require.NoError(t, c.writer.Flush(context.Background()))
	require.NoError(t, c.writer.ForceRotate(context.Background()))
	readLog := c.writer.ReadLog()
	require.Greater(t, readLog.FloorSeq(), uint64(1), "bounded retention must evict resident lifecycle rows")
	require.NoError(t, c.Close())
	c, err = Open(cfg)
	require.NoError(t, err)
	require.Greater(t, c.writer.ReadLog().FloorSeq(), uint64(1))
	require.Equal(t, uint64(4), c.writer.ReadLog().TipSeq())
	require.Equal(t, uint64(4), c.writer.ReadLog().DurableSeq())
	require.NoError(t, c.writer.ForceRotate(context.Background()))
	files, err := ingest.SegmentFiles(segmentsDir)
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, file := range files {
		reader, openErr := segment.Open(segment.ReaderConfig{Path: file.Path})
		if errors.Is(openErr, segment.ErrActiveSegment) {
			continue
		}
		require.NoError(t, openErr)
		for block := 0; block < int(reader.Header().BlockCount); block++ {
			rows, decodeErr := reader.DecodeBlock(block)
			require.NoError(t, decodeErr)
			for _, event := range rows {
				switch event.Kind {
				case segment.KindAccount:
					if event.Seq > currentAccount.Seq {
						currentAccount = event
					}
				case segment.KindIdentity:
					if event.Seq > currentIdentity.Seq {
						currentIdentity = event
					}
				}
			}
		}
		require.NoError(t, reader.Close())
	}
	require.NotEmpty(t, currentAccount.Payload)
	require.NotEmpty(t, currentIdentity.Payload)
	var gotAccount comatproto.SyncSubscribeRepos_Account
	require.NoError(t, gotAccount.UnmarshalCBOR(currentAccount.Payload))
	require.True(t, gotAccount.Active, "delayed inactive account must not roll back current state")
	var gotIdentity comatproto.SyncSubscribeRepos_Identity
	require.NoError(t, gotIdentity.UnmarshalCBOR(currentIdentity.Payload))
	require.Equal(t, "new.test", gotIdentity.Handle.Val(), "delayed identity must not roll back current state")
}

func TestLifecycleHistoryProjectionFailureKeepsWatermarkAndRetryDeduplicates(t *testing.T) {
	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	streamID, domain := "lifecycle-fault-test", "lifecycle-fault-test"
	attr, err := attribution.Open(db, streamID, domain)
	require.NoError(t, err)
	policy, err := selection.Open(db, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	initial := attribution.Policy{Revision: 1, Collections: []string{"app.bsky.feed.post"}}
	require.NoError(t, attr.EnsureInitialPolicyHistory(initial, nil))
	gate := policygate.New(policy, nil, domain)
	journal, err := history.Open(db)
	require.NoError(t, err)
	evt := streaming.Event{Seq: 50, Identity: &comatproto.SyncSubscribeRepos_Identity{DID: "did:plc:fault", Handle: gt.Some("fault.test"), Seq: 50, Time: "2026-08-01T00:00:00Z"}}
	open := func() *Consumer {
		consumer, err := Open(Config{SegmentsDir: filepath.Join(t.TempDir(), "segments"), Store: db, SeqKey: SteadySeqKey, CursorKey: CursorKey,
			RelayURL: "https://example.invalid", Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Verifier: newTestVerifier(t),
			Attribution: attr, AttributionDomain: domain, AttributionStreamID: streamID, CollectionPolicy: policy, PolicyGate: gate,
			History: journal, HistoryProjectionTarget: history.TargetMain, SyncStateStore: syncstate.New(db), LifecycleOrdering: journalLifecycleOrdering{journal: journal}, HistoryCapture: &OriginalCapture{Journal: journal, Domain: domain, Epoch: "fault-epoch"}})
		require.NoError(t, err)
		consumer.watermarkReader = func() int64 { return 49 }
		return consumer
	}
	c := open()
	require.NoError(t, c.processBatch(context.Background(), []streaming.Event{evt}))
	require.Equal(t, int64(0), c.cursorValueForDurableBatch().(int64))
	require.NoError(t, c.Close())
	c = open()
	require.NoError(t, c.processBatch(context.Background(), []streaming.Event{evt}))
	page, err := journal.Read(0, 0, 10)
	require.NoError(t, err)
	require.Len(t, page.Events, 1, "restart retry must reuse the journal admission")
	require.NoError(t, c.Close())
}
