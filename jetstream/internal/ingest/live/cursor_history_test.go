package live

import (
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/history"
	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/policygate"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/jcalabro/atmos/streaming"
	"github.com/stretchr/testify/require"
)

type cursorHistoryFixture struct {
	consumer *Consumer
	db       *store.Store
	dbClosed bool
	closed   bool
}

func (f *cursorHistoryFixture) close(t *testing.T) {
	t.Helper()
	if !f.closed {
		f.closed = true
		require.NoError(t, f.consumer.Close())
	}
	if !f.dbClosed {
		f.dbClosed = true
		require.NoError(t, f.db.Close())
	}
}

func openCursorHistoryConsumer(t *testing.T, db *store.Store, cursor int64) (*cursorHistoryFixture, *history.Store) {
	t.Helper()
	if _, err := LoadUpstreamCursor(db, CursorKey); err != nil {
		require.NoError(t, SaveUpstreamCursor(db, CursorKey, cursor))
	}
	journal, err := history.Open(db)
	require.NoError(t, err)
	attr, err := attribution.Open(db, "cursor-history-test", "cursor-history-domain")
	require.NoError(t, err)
	policy, err := selection.Open(db, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	initial := attribution.Policy{Revision: 1, Collections: []string{"app.bsky.feed.post"}}
	require.NoError(t, attr.EnsureInitialPolicyHistory(initial, nil))
	gate := policygate.New(policy, nil, "cursor-history-domain")
	consumer, err := Open(Config{
		SegmentsDir: filepath.Join(t.TempDir(), "segments"), Store: db, SeqKey: SteadySeqKey,
		CursorKey: CursorKey, RelayURL: "https://example.invalid", Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier: newTestVerifier(t), Attribution: attr, AttributionDomain: "cursor-history-domain",
		AttributionStreamID: "cursor-history-test", CollectionPolicy: policy, PolicyGate: gate,
		History: journal, HistoryProjectionTarget: history.TargetMain,
		LifecycleOrdering: journalLifecycleOrdering{journal: journal},
		HistoryCapture: &OriginalCapture{Journal: journal, Domain: "cursor-history-domain", Epoch: "cursor-history-epoch", Policy: func() selection.Policy {
			return selection.Policy{Revision: 1, Collections: []string{"app.bsky.feed.post"}}
		}},
	})
	require.NoError(t, err)
	consumer.watermarkReader = func() int64 { return cursor }
	consumer.durableHistoryCursor.Store(cursor)
	fixture := &cursorHistoryFixture{consumer: consumer, db: db}
	t.Cleanup(func() { fixture.close(t) })
	return fixture, journal
}

func requireCursorHistory(t *testing.T, c *Consumer, want int64) {
	t.Helper()
	require.NoError(t, c.writer.DrainDurability(t.Context()))
	require.Equal(t, want, c.cursorSample.Load())
	got, err := LoadUpstreamCursor(c.cfg.Store, c.cfg.CursorKey)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestCursorHistoryMultiOperationJournalFaultReopensWithoutSkipping(t *testing.T) {
	path := t.TempDir()
	segments := filepath.Join(path, "segments")
	db, err := store.Open(path, nil)
	require.NoError(t, err)
	require.NoError(t, SaveUpstreamCursor(db, CursorKey, 40))
	require.NoError(t, db.Close())
	fault := &store.KeyPrefixFault{Prefix: []byte("selected-history/v1/event/"), Op: store.WriteOpBatchCommit, Ordinal: 2, Err: errors.New("injected second selected-history commit failure")}
	db, err = store.Open(path, nil, store.WithFaultInjector(fault))
	require.NoError(t, err)
	fixture, journal := openCursorHistoryConsumer(t, db, 40)
	fixture.consumer.cfg.SegmentsDir = segments
	c := fixture.consumer
	c.watermarkReader = func() int64 { return 99 }
	event, _ := buildCommit(t, "did:plc:cursorfault", "3l3qo2vutsw2b",
		struct{ Coll, Rkey string }{"app.bsky.feed.post", "first"},
		struct{ Coll, Rkey string }{"app.bsky.feed.post", "second"})
	event.Seq = 100
	c.cfg.now = func() time.Time { return time.Unix(100, 0).UTC() }
	err = c.processBatch(t.Context(), []streaming.Event{event})
	require.ErrorContains(t, err, "injected second selected-history commit failure")
	page, err := journal.Read(0, 0, 10)
	require.NoError(t, err)
	require.Len(t, page.Events, 1)
	require.Equal(t, uint32(0), page.Events[0].Source.Operation)
	requireCursorHistory(t, c, 40)
	require.NoError(t, c.Close())
	fixture.closed = true
	require.NoError(t, db.Close())
	fixture.dbClosed = true

	db, err = store.Open(path, nil)
	require.NoError(t, err)
	persisted, err := LoadUpstreamCursor(db, CursorKey)
	require.NoError(t, err)
	require.EqualValues(t, 40, persisted)
	fixture, journal = openCursorHistoryConsumer(t, db, 40)
	fixture.consumer.cfg.SegmentsDir = segments
	c = fixture.consumer
	c.watermarkReader = func() int64 { return 99 }
	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{event}))
	page, err = journal.Read(0, 0, 10)
	require.NoError(t, err)
	require.Len(t, page.Events, 2)
	require.Equal(t, uint32(0), page.Events[0].Source.Operation)
	require.Equal(t, uint32(1), page.Events[1].Source.Operation)
	require.Equal(t, int64(100_000_000), page.Events[0].Time)
	requireCursorHistory(t, c, 40)
	c.watermarkReader = func() int64 { return 100 }
	requireCursorHistory(t, c, 100)

	waiting, _ := buildCommit(t, "did:plc:cursorfault", "3l3qo2vutsw2c", struct{ Coll, Rkey string }{"app.bsky.feed.post", "later"})
	waiting.Seq = 900
	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{waiting}))
	requireCursorHistory(t, c, 100)
	c.watermarkReader = func() int64 { return 900 }
	requireCursorHistory(t, c, 900)
}
