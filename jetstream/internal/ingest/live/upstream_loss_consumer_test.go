package live

import (
	"context"
	"errors"
	"io"
	"iter"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/streaming"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

func newLossConsumer(t *testing.T, selected bool, onLoss func(UpstreamLoss)) (*Consumer, *selection.Manager) {
	t.Helper()
	st := newTestStore(t)
	var policy *selection.Manager
	if selected {
		var err error
		policy, err = selection.Open(st, []string{"app.bsky.feed.post"})
		require.NoError(t, err)
	}
	cfg := Config{
		SegmentsDir:       filepath.Join(t.TempDir(), "segments"),
		SeqKey:            SteadySeqKey,
		CursorKey:         CursorKey,
		Store:             st,
		RelayURL:          "https://example.invalid",
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:          newTestVerifier(t),
		CollectionPolicy:  policy,
		ReconcileSnapshot: func(context.Context, ingest.Snapshot) error { return nil },
		OnUpstreamLoss:    onLoss,
	}
	c, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c, policy
}

func identityAt(seq int64) streaming.Event {
	return streaming.Event{Seq: seq, Identity: &comatproto.SyncSubscribeRepos_Identity{DID: "did:plc:loss", Handle: gt.Some("loss.test"), Seq: seq, Time: "2026-09-16T00:00:00Z"}}
}

func outdatedCursorNotice() streaming.Event {
	return streaming.Event{Info: &comatproto.SyncSubscribeRepos_Info{Name: "OutdatedCursor"}}
}

// staticEvents yields the supplied batches in order, stopping after the
// first returned error like atmos's engine does.
func staticEvents(batches ...any) iter.Seq2[[]streaming.Event, error] {
	return func(yield func([]streaming.Event, error) bool) {
		for _, item := range batches {
			switch v := item.(type) {
			case []streaming.Event:
				if !yield(v, nil) {
					return
				}
			case error:
				yield(nil, v)
				return
			}
		}
	}
}

func TestOutdatedCursorSkipsAndLogsSelectedIngestion(t *testing.T) {
	t.Parallel()
	var losses []UpstreamLoss
	c, _ := newLossConsumer(t, true, func(l UpstreamLoss) { losses = append(losses, l) })

	require.NoError(t, c.consumeEvents(context.Background(), staticEvents(
		[]streaming.Event{identityAt(10)},
		[]streaming.Event{outdatedCursorNotice()},
		[]streaming.Event{identityAt(11), identityAt(12)},
	)), "a loss is skipped, not fatal")
	require.Equal(t, int64(12), c.LastUpstreamSeq(), "ingestion continues past the notice")
	require.Len(t, losses, 1)
	require.Equal(t, "outdated_cursor", losses[0].Reason)

	marker, readErr := ReadUpstreamLoss(c.cfg.Store)
	require.NoError(t, readErr)
	require.NotNil(t, marker)
	require.Equal(t, "outdated_cursor", marker.Reason)

	require.NoError(t, c.Close())
	cursor, cursorErr := LoadUpstreamCursor(c.cfg.Store, CursorKey)
	require.NoError(t, cursorErr)
	require.Equal(t, int64(12), cursor, "persisted cursor advances past the skipped notice")
}

func TestSequenceGapSkipsAndLogsSelectedIngestion(t *testing.T) {
	t.Parallel()
	c, _ := newLossConsumer(t, true, nil)
	require.NoError(t, c.consumeEvents(context.Background(), staticEvents(
		[]streaming.Event{identityAt(10)},
		&streaming.GapError{Expected: 11, Got: 15},
		[]streaming.Event{identityAt(15)},
	)), "a sequence gap is skipped, not fatal")
	require.Equal(t, int64(10), c.LastUpstreamSeq(), "the post-gap event is never dispatched")
	require.Equal(t, int64(10), c.processedUpstream.Load(), "the boundary stays at the last fully processed event")

	marker, readErr := ReadUpstreamLoss(c.cfg.Store)
	require.NoError(t, readErr)
	require.NotNil(t, marker)
	require.Equal(t, "sequence_gap", marker.Reason)
	require.Equal(t, int64(15), marker.Got)
}

func TestReopenedConsumerReportsRecordedLossAndResumes(t *testing.T) {
	t.Parallel()
	c, _ := newLossConsumer(t, true, nil)
	require.NoError(t, c.processBatch(context.Background(), []streaming.Event{identityAt(7)}))
	require.NoError(t, c.Close())
	require.NoError(t, RecordUpstreamLoss(c.cfg.Store, UpstreamLoss{Version: 1, Reason: "outdated_cursor", CursorKey: CursorKey, SafeCursor: 7}))

	reopened, err := Open(c.cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NotErrorIs(t, reopened.Run(ctx), ErrUpstreamLoss, "a recorded loss must not block restart")
}

func TestLossMarkerDoesNotBlockDisabledSelection(t *testing.T) {
	t.Parallel()
	c, _ := newLossConsumer(t, false, nil)
	require.NoError(t, RecordUpstreamLoss(c.cfg.Store, UpstreamLoss{Version: 1, Reason: "sequence_gap", CursorKey: CursorKey, SafeCursor: 3, Expected: 4, Got: 9}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NotErrorIs(t, c.Run(ctx), ErrUpstreamLoss, "a recorded loss is reported, not a startup barrier")
}

func TestBenignNoticesAndUnmanagedConsumersKeepStreaming(t *testing.T) {
	benign := streaming.Event{Info: &comatproto.SyncSubscribeRepos_Info{Name: "ConsumerTooSlow"}}
	t.Run("selected", func(t *testing.T) {
		t.Parallel()
		c, _ := newLossConsumer(t, true, nil)
		require.NoError(t, c.consumeEvents(context.Background(), staticEvents(
			[]streaming.Event{benign, identityAt(4)},
			[]streaming.Event{identityAt(5)},
		)))
		require.Equal(t, int64(5), c.LastUpstreamSeq())
		marker, err := ReadUpstreamLoss(c.cfg.Store)
		require.NoError(t, err)
		require.Nil(t, marker)
	})
	t.Run("unmanaged", func(t *testing.T) {
		t.Parallel()
		c, _ := newLossConsumer(t, false, nil)
		require.NoError(t, c.consumeEvents(context.Background(), staticEvents(
			[]streaming.Event{identityAt(1)},
			&streaming.GapError{Expected: 2, Got: 4},
			[]streaming.Event{identityAt(4)},
		)))
		require.GreaterOrEqual(t, c.LastUpstreamSeq(), int64(1), "unmanaged consumers keep consuming after a gap")
		marker, err := ReadUpstreamLoss(c.cfg.Store)
		require.NoError(t, err)
		require.Nil(t, marker)
	})
}

func TestExcludedOnlyEventStillAdvancesProcessedBoundary(t *testing.T) {
	t.Parallel()
	c, _ := newLossConsumer(t, true, nil)
	excluded, _ := buildCommit(t, "did:plc:loss", "3l3qo2vutsw2b", struct{ Coll, Rkey string }{"app.bsky.feed.like", "excluded"})
	excluded.Seq = 21
	require.NoError(t, c.processBatch(context.Background(), []streaming.Event{excluded}))
	require.Equal(t, int64(21), c.processedUpstream.Load())
	require.NoError(t, c.Close())
	cursor, err := LoadUpstreamCursor(c.cfg.Store, CursorKey)
	require.NoError(t, err)
	require.Equal(t, int64(21), cursor)
}

func TestRecordUpstreamLossOnceReportsRecordedLoss(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	loss := UpstreamLoss{Version: 1, Reason: "outdated_cursor", CursorKey: CursorKey, SafeCursor: 1}
	require.NoError(t, RecordUpstreamLossOnce(st, loss), "the first detection returns the terminal consumer error")
	err := RecordUpstreamLossOnce(st, loss)
	require.ErrorIs(t, err, ErrUpstreamLossPending)
	require.ErrorIs(t, err, ErrUpstreamLoss)

	other := UpstreamLoss{Version: 1, Reason: "sequence_gap", CursorKey: CursorKey, SafeCursor: 1, Got: 2}
	require.ErrorIs(t, RecordUpstreamLossOnce(st, other), ErrUpstreamLossPending, "any existing marker stops a restart")

	marker, readErr := ReadUpstreamLoss(st)
	require.NoError(t, readErr)
	require.Equal(t, loss, *marker, "the first marker is never replaced")
	require.False(t, errors.Is(err, context.Canceled))
}
