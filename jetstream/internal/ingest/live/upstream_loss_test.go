package live

import (
	"errors"
	"sync"
	"testing"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

func TestUpstreamLossRoundTripAndFirstPreserved(t *testing.T) {
	db := newTestStore(t)
	got, err := ReadUpstreamLoss(db)
	require.NoError(t, err)
	require.Nil(t, got)
	first := UpstreamLoss{Version: 1, Reason: "sequence_gap", CursorKey: "relay/cursor", SafeCursor: 5, Expected: 0, Got: 8}
	require.NoError(t, RecordUpstreamLoss(db, first))
	require.NoError(t, RecordUpstreamLoss(db, UpstreamLoss{Version: 1, Reason: "outdated_cursor", CursorKey: "other", SafeCursor: 9}))
	got, err = ReadUpstreamLoss(db)
	require.NoError(t, err)
	require.Equal(t, first, *got)
}

func TestUpstreamLossRejectsMalformed(t *testing.T) {
	cases := []string{
		`{`, `null`, `{} {}`, `{"version":1,"version":1,"reason":"outdated_cursor","cursorKey":"c","safeCursor":0}`,
		`{"version":1,"reason":"outdated_cursor","cursorKey":"c","safeCursor":0,"extra":1}`,
		`{"version":1,"reason":"outdated_cursor","cursorKey":"c","safeCursor":null}`,
		`{"version":1,"reason":"outdated_cursor","cursorKey":"c"}`,
		`{"version":2,"reason":"outdated_cursor","cursorKey":"c","safeCursor":0}`,
		`{"version":1,"reason":"unknown","cursorKey":"c","safeCursor":0}`,
		`{"version":1,"reason":"outdated_cursor","cursorKey":"","safeCursor":0}`,
		`{"version":1,"reason":"outdated_cursor","cursorKey":"c","safeCursor":-1}`,
		`{"version":1,"reason":"sequence_gap","cursorKey":"c","safeCursor":0,"expected":2,"got":2}`,
		`{"version":1,"reason":"sequence_gap","cursorKey":"c","safeCursor":0,"expected":-1,"got":2}`,
	}
	db := newTestStore(t)
	for _, input := range cases {
		t.Run(input, func(t *testing.T) {
			require.NoError(t, db.Set([]byte(upstreamLossKey), []byte(input), store.SyncWrites))
			_, err := ReadUpstreamLoss(db)
			require.Error(t, err)
			require.Error(t, RecordUpstreamLoss(db, UpstreamLoss{Version: 1, Reason: "outdated_cursor", CursorKey: "c"}))
		})
	}
}

func TestUpstreamLossWriteFailure(t *testing.T) {
	injected := errors.New("injected write failure")
	db, err := store.Open(t.TempDir(), nil, store.WithFaultInjector(&store.KeyPrefixFault{Prefix: []byte(upstreamLossKey), Ordinal: 1, Err: injected}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	loss := UpstreamLoss{Version: 1, Reason: "outdated_cursor", CursorKey: "c"}
	require.ErrorIs(t, RecordUpstreamLoss(db, loss), injected)
	got, err := ReadUpstreamLoss(db)
	require.NoError(t, err)
	require.Nil(t, got)
	require.NoError(t, RecordUpstreamLoss(db, loss))
}

func TestUpstreamLossConcurrentFirstPreserved(t *testing.T) {
	db := newTestStore(t)
	const count = 32
	var group sync.WaitGroup
	errs := make(chan error, count)
	for i := 0; i < count; i++ {
		group.Add(1)
		go func(cursor int64) {
			defer group.Done()
			errs <- RecordUpstreamLoss(db, UpstreamLoss{Version: 1, Reason: "outdated_cursor", CursorKey: "c", SafeCursor: cursor})
		}(int64(i))
	}
	group.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	first, err := ReadUpstreamLoss(db)
	require.NoError(t, err)
	require.NotNil(t, first)
	for i := 0; i < count; i++ {
		require.NoError(t, RecordUpstreamLoss(db, UpstreamLoss{Version: 1, Reason: "outdated_cursor", CursorKey: "c", SafeCursor: int64(i)}))
	}
	got, err := ReadUpstreamLoss(db)
	require.NoError(t, err)
	require.Equal(t, first, got)
}
