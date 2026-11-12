package ingest

import (
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func TestReadLog_AppendedEventRemainsVisibleAfterFlushWithRetention(t *testing.T) {
	t.Parallel()

	w := newTestWriter(t, Config{
		MaxEventsPerBlock:     10,
		ReadLogRetentionBytes: 1 << 20,
	})

	ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "1", Payload: []byte{1}}
	require.NoError(t, w.Append(t.Context(), &ev))

	entries, _, ok, atTip := w.ReadLog().ReadFrom(ev.Seq, 10)
	require.True(t, ok)
	require.False(t, atTip)
	require.Len(t, entries, 1)
	require.Equal(t, ev.Seq, entries[0].Event().Seq)
	require.Equal(t, uint64(1), w.ReadLog().FloorSeq(), "retained entries stay readable before durability")
	require.Equal(t, uint64(1), w.ReadLog().DurableSeq())

	require.NoError(t, w.Flush(t.Context()))
	require.Equal(t, w.NextSeq(), w.ReadLog().DurableSeq())
	require.Equal(t, ev.Seq, w.ReadLog().FloorSeq(), "retained event remains within the readable floor")
	entries, _, ok, _ = w.ReadLog().ReadFrom(ev.Seq, 10)
	require.True(t, ok, "flushed entry remains observable in the retained hot log")
	require.Len(t, entries, 1)
	require.Equal(t, ev.Seq, entries[0].Event().Seq)
}

// brutePinned recomputes pinned bytes the old O(n) way: every resident
// entry with Seq >= durable. Used to assert the incremental pinnedBytes
// accumulator never drifts.
func brutePinned(l *ReadableLog) int64 {
	var sum int64
	for _, e := range l.entries {
		if e.event.Seq >= l.durable {
			sum += e.bytes
		}
	}
	return sum
}

func TestReadCommittedFromExcludesPendingAndWakesOnDurableCommit(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{MaxEventsPerBlock: 10})
	w.readLog = newReadableLog(w.NextSeq(), 1<<20, nil)
	ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "1", Payload: []byte{1}}
	require.NoError(t, w.Append(t.Context(), &ev))
	entries, notify, ok, atTip := w.ReadLog().ReadCommittedFrom(ev.Seq, 10)
	require.False(t, ok)
	require.True(t, atTip)
	require.Empty(t, entries)
	select {
	case <-notify:
		t.Fatal("pending append woke committed reader")
	default:
	}
	require.NoError(t, w.Flush(t.Context()))
	select {
	case <-notify:
	default:
		t.Fatal("durable commit did not wake reader")
	}
	entries, _, ok, atTip = w.ReadLog().ReadCommittedFrom(ev.Seq, 10)
	require.True(t, ok)
	require.False(t, atTip)
	require.Len(t, entries, 1)
	require.Less(t, entries[0].Event().Seq, w.ReadLog().DurableSeq())
}

func TestReadCommittedFromReturnsColdReadForEvictedDurableCursor(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{MaxEventsPerBlock: 10, ReadLogRetentionBytes: 0})
	ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "1"}
	require.NoError(t, w.Append(t.Context(), &ev))
	require.NoError(t, w.Flush(t.Context()))
	entries, notify, ok, atTip := w.ReadLog().ReadCommittedFrom(ev.Seq, 10)
	require.Empty(t, entries)
	require.Nil(t, notify)
	require.False(t, ok)
	require.False(t, atTip)
}

func TestReadLog_PinnedBytesAccumulatorMatchesScan(t *testing.T) {
	t.Parallel()
	l := newReadableLog(1, 512, nil)
	seq := uint64(1)
	appendN := func(n int) {
		for range n {
			ev := &segment.Event{
				Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c",
				Rkey: "r", Rev: "1", Payload: []byte{byte(seq)}, Seq: seq,
			}
			l.append(ev)
			seq++
			require.Equal(t, brutePinned(l), l.pinnedBytes, "pinned drift after append")
		}
	}
	appendN(20)
	require.Equal(t, brutePinned(l), l.pinnedBytes)
	for _, d := range []uint64{5, 5, 12, 21} {
		l.advanceDurable(d)
		require.Equal(t, brutePinned(l), l.pinnedBytes, "pinned drift after advanceDurable(%d)", d)
	}
	appendN(15)
	l.advanceDurable(l.tipSeq)
	require.Equal(t, brutePinned(l), l.pinnedBytes, "pinned drift after tail drain")
	require.GreaterOrEqual(t, l.pinnedBytes, int64(0), "pinned must never go negative")
}

func TestReadLog_DeepCopiesPayload(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{MaxEventsPerBlock: 10})
	payload := []byte{1, 2, 3}
	ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "1", Payload: payload}
	require.NoError(t, w.Append(t.Context(), &ev))
	payload[0] = 9
	entries, _, ok, _ := w.ReadLog().ReadFrom(ev.Seq, 1)
	require.True(t, ok)
	require.Equal(t, []byte{1, 2, 3}, entries[0].Event().Payload)
}

func TestReadLog_AppendErrorAfterSeqAllocationStillVisible(t *testing.T) {
	t.Parallel()
	want := errors.New("observe failed")
	st := newTestStore(t)
	w, err := Open(Config{
		SegmentsDir:           filepath.Join(t.TempDir(), "segments"),
		Store:                 st,
		Logger:                slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxEventsPerBlock:     10,
		ReadLogRetentionBytes: 1 << 20,
		OnAppend: func(ev *segment.Event) error {
			if ev.Seq == 2 {
				return want
			}
			return nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	err = w.AppendBatch(t.Context(), []segment.Event{
		{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r1", Rev: "1"},
		{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r2", Rev: "1"},
		{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r3", Rev: "1"},
	})
	require.ErrorIs(t, err, want)
	entries, _, ok, _ := w.ReadLog().ReadFrom(1, 10)
	require.True(t, ok)
	require.Len(t, entries, 2)
	require.Equal(t, uint64(1), entries[0].Event().Seq)
	require.Equal(t, uint64(2), entries[1].Event().Seq)
}
