package history

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func TestProjectionIntentDurableRetryConflictAndRecovery(t *testing.T) {
	h, db := openTest(t, filepath.Join(t.TempDir(), "db"))
	defer db.Close()
	_, err := h.Append(record("event", 1))
	require.NoError(t, err)
	i := ProjectionIntent{Target: TargetMain, JournalSequence: 1, SegmentSequence: 1, Fingerprint: OriginalHash([]byte("event")), Disposition: Apply}
	require.NoError(t, h.PrepareProjection(i))
	require.NoError(t, h.PrepareProjection(i))
	require.ErrorIs(t, h.PrepareProjection(ProjectionIntent{JournalSequence: 1, SegmentSequence: 2, Fingerprint: i.Fingerprint, Disposition: Apply}), ErrProjectionConflict)
	got, err := h.RecoverProjection(func(seq uint64) (ProjectionRow, bool, error) {
		if seq == 0 {
			t.Fatal("historical-only recovery attempted sequence-zero lookup")
		}
		return ProjectionRow{SegmentSequence: seq, Fingerprint: i.Fingerprint}, true, nil
	})
	require.NoError(t, err)
	require.Equal(t, []ProjectionRecovery{{Intent: i, Status: RecoveryMatched}}, got)
	got, err = h.RecoverProjection(func(uint64) (ProjectionRow, bool, error) { return ProjectionRow{}, false, nil })
	require.NoError(t, err)
	require.Equal(t, []ProjectionRecovery{{Intent: i, Status: RecoveryNeedsReplay}}, got)
	_, err = h.RecoverProjection(func(seq uint64) (ProjectionRow, bool, error) {
		return ProjectionRow{SegmentSequence: seq, Fingerprint: "wrong"}, true, nil
	})
	require.ErrorIs(t, err, ErrProjectionConflict)
	noOp := ProjectionIntent{Target: TargetMain, JournalSequence: 2, Fingerprint: OriginalHash([]byte("noop")), Disposition: Noop}
	_, err = h.Append(Append{Source: Source{Domain: "relay-a", Epoch: "epoch-1", Cursor: 2, Operation: 1, Path: "sync", ContentHash: noOp.Fingerprint}, Kind: "sync", Original: []byte("noop"), InputEvent: segment.Event{Kind: segment.KindSync, DID: "did:plc:example"}, Policy: Policy{Revision: 3}, Provenance: "live", Disposition: Noop, Attribution: Attribution{StreamID: h.StreamID(), Policy: EffectivePolicy{Revision: 3, Collections: []string{}}, Provenance: "live"}})
	require.NoError(t, err)
	noOp.JournalSequence = 2
	require.NoError(t, h.PrepareProjection(noOp))
	historical := ProjectionIntent{Target: TargetMain, JournalSequence: 3, Fingerprint: OriginalHash([]byte("historical")), Disposition: HistoricalOnly}
	_, err = h.Append(Append{Source: Source{Domain: "relay-a", Epoch: "epoch-1", Cursor: 3, Operation: 1, Path: "record", ContentHash: historical.Fingerprint}, Kind: "record", DID: "did:plc:example", Collection: "app.example.record", Revision: "r1", Original: []byte("historical"), InputEvent: segment.Event{Kind: segment.KindCreate, DID: "did:plc:example", Collection: "app.example.record", Rkey: "record", Rev: "r1"}, Policy: Policy{Revision: 1, Collections: []string{"app.example.record"}}, Provenance: "live", Disposition: HistoricalOnly, Attribution: Attribution{StreamID: h.StreamID(), Policy: EffectivePolicy{Revision: 1, Collections: []string{"app.example.record"}}, Provenance: "live"}})
	require.NoError(t, err)
	historical.JournalSequence = 3
	require.NoError(t, h.PrepareProjection(historical))
	lookupCalls := 0
	got, err = h.RecoverProjection(func(seq uint64) (ProjectionRow, bool, error) {
		if seq == 0 {
			t.Fatal("historical-only recovery attempted sequence-zero lookup")
		}
		lookupCalls++
		return ProjectionRow{}, false, nil
	})
	require.NoError(t, err)
	require.Equal(t, []ProjectionRecovery{{Intent: i, Status: RecoveryNeedsReplay}, {Intent: noOp, Status: RecoveryNoopReady}, {Intent: historical, Status: RecoveryNoopReady}}, got)
	require.Equal(t, 1, lookupCalls)
}

func TestPrepareProjectionEnforcesUniqueSegmentSequence(t *testing.T) {
	newStore := func(t *testing.T) (*Store, *store.Store) {
		h, db := openTest(t, filepath.Join(t.TempDir(), "db"))
		return h, db
	}
	appendEvent := func(t *testing.T, h *Store, sequence uint64, content string, disposition Disposition) {
		_, err := h.Append(Append{Source: Source{Domain: "relay-a", Epoch: "epoch-1", Cursor: sequence, Operation: 1, Path: "record", ContentHash: OriginalHash([]byte(content))}, Kind: "record", DID: "did:plc:example", Collection: "app.example.record", Revision: "r1", Original: []byte(content), InputEvent: segment.Event{Kind: segment.KindCreate, DID: "did:plc:example", Collection: "app.example.record", Rkey: "record", Rev: "r1", Payload: []byte(content)}, Policy: Policy{Revision: 1, Collections: []string{"app.example.record"}}, Provenance: "live", Disposition: disposition, Attribution: Attribution{StreamID: h.StreamID(), Policy: EffectivePolicy{Revision: 1, Collections: []string{"app.example.record"}}, Provenance: "live"}})
		require.NoError(t, err)
	}
	intent := func(sequence uint64) ProjectionIntent {
		return ProjectionIntent{JournalSequence: sequence, SegmentSequence: 1, Fingerprint: OriginalHash([]byte("event")), Disposition: Apply}
	}

	t.Run("pending reservation conflicts", func(t *testing.T) {
		h, db := newStore(t)
		defer db.Close()
		appendEvent(t, h, 1, "one", Apply)
		appendEvent(t, h, 2, "two", Apply)
		require.NoError(t, h.PrepareProjection(intent(1)))
		require.NoError(t, h.PrepareProjection(intent(1)))
		require.ErrorIs(t, h.PrepareProjection(intent(2)), ErrProjectionConflict)
	})

	t.Run("completed reservation conflicts", func(t *testing.T) {
		h, db := newStore(t)
		defer db.Close()
		appendEvent(t, h, 1, "one", Apply)
		appendEvent(t, h, 2, "two", Apply)
		first := intent(1)
		require.NoError(t, h.PrepareProjection(first))
		require.NoError(t, h.CommitProjection([]uint64{1}, func(*pebble.Batch) error { return nil }))
		require.ErrorIs(t, h.PrepareProjection(intent(2)), ErrProjectionConflict)
	})

	t.Run("distinct and zero coordinates", func(t *testing.T) {
		h, db := newStore(t)
		defer db.Close()
		for i := uint64(1); i <= 4; i++ {
			disposition := Apply
			if i >= 3 {
				disposition = Noop
			}
			appendEvent(t, h, i, fmt.Sprintf("event-%d", i), disposition)
		}
		first := intent(1)
		require.NoError(t, h.PrepareProjection(first))
		second := intent(2)
		second.SegmentSequence = 2
		require.NoError(t, h.PrepareProjection(second))
		for i := uint64(3); i <= 4; i++ {
			noop := ProjectionIntent{JournalSequence: i, Fingerprint: OriginalHash([]byte(fmt.Sprintf("event-%d", i))), Disposition: Noop}
			require.NoError(t, h.PrepareProjection(noop))
		}
	})
}

func TestCommitProjectionRejectsHistoryNamespaceMutationsAtomically(t *testing.T) {
	for _, name := range []string{"delete-event", "overwrite-identity", "overwrite-allocator", "dedup", "delete-range", "merge"} {
		t.Run(name, func(t *testing.T) {
			h, db := openTest(t, filepath.Join(t.TempDir(), "db"))
			defer db.Close()
			_, err := h.Append(record("event", 1))
			require.NoError(t, err)
			i := ProjectionIntent{JournalSequence: 1, SegmentSequence: 1, Fingerprint: OriginalHash([]byte("event")), Disposition: Apply}
			require.NoError(t, h.PrepareProjection(i))
			err = h.CommitProjection([]uint64{1}, func(b *pebble.Batch) error {
				switch name {
				case "delete-event":
					return b.Delete(eventKey(1), nil)
				case "overwrite-identity":
					return b.Set([]byte(keyStream), []byte("changed"), nil)
				case "overwrite-allocator":
					return b.Set([]byte(keyNext), []byte("changed"), nil)
				case "dedup":
					source := record("event", 1).Source
					identity, err := json.Marshal(struct {
						Domain, Epoch string
						Cursor        uint64
						Operation     uint32
						Path          string
					}{source.Domain, source.Epoch, source.Cursor, source.Operation, source.Path})
					if err != nil {
						return err
					}
					hash := sha256.Sum256(identity)
					key := append([]byte(metaPrefix+"source/"), []byte(hex.EncodeToString(hash[:]))...)
					return b.Set(key, []byte("changed"), nil)
				case "delete-range":
					return b.DeleteRange([]byte("selected-history/"), []byte("selected-history0/"), nil)
				default:
					return b.Merge([]byte(keyStream), []byte("changed"), nil)
				}
			})
			require.ErrorContains(t, err, "forbidden mutation")
			applied, err := h.AppliedThrough()
			require.NoError(t, err)
			require.Zero(t, applied)
			page, err := h.Read(0, 0, 10)
			require.NoError(t, err)
			require.Len(t, page.Events, 1)
		})
	}
}

func TestCommitProjectionOwnsAtomicBatch(t *testing.T) {
	h, db := openTest(t, filepath.Join(t.TempDir(), "db"))
	defer db.Close()
	_, err := h.Append(record("event", 1))
	require.NoError(t, err)
	i := ProjectionIntent{JournalSequence: 1, SegmentSequence: 1, Fingerprint: OriginalHash([]byte("event")), Disposition: Apply}
	require.NoError(t, h.PrepareProjection(i))

	callbackErr := h.CommitProjection([]uint64{1}, func(b *pebble.Batch) error {
		if err := b.Set([]byte("segment/not-committed"), []byte("row"), nil); err != nil {
			return err
		}
		return errors.New("injected projection callback failure")
	})
	require.ErrorContains(t, callbackErr, "injected projection callback failure")
	applied, err := h.AppliedThrough()
	require.NoError(t, err)
	require.Zero(t, applied)

	err = h.CommitProjection([]uint64{1}, func(b *pebble.Batch) error {
		return b.Set([]byte("segment/metadata"), []byte("row"), nil)
	})
	require.NoError(t, err)
	got, closer, err := db.Get([]byte("segment/metadata"))
	require.NoError(t, err)
	require.Equal(t, []byte("row"), got)
	require.NoError(t, closer.Close())
	applied, err = h.AppliedThrough()
	require.NoError(t, err)
	require.Equal(t, uint64(1), applied)
	v, closer, err := db.Get(projectionIntentKey(1))
	require.NoError(t, err)
	require.Equal(t, []byte("complete"), v)
	require.NoError(t, closer.Close())
	// A retry of an already committed prefix is a no-op and does not invoke
	// the segment writer a second time.
	called := false
	require.NoError(t, h.CommitProjection([]uint64{1}, func(*pebble.Batch) error { called = true; return nil }))
	require.False(t, called)
	require.NoError(t, h.PrepareProjection(i))
	stored, closer, err := db.Get(projectionIntentKey(1))
	require.NoError(t, err)
	require.Equal(t, []byte("complete"), stored)
	require.NoError(t, closer.Close())
	completed, closer, err := db.Get(projectionCompletionKey(1))
	require.NoError(t, err)
	require.JSONEq(t, string(completed), string(completed))
	var persisted ProjectionIntent
	require.NoError(t, json.Unmarshal(completed, &persisted))
	require.Equal(t, i.JournalSequence, persisted.JournalSequence)
	require.Equal(t, i.SegmentSequence, persisted.SegmentSequence)
	require.Equal(t, i.Fingerprint, persisted.Fingerprint)
	require.Equal(t, i.Disposition, persisted.Disposition)
	require.NoError(t, closer.Close())
	recovered, err := h.RecoverProjection(func(uint64) (ProjectionRow, bool, error) { return ProjectionRow{}, false, nil })
	require.NoError(t, err)
	require.Empty(t, recovered)

}
