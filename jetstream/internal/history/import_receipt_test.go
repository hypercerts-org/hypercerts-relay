package history

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func importReceipt(sequence uint64, payload string) ImportReceipt {
	event := segment.Event{Seq: sequence, Kind: segment.KindCreate, DID: "did:plc:test", Collection: "app.example.record", Rkey: "x", Rev: "r1", WitnessedAt: 100, IndexedAt: 200, Payload: []byte(payload)}
	return ImportReceipt{Source: ImportSource{Domain: "source-domain", StreamID: "source-stream", Sequence: 42, Operation: 1}, Fingerprint: OriginalHash([]byte(payload)), Target: "main", SegmentSequence: sequence, PlannedEvent: event, SourceAttribution: []byte(`{"source":true}`), DestinationAttribution: []byte(`{"destination":true}`), Policy: []byte(`{"revision":4}`)}
}

func TestImportReceiptPrepareReopenDuplicateAndConflict(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	db, err := store.Open(dir, nil)
	require.NoError(t, err)
	h, err := Open(db)
	require.NoError(t, err)
	original := importReceipt(7, "same")
	first, duplicate, err := h.PrepareImport(original)
	require.NoError(t, err)
	require.False(t, duplicate)
	changedLocalTimes := importReceipt(9, "same")
	changedLocalTimes.PlannedEvent.WitnessedAt = 999
	changedLocalTimes.PlannedEvent.IndexedAt = 888
	got, duplicate, err := h.PrepareImport(changedLocalTimes)
	require.NoError(t, err)
	require.True(t, duplicate)
	require.Equal(t, first, got)
	require.NoError(t, db.Close())
	db, err = store.Open(dir, nil)
	require.NoError(t, err)
	defer db.Close()
	h, err = Open(db)
	require.NoError(t, err)
	pending, err := h.ListPendingImports("main")
	require.NoError(t, err)
	require.Equal(t, []ImportReceipt{first}, pending)
	conflict := importReceipt(7, "changed")
	_, _, err = h.PrepareImport(conflict)
	require.ErrorIs(t, err, ErrImportConflict)
}

func TestImportReceiptReservationAndAtomicCompletion(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "data"), nil)
	require.NoError(t, err)
	defer db.Close()
	h, err := Open(db)
	require.NoError(t, err)
	first := importReceipt(1, "one")
	_, _, err = h.PrepareImport(first)
	require.NoError(t, err)
	colliding := importReceipt(1, "two")
	colliding.Source.Sequence = 43
	_, _, err = h.PrepareImport(colliding)
	require.ErrorIs(t, err, ErrImportConflict)
	boom := errors.New("fault")
	err = h.CompleteImport(first.Source, func(_ *pebble.Batch) error { return boom })
	require.ErrorIs(t, err, boom)
}
