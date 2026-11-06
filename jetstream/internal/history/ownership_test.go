package history

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func ownedRecord(body string, cursor uint64, target ProjectionTarget) Append {
	a := record(body, cursor)
	a.Owner = target
	return a
}

func TestProjectionTargetsHaveIndependentSparseCheckpoints(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	db, err := store.Open(dir, nil)
	require.NoError(t, err)
	h, err := Open(db)
	require.NoError(t, err)
	mainSegments := filepath.Join(t.TempDir(), "main")
	bootSegments := filepath.Join(t.TempDir(), "boot")
	appendRow := func(cursor uint64, owner ProjectionTarget) uint64 {
		t.Helper()
		body := fmt.Sprintf("event-%d", cursor)
		r, err := h.Append(ownedRecord(body, cursor, owner))
		require.NoError(t, err)
		return r.Sequence
	}
	rows := []struct {
		owner ProjectionTarget
		seq   uint64
	}{
		{TargetBootstrapLive, appendRow(1, TargetBootstrapLive)},
		{TargetMain, appendRow(2, TargetMain)},
		{TargetBootstrapLive, appendRow(3, TargetBootstrapLive)},
		{TargetMain, appendRow(4, TargetMain)},
	}
	write := func(dir string, seq uint64) {
		t.Helper()
		batch := db.NewBatch()
		require.NoError(t, batch.Set([]byte(fmt.Sprintf("test-segment/%s/%d", dir, seq)), []byte("row"), nil))
		require.NoError(t, db.Commit(batch, store.SyncWrites))
		require.NoError(t, batch.Close())
	}
	for i, row := range rows {
		body := fmt.Sprintf("event-%d", i+1)
		ownerSeq := uint64(1)
		if i >= 2 {
			ownerSeq = 2
		}
		if row.owner == TargetBootstrapLive {
			write(bootSegments, ownerSeq)
		} else {
			write(mainSegments, ownerSeq)
		}
		intent := ProjectionIntent{Target: row.owner, JournalSequence: row.seq, SegmentSequence: ownerSeq, Fingerprint: OriginalHash([]byte(body)), Disposition: Apply}
		require.NoError(t, h.PrepareProjection(intent))
	}
	mainPending, err := h.PendingProjectionsFor(TargetMain)
	require.NoError(t, err)
	require.Len(t, mainPending, 2)
	for _, p := range mainPending {
		require.Equal(t, TargetMain, p.Intent.Target)
	}
	bootPending, err := h.PendingProjectionsFor(TargetBootstrapLive)
	require.NoError(t, err)
	require.Len(t, bootPending, 2)
	for _, p := range bootPending {
		require.Equal(t, TargetBootstrapLive, p.Intent.Target)
	}
	commit := func(target ProjectionTarget, seqs ...uint64) {
		t.Helper()
		require.NoError(t, h.CommitProjectionFor(target, seqs, func(*pebble.Batch) error { return nil }))
	}
	commit(TargetMain, 2, 4)
	commit(TargetBootstrapLive, 1, 3)
	for target, want := range map[ProjectionTarget]uint64{TargetMain: 4, TargetBootstrapLive: 4} {
		got, err := h.AppliedThroughFor(target)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	require.NoError(t, db.Close())

	db, err = store.Open(dir, nil)
	require.NoError(t, err)
	defer db.Close()
	h, err = Open(db)
	require.NoError(t, err)
	for _, target := range []ProjectionTarget{TargetMain, TargetBootstrapLive} {
		pending, err := h.PendingProjectionsFor(target)
		require.NoError(t, err)
		require.Empty(t, pending)
	}
}

func TestOwnerPersistsBeforeProjectionIntentAndCannotBeChanged(t *testing.T) {
	h, db := openTest(t, filepath.Join(t.TempDir(), "db"))
	defer db.Close()
	r, err := h.Append(ownedRecord("crash", 1, TargetBootstrapLive))
	require.NoError(t, err)
	event, found, err := h.Event(r.Sequence)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, TargetBootstrapLive, event.Owner)
	replayed, err := h.Append(ownedRecord("crash", 1, TargetMain))
	require.NoError(t, err)
	require.True(t, replayed.Duplicate)
	require.Equal(t, r.Sequence, replayed.Sequence)
	_, err = h.Append(ownedRecord("different", 1, TargetMain))
	require.ErrorIs(t, err, ErrConflict)
	require.ErrorIs(t, h.PrepareProjection(ProjectionIntent{Target: TargetMain, JournalSequence: 1, SegmentSequence: 1, Fingerprint: OriginalHash([]byte("crash")), Disposition: Apply}), ErrProjectionConflict)
}

func TestProjectionReservationScopedByTargetAndFilterPrecedesLookup(t *testing.T) {
	h, db := openTest(t, filepath.Join(t.TempDir(), "db"))
	defer db.Close()
	for i, owner := range []ProjectionTarget{TargetBootstrapLive, TargetMain} {
		body := fmt.Sprintf("event-%d", i+1)
		_, err := h.Append(ownedRecord(body, uint64(i+1), owner))
		require.NoError(t, err)
		require.NoError(t, h.PrepareProjection(ProjectionIntent{Target: owner, JournalSequence: uint64(i + 1), SegmentSequence: 1, Fingerprint: OriginalHash([]byte(body)), Disposition: Apply}))
	}
	calls := 0
	pending, err := h.RecoverProjectionFor(TargetMain, func(seq uint64) (ProjectionRow, bool, error) {
		calls++
		require.Equal(t, uint64(1), seq)
		return ProjectionRow{}, false, nil
	})
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, 1, calls)
	intent, found, err := h.ProjectionIntentFor(TargetMain, 1)
	require.NoError(t, err)
	require.False(t, found)
	require.Empty(t, intent)
}
