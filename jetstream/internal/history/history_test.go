package history

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func openTest(t *testing.T, dir string) (*Store, *store.Store) {
	t.Helper()
	db, err := store.Open(dir, nil)
	require.NoError(t, err)
	h, err := Open(db)
	require.NoError(t, err)
	return h, db
}

func record(body string, cursor uint64) Append {
	data := []byte(body)
	return Append{Source: Source{Domain: "relay-a", Epoch: "epoch-1", Cursor: cursor, Operation: 1, Path: "app.example.record/a", ContentHash: OriginalHash(data)}, Kind: "put", DID: "did:plc:example", Collection: "app.example.record", Revision: "r1", Original: data, InputEvent: segment.Event{Kind: segment.KindCreate, DID: "did:plc:example", Collection: "app.example.record", Rkey: "a", Rev: "r1", Payload: data}, Policy: Policy{Revision: 3, Collections: []string{"app.example.record"}}, Provenance: "live", Disposition: Apply, Attribution: Attribution{StreamID: "test-stream", Policy: EffectivePolicy{Revision: 3, Collections: []string{"app.example.record"}}, Provenance: "live"}}
}

func TestAppendReopenDedupConflictAndPendingCheckpoint(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	h, db := openTest(t, dir)
	firstID := h.StreamID()
	activation, err := h.Activation()
	require.NoError(t, err)
	require.Equal(t, uint64(1), activation.JournalStart)
	require.False(t, activation.HistoricalComplete)
	require.Equal(t, "indefinite", activation.Retention)
	r, err := h.Append(record("original", 1))
	require.NoError(t, err)
	require.Equal(t, uint64(1), r.Sequence)
	dup, err := h.Append(record("original", 1))
	require.NoError(t, err)
	require.True(t, dup.Duplicate)
	require.Equal(t, r.Sequence, dup.Sequence)
	conflicting := record("different", 1)
	_, err = h.Append(conflicting)
	require.ErrorIs(t, err, ErrConflict)
	_, err = h.Append(record("older snapshot", 2))
	require.NoError(t, err)
	require.NoError(t, db.Close())
	h, db = openTest(t, dir)
	defer db.Close()
	require.Equal(t, firstID, h.StreamID())
	reopenedActivation, err := h.Activation()
	require.NoError(t, err)
	require.Equal(t, activation, reopenedActivation)
	page, err := h.Read(0, 0, 10)
	require.NoError(t, err)
	require.Len(t, page.Events, 2)
	require.Equal(t, []byte("original"), page.Events[0].Original)
	require.Equal(t, []byte("older snapshot"), page.Events[1].Original)
	applied, err := h.AppliedThrough()
	require.NoError(t, err)
	require.Zero(t, applied)
	before, err := h.AppliedThrough()
	require.NoError(t, err)
	require.Error(t, h.Acknowledge(1))
	after, err := h.AppliedThrough()
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Error(t, h.Acknowledge(2))
	require.Error(t, h.Acknowledge(4))
	require.Error(t, h.Acknowledge(1))
}

func TestLatestAppliedLifecycleUsesCompletedProjectionAndJournalSequence(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	h, db := openTest(t, dir)
	makeLifecycle := func(cursor uint64, kind segment.Kind, target ProjectionTarget) Receipt {
		t.Helper()
		data := []byte(fmt.Sprintf("lifecycle-%d-%d", kind, cursor))
		appendEvent := Append{Source: Source{Domain: "lifecycle-boundary", Epoch: "epoch", Cursor: cursor, Operation: uint32(kind), Path: fmt.Sprintf("$%d", kind), ContentHash: OriginalHash(data)}, Kind: fmt.Sprintf("%d", kind), DID: "did:plc:lifecycle-boundary", Original: data, InputEvent: segment.Event{Kind: kind, DID: "did:plc:lifecycle-boundary", Payload: data}, Policy: Policy{Revision: 1, Collections: []string{}}, Provenance: "live", Disposition: Apply, Attribution: Attribution{StreamID: "lifecycle-boundary", Policy: EffectivePolicy{Revision: 1, Collections: []string{}}, Provenance: "live"}, Owner: target}
		receipt, err := h.Append(appendEvent)
		require.NoError(t, err)
		return receipt
	}
	applied, err := h.Append(record("projection row", 1000))
	require.NoError(t, err)
	lifecycle := makeLifecycle(1000, segment.KindAccount, TargetMain)
	projection := ProjectionIntent{Target: TargetMain, JournalSequence: lifecycle.Sequence, SegmentSequence: applied.Sequence, Fingerprint: "lifecycle-boundary", Disposition: Apply}
	require.NoError(t, h.PrepareProjection(projection))
	require.NoError(t, h.CommitProjectionFor(TargetMain, []uint64{lifecycle.Sequence}, func(*pebble.Batch) error { return nil }))
	delayed := makeLifecycle(900, segment.KindAccount, TargetMain)
	pending := makeLifecycle(1200, segment.KindAccount, TargetMain)
	pendingIntent := ProjectionIntent{Target: TargetMain, JournalSequence: pending.Sequence, SegmentSequence: pending.Sequence, Fingerprint: "pending-lifecycle", Disposition: Apply}
	require.NoError(t, h.PrepareProjection(pendingIntent))
	seq, boundary, err := h.LatestAppliedLifecycle("did:plc:lifecycle-boundary", segment.KindAccount, TargetMain)
	require.NoError(t, err)
	require.EqualValues(t, 1000, seq)
	require.Equal(t, lifecycle.Sequence, boundary)
	_, err = h.Append(record("unrelated identity", 1300))
	require.NoError(t, err)
	require.NoError(t, db.Close())
	h, db = openTest(t, dir)
	defer db.Close()
	seq, boundary, err = h.LatestAppliedLifecycle("did:plc:lifecycle-boundary", segment.KindAccount, TargetMain)
	require.NoError(t, err)
	require.EqualValues(t, 1000, seq)
	require.Equal(t, lifecycle.Sequence, boundary)
	require.NotEqual(t, delayed.Sequence, boundary)
}

func TestEnsurePolicyIsIdempotentAndRejectsConflictingSameRevision(t *testing.T) {
	h, db := openTest(t, filepath.Join(t.TempDir(), "data"))
	defer db.Close()
	policy := Policy{Revision: 1, Collections: []string{"app.example.record"}}
	first, err := h.EnsurePolicy(policy, 10)
	require.NoError(t, err)
	second, err := h.EnsurePolicy(policy, 20)
	require.NoError(t, err)
	require.Equal(t, first, second)
	_, err = h.EnsurePolicy(Policy{Revision: 1, Collections: []string{}}, 30)
	require.ErrorIs(t, err, ErrPolicyRevision)
	_, err = h.EnsurePolicy(Policy{Revision: 2, Collections: []string{}}, 40)
	require.NoError(t, err)
}

func TestPolicyCaptureIntervalsKeepRemovalGapAndUnknownSource(t *testing.T) {
	h, db := openTest(t, filepath.Join(t.TempDir(), "data"))
	defer db.Close()
	first, err := h.AdoptPolicy(Policy{Revision: 1, Collections: []string{"app.example.record"}}, 10)
	require.NoError(t, err)
	require.Equal(t, uint64(0), first.AdoptedAtJournalSequence)
	require.Equal(t, uint64(1), first.Intervals[0].FromJournalSequence)
	require.Equal(t, "unknown", first.Intervals[0].SourceEvidence)
	_, err = h.Append(record("original", 1))
	require.NoError(t, err)
	removed, err := h.AdoptPolicy(Policy{Revision: 2, Collections: []string{}}, 20)
	require.NoError(t, err)
	require.Equal(t, uint64(1), removed.AdoptedAtJournalSequence)
	require.Equal(t, uint64(1), *removed.Intervals[0].ThroughJournalSequence)
	_, err = h.Append(record("accepted while excluded is not emitted", 2))
	require.NoError(t, err)
	readded, err := h.AdoptPolicy(Policy{Revision: 3, Collections: []string{"app.example.record"}}, 30)
	require.NoError(t, err)
	require.Equal(t, uint64(2), readded.AdoptedAtJournalSequence)
	require.Len(t, readded.Intervals, 2)
	require.Equal(t, uint64(3), readded.Intervals[1].FromJournalSequence)
	require.Nil(t, readded.Intervals[1].ThroughJournalSequence)
	require.Equal(t, "unknown", readded.Intervals[1].SourceEvidence)
	page, err := h.Read(0, 0, 10)
	require.NoError(t, err)
	require.Len(t, page.Events, 2)
	state, ok, err := h.ReadPolicyState()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, h.StreamID(), state.CaptureGeneration)
}

func TestAppendCommitFailureIsAtomic(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	_, db := openTest(t, dir)
	require.NoError(t, db.Close())
	fault := store.WithFaultInjector(&store.KeyPrefixFault{Prefix: []byte("selected-history/v1/event/"), Op: store.WriteOpBatchCommit, Ordinal: 1, Err: errors.New("injected commit failure")})
	db, err := store.Open(dir, nil, fault)
	require.NoError(t, err)
	defer db.Close()
	h, err := Open(db)
	require.NoError(t, err)
	_, err = h.Append(record("not committed", 1))
	require.ErrorContains(t, err, "injected commit failure")
	tip, err := h.Tip()
	require.NoError(t, err)
	require.Zero(t, tip)
}

func TestTipReadAndWatchRemainConsistentDuringCrossHandleAppends(t *testing.T) {
	h1, db := openTest(t, filepath.Join(t.TempDir(), "data"))
	defer db.Close()
	h2, err := Open(db)
	require.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := uint64(1); i <= 200; i++ {
			if _, err := h1.Append(record(fmt.Sprintf("concurrent-%d", i), i)); err != nil {
				t.Errorf("append %d: %v", i, err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		var after uint64
		for after < 200 {
			page, _, err := h2.WatchRead(after, 7)
			if err != nil {
				t.Errorf("watch read after %d: %v", after, err)
				return
			}
			if len(page.Events) == 0 {
				time.Sleep(time.Millisecond)
				continue
			}
			for _, event := range page.Events {
				require.Equal(t, after+1, event.Sequence)
				after = event.Sequence
			}
		}
	}()
	wg.Wait()
	require.NoError(t, h1.Verify())
	tip, err := h1.Tip()
	require.NoError(t, err)
	require.EqualValues(t, 200, tip)
}

func TestReadPageLimitIsValidTruncation(t *testing.T) {
	h, db := openTest(t, filepath.Join(t.TempDir(), "data"))
	defer db.Close()
	for i := uint64(1); i <= 3; i++ {
		_, err := h.Append(record(fmt.Sprintf("event-%d", i), i))
		require.NoError(t, err)
	}
	page, err := h.Read(0, 3, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(3), page.Tip)
	require.Equal(t, uint64(1), page.Next)
	require.Len(t, page.Events, 1)
	page, err = h.Read(page.Next, page.Tip, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(2), page.Next)
	require.Len(t, page.Events, 1)
}

func TestNotificationsBroadcastAcrossHandlesAndWatchReadIsRaceFree(t *testing.T) {
	h1, db := openTest(t, filepath.Join(t.TempDir(), "data"))
	defer db.Close()
	h2, err := Open(db)
	require.NoError(t, err)
	page, wake1, err := h1.WatchRead(0, 10)
	require.NoError(t, err)
	require.Empty(t, page.Events)
	_, wake2, err := h2.WatchRead(0, 10)
	require.NoError(t, err)
	_, err = h1.Append(record("broadcast", 1))
	require.NoError(t, err)
	select {
	case <-wake1:
	default:
		t.Fatal("first subscriber did not wake")
	}
	select {
	case <-wake2:
	default:
		t.Fatal("second subscriber did not wake")
	}
	page, wake, err := h1.WatchRead(0, 10)
	require.NoError(t, err)
	require.Len(t, page.Events, 1)
	_, err = h2.Append(record("live", 2))
	require.NoError(t, err)
	select {
	case <-wake:
	case <-time.After(time.Second):
		t.Fatal("live notification was lost")
	}
}

func TestStagePolicyNotifiesOnlyAfterCommittedAdoption(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	h1, db := openTest(t, dir)
	defer db.Close()
	h2, err := Open(db)
	require.NoError(t, err)
	_, wake1, err := h1.WatchRead(0, 10)
	require.NoError(t, err)
	_, wake2, err := h2.WatchRead(0, 10)
	require.NoError(t, err)
	batch := db.NewBatch()
	state, err := h1.StagePolicyAfterCommit(batch, Policy{Revision: 1, Collections: []string{"app.example.record"}}, 11, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), state.AdoptedAtJournalSequence)
	select {
	case <-wake1:
		t.Fatal("watcher woke before policy batch commit")
	default:
	}
	require.NoError(t, db.Commit(batch, store.SyncWrites))
	h1.NotifyAfterCommit()
	select {
	case <-wake1:
	default:
		t.Fatal("first handle watcher did not wake")
	}
	select {
	case <-wake2:
	default:
		t.Fatal("second handle watcher did not wake")
	}
	adoptions, err := h1.ReadPolicyAdoptions()
	require.NoError(t, err)
	require.Len(t, adoptions, 1)
	require.Equal(t, uint64(0), *adoptions[0].EffectiveAfterJournalSeq)
	require.NoError(t, batch.Close())
}

func TestReadPinsTipAndPreservesAdmissionPolicy(t *testing.T) {
	h, db := openTest(t, filepath.Join(t.TempDir(), "data"))
	defer db.Close()
	_, err := h.Append(record("one", 1))
	require.NoError(t, err)
	p, err := h.Read(0, 1, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), p.Tip)
	a := record("two", 2)
	a.Policy = Policy{Revision: 4, Collections: []string{}}
	a.Attribution.Policy = EffectivePolicy{Revision: 4, Collections: []string{}}
	a.Disposition = HistoricalOnly
	_, err = h.Append(a)
	require.NoError(t, err)
	p, err = h.Read(0, p.Tip, 10)
	require.NoError(t, err)
	require.Len(t, p.Events, 1)
	require.Equal(t, uint64(1), p.Tip)
	all, err := h.Read(1, 0, 10)
	require.NoError(t, err)
	require.Equal(t, HistoricalOnly, all.Events[0].Disposition)

}

func TestCrossHandleSequenceAndAcknowledgmentGuards(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	h1, db1 := openTest(t, dir)
	h2, err := Open(db1)
	require.NoError(t, err)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			h := h1
			if i%2 != 0 {
				h = h2
			}
			_, err := h.Append(record(fmt.Sprintf("body-%d", i), uint64(i+1)))
			require.NoError(t, err)
		}(i)
	}
	wg.Wait()
	page, err := h1.Read(0, 0, 30)
	require.NoError(t, err)
	require.Len(t, page.Events, 20)
	for i, event := range page.Events {
		require.Equal(t, uint64(i+1), event.Sequence)
	}
	before, err := h1.AppliedThrough()
	require.NoError(t, err)
	require.Error(t, h2.Acknowledge(1))
	after, err := h1.AppliedThrough()
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.NoError(t, db1.Close())
}

func TestRejectInvalidSourceAndCheckpointGaps(t *testing.T) {
	h, db := openTest(t, filepath.Join(t.TempDir(), "data"))
	defer db.Close()
	a := record("x", 0)
	_, err := h.Append(a)
	require.ErrorIs(t, err, ErrInvalid)
	_, err = h.Append(record("x", 1))
	require.NoError(t, err)
	require.Error(t, h.Acknowledge(2))
}
