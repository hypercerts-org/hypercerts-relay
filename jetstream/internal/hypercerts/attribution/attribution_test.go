package attribution

import (
	"bytes"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func openTest(t *testing.T) (*store.Store, *Manager) {
	t.Helper()
	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	m, err := Open(db, "crypto-identity", "selected-stream")
	require.NoError(t, err)
	return db, m
}
func sampleEvent() Event {
	return Event{StreamID: "crypto-identity", Seq: 7, PolicyRev: uint64Ptr(2), Provenance: ProvenanceLive, Payload: json.RawMessage(`{"operation":"create"}`)}
}
func samplePolicy() Policy { return Policy{Revision: 2, Collections: []string{"org.example.record"}} }
func commit(t *testing.T, _ *store.Store, m *Manager, p Prepared) {
	t.Helper()
	require.NoError(t, m.atomicCommit(p, samplePolicy(), nil))
}
func TestSegmentFingerprintAndAdapter(t *testing.T) {
	event := segment.Event{Seq: 7, WitnessedAt: 10, IndexedAt: 11, UpstreamRelayCursor: 99, Kind: segment.KindCreate, DID: "did:plc:test", Collection: "org.example.record", Rkey: "r1", Rev: "rev1", Payload: []byte{1, 2, 3}}
	first := FingerprintSegment(event)
	event.UpstreamRelayCursor++
	require.Equal(t, first, FingerprintSegment(event))
	changed := event
	changed.Payload = []byte{3, 2, 1}
	require.NotEqual(t, first, FingerprintSegment(changed))
	changed = event
	changed.Seq++
	require.NotEqual(t, first, FingerprintSegment(changed))

	_, manager := openTest(t)
	prepared, err := manager.PrepareSegment(event, "crypto-identity", samplePolicy(), ProvenanceLive, Boundary{})
	require.NoError(t, err)
	require.Equal(t, first, prepared.SegmentFingerprint)
	require.Equal(t, samplePolicy(), *prepared.Policy)
}

func TestWithPolicyCommitDoesNotActivateAndDeepCopiesPolicy(t *testing.T) {
	_, m := openTest(t)
	policy := Policy{Revision: 2, Collections: []string{"org.example.record"}}
	require.NoError(t, m.WithPolicyCommit(policy, func(stage func(*pebble.Batch) error) error {
		policy.Collections[0] = "org.changed.record"
		batch := m.db.NewBatch()
		defer batch.Close()
		if err := stage(batch); err != nil {
			return err
		}
		return m.db.Commit(batch, store.SyncWrites)
	}))
	activation, err := m.ActivationSeq()
	require.NoError(t, err)
	require.Zero(t, activation)
	history, err := m.ReadPolicyHistory()
	require.NoError(t, err)
	require.Equal(t, []string{"org.example.record"}, history[0].Collections)
	require.ErrorIs(t, m.WithPolicyCommit(Policy{Revision: 2, Collections: []string{"org.other.record"}}, func(stage func(*pebble.Batch) error) error {
		batch := m.db.NewBatch()
		defer batch.Close()
		return stage(batch)
	}), ErrConflict)
}

func TestWithPolicyCommitSerializesManagersThroughCommit(t *testing.T) {
	db, first := openTest(t)
	second, err := Open(db, "crypto-identity", "selected-stream")
	require.NoError(t, err)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- first.WithPolicyCommit(Policy{Revision: 2, Collections: []string{"org.example.record"}}, func(stage func(*pebble.Batch) error) error {
			batch := db.NewBatch()
			defer batch.Close()
			if err := stage(batch); err != nil {
				return err
			}
			close(entered)
			<-release
			return db.Commit(batch, store.SyncWrites)
		})
	}()
	<-entered
	secondDone := make(chan error, 1)
	secondAttempted := make(chan struct{})
	go func() {
		close(secondAttempted)
		secondDone <- second.WithPolicyCommit(Policy{Revision: 2, Collections: []string{"org.other.record"}}, func(stage func(*pebble.Batch) error) error {
			batch := db.NewBatch()
			defer batch.Close()
			if err := stage(batch); err != nil {
				return err
			}
			return db.Commit(batch, store.SyncWrites)
		})
	}()
	<-secondAttempted
	select {
	case err := <-secondDone:
		t.Fatalf("second manager passed blocked commit: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-done)
	require.ErrorIs(t, <-secondDone, ErrConflict)
}

func TestWithPolicyCommitRequiresOneShotAndRejectsPreexistingMutations(t *testing.T) {
	_, m := openTest(t)
	policy := Policy{Revision: 2, Collections: []string{"org.example.record"}}
	require.ErrorIs(t, m.WithPolicyCommit(policy, func(func(*pebble.Batch) error) error { return nil }), ErrInvalid)
	var escaped func(*pebble.Batch) error
	batch := m.db.NewBatch()
	require.NoError(t, m.WithPolicyCommit(policy, func(stage func(*pebble.Batch) error) error { escaped = stage; return stage(batch) }))
	require.ErrorIs(t, escaped(batch), ErrInvalid)
	require.NoError(t, batch.Close())
	batch = m.db.NewBatch()
	require.NoError(t, batch.Set(m.policyKey(2), []byte("prior"), nil))
	require.ErrorIs(t, m.WithPolicyCommit(policy, func(stage func(*pebble.Batch) error) error { return stage(batch) }), ErrConflict)
	require.NoError(t, batch.Close())
	batch = m.db.NewBatch()
	key := m.policyKey(2)
	require.NoError(t, batch.DeleteRange(key[:len(key)-1], append(bytes.Clone(key), 0), nil))
	require.ErrorIs(t, m.WithPolicyCommit(policy, func(stage func(*pebble.Batch) error) error { return stage(batch) }), ErrConflict)
	require.NoError(t, batch.Close())
	boom := errors.New("callback failed")
	require.ErrorIs(t, m.WithPolicyCommit(policy, func(func(*pebble.Batch) error) error { return boom }), boom)
	require.NoError(t, m.WithPolicyCommit(policy, func(stage func(*pebble.Batch) error) error { b := m.db.NewBatch(); defer b.Close(); return stage(b) }))
}

func TestCommitRequiresPolicyArgumentToMatchPreparedPolicy(t *testing.T) {
	_, m := openTest(t)
	p, err := m.Prepare(sampleEvent(), samplePolicy())
	require.NoError(t, err)
	policy := samplePolicy()
	policy.Collections = []string{"org.other.record"}
	require.ErrorIs(t, m.Commit(p, policy, nil), ErrInvalid)
	_, err = m.Lookup(p.Event.Seq)
	require.ErrorIs(t, err, ErrLegacyUnknown)
}

func TestCommitBatchRejectsInBatchPolicyConflict(t *testing.T) {
	_, m := openTest(t)
	p1, err := m.Prepare(sampleEvent(), samplePolicy())
	require.NoError(t, err)
	event := sampleEvent()
	event.Seq = 8
	p2, err := m.Prepare(event, samplePolicy())
	require.NoError(t, err)
	p2.Policy.Collections = []string{"org.other.record"}
	require.ErrorIs(t, m.CommitBatch([]Prepared{p1, p2}, nil), ErrConflict)
	_, err = m.Lookup(7)
	require.ErrorIs(t, err, ErrLegacyUnknown)
}

func TestPrepareSegmentRecoveryUsesPersistedSegmentFingerprint(t *testing.T) {
	_, m := openTest(t)
	event := segment.Event{Seq: 7, Kind: segment.KindCreate, Collection: "org.example.record", DID: "did:plc:test", Rkey: "r1", Payload: []byte("payload")}
	p, err := m.PrepareSegment(event, "crypto-identity", samplePolicy(), ProvenanceLive, Boundary{})
	require.NoError(t, err)
	require.NotEqual(t, p.Fingerprint, p.SegmentFingerprint)
	require.NoError(t, m.Recover(func(uint64) (DurableEvent, error) {
		return DurableEvent{Exists: true, SegmentFingerprint: FingerprintSegment(event)}, nil
	}, nil))
	_, err = m.Lookup(7)
	require.NoError(t, err)
}

func TestPrepareSegmentRecoveryRequiresActualMatchingSegmentFingerprint(t *testing.T) {
	_, m := openTest(t)
	event := segment.Event{Seq: 7, Kind: segment.KindCreate, Collection: "org.example.record", DID: "did:plc:test", Rkey: "r1", Payload: []byte("payload")}
	_, err := m.PrepareSegment(event, "crypto-identity", samplePolicy(), ProvenanceLive, Boundary{})
	require.NoError(t, err)
	require.ErrorIs(t, m.Recover(func(uint64) (DurableEvent, error) {
		return DurableEvent{Exists: true}, nil
	}, nil), ErrFingerprint)
}

func TestPrepareSegmentRejectsDifferentSegmentAtSameCoordinate(t *testing.T) {
	_, m := openTest(t)
	event := segment.Event{Seq: 7, Kind: segment.KindCreate, Collection: "org.example.record", DID: "did:plc:test", Rkey: "r1", Payload: []byte("payload")}
	_, err := m.PrepareSegment(event, "crypto-identity", samplePolicy(), ProvenanceLive, Boundary{})
	require.NoError(t, err)
	event.Payload = []byte("different")
	_, err = m.PrepareSegment(event, "crypto-identity", samplePolicy(), ProvenanceLive, Boundary{})
	require.ErrorIs(t, err, ErrConflict)
}

func TestPreparePromoteRestartAndPolicyHistory(t *testing.T) {
	db, m := openTest(t)
	p, err := m.Prepare(sampleEvent(), samplePolicy())
	require.NoError(t, err)
	commit(t, db, m, p)
	got, err := m.Lookup(7)
	require.NoError(t, err)
	require.Equal(t, p.Event, got)
	history, err := m.ReadPolicyHistory()
	require.NoError(t, err)
	require.Equal(t, []Policy{samplePolicy()}, history)
	lookup, err := m.LookupPolicy(samplePolicy().Revision)
	require.NoError(t, err)
	require.Equal(t, samplePolicy(), lookup)
	lookup.Collections[0] = "org.changed.record"
	lookupAgain, err := m.LookupPolicy(samplePolicy().Revision)
	require.NoError(t, err)
	require.Equal(t, samplePolicy(), lookupAgain)
	m2, err := Open(db, "crypto-identity", "selected-stream")
	require.NoError(t, err)
	got, err = m2.Lookup(7)
	require.NoError(t, err)
	require.Equal(t, p.Event, got)
}
func TestPreparedPolicyIsCopiedAndUsedByRecovery(t *testing.T) {
	_, manager := openTest(t)
	policy := samplePolicy()
	prepared, err := manager.Prepare(sampleEvent(), policy)
	require.NoError(t, err)
	policy.Collections[0] = "org.changed.record"
	policy.EffectiveAfterSeq = new(uint64)
	*policy.EffectiveAfterSeq = 100
	require.Equal(t, []string{"org.example.record"}, prepared.Policy.Collections)
	require.NoError(t, manager.Recover(func(uint64) (DurableEvent, error) {
		return DurableEvent{Exists: true, Fingerprint: prepared.Fingerprint}, nil
	}, nil))
	history, err := manager.ReadPolicyHistory()
	require.NoError(t, err)
	require.Equal(t, []Policy{samplePolicy()}, history)
}

func TestRecoveryOrphanPromoteAndMismatch(t *testing.T) {
	t.Run("orphan deleted", func(t *testing.T) {
		db, m := openTest(t)
		_, err := m.Prepare(sampleEvent(), samplePolicy())
		require.NoError(t, err)
		require.NoError(t, m.Recover(func(uint64) (DurableEvent, error) { return DurableEvent{}, nil }, func(uint64) (Policy, error) { return samplePolicy(), nil }))
		_, closer, err := db.Get(m.preparedKey(7))
		if closer != nil {
			_ = closer.Close()
		}
		require.ErrorIs(t, err, pebble.ErrNotFound)
	})
	t.Run("matching promoted", func(t *testing.T) {
		_, m := openTest(t)
		p, err := m.Prepare(sampleEvent(), samplePolicy())
		require.NoError(t, err)
		require.NoError(t, m.Recover(func(uint64) (DurableEvent, error) { return DurableEvent{Exists: true, Fingerprint: p.Fingerprint}, nil }, nil))
		_, err = m.Lookup(7)
		require.NoError(t, err)
	})
	t.Run("mismatch fails", func(t *testing.T) {
		_, m := openTest(t)
		_, err := m.Prepare(sampleEvent(), samplePolicy())
		require.NoError(t, err)
		require.ErrorIs(t, m.Recover(func(uint64) (DurableEvent, error) { return DurableEvent{Exists: true, Fingerprint: "different"}, nil }, func(uint64) (Policy, error) { return samplePolicy(), nil }), ErrFingerprint)
	})
}
func TestActivationBoundaryRestartAndDomain(t *testing.T) {
	db, m := openTest(t)
	_, err := m.Lookup(3)
	require.ErrorIs(t, err, ErrLegacyUnknown)
	p, err := m.Prepare(sampleEvent(), samplePolicy())
	require.NoError(t, err)
	commit(t, db, m, p)
	m2, err := Open(db, "crypto-identity", "selected-stream")
	require.NoError(t, err)
	_, err = m2.Lookup(6)
	require.ErrorIs(t, err, ErrLegacyUnknown)
	_, err = m2.Lookup(8)
	require.ErrorIs(t, err, ErrInvalid)
	other, err := Open(db, "crypto-identity", "other-stream")
	require.NoError(t, err)
	_, err = other.Lookup(7)
	require.ErrorIs(t, err, ErrLegacyUnknown)
	require.NotEqual(t, m.eventKey(7), other.eventKey(7))
}
func TestConflictingCoordinates(t *testing.T) {
	_, m := openTest(t)
	p, err := m.Prepare(sampleEvent(), samplePolicy())
	require.NoError(t, err)
	different := sampleEvent()
	different.Payload = json.RawMessage(`{"operation":"update"}`)
	_, err = m.Prepare(different, samplePolicy())
	require.ErrorIs(t, err, ErrConflict)
	require.NoError(t, m.atomicCommit(p, samplePolicy(), nil))
	p2 := p
	p2.Event.Seq = 8
	p2.Fingerprint = fingerprint(p2.Event)
	conflicting := samplePolicy()
	conflicting.Collections = []string{"org.other.record"}
	require.ErrorIs(t, m.atomicCommit(p2, conflicting, nil), ErrInvalid)
	require.False(t, errors.Is(nil, ErrConflict))
}
func TestLegacyV1PreparedFingerprintRecovery(t *testing.T) {
	_, m := openTest(t)
	event := sampleEvent()
	payload, err := json.Marshal(event)
	require.NoError(t, err)
	// Literal v1 persisted JSON: PolicyRev and Prepared.Policy used numeric values.
	legacyEvent := bytes.Clone(payload)
	legacyPrepared := []byte(`{"event":` + string(legacyEvent) + `,"policy":{"revision":2,"collections":["org.example.record"]},"fingerprint":"` + fingerprint(event) + `"}`)
	require.NoError(t, m.db.Set(m.preparedKey(event.Seq), append([]byte{1}, legacyPrepared...), store.SyncWrites))
	require.NoError(t, m.Recover(func(uint64) (DurableEvent, error) {
		return DurableEvent{Exists: true, Fingerprint: fingerprint(event)}, nil
	}, nil))
	got, err := m.Lookup(event.Seq)
	require.NoError(t, err)
	require.Equal(t, uint64Ptr(2), got.PolicyRev)
}

func TestPolicyUnknownPrepareAndCommit(t *testing.T) {
	_, m := openTest(t)
	e := sampleEvent()
	e.PolicyRev = nil
	e.Provenance = ProvenanceLegacyUnknown
	p, err := m.Prepare(e)
	require.NoError(t, err)
	require.NoError(t, m.CommitBatch([]Prepared{p}, nil))
	got, err := m.Lookup(e.Seq)
	require.NoError(t, err)
	require.Nil(t, got.PolicyRev)
	history, err := m.ReadPolicyHistory()
	require.NoError(t, err)
	require.Empty(t, history)
}

func TestStrictCodecAndBoundary(t *testing.T) {
	var out Event
	require.NoError(t, decode([]byte{2, '{', '}'}, &out))
	require.ErrorIs(t, decode([]byte{3, '{', '}'}, &out), ErrInvalid)
	require.ErrorIs(t, decode([]byte{1, '{', '"', 'u', 'n', 'k', 'n', 'o', 'w', 'n', '"', ':', '1', '}'}, &out), ErrInvalid)
	require.NoError(t, Validate(sampleEvent()))
	e := sampleEvent()
	e.Provenance = ProvenanceSnapshot
	e.Boundary.Seq = new(uint64)
	*e.Boundary.Seq = 4
	e.Boundary.SnapshotID = new(string)
	*e.Boundary.SnapshotID = "snap"
	require.NoError(t, Validate(e))
}
func TestConcurrentManagersSerializeAndRetryIdempotently(t *testing.T) {
	db, m1 := openTest(t)
	m2, err := Open(db, "crypto-identity", "selected-stream")
	require.NoError(t, err)
	p, err := m1.Prepare(sampleEvent(), samplePolicy())
	require.NoError(t, err)
	var success atomic.Int32
	var conflict atomic.Int32
	var wg sync.WaitGroup
	for _, m := range []*Manager{m1, m2} {
		wg.Add(1)
		go func(m *Manager) {
			defer wg.Done()
			policy := samplePolicy()
			policy.Collections = []string{"org.example.record"}
			err := m.atomicCommit(p, policy, nil)
			if err == nil {
				success.Add(1)
			} else if errors.Is(err, ErrConflict) {
				conflict.Add(1)
			}
		}(m)
	}
	wg.Wait()
	require.EqualValues(t, 2, success.Load())
	require.Zero(t, conflict.Load())
}
func TestAtomicCommitMetadataFailureLeavesNoPartialWrites(t *testing.T) {
	_, m := openTest(t)
	p, err := m.Prepare(sampleEvent(), samplePolicy())
	require.NoError(t, err)
	failure := errors.New("metadata staging failure")
	require.ErrorIs(t, m.atomicCommit(p, samplePolicy(), func(*pebble.Batch) error { return failure }), failure)
	_, err = m.Lookup(7)
	require.ErrorIs(t, err, ErrLegacyUnknown)
	history, err := m.ReadPolicyHistory()
	require.NoError(t, err)
	require.Empty(t, history)
	require.NoError(t, m.atomicCommit(p, samplePolicy(), nil))
}
func TestRecoveryRejectsInvalidPolicyBeforePersist(t *testing.T) {
	t.Run("invalid policy", func(t *testing.T) {
		db, m := openTest(t)
		p, err := m.Prepare(sampleEvent(), samplePolicy())
		require.NoError(t, err)
		p.Policy.Collections = []string{"", "org.example.record"}
		value, err := encode(p)
		require.NoError(t, err)
		require.NoError(t, m.db.Set(m.preparedKey(7), value, store.SyncWrites))
		err = m.Recover(func(uint64) (DurableEvent, error) { return DurableEvent{Exists: true, Fingerprint: p.Fingerprint}, nil }, nil)
		require.ErrorIs(t, err, ErrInvalid)
		_, err = m.Lookup(7)
		require.ErrorIs(t, err, ErrLegacyUnknown)
		_, closer, err := db.Get(m.preparedKey(7))
		require.NoError(t, err)
		require.NoError(t, closer.Close())
	})
	t.Run("revision mismatch", func(t *testing.T) {
		_, m := openTest(t)
		p, err := m.Prepare(sampleEvent(), samplePolicy())
		require.NoError(t, err)
		p.Policy.Revision++
		value, err := encode(p)
		require.NoError(t, err)
		require.NoError(t, m.db.Set(m.preparedKey(7), value, store.SyncWrites))
		require.ErrorIs(t, m.Recover(func(uint64) (DurableEvent, error) { return DurableEvent{Exists: true, Fingerprint: p.Fingerprint}, nil }, nil), ErrInvalid)
	})
}

func TestRecoveryCoordinatesWithCommitWithoutDeadlock(t *testing.T) {
	_, m := openTest(t)
	p, err := m.Prepare(sampleEvent(), samplePolicy())
	require.NoError(t, err)
	started := make(chan struct{})
	continueLookup := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- m.Recover(func(uint64) (DurableEvent, error) {
			close(started)
			<-continueLookup
			return DurableEvent{Exists: true, Fingerprint: p.Fingerprint}, nil
		}, func(uint64) (Policy, error) { return samplePolicy(), nil })
	}()
	<-started
	commitDone := make(chan error, 1)
	go func() { commitDone <- m.atomicCommit(p, samplePolicy(), nil) }()
	close(continueLookup)
	require.NoError(t, <-done)
	require.NoError(t, <-commitDone)
	_, err = m.Lookup(7)
	require.NoError(t, err)
}
func TestPolicyHistoryRejectsMalformedKey(t *testing.T) {
	db, m := openTest(t)
	bad, err := encode(samplePolicy())
	require.NoError(t, err)
	key := append(append([]byte(nil), m.policyKey(2)...), []byte("/suffix")...)
	require.NoError(t, db.Set(key, bad, store.SyncWrites))
	_, err = m.ReadPolicyHistory()
	require.ErrorIs(t, err, ErrInvalid)
}

func TestPolicyHistoryRejectsKeyRevisionMismatch(t *testing.T) {
	db, m := openTest(t)
	bad, err := encode(Policy{Revision: 3, Collections: []string{"org.example.record"}})
	require.NoError(t, err)
	require.NoError(t, db.Set(m.policyKey(2), bad, store.SyncWrites))
	_, err = m.ReadPolicyHistory()
	require.ErrorIs(t, err, ErrInvalid)
}
