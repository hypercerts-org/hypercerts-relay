package attribution

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func TestDecodeRejectsTrailingValuesAndMalformedSuffix(t *testing.T) {
	for _, suffix := range []string{` {}`, ` null`, ` garbage`, ` {`} {
		t.Run(suffix, func(t *testing.T) {
			var event Event
			require.ErrorIs(t, decode(append([]byte{valueVersion}, []byte(`{}`+suffix)...), &event), ErrInvalid)
		})
	}
	var event Event
	require.NoError(t, decode(append([]byte{valueVersion}, []byte("{} \n\t")...), &event))
}

func TestCommitBatchMinimumActivationAndIdempotence(t *testing.T) {
	_, manager := openTest(t)
	for _, sequences := range [][]uint64{{9, 7, 8}, {12, 11}, {5, 6}} {
		var entries []Prepared
		for _, seq := range sequences {
			event := sampleEvent()
			event.Seq = seq
			prepared, err := manager.Prepare(event, samplePolicy())
			require.NoError(t, err)
			entries = append(entries, prepared)
		}
		require.NoError(t, manager.CommitBatch(entries, nil))
		require.NoError(t, manager.CommitBatch(entries, nil))
		activation, err := manager.ActivationSeq()
		require.NoError(t, err)
		require.Equal(t, min(uint64(7), sequences[0]), activation)
		pending, err := manager.PreparedSeqs()
		require.NoError(t, err)
		require.Empty(t, pending)
	}
}

func TestCommitBatchValidationPrecedesConflictAndDoesNotActivate(t *testing.T) {
	_, manager := openTest(t)
	prepared, err := manager.Prepare(sampleEvent(), samplePolicy())
	require.NoError(t, err)
	conflicting := prepared
	conflicting.Event.Payload = json.RawMessage(`{"operation":"update"}`)
	conflicting.Fingerprint = fingerprint(conflicting.Event)
	invalid := prepared
	invalid.Event.Seq = 0
	require.ErrorIs(t, manager.CommitBatch([]Prepared{prepared, conflicting, invalid}, nil), ErrInvalid)
	require.ErrorIs(t, manager.CommitBatch([]Prepared{prepared, conflicting}, nil), ErrConflict)
	activation, err := manager.ActivationSeq()
	require.NoError(t, err)
	require.Zero(t, activation)
	pending, err := manager.PreparedSeqs()
	require.NoError(t, err)
	require.Equal(t, []uint64{7}, pending)
}

func TestRecoveryResolvesOnlyMissingLegacyPolicy(t *testing.T) {
	for _, missing := range []*Policy{nil, {}} {
		_, manager := openTest(t)
		prepared, err := manager.Prepare(sampleEvent(), samplePolicy())
		require.NoError(t, err)
		prepared.Policy = missing
		encoded, err := encode(prepared)
		require.NoError(t, err)
		require.NoError(t, manager.db.Set(manager.preparedKey(7), encoded, store.SyncWrites))
		calls := 0
		require.NoError(t, manager.Recover(func(uint64) (DurableEvent, error) {
			return DurableEvent{Exists: true, Fingerprint: prepared.Fingerprint}, nil
		}, func(revision uint64) (Policy, error) {
			calls++
			require.Equal(t, uint64(2), revision)
			return samplePolicy(), nil
		}))
		require.Equal(t, 1, calls)
		policy, err := manager.LookupPolicy(2)
		require.NoError(t, err)
		require.Equal(t, samplePolicy(), policy)
	}
}

func TestRecoveryPreservesLiteralEmptyCollectionsPolicy(t *testing.T) {
	for _, history := range []bool{false, true} {
		t.Run(map[bool]string{false: "without history", true: "with history"}[history], func(t *testing.T) {
			_, manager := openTest(t)
			event := sampleEvent()
			payload, err := json.Marshal(event)
			require.NoError(t, err)
			literal := []byte(`{"event":` + string(payload) + `,"policy":{"revision":2,"collections":[]},"fingerprint":"` + fingerprint(event) + `"}`)
			persisted := append([]byte{valueVersion}, literal...)
			require.NoError(t, manager.db.Set(manager.preparedKey(event.Seq), persisted, store.SyncWrites))
			if history {
				require.NoError(t, manager.db.Set(manager.policyKey(2), append([]byte{valueVersion}, []byte(`{"revision":2,"collections":[]}`)...), store.SyncWrites))
			}
			require.ErrorIs(t, manager.Recover(func(uint64) (DurableEvent, error) {
				return DurableEvent{Exists: true, Fingerprint: fingerprint(event)}, nil
			}, func(uint64) (Policy, error) {
				t.Fatal("valid persisted policy must not use fallback resolution")
				return Policy{}, nil
			}), ErrInvalid)
			activation, err := manager.ActivationSeq()
			require.NoError(t, err)
			require.Zero(t, activation)
			_, err = manager.Lookup(event.Seq)
			require.ErrorIs(t, err, ErrLegacyUnknown)
			stored, closer, err := manager.db.Get(manager.preparedKey(event.Seq))
			require.NoError(t, err)
			require.Equal(t, persisted, stored)
			require.NoError(t, closer.Close())
			policies, err := manager.ReadPolicyHistory()
			require.NoError(t, err)
			if history {
				require.Equal(t, []Policy{{Revision: 2, Collections: []string{}}}, policies)
			} else {
				require.Empty(t, policies)
			}
		})
	}
}

func TestRecoveryValidatesFingerprintBeforeDeletingOrphan(t *testing.T) {
	_, manager := openTest(t)
	prepared, err := manager.Prepare(sampleEvent(), samplePolicy())
	require.NoError(t, err)
	prepared.Fingerprint = "invalid"
	encoded, err := encode(prepared)
	require.NoError(t, err)
	require.NoError(t, manager.db.Set(manager.preparedKey(7), encoded, store.SyncWrites))
	require.ErrorIs(t, manager.Recover(func(uint64) (DurableEvent, error) {
		t.Fatal("lookup must follow fingerprint validation")
		return DurableEvent{}, nil
	}, nil), ErrFingerprint)
	_, closer, err := manager.db.Get(manager.preparedKey(7))
	require.NoError(t, err)
	require.NoError(t, closer.Close())
}

func TestPrepareSegmentImportedSourceCopiesAndRejectsPolicyMismatch(t *testing.T) {
	_, manager := openTest(t)
	snapshotID := "snapshot"
	source := &ImportedSource{Domain: "bootstrap", StreamID: "source-stream", Seq: 3, PolicyRev: uint64Ptr(2), Policy: policyPtr(samplePolicy()), Provenance: ProvenanceSnapshot, Boundary: Boundary{Seq: uint64Ptr(1), SnapshotID: &snapshotID}, SegmentFingerprint: "source-fingerprint"}
	event := segment.Event{Seq: 7, Kind: segment.KindCreate}
	prepared, err := manager.PrepareSegment(event, "crypto-identity", samplePolicy(), ProvenanceLive, Boundary{}, source)
	require.NoError(t, err)
	*source.PolicyRev = 9
	source.Policy.Collections[0] = "org.changed.record"
	*source.Boundary.Seq = 99
	snapshotID = "changed"
	require.Equal(t, uint64(2), *prepared.Event.Source.PolicyRev)
	require.Equal(t, samplePolicy(), *prepared.Event.Source.Policy)
	require.Equal(t, uint64(1), *prepared.Event.Source.Boundary.Seq)
	require.Equal(t, "snapshot", *prepared.Event.Source.Boundary.SnapshotID)
	_, err = manager.PrepareSegment(event, "crypto-identity", samplePolicy(), ProvenanceLive, Boundary{}, source)
	require.True(t, errors.Is(err, ErrInvalid))
}
