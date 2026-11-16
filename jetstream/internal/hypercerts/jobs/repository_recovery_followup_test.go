package jobs

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

func recoveryJobs(m *Manager) []Job {
	var result []Job
	for _, job := range m.List() {
		if job.Reason == ReasonRepositoryRecovery {
			result = append(result, job)
		}
	}
	return result
}

func TestRecoveryFollowupPromotionAndReceipt(t *testing.T) {
	for _, outcome := range []error{nil, &InputError{Code: "invalid"}, &InputError{Code: "unavailable", Unavailable: true}} {
		t.Run(fmt.Sprint(outcome), func(t *testing.T) {
			dir := t.TempDir()
			m, db := newManager(t, dir)
			_, err := m.AddSourceWithRevision("https://pds.example", 4)
			require.NoError(t, err)
			first := seedRecoveryJob(t, m, []string{recoveryDID1})
			claimForSeed(t, m, first.ID)
			second := seedRecoveryJob(t, m, []string{recoveryDID2})
			mixed, err := m.RequestRepositoryRecovery("https://pds.example", []string{recoveryDID1, recoveryDID2, recoveryDID3}, "mixed")
			require.NoError(t, err)
			require.Equal(t, min(first.ID, second.ID), mixed.ID)
			for range 10 {
				seedRecoveryJob(t, m, []string{recoveryDID1})
			}
			require.Len(t, recoveryJobs(m), 2)
			require.NoError(t, db.Close())
			m, db = newManager(t, dir)
			resumed, err := m.Get(first.ID)
			require.NoError(t, err)
			require.True(t, recoverySealed(resumed))
			require.Equal(t, Pending, resumed.State)
			require.Equal(t, []string{recoveryDID1}, resumed.RecoveryFollowupDIDs)
			claimForSeed(t, m, first.ID)
			_, _, err = m.persistJobOutcome(t.Context(), first.ID, outcome)
			require.NoError(t, err)
			all := recoveryJobs(m)
			require.Len(t, all, 3)
			var successor Job
			for _, job := range all {
				if job.ID != first.ID && job.ID != second.ID {
					successor = job
				}
			}
			require.Equal(t, []string{recoveryDID1}, successor.RecoveryDIDs)
			require.False(t, recoverySealed(successor))
			require.Empty(t, successor.CompletedRepos)
			require.False(t, successor.TotalReposKnown)
			require.Zero(t, successor.Attempts)
			require.Greater(t, successor.AcquisitionGeneration, first.AcquisitionGeneration)
			require.ErrorIs(t, m.Retry(first.ID), ErrConflict)
			require.NoError(t, db.Close())
			m, db = newManager(t, dir)
			defer db.Close()
			replay, err := m.RequestRepositoryRecovery("https://pds.example", []string{recoveryDID3}, "mixed")
			require.NoError(t, err)
			require.Equal(t, mixed.ID, replay.ID)
			require.Len(t, recoveryJobs(m), 3)
			for _, job := range recoveryJobs(m) {
				require.Empty(t, job.RecoveryFollowupDIDs)
			}
		})
	}
}

func TestRecoveryYieldAndRootShutdownRetainIntent(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	_, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	first := seedRecoveryJob(t, m, []string{recoveryDID1})
	claimForSeed(t, m, first.ID)
	seedRecoveryJob(t, m, []string{recoveryDID1})
	_, _, err = m.persistJobOutcome(t.Context(), first.ID, errJobYield)
	require.NoError(t, err)
	seedRecoveryJob(t, m, []string{recoveryDID2})
	stored, err := m.Get(first.ID)
	require.NoError(t, err)
	require.Equal(t, []string{recoveryDID1}, stored.RecoveryDIDs)
	require.True(t, recoverySealed(stored))
	claimForSeed(t, m, first.ID)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err = m.persistJobOutcome(ctx, first.ID, context.Canceled)
	require.ErrorIs(t, err, context.Canceled)
	stored, err = m.Get(first.ID)
	require.NoError(t, err)
	require.Equal(t, Running, stored.State)
	require.Equal(t, []string{recoveryDID1}, stored.RecoveryFollowupDIDs)
	require.NoError(t, m.Cancel(first.ID))
	stored, err = m.Get(first.ID)
	require.NoError(t, err)
	require.Empty(t, stored.RecoveryFollowupDIDs)
	require.NoError(t, m.Retry(first.ID))
}

func TestRecoveryAdmissionAndClaimRace(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	source, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	require.NoError(t, m.Cancel(source.ID))
	first := seedRecoveryJob(t, m, []string{recoveryDID1})
	var group sync.WaitGroup
	for range 32 {
		group.Go(func() {
			_, err := m.RequestRepositoryRecovery("https://pds.example", []string{recoveryDID1}, "")
			if err != nil {
				t.Error(err)
			}
		})
	}
	group.Go(func() {
		m.mu.Lock()
		_, err := m.claimNextLocked()
		m.mu.Unlock()
		if err != nil {
			t.Error(err)
		}
	})
	group.Wait()
	require.Len(t, recoveryJobs(m), 1)
	stored, err := m.Get(first.ID)
	require.NoError(t, err)
	require.True(t, recoverySealed(stored))
	require.LessOrEqual(t, len(stored.RecoveryFollowupDIDs), 1)
	seedRecoveryJob(t, m, []string{recoveryDID1})
	stored, err = m.Get(first.ID)
	require.NoError(t, err)
	require.Equal(t, []string{recoveryDID1}, stored.RecoveryFollowupDIDs)
}

func TestRecoverySourceAndPolicyInvalidation(t *testing.T) {
	for _, change := range []string{"remove", "revision", "policy"} {
		t.Run(change, func(t *testing.T) {
			m, db := newManager(t, t.TempDir())
			defer db.Close()
			_, err := m.AddSource("https://pds.example")
			require.NoError(t, err)
			first := seedRecoveryJob(t, m, []string{recoveryDID1})
			claimForSeed(t, m, first.ID)
			seedRecoveryJob(t, m, []string{recoveryDID1})
			switch change {
			case "remove":
				err = m.RemoveSource("https://pds.example")
			case "revision":
				_, err = m.AddSourceWithRevision("https://pds.example", 2)
			case "policy":
				_, err = m.SetPolicy(t.Context(), m.policy.Current().Revision, []string{"app.bsky.feed.like"})
			}
			require.NoError(t, err)
			stored, err := m.Get(first.ID)
			require.NoError(t, err)
			require.Equal(t, Canceled, stored.State)
			require.Empty(t, stored.RecoveryFollowupDIDs)
			require.ErrorIs(t, m.Retry(first.ID), ErrConflict)
			_, _, err = m.persistJobOutcome(t.Context(), first.ID, nil)
			require.NoError(t, err)
			require.Len(t, recoveryJobs(m), 1)
		})
	}
}

func TestRecoveryGlobalLimitsAndCoordinateConflict(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	_, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	_, err = m.AddSource("https://other.example")
	require.NoError(t, err)
	dids := make([]string, recoveryScopeLimit)
	for index := range dids {
		dids[index] = fmt.Sprintf("did:web:repo%d.example", index)
	}
	first := seedRecoveryJob(t, m, dids)
	claimForSeed(t, m, first.ID)
	before := clone(m.data)
	_, err = m.RequestRepositoryRecovery("https://pds.example", []string{dids[0], recoveryDID1}, "cap")
	require.ErrorIs(t, err, ErrRecoveryBusy)
	require.Equal(t, before, clone(m.data))
	_, err = m.RequestRepositoryRecovery("https://other.example", []string{dids[0]}, "")
	require.ErrorIs(t, err, ErrRecoveryBusy)
	seedRecoveryJob(t, m, []string{dids[0]})
	_, err = m.RequestRepositoryRecovery("https://pds.example", append(dids, recoveryDID1), "")
	require.ErrorIs(t, err, ErrInvalidInput)
	stored, err := m.Get(first.ID)
	require.NoError(t, err)
	require.Equal(t, []string{dids[0]}, stored.RecoveryFollowupDIDs)
}

func TestRecoveryMigrationRejectsDuplicateWithoutRewrite(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	_, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	first := seedRecoveryJob(t, m, []string{recoveryDID1})
	legacy := clone(m.data)
	job := legacy.Jobs[first.ID]
	job.RecoveryScopeSealed = nil
	legacy.Jobs[first.ID] = job
	require.NoError(t, m.save(legacy))
	reopened, err := Open(db, m.policy)
	require.NoError(t, err)
	stored, err := reopened.Get(first.ID)
	require.NoError(t, err)
	require.True(t, recoverySealed(stored))
	duplicate := clone(reopened.data)
	job.ID = "duplicate"
	duplicate.Jobs[job.ID] = job
	require.NoError(t, m.save(duplicate))
	_, err = Open(db, m.policy)
	require.ErrorContains(t, err, "ambiguous")
	persisted, found, err := readPersistedData(db)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, clone(duplicate), clone(persisted))
}

func TestRecoveryAtomicAdmissionPromotionAndClaimFailures(t *testing.T) {
	for _, stage := range []string{"admission", "promotion", "claim", "cancel", "revision", "policy"} {
		t.Run(stage, func(t *testing.T) {
			injected := errors.New("injected")
			enabled := false
			fault := recoveryWriteFault{fail: func() bool { return enabled }, err: injected}
			m, db := newManagerWithOptions(t, t.TempDir(), store.WithFaultInjector(fault))
			defer db.Close()
			source, err := m.AddSource("https://pds.example")
			require.NoError(t, err)
			require.NoError(t, m.Cancel(source.ID))
			first := seedRecoveryJob(t, m, []string{recoveryDID1})
			if stage != "claim" {
				claimForSeed(t, m, first.ID)
			}
			if stage != "admission" {
				seedRecoveryJob(t, m, []string{recoveryDID1})
			}
			before := clone(m.data)
			enabled = true
			switch stage {
			case "admission":
				_, err = m.RequestRepositoryRecovery("https://pds.example", []string{recoveryDID1, recoveryDID2}, "atomic")
			case "promotion":
				_, _, err = m.persistJobOutcome(t.Context(), first.ID, nil)
			case "claim":
				m.mu.Lock()
				_, err = m.claimNextLocked()
				m.mu.Unlock()
			case "cancel":
				err = m.Cancel(first.ID)
			case "revision":
				_, err = m.AddSourceWithRevision("https://pds.example", 2)
			case "policy":
				_, err = m.SetPolicy(t.Context(), m.policy.Current().Revision, []string{"app.bsky.feed.like"})
			}
			require.ErrorIs(t, err, injected)
			require.Equal(t, before, clone(m.data))
			persisted, _, err := readPersistedData(db)
			require.NoError(t, err)
			require.Equal(t, before, clone(persisted))
		})
	}
}

type recoveryWriteFault struct {
	fail func() bool
	err  error
}

func (fault recoveryWriteFault) BeforeWrite(_ store.WriteOp, keys [][]byte) error {
	if fault.fail() && slices.ContainsFunc(keys, func(key []byte) bool { return string(key) == stateKey }) {
		return fault.err
	}
	return nil
}

func TestRecoveryOutcomeObservationRace(t *testing.T) {
	for range 20 {
		m, db := newManager(t, t.TempDir())
		_, err := m.AddSource("https://pds.example")
		require.NoError(t, err)
		first := seedRecoveryJob(t, m, []string{recoveryDID1})
		claimForSeed(t, m, first.ID)
		var group sync.WaitGroup
		group.Go(func() {
			_, err := m.RequestRepositoryRecovery("https://pds.example", []string{recoveryDID1}, "")
			if err != nil {
				t.Error(err)
			}
		})
		group.Go(func() {
			_, _, err := m.persistJobOutcome(t.Context(), first.ID, nil)
			if err != nil {
				t.Error(err)
			}
		})
		group.Wait()
		owners, err := recoveryOwners(m.data)
		require.NoError(t, err)
		require.Len(t, owners, 1)
		require.NotEqual(t, first.ID, owners[recoveryDID1])
		require.Len(t, recoveryJobs(m), 2)
		require.NoError(t, db.Close())
	}
}

func TestRecoveryCollisionChainPreservesHistoryAndReceipts(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	_, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	first, err := m.RequestRepositoryRecovery("https://pds.example", []string{recoveryDID1}, "original")
	require.NoError(t, err)
	claimForSeed(t, m, first.ID)
	seedRecoveryJob(t, m, []string{recoveryDID1})
	next := clone(m.data)
	collision := newJob(first.PDS, first.Policy, ReasonRepositoryRecovery, first.SourceRevision)
	collision.ID = distinctRecoveryJobID(first.ID, len(next.Jobs))
	collision.RecoveryDIDs = []string{recoveryDID3}
	collision.State = Complete
	next.Jobs[collision.ID] = collision
	require.NoError(t, m.commit(next))
	_, _, err = m.persistJobOutcome(t.Context(), first.ID, nil)
	require.NoError(t, err)
	stored, err := m.Get(collision.ID)
	require.NoError(t, err)
	require.Equal(t, collision, stored)
	replay, err := m.RequestRepositoryRecovery("https://pds.example", []string{recoveryDID2}, "original")
	require.NoError(t, err)
	require.Equal(t, first.ID, replay.ID)
	require.Len(t, recoveryJobs(m), 3)
}
