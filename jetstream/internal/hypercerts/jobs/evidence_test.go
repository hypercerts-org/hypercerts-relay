package jobs

import (
	"context"
	"encoding/json"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestAcquisitionEvidenceRestartAndIdempotency(t *testing.T) {
	dir := t.TempDir()
	m, db := newManager(t, dir)
	j, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	m.mu.Lock()
	j, err = m.claimNextLocked()
	m.mu.Unlock()
	require.NoError(t, err)
	require.NoError(t, m.CheckpointInventory(j.ID, "", map[string]string{"a": "r1", "b": "r2", "c": "r3"}, true))
	require.NoError(t, m.CheckpointAcquisition(j.ID, "a", "r1", "", 2))
	require.NoError(t, m.CheckpointAcquisition(j.ID, "a", "r1", "", 2))
	require.NoError(t, m.CheckpointAcquisition(j.ID, "b", "r2", "", 0))
	saved, err := m.Get(j.ID)
	require.NoError(t, err)
	require.Equal(t, 2, saved.Evidence.Scanned)
	require.Equal(t, 1, saved.Evidence.Matching)
	require.Equal(t, 1, saved.Evidence.NoMatch)
	require.Equal(t, 2, saved.Evidence.AttributableRecords)
	progress := saved.Evidence.LastProgressAt
	require.False(t, progress.IsZero())
	require.NoError(t, db.Close())
	m, db = newManager(t, dir)
	defer db.Close()
	reopened, err := m.Get(j.ID)
	require.NoError(t, err)
	require.Equal(t, saved.Evidence, reopened.Evidence)
	m.mu.Lock()
	_, err = m.claimNextLocked()
	m.mu.Unlock()
	require.NoError(t, err)
	require.NoError(t, m.CheckpointAcquisition(j.ID, "a", "r1", "", 2))
	duplicate, _ := m.Get(j.ID)
	require.Equal(t, progress, duplicate.Evidence.LastProgressAt)
	require.NoError(t, m.persistEvidenceFailure(j.ID))
	failed, _ := m.Get(j.ID)
	require.Equal(t, Failed, failed.State)
	require.NoError(t, m.Retry(j.ID))
	retry, _ := m.Get(j.ID)
	require.Equal(t, &AcquisitionEvidence{}, retry.Evidence)
	require.False(t, retry.TotalReposKnown)
	require.Empty(t, retry.CompletedRepos)
}
func (m *Manager) persistEvidenceFailure(id string) error {
	_, _, err := m.persistJobOutcome(context.Background(), id, &InputError{Code: "invalid_repository"})
	return err
}
func TestLegacyPersistedJobKeepsUnknownEvidence(t *testing.T) {
	dir := t.TempDir()
	m, db := newManager(t, dir)
	j, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	legacy := m.data
	j = legacy.Jobs[j.ID]
	j.Evidence = nil
	j.State = Complete
	j.CompletedRepos["did:plc:old"] = "revision"
	legacy.Jobs[j.ID] = j
	encoded, err := json.Marshal(legacy)
	require.NoError(t, err)
	require.NoError(t, db.Set([]byte(stateKey), encoded, store.SyncWrites))
	require.NoError(t, db.Close())
	m, db = newManager(t, dir)
	defer db.Close()
	old, err := m.Get(j.ID)
	require.NoError(t, err)
	require.Nil(t, old.Evidence)
	require.Len(t, old.CompletedRepos, 1)
}
func TestAllNoMatchInventoryCompletes(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	j, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	_, err = m.runNext(context.Background(), func(ctx context.Context, job Job) error {
		if err := m.CheckpointInventory(job.ID, "", map[string]string{"a": "rev"}, true); err != nil {
			return err
		}
		return m.CheckpointAcquisition(job.ID, "a", "rev", "", 0)
	})
	require.NoError(t, err)
	done, err := m.Get(j.ID)
	require.NoError(t, err)
	require.Equal(t, Complete, done.State)
	require.Equal(t, 1, done.Evidence.NoMatch)
	require.Zero(t, done.Evidence.Matching)
	require.Zero(t, done.Evidence.AttributableRecords)
	require.WithinDuration(t, time.Now(), done.Evidence.LastProgressAt, time.Second)
}

func TestRetryGenerationSurvivesRestartAndDuplicateReceipt(t *testing.T) {
	dir := t.TempDir()
	m, db := newManager(t, dir)
	old, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	require.Equal(t, uint64(1), old.AcquisitionGeneration)
	_, err = m.runNext(context.Background(), func(context.Context, Job) error { return &InputError{Code: "invalid_repository"} })
	require.NoError(t, err)
	newer, err := m.Request(old.PDS, "backfill")
	require.NoError(t, err)
	require.Equal(t, uint64(2), newer.AcquisitionGeneration)
	_, err = m.runNext(context.Background(), func(context.Context, Job) error { return nil })
	require.NoError(t, err)
	require.NoError(t, m.TransitionOnce(old.ID, Pending, "retry-older"))
	retried, err := m.Get(old.ID)
	require.NoError(t, err)
	require.Equal(t, uint64(3), retried.AcquisitionGeneration)
	require.Equal(t, old.CreatedAt, retried.CreatedAt)
	require.NoError(t, m.TransitionOnce(old.ID, Pending, "retry-older"))
	duplicate, err := m.Get(old.ID)
	require.NoError(t, err)
	require.Equal(t, retried.AcquisitionGeneration, duplicate.AcquisitionGeneration)
	require.NoError(t, db.Close())
	m, db = newManager(t, dir)
	defer db.Close()
	reopened, err := m.Get(old.ID)
	require.NoError(t, err)
	require.Equal(t, retried.AcquisitionGeneration, reopened.AcquisitionGeneration)
	require.NoError(t, m.TransitionOnce(old.ID, Pending, "retry-older"))
	_, err = m.runNext(context.Background(), func(ctx context.Context, claimed Job) error {
		require.Equal(t, old.ID, claimed.ID)
		require.Equal(t, uint64(3), claimed.AcquisitionGeneration)
		require.Equal(t, Running, claimed.State)
		return &InputError{Code: "repository_unavailable", Unavailable: true}
	})
	require.NoError(t, err)
	stopped, err := m.Get(old.ID)
	require.NoError(t, err)
	require.Equal(t, Incomplete, stopped.State)
	require.Equal(t, uint64(3), stopped.AcquisitionGeneration)
	require.NoError(t, m.Retry(old.ID))
	_, err = m.runNext(context.Background(), func(context.Context, Job) error { return nil })
	require.NoError(t, err)
	complete, err := m.Get(old.ID)
	require.NoError(t, err)
	require.Equal(t, Complete, complete.State)
	require.Equal(t, uint64(4), complete.AcquisitionGeneration)
}
