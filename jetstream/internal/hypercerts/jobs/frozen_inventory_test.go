package jobs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func cachedInventory(m *Manager, id string) (map[string]string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entries, ok := m.frozenInventories[id]
	return entries, ok
}

func TestFrozenInventoryCacheIsDroppedByFailedRetryInventoryReset(t *testing.T) {
	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	const oldRevision = "3l3qo2vutsw2b"
	const newRevision = "3l3qo2vutsw2c"
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	job, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	worker, _ := startRepositoryWorker(t, m, job.ID, map[string]string{did: oldRevision})
	_, err = m.BeginRepositoryAttempt(job.ID, did)
	require.NoError(t, err)
	cached, ok := cachedInventory(m, job.ID)
	require.True(t, ok, "a running job with a complete listing caches its inventory")
	require.Equal(t, map[string]string{did: oldRevision}, cached)
	_, err = m.RecordRepositoryFailure(job.ID, did, RepositoryRetryFailure{Category: RepositoryFailureUnknown, Stage: RepositoryFailureGetRepoRequest}, nil, nil)
	require.NoError(t, err)
	worker.finishAs(t, m, job.ID, &InputError{Code: "invalid_repository"}, Failed)
	_, ok = cachedInventory(m, job.ID)
	require.False(t, ok, "a job outcome drops its cached inventory")

	require.NoError(t, m.Retry(job.ID))
	worker, _ = startRepositoryWorker(t, m, job.ID, map[string]string{did: newRevision})
	defer worker.stop()
	ready, err := m.GetRepositoryRetry(job.ID, did)
	require.NoError(t, err)
	require.Equal(t, newRevision, ready.ListedRevision, "the refreshed listing replaces the cached inventory")
	_, err = m.BeginRepositoryAttempt(job.ID, did)
	require.NoError(t, err)
	require.ErrorIs(t, m.CheckpointRepository(job.ID, did, oldRevision, newRevision, ""), ErrConflict, "a stale listed revision is not accepted from a cache")
	require.NoError(t, m.CheckpointRepository(job.ID, did, newRevision, newRevision, ""))
}

func TestFrozenInventoryCacheSeesChangedListing(t *testing.T) {
	const didA = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	const didB = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"
	const revision = "3l3qo2vutsw2b"
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	job, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	worker, _ := startRepositoryWorker(t, m, job.ID, map[string]string{didA: revision})
	defer worker.stop()
	first, err := m.Inventory(job.ID)
	require.NoError(t, err)
	require.Equal(t, []ListedRepository{{DID: didA, Revision: revision}}, first)
	_, ok := cachedInventory(m, job.ID)
	require.True(t, ok)

	// A recovery-style re-seed writes through the only inventory writer.
	require.NoError(t, m.CheckpointInventory(job.ID, "", map[string]string{didB: revision}, true))
	_, ok = cachedInventory(m, job.ID)
	require.False(t, ok, "an inventory write drops the cached copy")
	second, err := m.Inventory(job.ID)
	require.NoError(t, err)
	require.Equal(t, []ListedRepository{{DID: didA, Revision: revision}, {DID: didB, Revision: revision}}, second)
	retry, err := m.GetRepositoryRetry(job.ID, didB)
	require.NoError(t, err)
	require.Equal(t, RepositoryRetryReady, retry.State)
}

func TestFrozenInventoryCallersCannotMutateCachedEntries(t *testing.T) {
	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	const revision = "3l3qo2vutsw2b"
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	job, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	worker, _ := startRepositoryWorker(t, m, job.ID, map[string]string{did: revision})
	defer worker.stop()
	listed, err := m.Inventory(job.ID)
	require.NoError(t, err)
	listed[0] = ListedRepository{DID: "did:plc:zzzzzzzzzzzzzzzzzzzzzzzz", Revision: "3l3qo2vutsw2z"}
	again, err := m.Inventory(job.ID)
	require.NoError(t, err)
	require.Equal(t, []ListedRepository{{DID: did, Revision: revision}}, again)
	cached, ok := cachedInventory(m, job.ID)
	require.True(t, ok)
	require.Equal(t, map[string]string{did: revision}, cached)
}

func TestFrozenInventoryIsNotCachedForIdleJobs(t *testing.T) {
	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	const revision = "3l3qo2vutsw2b"
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	job, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	worker, _ := startRepositoryWorker(t, m, job.ID, map[string]string{did: revision})
	worker.finishIncomplete(t, m, job.ID)
	listed, err := m.Inventory(job.ID)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	_, err = m.ListRepositoryRetries(job.ID)
	require.NoError(t, err)
	_, ok := cachedInventory(m, job.ID)
	require.False(t, ok, "inspecting a stopped job must not retain its inventory")
}
