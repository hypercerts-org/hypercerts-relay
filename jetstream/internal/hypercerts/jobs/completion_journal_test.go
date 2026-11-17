package jobs

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

const (
	journalDIDA     = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	journalDIDB     = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"
	journalRevision = "3l3qo2vutsw2b"
)

func journalKeys(t *testing.T, db *store.Store) [][]byte {
	t.Helper()
	prefix := []byte(repositoryCompletionPrefix)
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: store.PrefixUpperBound(prefix)})
	require.NoError(t, err)
	defer func() { require.NoError(t, iter.Close()) }()
	var keys [][]byte
	for iter.First(); iter.Valid(); iter.Next() {
		keys = append(keys, append([]byte(nil), iter.Key()...))
	}
	require.NoError(t, iter.Error())
	return keys
}

func rawJobState(t *testing.T, db *store.Store) []byte {
	t.Helper()
	value, closer, err := db.Get([]byte(stateKey))
	require.NoError(t, err)
	defer closer.Close()
	return append([]byte(nil), value...)
}

func persistedCompletedRepos(t *testing.T, db *store.Store, id string) map[string]string {
	t.Helper()
	var job struct {
		CompletedRepos map[string]string `json:"completedRepos"`
	}
	require.NoError(t, json.Unmarshal(persistedJobState(t, db, id), &job))
	return job.CompletedRepos
}

func checkpointJournaled(t *testing.T, m *Manager, jobID, did string, records int) {
	t.Helper()
	_, err := m.BeginRepositoryAttempt(jobID, did)
	require.NoError(t, err)
	require.NoError(t, m.CheckpointRepositoryAcquisition(jobID, did, journalRevision, journalRevision, "", records))
}

func TestRepositoryCheckpointWritesJournalRowInsteadOfJobState(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	job, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	worker, _ := startRepositoryWorker(t, m, job.ID, map[string]string{journalDIDA: journalRevision, journalDIDB: journalRevision})
	defer worker.stop()
	before := rawJobState(t, db)

	checkpointJournaled(t, m, job.ID, journalDIDA, 2)

	require.Equal(t, before, rawJobState(t, db), "a repository checkpoint must not rewrite the job-state document")
	require.Equal(t, [][]byte{repositoryCompletionKey(job.ID, journalDIDA)}, journalKeys(t, db))
	_, closer, err := db.Get(repositoryRetryKey(job.ID, journalDIDA))
	require.ErrorIs(t, err, store.ErrNotFound, "the retry row is deleted in the same batch")
	if closer != nil {
		closer.Close()
	}
	current, err := m.Get(job.ID)
	require.NoError(t, err)
	require.Equal(t, journalRevision, current.CompletedRepos[journalDIDA])
	require.Equal(t, 1, current.Evidence.Scanned)
	require.Equal(t, 1, current.Evidence.Matching)
	require.Equal(t, 2, current.Evidence.AttributableRecords)
}

func TestCompletionJournalReplaysOnceAfterUnfoldedClose(t *testing.T) {
	dir := t.TempDir()
	m, db := newManager(t, dir)
	job, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	worker, _ := startRepositoryWorker(t, m, job.ID, map[string]string{journalDIDA: journalRevision, journalDIDB: journalRevision})
	// Checkpoint against key order: replay must follow checkpoint order.
	checkpointJournaled(t, m, job.ID, journalDIDB, 3)
	checkpointJournaled(t, m, job.ID, journalDIDA, 0)
	beforeJob, err := m.Get(job.ID)
	require.NoError(t, err)
	beforeDiagnostics, err := m.DiagnosticSnapshots([]string{job.ID})
	require.NoError(t, err)
	// Shutdown cancellation persists no outcome, so the journal is not folded.
	require.ErrorIs(t, worker.stop(), context.Canceled)
	require.Len(t, journalKeys(t, db), 2)
	require.NotContains(t, persistedCompletedRepos(t, db, job.ID), journalDIDA)
	require.NoError(t, db.Close())

	m, db = newManager(t, dir)
	defer db.Close()
	require.Empty(t, journalKeys(t, db), "Open folds the journal into the job-state document")
	require.Equal(t, journalRevision, persistedCompletedRepos(t, db, job.ID)[journalDIDA])
	afterJob, err := m.Get(job.ID)
	require.NoError(t, err)
	require.Equal(t, Pending, afterJob.State)
	require.Equal(t, beforeJob.CompletedRepos, afterJob.CompletedRepos)
	require.Equal(t, beforeJob.Evidence, afterJob.Evidence)
	require.Equal(t, 2, afterJob.Evidence.Scanned, "evidence is counted once")
	require.Equal(t, 1, afterJob.Evidence.NoMatch)
	require.Equal(t, 3, afterJob.Evidence.AttributableRecords)
	require.Equal(t, beforeJob.LastProgressAt, afterJob.LastProgressAt)
	require.Equal(t, beforeJob.Cursor, afterJob.Cursor)
	afterDiagnostics, err := m.DiagnosticSnapshots([]string{job.ID})
	require.NoError(t, err)
	require.Equal(t, beforeDiagnostics[0].CompletedRepos, afterDiagnostics[0].CompletedRepos)
	require.Equal(t, beforeDiagnostics[0].Diagnostics.UnresolvedRepos, afterDiagnostics[0].Diagnostics.UnresolvedRepos)
	require.Equal(t, beforeDiagnostics[0].Job.Evidence, afterDiagnostics[0].Job.Evidence)
	retry, err := m.GetRepositoryRetry(job.ID, journalDIDA)
	require.NoError(t, err)
	require.Equal(t, RepositoryRetryComplete, retry.State)
	retries, err := m.ListRepositoryRetries(job.ID)
	require.NoError(t, err)
	require.Empty(t, retries, "the checkpointed retry row stays deleted")
	require.NoError(t, db.Close())

	m, db = newManager(t, dir)
	defer db.Close()
	again, err := m.Get(job.ID)
	require.NoError(t, err)
	require.Equal(t, afterJob.Evidence, again.Evidence)
}

func TestCompletionJournalReplayIsIdempotentAndIgnoresStaleGeneration(t *testing.T) {
	dir := t.TempDir()
	m, db := newManager(t, dir)
	job, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	worker, _ := startRepositoryWorker(t, m, job.ID, map[string]string{journalDIDA: journalRevision, journalDIDB: journalRevision})
	checkpointJournaled(t, m, job.ID, journalDIDA, 1)
	keys := journalKeys(t, db)
	require.Len(t, keys, 1)
	value, closer, err := db.Get(keys[0])
	require.NoError(t, err)
	row := append([]byte(nil), value...)
	require.NoError(t, closer.Close())
	require.ErrorIs(t, worker.stop(), context.Canceled)
	require.NoError(t, db.Close())

	m, db = newManager(t, dir)
	folded, err := m.Get(job.ID)
	require.NoError(t, err)
	require.Equal(t, 1, folded.Evidence.Scanned)
	// An older binary or a repeated write can leave an already-folded row.
	require.NoError(t, db.Set(keys[0], row, store.SyncWrites))
	var stale repositoryCompletion
	require.NoError(t, json.Unmarshal(row, &stale))
	stale.DID = journalDIDB
	stale.Generation = folded.AcquisitionGeneration + 1
	encodedStale, err := json.Marshal(stale)
	require.NoError(t, err)
	require.NoError(t, db.Set(repositoryCompletionKey(job.ID, journalDIDB), encodedStale, store.SyncWrites))
	require.NoError(t, db.Close())

	m, db = newManager(t, dir)
	defer db.Close()
	replayed, err := m.Get(job.ID)
	require.NoError(t, err)
	require.Equal(t, folded.Evidence, replayed.Evidence, "a duplicate row does not count twice")
	require.NotContains(t, replayed.CompletedRepos, journalDIDB, "a row from another acquisition generation is ignored")
	require.Empty(t, journalKeys(t, db))
}

func TestCompletionJournalNewerRevisionDoesNotCountEvidenceAgain(t *testing.T) {
	const newerRevision = "3l3qo2vutsw2c"
	for name, records := range map[string]int{"matching": 2, "no_match": 0} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			m, db := newManager(t, dir)
			job, err := m.AddSource("https://pds.example")
			require.NoError(t, err)
			worker, _ := startRepositoryWorker(t, m, job.ID, map[string]string{journalDIDA: journalRevision})
			checkpointJournaled(t, m, job.ID, journalDIDA, records)
			require.ErrorIs(t, worker.stop(), context.Canceled)
			require.NoError(t, db.Close())

			m, db = newManager(t, dir)
			folded, err := m.Get(job.ID)
			require.NoError(t, err)
			require.Equal(t, journalRevision, persistedCompletedRepos(t, db, job.ID)[journalDIDA])
			require.Equal(t, 1, folded.Evidence.Scanned)
			// A downgrade can fold an older completion while leaving this
			// newer revision from the same generation and listing unfolded.
			row, err := json.Marshal(repositoryCompletion{
				JobID: job.ID, DID: journalDIDA, ListedRevision: journalRevision,
				CompletedRevision: newerRevision, Generation: folded.AcquisitionGeneration,
				Records: &records, At: time.Now().UTC(),
			})
			require.NoError(t, err)
			require.NoError(t, db.Set(repositoryCompletionKey(job.ID, journalDIDA), row, store.SyncWrites))
			require.NoError(t, db.Close())

			m, db = newManager(t, dir)
			replayed, err := m.Get(job.ID)
			require.NoError(t, err)
			require.Equal(t, newerRevision, replayed.CompletedRepos[journalDIDA])
			require.Equal(t, folded.Evidence, replayed.Evidence, "advancing a completed DID must not count its evidence again")
			require.Empty(t, journalKeys(t, db))
			require.NoError(t, db.Close())

			m, db = newManager(t, dir)
			defer db.Close()
			again, err := m.Get(job.ID)
			require.NoError(t, err)
			require.Equal(t, replayed.CompletedRepos, again.CompletedRepos)
			require.Equal(t, replayed.Evidence, again.Evidence)
		})
	}
}

func TestCompletionJournalCorruptRowFailsOpen(t *testing.T) {
	for name, write := range map[string]func(*store.Store, string) error{
		"undecodable": func(db *store.Store, jobID string) error {
			return db.Set(repositoryCompletionKey(jobID, journalDIDA), []byte("{"), store.SyncWrites)
		},
		"key_mismatch": func(db *store.Store, jobID string) error {
			records := 1
			encoded, err := json.Marshal(repositoryCompletion{JobID: jobID, DID: journalDIDB, ListedRevision: journalRevision, CompletedRevision: journalRevision, Generation: 1, Records: &records, At: time.Now().UTC()})
			if err != nil {
				return err
			}
			return db.Set(repositoryCompletionKey(jobID, journalDIDA), encoded, store.SyncWrites)
		},
		"revision_behind_listing": func(db *store.Store, jobID string) error {
			encoded, err := json.Marshal(repositoryCompletion{JobID: jobID, DID: journalDIDA, ListedRevision: "3l3qo2vutsw2c", CompletedRevision: journalRevision, Generation: 1, At: time.Now().UTC()})
			if err != nil {
				return err
			}
			return db.Set(repositoryCompletionKey(jobID, journalDIDA), encoded, store.SyncWrites)
		},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			m, db := newManager(t, dir)
			job, err := m.AddSource("https://pds.example")
			require.NoError(t, err)
			require.NoError(t, write(db, job.ID))
			before := rawJobState(t, db)
			require.NoError(t, db.Close())

			db, err = store.Open(dir, nil)
			require.NoError(t, err)
			defer db.Close()
			policy, err := selection.Open(db, []string{"app.bsky.feed.post"})
			require.NoError(t, err)
			_, err = Open(db, policy)
			require.ErrorContains(t, err, "invalid persisted repository completion")
			require.Equal(t, before, rawJobState(t, db), "a failed open does not rewrite job state")
		})
	}
}

// startJournaledJob runs a fresh job for pds and checkpoints one repository,
// leaving one journal row while the job keeps running.
func startJournaledJob(t *testing.T, m *Manager, db *store.Store, pds string) (repositoryWorker, Job) {
	t.Helper()
	job, err := m.AddSource(pds)
	require.NoError(t, err)
	worker, _ := startRepositoryWorker(t, m, job.ID, map[string]string{journalDIDA: journalRevision, journalDIDB: journalRevision})
	checkpointJournaled(t, m, job.ID, journalDIDA, 1)
	require.Len(t, journalKeys(t, db), 1)
	return worker, job
}

func requireJournalFolded(t *testing.T, db *store.Store, jobID string) {
	t.Helper()
	require.Empty(t, journalKeys(t, db), "the state write deletes the journal")
	require.Equal(t, journalRevision, persistedCompletedRepos(t, db, jobID)[journalDIDA], "the state document holds the journaled completion")
}

func TestEveryJobStateWriteFoldsCompletionJournal(t *testing.T) {
	// A second, idle job lets each fold site run while the first job holds
	// unfolded journal rows. Only one job runs at a time, so it runs first.
	prepareIdle := func(t *testing.T, m *Manager, outcome error, state State) Job {
		t.Helper()
		idle, err := m.AddSource("https://idle.example")
		require.NoError(t, err)
		worker, _ := startRepositoryWorker(t, m, idle.ID, map[string]string{journalDIDB: journalRevision})
		_, err = m.BeginRepositoryAttempt(idle.ID, journalDIDB)
		require.NoError(t, err)
		_, err = m.RecordRepositoryFailure(idle.ID, journalDIDB, RepositoryRetryFailure{Category: RepositoryFailureUnknown, Stage: RepositoryFailureGetRepoRequest}, nil, nil)
		require.NoError(t, err)
		worker.finishAs(t, m, idle.ID, outcome, state)
		return idle
	}
	for name, run := range map[string]func(*testing.T, *Manager, *store.Store){
		"yield": func(t *testing.T, m *Manager, db *store.Store) {
			worker, job := startJournaledJob(t, m, db, "https://pds.example")
			defer worker.stop()
			// A yielded job is due again at once, so the runner may reclaim it
			// before its Pending state is observable; the fold is durable.
			worker.finish <- errJobYield
			require.Eventually(t, func() bool { return len(journalKeys(t, db)) == 0 }, time.Second, time.Millisecond)
			requireJournalFolded(t, db, job.ID)
		},
		"finish": func(t *testing.T, m *Manager, db *store.Store) {
			worker, job := startJournaledJob(t, m, db, "https://pds.example")
			worker.finishIncomplete(t, m, job.ID)
			requireJournalFolded(t, db, job.ID)
		},
		"cancel": func(t *testing.T, m *Manager, db *store.Store) {
			worker, job := startJournaledJob(t, m, db, "https://pds.example")
			defer worker.stop()
			require.NoError(t, m.Cancel(job.ID))
			requireJournalFolded(t, db, job.ID)
		},
		"failed_retry_inventory_reset": func(t *testing.T, m *Manager, db *store.Store) {
			idle := prepareIdle(t, m, &InputError{Code: "invalid_repository"}, Failed)
			worker, job := startJournaledJob(t, m, db, "https://pds.example")
			defer worker.stop()
			require.NoError(t, m.Retry(idle.ID))
			requireJournalFolded(t, db, job.ID)
		},
		"incomplete_retry_transition": func(t *testing.T, m *Manager, db *store.Store) {
			idle := prepareIdle(t, m, &InputError{Code: "repository_unavailable", Unavailable: true}, Incomplete)
			worker, job := startJournaledJob(t, m, db, "https://pds.example")
			defer worker.stop()
			require.NoError(t, m.Retry(idle.ID))
			requireJournalFolded(t, db, job.ID)
		},
		"policy_change": func(t *testing.T, m *Manager, db *store.Store) {
			worker, job := startJournaledJob(t, m, db, "https://pds.example")
			defer worker.stop()
			_, err := m.SetPolicy(t.Context(), job.Policy.Revision, []string{"app.bsky.feed.like"})
			require.NoError(t, err)
			requireJournalFolded(t, db, job.ID)
		},
		"source_added": func(t *testing.T, m *Manager, db *store.Store) {
			worker, job := startJournaledJob(t, m, db, "https://pds.example")
			defer worker.stop()
			_, err := m.AddSource("https://other.example")
			require.NoError(t, err)
			requireJournalFolded(t, db, job.ID)
		},
	} {
		t.Run(name, func(t *testing.T) {
			m, db := newManager(t, t.TempDir())
			defer db.Close()
			run(t, m, db)
		})
	}
}

func TestCompletionJournalSkipsRowForReplacedListing(t *testing.T) {
	const newerRevision = "3l3qo2vutsw2c"
	dir := t.TempDir()
	m, db := newManager(t, dir)
	job, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	worker, _ := startRepositoryWorker(t, m, job.ID, map[string]string{journalDIDA: journalRevision, journalDIDB: journalRevision})
	checkpointJournaled(t, m, job.ID, journalDIDA, 1)
	require.ErrorIs(t, worker.stop(), context.Canceled)
	generation := mustGetJob(t, m, job.ID).AcquisitionGeneration
	require.Len(t, journalKeys(t, db), 1)
	// Re-seed the inventory at a newer revision without a new acquisition
	// generation, leaving the earlier row in the journal.
	encoded, err := json.Marshal(inventory{Entries: map[string]string{journalDIDA: newerRevision, journalDIDB: journalRevision}})
	require.NoError(t, err)
	require.NoError(t, db.Set(jobInventoryKey(job.ID), encoded, store.SyncWrites))
	require.NoError(t, db.Close())

	m, db = newManager(t, dir)
	defer db.Close()
	reopened, err := m.Get(job.ID)
	require.NoError(t, err)
	require.Equal(t, generation, reopened.AcquisitionGeneration)
	require.NotContains(t, reopened.CompletedRepos, journalDIDA, "a row for a replaced listing is not applied")
	require.Zero(t, reopened.Evidence.Scanned, "a row for a replaced listing is not counted")
	require.Empty(t, journalKeys(t, db), "the skipped row is still deleted")
}
