package jobs

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

type repositoryWorker struct {
	finish chan error
	cancel context.CancelFunc
	done   chan error
}

func startRepositoryWorker(t *testing.T, m *Manager, jobID string, entries map[string]string) (repositoryWorker, Job) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	finish := make(chan error, 1)
	started := make(chan Job, 1)
	done := make(chan error, 1)
	go func() {
		done <- m.Run(ctx, func(ctx context.Context, job Job) error {
			if job.ID != jobID {
				return ErrConflict
			}
			if err := m.CheckpointInventory(job.ID, "", entries, true); err != nil {
				return err
			}
			started <- job
			select {
			case err := <-finish:
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	select {
	case job := <-started:
		return repositoryWorker{finish: finish, cancel: cancel, done: done}, job
	case err := <-done:
		cancel()
		require.NoError(t, err)
		return repositoryWorker{}, Job{}
	case <-time.After(time.Second):
		cancel()
		t.Fatal("job worker did not start")
		return repositoryWorker{}, Job{}
	}
}

func (w repositoryWorker) finishAs(t *testing.T, m *Manager, jobID string, outcome error, state State) {
	t.Helper()
	w.finish <- outcome
	require.Eventually(t, func() bool {
		job, err := m.Get(jobID)
		return err == nil && job.State == state
	}, time.Second, time.Millisecond)
	w.cancel()
	require.ErrorIs(t, <-w.done, context.Canceled)
}

func (w repositoryWorker) finishIncomplete(t *testing.T, m *Manager, jobID string) {
	t.Helper()
	w.finishAs(t, m, jobID, &InputError{Code: "source_unavailable", Unavailable: true}, Incomplete)
}

func TestRepositoryRetryAttemptPersistsAcrossRestart(t *testing.T) {
	const (
		didA = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
		didB = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"
		revA = "3l3qo2vutsw2b"
		revB = "3l3qo2vutsw2c"
	)
	dir := t.TempDir()
	m, db := newManager(t, dir)
	job, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	worker, _ := startRepositoryWorker(t, m, job.ID, map[string]string{didA: revA, didB: revB})

	ready, err := m.GetRepositoryRetry(job.ID, didA)
	require.NoError(t, err)
	require.Equal(t, RepositoryRetryReady, ready.State)
	require.Equal(t, revA, ready.ListedRevision)

	first, err := m.BeginRepositoryAttempt(job.ID, didA)
	require.NoError(t, err)
	require.Equal(t, RepositoryRetryInFlight, first.State)
	require.Equal(t, 1, first.Attempts)
	second, err := m.BeginRepositoryAttempt(job.ID, didB)
	require.NoError(t, err)
	require.Equal(t, 1, second.Attempts)
	worker.finishIncomplete(t, m, job.ID)

	require.NoError(t, db.Close())
	m, db = newManager(t, dir)
	defer db.Close()

	for did, revision := range map[string]string{didA: revA, didB: revB} {
		restored, getErr := m.GetRepositoryRetry(job.ID, did)
		require.NoError(t, getErr)
		require.Equal(t, RepositoryRetryInFlight, restored.State)
		require.Equal(t, 1, restored.Attempts)
		require.Equal(t, revision, restored.ListedRevision)
	}
	listed, err := m.ListRepositoryRetries(job.ID)
	require.NoError(t, err)
	require.Len(t, listed, 2)
}

func TestRepositoryRetryKeysAreIsolatedByJobAndDID(t *testing.T) {
	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	m, db := newManager(t, t.TempDir())
	defer db.Close()

	firstJob, err := m.AddSource("https://first.example")
	require.NoError(t, err)
	worker, _ := startRepositoryWorker(t, m, firstJob.ID, map[string]string{did: "3l3qo2vutsw2b"})
	firstAttempt, err := m.BeginRepositoryAttempt(firstJob.ID, did)
	require.NoError(t, err)
	require.Equal(t, 1, firstAttempt.Attempts)
	worker.finishIncomplete(t, m, firstJob.ID)

	secondJob, err := m.AddSource("https://second.example")
	require.NoError(t, err)
	worker, _ = startRepositoryWorker(t, m, secondJob.ID, map[string]string{did: "3l3qo2vutsw2c"})
	secondAttempt, err := m.BeginRepositoryAttempt(secondJob.ID, did)
	require.NoError(t, err)
	require.Equal(t, 1, secondAttempt.Attempts)
	worker.finishIncomplete(t, m, secondJob.ID)

	first, err := m.GetRepositoryRetry(firstJob.ID, did)
	require.NoError(t, err)
	second, err := m.GetRepositoryRetry(secondJob.ID, did)
	require.NoError(t, err)
	require.Equal(t, "3l3qo2vutsw2b", first.ListedRevision)
	require.Equal(t, "3l3qo2vutsw2c", second.ListedRevision)
	require.Equal(t, 1, first.Attempts)
	require.Equal(t, 1, second.Attempts)
}

func TestRepositoryRetryFailureAndPDSCooldownPersist(t *testing.T) {
	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	dir := t.TempDir()
	m, db := newManager(t, dir)
	job, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	worker, _ := startRepositoryWorker(t, m, job.ID, map[string]string{did: "3l3qo2vutsw2b"})
	_, err = m.BeginRepositoryAttempt(job.ID, did)
	require.NoError(t, err)

	retryAt := time.Now().Add(-time.Second).UTC()
	cooldownUntil := time.Now().Add(time.Minute).UTC()
	failure := RepositoryRetryFailure{Category: RepositoryFailureHTTP, HTTPStatus: 503, Stage: RepositoryFailureGetRepoRequest}
	stored, err := m.RecordRepositoryFailure(job.ID, did, failure, &retryAt, &cooldownUntil)
	require.NoError(t, err)
	require.Equal(t, RepositoryRetryWait, stored.State)
	require.Equal(t, retryAt, stored.RetryAt)
	require.Equal(t, failure, *stored.Failure)
	require.False(t, stored.FailedAt.IsZero())
	require.ErrorIs(t, func() error {
		_, beginErr := m.BeginRepositoryAttempt(job.ID, did)
		return beginErr
	}(), ErrRepositoryCoolingDown)
	cooldown, found, err := m.GetPDSCooldown("https://PDS.example/")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "https://pds.example", cooldown.PDS)
	require.Equal(t, cooldownUntil, cooldown.Until)
	worker.finishIncomplete(t, m, job.ID)

	require.NoError(t, db.Close())
	m, db = newManager(t, dir)
	defer db.Close()
	restored, err := m.GetRepositoryRetry(job.ID, did)
	require.NoError(t, err)
	require.Equal(t, RepositoryRetryWait, restored.State)
	require.Equal(t, 1, restored.Attempts)
	require.Equal(t, failure, *restored.Failure)
	require.Equal(t, retryAt, restored.RetryAt)
	require.False(t, restored.FailedAt.IsZero())
	cooldown, found, err = m.GetPDSCooldown("https://pds.example")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, cooldownUntil, cooldown.Until)
}

func TestRepositoryRetryRejectsInvalidInputAndRevisionMismatch(t *testing.T) {
	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	job, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	_, err = m.BeginRepositoryAttempt(job.ID, did)
	require.ErrorIs(t, err, ErrConflict, "attempts cannot begin before the job is running")
	worker, _ := startRepositoryWorker(t, m, job.ID, map[string]string{did: "3l3qo2vutsw2b"})

	_, err = m.GetRepositoryRetry(job.ID, "not-a-did")
	require.ErrorIs(t, err, ErrInvalidInput)
	_, err = m.GetRepositoryRetry(job.ID, "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb")
	require.ErrorIs(t, err, ErrConflict, "a repository outside the frozen inventory is ineligible")
	_, err = m.BeginRepositoryAttempt("missing-job", did)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = m.BeginRepositoryAttempt(job.ID, did)
	require.NoError(t, err)

	_, err = m.RecordRepositoryFailure(job.ID, did, RepositoryRetryFailure{Category: "raw error", Stage: RepositoryFailureGetRepoRequest}, nil, nil)
	require.ErrorIs(t, err, ErrInvalidInput)
	_, err = m.RecordRepositoryFailure(job.ID, did, RepositoryRetryFailure{Category: RepositoryFailureHTTP, Stage: RepositoryFailureGetRepoRequest}, nil, nil)
	require.ErrorIs(t, err, ErrInvalidInput)
	wrongRevision := "3l3qo2vutsw2c"
	require.ErrorIs(t, m.CheckpointRepository(job.ID, did, wrongRevision, "3l3qo2vutsw2b", ""), ErrConflict)
	require.ErrorIs(t, m.CheckpointRepository(job.ID, did, "3l3qo2vutsw2b", "3l3qo2vutsw2a", ""), ErrConflict)
	require.ErrorIs(t, m.CheckpointRepository(job.ID, did, "3l3qo2vutsw2b", "not-a-tid", ""), ErrInvalidInput)
	stillInFlight, err := m.GetRepositoryRetry(job.ID, did)
	require.NoError(t, err)
	require.Equal(t, RepositoryRetryInFlight, stillInFlight.State)
	require.Equal(t, 1, stillInFlight.Attempts)
	worker.finishIncomplete(t, m, job.ID)
}

func TestRepositoryFailureAndPDSCooldownCommitAtomically(t *testing.T) {
	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	injected := errors.New("injected batch failure")
	fault := &store.KeyPrefixFault{Prefix: []byte(pdsCooldownPrefix), Op: store.WriteOpBatchCommit, Ordinal: 1, Err: injected}
	m, db := newManagerWithOptions(t, t.TempDir(), store.WithFaultInjector(fault))
	defer db.Close()
	job, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	worker, _ := startRepositoryWorker(t, m, job.ID, map[string]string{did: "3l3qo2vutsw2b"})
	_, err = m.BeginRepositoryAttempt(job.ID, did)
	require.NoError(t, err)

	retryAt := time.Now().Add(time.Minute)
	cooldownUntil := time.Now().Add(2 * time.Minute)
	_, err = m.RecordRepositoryFailure(job.ID, did, RepositoryRetryFailure{Category: RepositoryFailureHTTP, HTTPStatus: 503, Stage: RepositoryFailureGetRepoRequest}, &retryAt, &cooldownUntil)
	require.ErrorIs(t, err, injected)
	unchanged, err := m.GetRepositoryRetry(job.ID, did)
	require.NoError(t, err)
	require.Equal(t, RepositoryRetryInFlight, unchanged.State)
	require.Equal(t, 1, unchanged.Attempts)
	require.Nil(t, unchanged.Failure)
	_, found, err := m.GetPDSCooldown(job.PDS)
	require.NoError(t, err)
	require.False(t, found, "the failed batch must not leave only the cooldown behind")
	worker.finishIncomplete(t, m, job.ID)
}

func TestRecordPDSCooldownRequiresActiveJobAndExtendsCanonicalOrigin(t *testing.T) {
	dir := t.TempDir()
	m, db := newManager(t, dir)
	defer func() { _ = db.Close() }()
	job, err := m.AddSource("https://PDS.example/")
	require.NoError(t, err)
	firstDeadline := time.Now().Add(5 * time.Minute).UTC()
	require.ErrorIs(t, m.RecordPDSCooldown(job.ID, firstDeadline), ErrConflict, "only the active job may record an origin cooldown")

	worker, _ := startRepositoryWorker(t, m, job.ID, map[string]string{})
	select {
	case <-m.wake:
	default:
	}
	require.NoError(t, m.RecordPDSCooldown(job.ID, firstDeadline))
	select {
	case <-m.wake:
	case <-time.After(time.Second):
		t.Fatal("persisting an origin cooldown must wake the scheduler")
	}
	cooldown, found, err := m.GetPDSCooldown("https://pds.example")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "https://pds.example", cooldown.PDS, "the cooldown uses the canonical origin stored on the active job")
	require.Equal(t, firstDeadline, cooldown.Until)

	shorterDeadline := firstDeadline.Add(-time.Minute)
	require.NoError(t, m.RecordPDSCooldown(job.ID, shorterDeadline))
	cooldown, found, err = m.GetPDSCooldown(job.PDS)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, firstDeadline, cooldown.Until, "cooldowns only extend monotonically")

	laterDeadline := firstDeadline.Add(time.Minute)
	require.NoError(t, m.RecordPDSCooldown(job.ID, laterDeadline))
	cooldown, found, err = m.GetPDSCooldown(job.PDS)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, laterDeadline, cooldown.Until)
	worker.finishIncomplete(t, m, job.ID)
	require.NoError(t, db.Close())

	m, db = newManager(t, dir)
	defer db.Close()
	cooldown, found, err = m.GetPDSCooldown(job.PDS)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, laterDeadline, cooldown.Until, "the cooldown batch is synced across restart")
}

func TestRepositoryCheckpointAndRetryStateCommitAtomically(t *testing.T) {
	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	injected := errors.New("injected checkpoint failure")
	fault := &store.KeyPrefixFault{Prefix: []byte(repositoryRetryPrefix), Op: store.WriteOpBatchCommit, Ordinal: 2, Err: injected}
	dir := t.TempDir()
	m, db := newManagerWithOptions(t, dir, store.WithFaultInjector(fault))
	job, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	worker, _ := startRepositoryWorker(t, m, job.ID, map[string]string{did: "3l3qo2vutsw2b"})
	_, err = m.BeginRepositoryAttempt(job.ID, did)
	require.NoError(t, err)

	require.ErrorIs(t, m.CheckpointRepository(job.ID, did, "3l3qo2vutsw2b", "3l3qo2vutsw2c", "cursor"), injected)
	inFlight, err := m.GetRepositoryRetry(job.ID, did)
	require.NoError(t, err)
	require.Equal(t, RepositoryRetryInFlight, inFlight.State)
	currentJob, err := m.Get(job.ID)
	require.NoError(t, err)
	require.NotContains(t, currentJob.CompletedRepos, did)

	require.NoError(t, m.CheckpointRepository(job.ID, did, "3l3qo2vutsw2b", "3l3qo2vutsw2c", "cursor"))
	complete, err := m.GetRepositoryRetry(job.ID, did)
	require.NoError(t, err)
	require.Equal(t, RepositoryRetryComplete, complete.State)
	require.Equal(t, "3l3qo2vutsw2b", complete.ListedRevision)
	currentJob, err = m.Get(job.ID)
	require.NoError(t, err)
	require.Equal(t, "3l3qo2vutsw2c", currentJob.CompletedRepos[did])
	listed, err := m.ListRepositoryRetries(job.ID)
	require.NoError(t, err)
	require.Empty(t, listed)
	worker.finishIncomplete(t, m, job.ID)

	require.NoError(t, db.Close())
	m, db = newManager(t, dir)
	defer db.Close()
	complete, err = m.GetRepositoryRetry(job.ID, did)
	require.NoError(t, err)
	require.Equal(t, RepositoryRetryComplete, complete.State)
}

func TestRepositoryRetryResetOnlyDeletesEligibleUnresolvedRows(t *testing.T) {
	const (
		didComplete = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
		didPending  = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"
		revision    = "3l3qo2vutsw2b"
	)
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	job, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	worker, _ := startRepositoryWorker(t, m, job.ID, map[string]string{didComplete: revision, didPending: revision})
	_, err = m.BeginRepositoryAttempt(job.ID, didComplete)
	require.NoError(t, err)
	require.NoError(t, m.CheckpointRepository(job.ID, didComplete, revision, revision, ""))
	_, err = m.BeginRepositoryAttempt(job.ID, didPending)
	require.NoError(t, err)
	_, err = m.RecordRepositoryFailure(job.ID, didPending, RepositoryRetryFailure{Category: RepositoryFailureUnknown, Stage: RepositoryFailureGetRepoBody}, nil, nil)
	require.NoError(t, err)
	worker.finishIncomplete(t, m, job.ID)

	reset, err := m.ResetUnresolvedRepositoryRetries(job.ID)
	require.NoError(t, err)
	require.Equal(t, 1, reset)
	complete, err := m.GetRepositoryRetry(job.ID, didComplete)
	require.NoError(t, err)
	require.Equal(t, RepositoryRetryComplete, complete.State)
	ready, err := m.GetRepositoryRetry(job.ID, didPending)
	require.NoError(t, err)
	require.Equal(t, RepositoryRetryReady, ready.State)
	require.Zero(t, ready.Attempts)
	listed, err := m.ListRepositoryRetries(job.ID)
	require.NoError(t, err)
	require.Empty(t, listed)
}

func TestFailedJobRetryPreservesCompletedCoordinateAndResetsSameRevisionBudget(t *testing.T) {
	const (
		didComplete = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
		didRetry    = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"
		listedRev   = "3l3qo2vutsw2b"
		newerRev    = "3l3qo2vutsw2c"
	)
	injected := errors.New("injected failed-job retry batch failure")
	fault := &store.KeyPrefixFault{Prefix: []byte(repositoryRetryPrefix), Op: store.WriteOpBatchCommit, Ordinal: 5, Err: injected}
	m, db := newManagerWithOptions(t, t.TempDir(), store.WithFaultInjector(fault))
	defer db.Close()
	job, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	entries := map[string]string{didComplete: listedRev, didRetry: listedRev}
	worker, _ := startRepositoryWorker(t, m, job.ID, entries)
	_, err = m.BeginRepositoryAttempt(job.ID, didComplete)
	require.NoError(t, err)
	require.NoError(t, m.CheckpointRepository(job.ID, didComplete, listedRev, newerRev, ""))
	_, err = m.BeginRepositoryAttempt(job.ID, didRetry)
	require.NoError(t, err)
	_, err = m.RecordRepositoryFailure(job.ID, didRetry, RepositoryRetryFailure{Category: RepositoryFailureHTTP, HTTPStatus: 503, Stage: RepositoryFailureGetRepoRequest}, nil, nil)
	require.NoError(t, err)
	worker.finishAs(t, m, job.ID, &InputError{Code: "invalid_repository"}, Failed)

	require.ErrorIs(t, m.Retry(job.ID), injected)
	unchanged, err := m.Get(job.ID)
	require.NoError(t, err)
	require.Equal(t, Failed, unchanged.State)
	require.Equal(t, map[string]string{didComplete: newerRev}, unchanged.CompletedRepos)
	unresolved, err := m.GetRepositoryRetry(job.ID, didRetry)
	require.NoError(t, err)
	require.Equal(t, RepositoryRetryUnresolved, unresolved.State)
	require.Equal(t, 1, unresolved.Attempts)

	require.NoError(t, m.Retry(job.ID))
	retried, err := m.Get(job.ID)
	require.NoError(t, err)
	require.Equal(t, map[string]string{didComplete: newerRev}, retried.CompletedRepos,
		"a failed retry refreshes inventory without discarding archive-backed checkpoints")
	require.False(t, retried.TotalReposKnown)
	require.Empty(t, retried.Cursor)

	worker, _ = startRepositoryWorker(t, m, job.ID, entries)
	completed, err := m.GetRepositoryRetry(job.ID, didComplete)
	require.NoError(t, err)
	require.Equal(t, RepositoryRetryComplete, completed.State)
	fresh, err := m.GetRepositoryRetry(job.ID, didRetry)
	require.NoError(t, err)
	require.Equal(t, RepositoryRetryReady, fresh.State,
		"a same-DID/same-revision retry row from the discarded inventory must not carry its old budget")
	require.Zero(t, fresh.Attempts)
	worker.finishIncomplete(t, m, job.ID)
}

func TestIncompleteRetryAtomicallyResetsOnlyUnresolvedBudget(t *testing.T) {
	const (
		didComplete = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
		didRetry    = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"
		listedRev   = "3l3qo2vutsw2b"
		newerRev    = "3l3qo2vutsw2c"
	)
	injected := errors.New("injected incomplete-job retry batch failure")
	fault := &store.KeyPrefixFault{Prefix: []byte(repositoryRetryPrefix), Op: store.WriteOpBatchCommit, Ordinal: 5, Err: injected}
	m, db := newManagerWithOptions(t, t.TempDir(), store.WithFaultInjector(fault))
	defer db.Close()
	job, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	worker, _ := startRepositoryWorker(t, m, job.ID, map[string]string{didComplete: listedRev, didRetry: listedRev})
	_, err = m.BeginRepositoryAttempt(job.ID, didComplete)
	require.NoError(t, err)
	require.NoError(t, m.CheckpointRepository(job.ID, didComplete, listedRev, newerRev, ""))
	_, err = m.BeginRepositoryAttempt(job.ID, didRetry)
	require.NoError(t, err)
	_, err = m.RecordRepositoryFailure(job.ID, didRetry, RepositoryRetryFailure{Category: RepositoryFailureHTTP, HTTPStatus: 503, Stage: RepositoryFailureGetRepoRequest}, nil, nil)
	require.NoError(t, err)
	worker.finishIncomplete(t, m, job.ID)

	require.ErrorIs(t, m.Retry(job.ID), injected)
	unchanged, err := m.Get(job.ID)
	require.NoError(t, err)
	require.Equal(t, Incomplete, unchanged.State)
	require.Equal(t, map[string]string{didComplete: newerRev}, unchanged.CompletedRepos)
	stillUnresolved, err := m.GetRepositoryRetry(job.ID, didRetry)
	require.NoError(t, err)
	require.Equal(t, RepositoryRetryUnresolved, stillUnresolved.State)
	require.Equal(t, 1, stillUnresolved.Attempts)

	require.NoError(t, m.Retry(job.ID))
	retried, err := m.Get(job.ID)
	require.NoError(t, err)
	require.Equal(t, Pending, retried.State)
	require.Equal(t, map[string]string{didComplete: newerRev}, retried.CompletedRepos)
	require.True(t, retried.TotalReposKnown, "incomplete retry resumes the frozen inventory")
	completed, err := m.GetRepositoryRetry(job.ID, didComplete)
	require.NoError(t, err)
	require.Equal(t, RepositoryRetryComplete, completed.State)
	fresh, err := m.GetRepositoryRetry(job.ID, didRetry)
	require.NoError(t, err)
	require.Equal(t, RepositoryRetryReady, fresh.State)
	require.Zero(t, fresh.Attempts)
}

func TestRepositoryRetryDoesNotMaterializeRowsForExistingJobs(t *testing.T) {
	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	dir := t.TempDir()
	m, db := newManager(t, dir)
	job, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	worker, _ := startRepositoryWorker(t, m, job.ID, map[string]string{did: "3l3qo2vutsw2b"})
	worker.finishIncomplete(t, m, job.ID)
	require.NoError(t, db.Close())

	m, db = newManager(t, dir)
	defer db.Close()
	ready, err := m.GetRepositoryRetry(job.ID, did)
	require.NoError(t, err)
	require.Equal(t, RepositoryRetryReady, ready.State)
	require.Zero(t, ready.Attempts)
	listed, err := m.ListRepositoryRetries(job.ID)
	require.NoError(t, err)
	require.Empty(t, listed, "reopening an existing job must not eagerly create retry rows")
}

func TestRepositoryDetailsContinuationUsesImmutableSnapshotAfterCheckpointAndStoreClose(t *testing.T) {
	const (
		didA = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
		didB = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"
		rev  = "3l3qo2vutsw2b"
	)
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	job, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	worker, _ := startRepositoryWorker(t, m, job.ID, map[string]string{didA: rev, didB: rev})

	first, err := m.RepositoryDetails(job.ID, "", 1)
	require.NoError(t, err)
	require.NotEmpty(t, first.NextCursor)
	require.Equal(t, Running, first.Snapshot.Job.State)
	require.Equal(t, 2, *first.Snapshot.Diagnostics.UnresolvedRepos)
	require.Equal(t, didA, first.Repositories[0].DID)

	_, err = m.BeginRepositoryAttempt(job.ID, didB)
	require.NoError(t, err)
	require.NoError(t, m.CheckpointRepository(job.ID, didB, rev, rev, ""))
	worker.cancel()
	require.ErrorIs(t, <-worker.done, context.Canceled)
	require.NoError(t, db.Close())

	second, err := m.RepositoryDetails(job.ID, first.NextCursor, 1)
	require.NoError(t, err, "continuation must use the captured projection without reading Pebble")
	require.Equal(t, Running, second.Snapshot.Job.State, "parent status must match the first page snapshot")
	require.Equal(t, 2, *second.Snapshot.Diagnostics.UnresolvedRepos)
	require.Equal(t, didB, second.Repositories[0].DID)
	require.Equal(t, RepositoryRetryReady, second.Repositories[0].State)
	require.Zero(t, second.Repositories[0].Attempts)
}

func TestRepositoryDetailsSnapshotExpiryAndLRUEviction(t *testing.T) {
	const (
		didA = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
		didB = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"
		rev  = "3l3qo2vutsw2b"
	)
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	firstJob, err := m.AddSource("https://first.example")
	require.NoError(t, err)
	firstWorker, _ := startRepositoryWorker(t, m, firstJob.ID, map[string]string{didA: rev, didB: rev})
	firstPage, err := m.RepositoryDetails(firstJob.ID, "", 1)
	require.NoError(t, err)
	id, _, err := decodeRepositoryDetailsCursor(firstPage.NextCursor)
	require.NoError(t, err)

	m.mu.Lock()
	m.repositoryDetailSnapshots[id].ExpiresAt = time.Now().UTC().Add(-time.Second)
	m.mu.Unlock()
	_, err = m.RepositoryDetails(firstJob.ID, firstPage.NextCursor, 1)
	require.ErrorIs(t, err, ErrRepositoryDetailsSnapshotExpired)
	freshPage, err := m.RepositoryDetails(firstJob.ID, "", 1)
	require.NoError(t, err, "expired cursors require a fresh first-page capture")
	require.NotEmpty(t, freshPage.NextCursor)
	firstWorker.finishIncomplete(t, m, firstJob.ID)

	cursors := []string{freshPage.NextCursor}
	jobIDs := []string{firstJob.ID}
	for i := 1; i < maxRepositoryDetailSnapshots; i++ {
		job, addErr := m.AddSource(fmt.Sprintf("https://pds-%d.example", i))
		require.NoError(t, addErr)
		worker, _ := startRepositoryWorker(t, m, job.ID, map[string]string{didA: rev, didB: rev})
		page, pageErr := m.RepositoryDetails(job.ID, "", 1)
		require.NoError(t, pageErr)
		require.NotEmpty(t, page.NextCursor)
		cursors = append(cursors, page.NextCursor)
		jobIDs = append(jobIDs, job.ID)
		worker.finishIncomplete(t, m, job.ID)
	}

	_, err = m.RepositoryDetails(jobIDs[0], cursors[0], 1)
	require.NoError(t, err, "access updates the snapshot's LRU position")
	lastJob, err := m.AddSource("https://last.example")
	require.NoError(t, err)
	lastWorker, _ := startRepositoryWorker(t, m, lastJob.ID, map[string]string{didA: rev, didB: rev})
	lastPage, err := m.RepositoryDetails(lastJob.ID, "", 1)
	require.NoError(t, err)
	require.NotEmpty(t, lastPage.NextCursor)
	lastWorker.finishIncomplete(t, m, lastJob.ID)

	_, err = m.RepositoryDetails(jobIDs[0], cursors[0], 1)
	require.NoError(t, err, "the least-recently-used snapshot must remain available")
	_, err = m.RepositoryDetails(jobIDs[1], cursors[1], 1)
	require.ErrorIs(t, err, ErrRepositoryDetailsSnapshotExpired, "the least-recently-used cursor must expire on capacity eviction")
}

func TestFailedJobRetryStartsFreshBudgetForRefreshedInventory(t *testing.T) {
	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	job, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	worker, _ := startRepositoryWorker(t, m, job.ID, map[string]string{did: "3l3qo2vutsw2b"})
	_, err = m.BeginRepositoryAttempt(job.ID, did)
	require.NoError(t, err)
	_, err = m.RecordRepositoryFailure(job.ID, did, RepositoryRetryFailure{Category: RepositoryFailureUnknown, Stage: RepositoryFailureGetRepoRequest}, nil, nil)
	require.NoError(t, err)
	worker.finishAs(t, m, job.ID, &InputError{Code: "invalid_repository"}, Failed)

	require.NoError(t, m.Retry(job.ID))
	worker, _ = startRepositoryWorker(t, m, job.ID, map[string]string{did: "3l3qo2vutsw2c"})
	ready, err := m.GetRepositoryRetry(job.ID, did)
	require.NoError(t, err)
	require.Equal(t, RepositoryRetryReady, ready.State, "failed-job retry clears rows from the discarded inventory")
	require.Zero(t, ready.Attempts)
	fresh, err := m.BeginRepositoryAttempt(job.ID, did)
	require.NoError(t, err)
	require.Equal(t, 1, fresh.Attempts)
	listed, err := m.ListRepositoryRetries(job.ID)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	worker.finishIncomplete(t, m, job.ID)
	reset, err := m.ResetUnresolvedRepositoryRetries(job.ID)
	require.NoError(t, err)
	require.Zero(t, reset, "reset must not mutate rows tied to a discarded inventory")
}
