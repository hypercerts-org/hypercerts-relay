package jobs

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

func newManager(t *testing.T, dir string) (*Manager, *store.Store) {
	return newManagerWithOptions(t, dir)
}

func newManagerWithOptions(t *testing.T, dir string, opts ...store.Option) (*Manager, *store.Store) {
	db, err := store.Open(dir, nil, opts...)
	require.NoError(t, err)
	policy, err := selection.Open(db, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	m, err := Open(db, policy)
	require.NoError(t, err)
	return m, db
}

func TestJobsPolicyAtomicSchedulingAndCancellation(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	j, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	again, err := m.AddSource("https://pds.example/")
	require.NoError(t, err)
	require.Equal(t, j.ID, again.ID)
	policy, err := m.SetPolicy(1, []string{"app.bsky.feed.like", "app.bsky.feed.post"})
	require.NoError(t, err)
	require.Equal(t, uint64(2), policy.Revision)
	all := m.List()
	require.Len(t, all, 2)
	for _, j := range all {
		if j.Policy.Revision == 1 {
			require.Equal(t, Canceled, j.State)
		} else {
			require.Equal(t, Pending, j.State)
			require.Equal(t, "https://pds.example", j.PDS)
		}
	}
	_, err = m.SetPolicy(1, nil)
	require.ErrorIs(t, err, selection.ErrRevision)
	require.ErrorIs(t, m.Retry(j.ID), ErrConflict)
	require.NoError(t, m.RemoveSource("https://pds.example"))
	for _, j := range m.List() {
		require.Equal(t, Canceled, j.State)
	}
}

func TestJobsPersistRepositoryTotalBeforeProgress(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	j, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- m.Run(ctx, func(ctx context.Context, running Job) error {
			if err := m.SetTotalRepos(running.ID, 12); err != nil {
				return err
			}
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	require.Eventually(t, func() bool {
		job := m.List()[0]
		return job.TotalReposKnown && job.TotalRepos == 12
	}, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Equal(t, j.ID, m.List()[0].ID)
	require.True(t, m.List()[0].TotalReposKnown)
	require.Equal(t, 12, m.List()[0].TotalRepos)
}

func TestJobsCrashResumeAndUnavailableCoverage(t *testing.T) {
	dir := t.TempDir()
	m, db := newManager(t, dir)
	j, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	checkpoint := make(chan struct{})
	go func() {
		done <- m.Run(ctx, func(ctx context.Context, j Job) error {
			if err := m.Checkpoint(j.ID, "did:plc:test", "3l3qo2vutsw2b", ""); err != nil {
				return err
			}
			close(checkpoint)
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	<-checkpoint
	cancel()
	<-done
	require.Equal(t, Running, m.List()[0].State, "process loss leaves resumable running state")
	require.NoError(t, db.Close())
	m, db = newManager(t, dir)
	defer db.Close()
	require.Equal(t, Pending, m.List()[0].State)
	require.Equal(t, "3l3qo2vutsw2b", m.List()[0].CompletedRepos["did:plc:test"])
	ctx, cancel = context.WithCancel(t.Context())
	defer cancel()
	go func() {
		done <- m.Run(ctx, func(context.Context, Job) error { return &InputError{Code: "source_unavailable", Unavailable: true} })
	}()
	require.Eventually(t, func() bool { return m.List()[0].State == Incomplete }, time.Second, time.Millisecond)
	require.False(t, m.List()[0].HistoryComplete)
	require.NoError(t, m.Retry(j.ID))
	require.Eventually(t, func() bool { return m.List()[0].Attempts >= 3 }, time.Second, time.Millisecond)
	cancel()
	<-done
}

func TestCanceledJobCannotPublishAndSourceSeedDoesNotRevive(t *testing.T) {
	dir := t.TempDir()
	m, db := newManager(t, dir)
	require.NoError(t, m.SeedSources([]string{"https://pds.example"}))
	id := m.List()[0].ID
	require.NoError(t, m.Cancel(id))
	called := false
	require.ErrorIs(t, m.Apply(id, func() error { called = true; return nil }), ErrConflict)
	require.False(t, called)
	require.NoError(t, m.Retry(id))
	require.Equal(t, Pending, m.List()[0].State)
	require.NoError(t, m.RemoveSource("https://pds.example"))
	require.NoError(t, db.Close())
	m, db = newManager(t, dir)
	defer db.Close()
	require.NoError(t, m.SeedSources([]string{"https://pds.example"}))
	require.False(t, m.Sources()["https://pds.example"])
}

func TestJobsRepeatedQuotaRecoveryStartsFreshWork(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	_, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	first, err := m.Request("https://pds.example", "quota_recovery")
	require.NoError(t, err)
	m.mu.Lock()
	j := m.data.Jobs[first.ID]
	j.State = Complete
	j.CompletedRepos["did:plc:test"] = "old"
	m.data.Jobs[j.ID] = j
	m.mu.Unlock()
	second, err := m.Request("https://pds.example", "quota_recovery")
	require.NoError(t, err)
	require.NotEqual(t, first.ID, second.ID)
	require.Equal(t, Pending, second.State)
	require.Empty(t, second.CompletedRepos)
}

func TestRequestReceiptSurvivesCompletionAndRestart(t *testing.T) {
	dir := t.TempDir()
	m, db := newManager(t, dir)
	_, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	first, err := m.RequestOnce("https://pds.example", "backfill", "request-1")
	require.NoError(t, err)
	m.mu.Lock()
	next := clone(m.data)
	j := next.Jobs[first.ID]
	j.State = Complete
	next.Jobs[j.ID] = j
	require.NoError(t, m.commit(next))
	m.mu.Unlock()
	require.NoError(t, db.Close())
	m, db = newManager(t, dir)
	defer db.Close()
	again, err := m.RequestOnce("https://pds.example", "backfill", "request-1")
	require.NoError(t, err)
	require.Equal(t, first.ID, again.ID)
	require.Equal(t, Complete, again.State)
	_, err = m.RequestOnce("https://different.example", "backfill", "request-1")
	require.ErrorIs(t, err, ErrConflict)
}

func TestNoopActionReceiptsDoNotChangeLaterJobState(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	job, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	require.NoError(t, m.TransitionOnce(job.ID, Pending, "retry-current"))
	require.NoError(t, m.Cancel(job.ID))
	require.NoError(t, m.TransitionOnce(job.ID, Pending, "retry-current"))
	require.Equal(t, Canceled, m.List()[0].State, "a repeated receipt must not restart later work")
	require.NoError(t, m.TransitionOnce(job.ID, Canceled, "cancel-current"))
	require.NoError(t, m.Retry(job.ID))
	require.NoError(t, m.TransitionOnce(job.ID, Canceled, "cancel-current"))
	require.Equal(t, Pending, m.List()[0].State, "a repeated receipt must not cancel a later retry")
}

func TestSnapshotRejectionBoundsUntrustedSourcePositions(t *testing.T) {
	dir := t.TempDir()
	m, db := newManager(t, dir)
	defer db.Close()

	const pds = "https://pds.example"
	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	const revision = "3l3qo2vutsw2b"
	normal, err := m.recordSnapshotRejection(pds, 1, did, revision, directPDSSnapshotRejectionKind, "verification_failed")
	require.NoError(t, err)
	require.Equal(t, pds, normal.PDS)
	require.Equal(t, did, normal.DID)
	require.Equal(t, revision, normal.ListedRevision)

	oversizedPDS := "https://" + strings.Repeat("p", maxSnapshotRejectionPDSLength) + ".example"
	noncanonicalDID := "DID:PLC:NOT-CANONICAL"
	noncanonicalRevision := strings.Repeat("x", 4096)
	bounded, err := m.recordSnapshotRejection(oversizedPDS, 1, noncanonicalDID, noncanonicalRevision, directPDSSnapshotRejectionKind, "verification_failed")
	require.NoError(t, err)
	require.Equal(t, snapshotRejectionDigest(oversizedPDS), bounded.PDS)
	require.Equal(t, snapshotRejectionDigest(noncanonicalDID), bounded.DID)
	require.Equal(t, snapshotRejectionDigest(noncanonicalRevision), bounded.ListedRevision)
	require.NotContains(t, bounded.PDS, oversizedPDS)
	require.NotContains(t, bounded.DID, noncanonicalDID)
	require.NotContains(t, bounded.ListedRevision, noncanonicalRevision)

	again, err := m.recordSnapshotRejection(oversizedPDS, 1, noncanonicalDID, noncanonicalRevision, directPDSSnapshotRejectionKind, "invalid_repository")
	require.NoError(t, err)
	require.Equal(t, bounded, again, "the bounded position must retain its first verdict")
	found, ok := m.lookupSnapshotRejection(oversizedPDS, 1, noncanonicalDID, noncanonicalRevision, directPDSSnapshotRejectionKind)
	require.True(t, ok)
	require.Equal(t, bounded, found)

	require.NoError(t, db.Close())
	m, db = newManager(t, dir)
	defer db.Close()
	found, ok = m.lookupSnapshotRejection(oversizedPDS, 1, noncanonicalDID, noncanonicalRevision, directPDSSnapshotRejectionKind)
	require.True(t, ok)
	require.Equal(t, bounded, found)
}

func TestSnapshotRejectionsRetainRecentBoundedLedgerAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	m, db := newManager(t, dir)

	const pds = "https://pds.example"
	base := time.Now().UTC().Add(-time.Hour)
	seeded := make(map[string]SnapshotRejection, maxSnapshotRejections)
	var oldestKey string
	for i := range maxSnapshotRejections {
		rejection := SnapshotRejection{
			PDS:            pds,
			PolicyRevision: 1,
			DID:            snapshotRejectionDigest(fmt.Sprintf("seed-did-%d", i)),
			ListedRevision: snapshotRejectionDigest(fmt.Sprintf("seed-revision-%d", i)),
			Kind:           directPDSSnapshotRejectionKind,
			Code:           "verification_failed",
			RejectedAt:     base.Add(time.Duration(i) * time.Second),
		}
		key := snapshotRejectionStoredKey(rejection.PDS, rejection.PolicyRevision, rejection.DID, rejection.ListedRevision, rejection.Kind)
		seeded[key] = rejection
		if i == 0 {
			oldestKey = key
		}
	}
	m.data.SnapshotRejections = seeded
	require.NoError(t, m.save(m.data))

	latest, err := m.recordSnapshotRejection(pds, 1, "did:plc:latest", "3l3qo2vutsw2b", directPDSSnapshotRejectionKind, "verification_failed")
	require.NoError(t, err)
	latestKey := snapshotRejectionStoredKey(latest.PDS, latest.PolicyRevision, latest.DID, latest.ListedRevision, latest.Kind)
	require.Len(t, m.ListSnapshotRejections(), maxSnapshotRejections)
	_, foundOldest := m.data.SnapshotRejections[oldestKey]
	require.False(t, foundOldest, "the oldest ledger entry must be evicted at the retention cap")
	_, foundLatest := m.data.SnapshotRejections[latestKey]
	require.True(t, foundLatest)

	require.NoError(t, db.Close())
	m, db = newManager(t, dir)
	defer db.Close()
	require.Len(t, m.ListSnapshotRejections(), maxSnapshotRejections)
	_, foundOldest = m.data.SnapshotRejections[oldestKey]
	require.False(t, foundOldest)
	_, foundLatest = m.data.SnapshotRejections[latestKey]
	require.True(t, foundLatest)
}

func TestSnapshotRejectionsPersistIdempotently(t *testing.T) {
	dir := t.TempDir()
	m, db := newManager(t, dir)
	// Simulate a document from before the rejection ledger migration.
	legacy := clone(m.data)
	legacy.SnapshotRejections = nil
	require.NoError(t, m.save(legacy))
	require.NoError(t, db.Close())
	m, db = newManager(t, dir)
	require.Empty(t, m.ListSnapshotRejections())

	rejection, err := m.recordSnapshotRejection("https://pds.example", 1, "did:plc:test", "3l3qo2vutsw2b", directPDSSnapshotRejectionKind, "verification_failed")
	require.NoError(t, err)
	again, err := m.recordSnapshotRejection("https://pds.example", 1, "did:plc:test", "3l3qo2vutsw2b", directPDSSnapshotRejectionKind, "invalid_repository")
	require.NoError(t, err)
	require.Equal(t, rejection, again, "the first durable verdict is idempotent")
	found, ok := m.lookupSnapshotRejection("https://pds.example", 1, "did:plc:test", "3l3qo2vutsw2b", directPDSSnapshotRejectionKind)
	require.True(t, ok)
	require.Equal(t, rejection, found)
	_, ok = m.lookupSnapshotRejection("https://pds.example", 1, "did:plc:test", "3l3qo2vutsw2c", directPDSSnapshotRejectionKind)
	require.False(t, ok, "a changed listing revision requires a fresh snapshot")
	listed := m.ListSnapshotRejections()
	require.Equal(t, []SnapshotRejection{rejection}, listed)
	listed[0].Code = "mutated"
	require.Equal(t, rejection, m.ListSnapshotRejections()[0], "the read model must not expose durable state")
	require.NoError(t, db.Close())

	m, db = newManager(t, dir)
	defer db.Close()
	found, ok = m.lookupSnapshotRejection("https://pds.example", 1, "did:plc:test", "3l3qo2vutsw2b", directPDSSnapshotRejectionKind)
	require.True(t, ok)
	require.Equal(t, rejection, found)
	require.Equal(t, []SnapshotRejection{rejection}, m.ListSnapshotRejections())
}

func TestReceiptNamespacesAndLegacyMigration(t *testing.T) {
	dir := t.TempDir()
	m, db := newManager(t, dir)
	_, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	job, err := m.RequestOnce("https://pds.example", "backfill", "action/shared")
	require.NoError(t, err)
	// These keys collided in the old combined map. Each operation keeps its receipt.
	require.NoError(t, m.TransitionOnce(job.ID, Canceled, "shared"))
	again, err := m.RequestOnce("https://pds.example", "backfill", "action/shared")
	require.NoError(t, err)
	require.Equal(t, job.ID, again.ID)
	require.Equal(t, Canceled, again.State)
	// Model an existing installation: typed action values live in Requests.
	m.data.Requests["action/legacy"] = string(Canceled) + ":" + job.ID
	require.NoError(t, m.save(m.data))
	require.NoError(t, db.Close())
	m, db = newManager(t, dir)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.Equal(t, job.ID, m.data.Requests["action/shared"])
	require.NotContains(t, m.data.Requests, "action/legacy")
	require.NoError(t, m.Retry(job.ID))
	require.NoError(t, m.TransitionOnce(job.ID, Canceled, "legacy"))
	require.NoError(t, m.TransitionOnce(job.ID, Canceled, "shared"))
	require.Equal(t, Pending, m.data.Jobs[job.ID].State, "old cancellation receipts must not cancel a later retry")
	require.ErrorIs(t, m.TransitionOnce(job.ID, Pending, "legacy"), ErrConflict)
}
