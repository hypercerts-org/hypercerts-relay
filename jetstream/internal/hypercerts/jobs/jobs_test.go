package jobs

import (
	"context"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

func newManager(t *testing.T, dir string) (*Manager, *store.Store) {
	db, err := store.Open(dir, nil)
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
