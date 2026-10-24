package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/policygate"
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

func TestRevisionedSourceRemovalFencesDelayedAdds(t *testing.T) {
	dir := t.TempDir()
	m, db := newManager(t, dir)
	const pds = "https://revision-fence.example"

	// A removal is durable even when an add/remove race means this manager has
	// never observed the source as registered.
	require.NoError(t, m.RemoveSourceWithRevision(pds, 5))
	require.ErrorIs(t, func() error { _, err := m.AddSourceWithRevision(pds, 4); return err }(), ErrConflict)
	require.ErrorIs(t, func() error { _, err := m.AddSourceWithRevision(pds, 5); return err }(), ErrConflict)

	// Explicit re-admission is allowed only at a strictly newer coordinate.
	job, err := m.AddSourceWithRevision(pds, 6)
	require.NoError(t, err)
	require.Equal(t, uint64(6), job.SourceRevision)
	require.True(t, m.Sources()[pds])
	require.NoError(t, db.Close())

	m, db = newManager(t, dir)
	defer db.Close()
	require.True(t, m.Sources()[pds])
	require.ErrorIs(t, func() error { _, err := m.AddSourceWithRevision(pds, 5); return err }(), ErrConflict)
	_, err = m.AddSourceWithRevision(pds, 6)
	require.NoError(t, err, "equal revision remains idempotent after explicit re-admission")
}

func TestBoundPolicyAdoptionRecordsHistoryWithoutBoundary(t *testing.T) {
	dir := t.TempDir()
	m, db := newManager(t, dir)
	drainer := &testPolicyDrainer{next: 1}
	history, err := attribution.Open(db, "crypto", "selected")
	require.NoError(t, err)
	m.BindPolicyAdoption(policygate.New(m.policy, drainer, "selected"), history, "selected")
	adopted, err := m.SetPolicy(t.Context(), 1, []string{"app.bsky.feed.like"})
	require.NoError(t, err)
	require.Equal(t, uint64(2), adopted.Revision)
	policies, err := history.ReadPolicyHistory()
	require.NoError(t, err)
	require.Len(t, policies, 1)
	require.Nil(t, policies[0].EffectiveAfterSeq)
	require.Zero(t, mustActivation(t, history), "policy staging must not activate event attribution")
	require.NoError(t, db.Close())

	m, db = newManager(t, dir)
	defer db.Close()
	history, err = attribution.Open(db, "crypto", "selected")
	require.NoError(t, err)
	policies, err = history.ReadPolicyHistory()
	require.NoError(t, err)
	require.Len(t, policies, 1)
	require.Nil(t, policies[0].EffectiveAfterSeq)
	require.Equal(t, uint64(2), m.policy.Current().Revision)
}

func TestApplyAdmitsBeforeTakingJobsLock(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	job, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	job, err = m.Request("https://pds.example", "backfill")
	require.NoError(t, err)
	m.mu.Lock()
	job.State = Running
	m.data.Jobs[job.ID] = job
	m.mu.Unlock()
	drainer := &testPolicyDrainer{next: 1}
	history, err := attribution.Open(db, "crypto", "selected")
	require.NoError(t, err)
	m.BindPolicyAdoption(policygate.New(m.policy, drainer, "selected"), history, "selected")

	finished := make(chan error, 1)
	go func() {
		finished <- m.ApplyContext(t.Context(), job.ID, func(ctx context.Context) error {
			_, ok := policygate.Admission(ctx)
			require.True(t, ok)
			return nil
		})
	}()
	select {
	case err := <-finished:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("policy admission and jobs lock deadlocked")
	}
}

func TestBoundPolicyNoopDoesNotAddHistory(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	history, err := attribution.Open(db, "crypto", "selected")
	require.NoError(t, err)
	m.BindPolicyAdoption(policygate.New(m.policy, &testPolicyDrainer{next: 1}, "selected"), history, "selected")
	policy, err := m.SetPolicy(t.Context(), 1, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	require.Equal(t, uint64(1), policy.Revision)
	policies, err := history.ReadPolicyHistory()
	require.NoError(t, err)
	require.Empty(t, policies)
}

type testPolicyDrainer struct{ next uint64 }

func (d *testPolicyDrainer) DrainDurability(context.Context) error { return nil }
func (d *testPolicyDrainer) NextSeq() uint64                       { return d.next }
func mustActivation(t *testing.T, m *attribution.Manager) uint64 {
	t.Helper()
	n, err := m.ActivationSeq()
	require.NoError(t, err)
	return n
}

func TestJobsPolicyAtomicSchedulingAndCancellation(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	manager, err := attribution.Open(db, "selected", "test-stream")
	require.NoError(t, err)
	m.BindPolicyAdoption(policygate.New(m.policy, &testPolicyDrainer{next: 1}, "selected"), manager, "selected")
	j, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	again, err := m.AddSource("https://pds.example/")
	require.NoError(t, err)
	require.Equal(t, j.ID, again.ID)
	policy, err := m.SetPolicy(t.Context(), 1, []string{"app.bsky.feed.like", "app.bsky.feed.post"})
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
	_, err = m.SetPolicy(t.Context(), 1, nil)
	require.ErrorIs(t, err, selection.ErrRevision)
	require.ErrorIs(t, m.Retry(j.ID), ErrConflict)
	require.NoError(t, m.RemoveSource("https://pds.example"))
	for _, j := range m.List() {
		require.Equal(t, Canceled, j.State)
	}
}

func TestInventoryCheckpointCommitsProgressWithInventory(t *testing.T) {
	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	injected := errors.New("injected inventory checkpoint failure")
	fault := &store.KeyPrefixFault{Prefix: []byte(inventoryKeyPrefix), Op: store.WriteOpBatchCommit, Ordinal: 1, Err: injected}
	m, db := newManagerWithOptions(t, t.TempDir(), store.WithFaultInjector(fault))
	defer db.Close()
	job, err := m.AddSource("https://pds.example")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- m.Run(ctx, func(ctx context.Context, _ Job) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	<-started

	require.ErrorIs(t, m.CheckpointInventory(job.ID, "next", map[string]string{did: "3l3qo2vutsw2b"}, true), injected)
	current, err := m.Get(job.ID)
	require.NoError(t, err)
	require.Nil(t, current.LastProgressAt, "failed inventory batch must not publish progress")
	require.False(t, current.TotalReposKnown)
	inventory, err := m.readInventory(job.ID)
	require.NoError(t, err)
	require.Empty(t, inventory.Entries, "inventory and job progress commit together")
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
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

func TestLegacyJobWithoutProgressTimestampStaysUnknownAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	m, db := newManager(t, dir)
	job, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	require.Nil(t, job.LastProgressAt)
	require.NoError(t, db.Close())

	m, db = newManager(t, dir)
	defer db.Close()
	restored, err := m.Get(job.ID)
	require.NoError(t, err)
	require.Nil(t, restored.LastProgressAt, "missing legacy timestamp remains unknown")
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
	checkpointed, err := m.Get(j.ID)
	require.NoError(t, err)
	require.NotNil(t, checkpointed.LastProgressAt)
	lastProgressAt := *checkpointed.LastProgressAt
	require.NoError(t, db.Close())
	m, db = newManager(t, dir)
	defer db.Close()
	resumed, err := m.Get(j.ID)
	require.NoError(t, err)
	require.Equal(t, Pending, resumed.State)
	require.Equal(t, "3l3qo2vutsw2b", resumed.CompletedRepos["did:plc:test"])
	require.Equal(t, lastProgressAt, *resumed.LastProgressAt, "repository checkpoint time survives reopen")
	ctx, cancel = context.WithCancel(t.Context())
	defer cancel()
	go func() {
		done <- m.Run(ctx, func(context.Context, Job) error { return &InputError{Code: "source_unavailable", Unavailable: true} })
	}()
	require.Eventually(t, func() bool { return m.List()[0].State == Incomplete }, time.Second, time.Millisecond)
	require.Equal(t, "current_state", m.List()[0].Coverage)
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

func TestSourceAcceptanceRetrySurvivesPolicyChangeAndRestart(t *testing.T) {
	dir := t.TempDir()
	m, db := newManager(t, dir)
	history, err := attribution.Open(db, "selected", "test-stream")
	require.NoError(t, err)
	m.BindPolicyAdoption(policygate.New(m.policy, &testPolicyDrainer{next: 1}, "selected"), history, "selected")
	const pds = "https://lost-ack.example"
	original, err := m.AddSourceWithRevision(pds, 7)
	require.NoError(t, err)
	m.SetPolicyAdvanceSender(func(context.Context, string, uint64, uint64) error { return nil })
	_, err = m.SetPolicy(t.Context(), 1, []string{"app.bsky.feed.like", "app.bsky.feed.post"})
	require.NoError(t, err)
	policyJobsBefore := len(m.List())
	require.NoError(t, db.Close())

	m, db = newManager(t, dir)
	defer db.Close()
	retry, err := m.AddSourceWithRevision(pds, 7)
	require.NoError(t, err)
	require.Equal(t, original.ID, retry.ID)
	require.Equal(t, policyJobsBefore, len(m.List()), "retry must not schedule source_added under new policy")
}

func TestCompletedRecoveryReceiptRetriesAfterRestart(t *testing.T) {
	dir := t.TempDir()
	m, db := newManager(t, dir)
	job, err := m.AddSourceWithRevision("https://pds.example", 7)
	require.NoError(t, err)
	m.SetReceiptSender(func(context.Context, Job) error { return errors.New("relay unavailable") })
	started := make(chan struct{})
	var startedOnce sync.Once
	m.SetReceiptSender(func(context.Context, Job) error {
		startedOnce.Do(func() { close(started) })
		return errors.New("relay unavailable")
	})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx, func(context.Context, Job) error { return nil }) }()
	<-started
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	current, err := m.Get(job.ID)
	require.NoError(t, err)
	require.Equal(t, Complete, current.State)
	require.True(t, current.ReceiptPending)
	current, err = m.Get(job.ID)
	require.NoError(t, err)
	require.Equal(t, Complete, current.State)
	require.True(t, current.ReceiptPending)
	require.NoError(t, db.Close())

	m, db = newManager(t, dir)
	defer db.Close()
	received := make(chan Job, len(m.List()))
	m.SetReceiptSender(func(_ context.Context, receipt Job) error {
		select {
		case received <- receipt:
		default:
		}
		return nil
	})
	ctx, cancel = context.WithCancel(t.Context())
	done = make(chan error, 1)
	go func() { done <- m.Run(ctx, func(context.Context, Job) error { return nil }) }()
	receivedJob := <-received
	require.Equal(t, uint64(7), receivedJob.SourceRevision)
	require.Equal(t, Complete, receivedJob.State)
	require.Eventually(t, func() bool {
		stored, getErr := m.Get(job.ID)
		return getErr == nil && !stored.ReceiptPending
	}, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestSourceRevisionAdvanceCancelsInFlightJobAndSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	m, db := newManager(t, dir)
	first, err := m.AddSourceWithRevision("https://pds.example", 1)
	require.NoError(t, err)
	started := make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- m.Run(ctx, func(jobCtx context.Context, _ Job) error {
			select {
			case <-started:
			default:
				close(started)
			}
			<-jobCtx.Done()
			return jobCtx.Err()
		})
	}()
	<-started
	second, err := m.AddSourceWithRevision("https://pds.example", 2)
	require.NoError(t, err)
	require.NotEqual(t, first.ID, second.ID)
	require.Eventually(t, func() bool {
		old, oldErr := m.Get(first.ID)
		return oldErr == nil && old.State == Canceled && old.ErrorCode == "source_revision_changed"
	}, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.NoError(t, db.Close())

	m, db = newManager(t, dir)
	defer db.Close()
	old, err := m.Get(first.ID)
	require.NoError(t, err)
	require.Equal(t, Canceled, old.State)
	current, err := m.Get(second.ID)
	require.NoError(t, err)
	require.Equal(t, Pending, current.State)
	require.Equal(t, uint64(2), current.SourceRevision)
}

func TestLowerSourceRevisionIsRejectedForSourceAndQuotaRequests(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	newest, err := m.AddSourceWithRevision("https://pds.example", 9)
	require.NoError(t, err)
	_, err = m.AddSourceWithRevision("https://pds.example", 8)
	require.ErrorIs(t, err, ErrConflict)
	_, err = m.RequestOnceWithSourceRevision("https://pds.example", "quota_recovery", "quota-old", 8)
	require.ErrorIs(t, err, ErrConflict)
	current, err := m.Get(newest.ID)
	require.NoError(t, err)
	require.Equal(t, uint64(9), current.SourceRevision)
	require.Len(t, m.List(), 1, "delayed source and quota commands must not create jobs")
}

func TestStaleRecoveryReceiptIsTerminalWithoutStoppingJobs(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	job, err := m.AddSourceWithRevision("https://pds.example", 4)
	require.NoError(t, err)
	m.SetReceiptSender(func(context.Context, Job) error { return ErrReceiptStale })
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx, func(context.Context, Job) error { return nil }) }()
	require.Eventually(t, func() bool {
		current, getErr := m.Get(job.ID)
		return getErr == nil && current.State == Complete && !current.ReceiptPending && current.ReceiptError == "stale_source_revision"
	}, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestSupersededPolicyJobDoesNotSubmitRecoveryReceipt(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	job, err := m.AddSourceWithRevision("https://pds.example", 4)
	require.NoError(t, err)
	m.SetPolicyAdvanceSender(func(context.Context, string, uint64, uint64) error { return nil })

	blocked := make(chan struct{})
	resume := make(chan struct{})
	m.SetReceiptSender(func(ctx context.Context, _ Job) error {
		select {
		case <-blocked:
		default:
			close(blocked)
		}
		select {
		case <-resume:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx, func(context.Context, Job) error { return nil }) }()
	<-blocked
	require.Eventually(t, func() bool {
		current, getErr := m.Get(job.ID)
		return getErr == nil && current.State == Complete && current.ReceiptPending
	}, time.Second, time.Millisecond)

	_, err = m.SetPolicy(t.Context(), m.policy.Current().Revision, []string{"app.bsky.feed.like", "app.bsky.feed.post"})
	require.NoError(t, err)
	close(resume)
	receipts := make(chan Job, 1)
	m.SetReceiptSender(func(_ context.Context, received Job) error {
		receipts <- received
		return nil
	})
	require.Eventually(t, func() bool {
		current, getErr := m.Get(job.ID)
		if getErr == nil && current.State == Complete && !current.ReceiptPending && current.ReceiptError == "policy_superseded" {
			return true
		}
		m.signalScheduler()
		return false
	}, time.Second, time.Millisecond)
	require.Never(t, func() bool {
		select {
		case received := <-receipts:
			return received.ID == job.ID
		default:
			return false
		}
	}, 200*time.Millisecond, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestLegacyUnversionedQuotaRequestRetainsLifecycleRevision(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	const pds = "https://legacy-quota.example"
	_, err := m.AddSourceWithRevision(pds, 4)
	require.NoError(t, err)
	job, err := m.RequestOnce(pds, "quota_recovery", "legacy-request")
	require.NoError(t, err)
	require.Equal(t, uint64(4), job.SourceRevision)
	require.Equal(t, uint64(4), m.SourceRevision(pds))
	legacy, err := m.AddSourceWithRevision("https://legacy-enabled.example", 1)
	require.NoError(t, err)
	legacyPDS := legacy.PDS
	require.NoError(t, m.RemoveSourceWithRevision(legacyPDS, 2))
	_, err = m.RequestOnce(legacyPDS, "quota_recovery", "legacy-after-removal")
	require.ErrorIs(t, err, ErrConflict)
}

func TestLegacyUnversionedQuotaRequestAfterPolicyAdvance(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	const pds = "https://legacy-policy-quota.example"
	_, err := m.AddSourceWithRevision(pds, 2)
	require.NoError(t, err)
	m.SetPolicyAdvanceSender(func(context.Context, string, uint64, uint64) error { return nil })
	_, err = m.SetPolicy(t.Context(), 1, []string{"app.bsky.feed.like"})
	require.NoError(t, err)
	job, err := m.RequestOnce(pds, "quota_recovery", "legacy-policy-request")
	require.NoError(t, err)
	require.Equal(t, uint64(2), job.SourceRevision)
	require.Equal(t, uint64(2), m.SourceRevision(pds))
}

func TestPolicyChangeRequiresRelayRecoveryMirror(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	_, err := m.AddSourceWithRevision("https://pds.example", 4)
	require.NoError(t, err)
	_, err = m.SetPolicy(t.Context(), 1, []string{"app.bsky.feed.like", "app.bsky.feed.post"})
	require.ErrorIs(t, err, ErrPolicyMirrorRequired)
	require.Equal(t, uint64(1), m.policy.Current().Revision)

	var advanced []struct {
		pds            string
		sourceRevision uint64
		policyRevision uint64
	}
	m.SetPolicyAdvanceSender(func(_ context.Context, pds string, sourceRevision, policyRevision uint64) error {
		advanced = append(advanced, struct {
			pds            string
			sourceRevision uint64
			policyRevision uint64
		}{pds, sourceRevision, policyRevision})
		return nil
	})
	policy, err := m.SetPolicy(t.Context(), 1, []string{"app.bsky.feed.like", "app.bsky.feed.post"})
	require.NoError(t, err)
	require.Equal(t, uint64(2), policy.Revision)
	require.Equal(t, []struct {
		pds            string
		sourceRevision uint64
		policyRevision uint64
	}{{"https://pds.example", 4, 2}}, advanced)

	m.SetPolicyAdvanceSender(func(context.Context, string, uint64, uint64) error { return errors.New("relay unavailable") })
	_, err = m.SetPolicy(t.Context(), 2, []string{"app.bsky.actor.profile", "app.bsky.feed.like", "app.bsky.feed.post"})
	require.ErrorContains(t, err, "advance Relay recovery policy")
	require.Equal(t, uint64(2), m.policy.Current().Revision)
	for _, job := range m.List() {
		require.NotEqual(t, uint64(3), job.Policy.Revision)
	}
}

func TestMissingRecoveryReceiptSourceIsTerminalWithoutStoppingJobs(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	job, err := m.AddSourceWithRevision("https://pds.example", 4)
	require.NoError(t, err)
	m.SetReceiptSender(func(context.Context, Job) error { return ErrReceiptSourceMissing })
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx, func(context.Context, Job) error { return nil }) }()
	require.Eventually(t, func() bool {
		current, getErr := m.Get(job.ID)
		return getErr == nil && current.State == Complete && !current.ReceiptPending && current.ReceiptError == "source_not_found"
	}, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestRelayReceiptSenderClassifiesMissingSource(t *testing.T) {
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/hypercerts/v1/source/recovery-receipt", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"source_not_found"}`))
	}))
	defer relay.Close()
	err := (RelayReceiptSender{URL: relay.URL, Token: "fixture-service-credential-32-bytes-minimum", Client: relay.Client()}).Send(t.Context(), Job{ID: "job-1", PDS: "https://pds.example", SourceRevision: 1, Policy: selection.Policy{Revision: 1}})
	require.ErrorIs(t, err, ErrReceiptSourceMissing)
}

func TestRelayReceiptSenderAdvancesRecoveryPolicy(t *testing.T) {
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/hypercerts/v1/source/recovery-policy", r.URL.Path)
		var input struct {
			PDS            string `json:"pds"`
			SourceRevision uint64 `json:"sourceRevision"`
			PolicyRevision uint64 `json:"policyRevision"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&input))
		require.Equal(t, "https://pds.example", input.PDS)
		require.Equal(t, uint64(4), input.SourceRevision)
		require.Equal(t, uint64(2), input.PolicyRevision)
		w.WriteHeader(http.StatusOK)
	}))
	defer relay.Close()
	err := (RelayReceiptSender{URL: relay.URL, Token: "fixture-service-credential-32-bytes-minimum", Client: relay.Client()}).AdvancePolicy(t.Context(), "https://pds.example", 4, 2)
	require.NoError(t, err)
}

func TestRelayReceiptSenderRetriesAmbiguousNotFoundResponses(t *testing.T) {
	for _, response := range []struct {
		name string
		body string
	}{
		{name: "empty", body: ""},
		{name: "html", body: "<html>not found</html>"},
		{name: "generic json", body: `{"error":"not_found"}`},
	} {
		t.Run(response.name, func(t *testing.T) {
			relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(response.body))
			}))
			defer relay.Close()
			err := (RelayReceiptSender{URL: relay.URL, Token: "fixture-service-credential-32-bytes-minimum", Client: relay.Client()}).Send(t.Context(), Job{ID: "job-1", PDS: "https://pds.example", SourceRevision: 1, Policy: selection.Policy{Revision: 1}})
			require.Error(t, err)
			require.NotErrorIs(t, err, ErrReceiptSourceMissing)
		})
	}
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
