package jobs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/policygate"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

func TestPolicyStagingFailurePreservesJobsAndRunningWorker(t *testing.T) {
	injected := errors.New("policy batch failed")
	fault := &store.KeyPrefixFault{Prefix: []byte(stateKey), Op: store.WriteOpBatchCommit, Ordinal: 1, Err: injected}
	m, db := newManagerWithOptions(t, t.TempDir(), store.WithFaultInjector(fault))
	defer db.Close()
	job, err := m.AddSourceWithRevision("https://policy.example", 3)
	require.NoError(t, err)
	history, err := attribution.Open(db, "crypto", "selected")
	require.NoError(t, err)
	m.BindPolicyAdoption(policygate.New(m.policy, &testPolicyDrainer{next: 1}, "selected"), history, "selected")
	workerCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	m.runningID, m.cancel = job.ID, cancel
	before := m.List()
	var mirrored []uint64
	m.SetPolicyAdvanceSender(func(_ context.Context, _ string, sourceRevision, policyRevision uint64) error {
		require.Equal(t, uint64(1), m.policy.Current().Revision)
		require.Equal(t, uint64(3), sourceRevision)
		mirrored = append(mirrored, policyRevision)
		return nil
	})
	_, err = m.SetPolicy(t.Context(), 1, []string{"app.bsky.feed.like"})
	require.ErrorIs(t, err, injected)
	require.Equal(t, []uint64{2}, mirrored)
	require.Equal(t, before, m.List())
	require.NoError(t, workerCtx.Err())
	require.Equal(t, uint64(1), m.policy.Current().Revision)
	policies, err := history.ReadPolicyHistory()
	require.NoError(t, err)
	require.Empty(t, policies)
	persisted, found, err := readPersistedData(db)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, before[0], persisted.Jobs[job.ID])

	_, err = m.SetPolicy(t.Context(), 1, []string{"app.bsky.feed.like"})
	require.NoError(t, err)
	require.ErrorIs(t, workerCtx.Err(), context.Canceled)
	require.Equal(t, []uint64{2, 2}, mirrored)
	require.ErrorIs(t, m.Retry(job.ID), ErrConflict)
	policies, err = history.ReadPolicyHistory()
	require.NoError(t, err)
	require.Len(t, policies, 1)
	require.Equal(t, uint64(2), policies[0].Revision)
}

func TestReceiptDeliveryRetainsSourceAndCancellationFences(t *testing.T) {
	for _, mutation := range []string{"source", "parent", "policy"} {
		t.Run(mutation, func(t *testing.T) {
			m, db := newManager(t, t.TempDir())
			defer db.Close()
			job, err := m.AddSourceWithRevision("https://receipt.example", 1)
			require.NoError(t, err)
			job.State, job.ReceiptPending = Complete, true
			m.data.Jobs[job.ID] = job
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			started, resume := make(chan struct{}), make(chan struct{})
			m.SetReceiptSender(func(context.Context, Job) error {
				close(started)
				<-resume
				return nil
			})
			var receipts sync.WaitGroup
			require.NoError(t, m.flushReceipts(ctx, &receipts))
			<-started
			require.NoError(t, m.flushReceipts(ctx, &receipts))
			switch mutation {
			case "source":
				m.mu.Lock()
				current := m.data.Jobs[job.ID]
				current.SourceRevision++
				m.data.Jobs[job.ID] = current
				m.mu.Unlock()
			case "parent":
				cancel()
			case "policy":
				m.SetPolicyAdvanceSender(func(context.Context, string, uint64, uint64) error { return nil })
				_, err = m.SetPolicy(t.Context(), 1, []string{"app.bsky.feed.like"})
				require.NoError(t, err)
			}
			close(resume)
			receipts.Wait()
			current, err := m.Get(job.ID)
			require.NoError(t, err)
			if mutation == "policy" {
				require.False(t, current.ReceiptPending)
				require.Equal(t, "policy_superseded", current.ReceiptError)
			} else {
				require.True(t, current.ReceiptPending)
			}
			require.Zero(t, current.ReceiptAttempts)
			require.Equal(t, time.Time{}, current.ReceiptRetryAt)
			require.Empty(t, m.receiptRunning)
		})
	}
}
