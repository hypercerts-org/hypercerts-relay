package jetstreamd

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/jobs"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

const (
	coalesceDID1 = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	coalesceDID2 = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"
	coalescePDS  = "https://pds.example"
)

func newCoalescer(t *testing.T) (*recoveryCoalescer, *jobs.Manager) {
	t.Helper()
	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	policy, err := selection.Open(db, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	manager, err := jobs.Open(db, policy)
	require.NoError(t, err)
	require.NoError(t, manager.SeedSources([]string{coalescePDS}))

	return &recoveryCoalescer{
		jobs:     manager,
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		interval: time.Millisecond,
		pending:  map[string]struct{}{},
		inflight: map[string]struct{}{},
		resolve:  func(context.Context, string) string { return coalescePDS },
	}, manager
}

// observe runs on the ingestion path, so it must never book work itself.
func TestCoalescerObserveDoesNotBookUntilFlush(t *testing.T) {
	c, manager := newCoalescer(t)
	c.observe(coalesceDID1, 1)

	for _, job := range manager.List() {
		require.NotEqual(t, jobs.ReasonRepositoryRecovery, job.Reason, "observe must not book work")
	}

	c.flush(t.Context())
	var booked []jobs.Job
	for _, job := range manager.List() {
		if job.Reason == jobs.ReasonRepositoryRecovery {
			booked = append(booked, job)
		}
	}
	require.Len(t, booked, 1)
	require.Equal(t, []string{coalesceDID1}, booked[0].RecoveryDIDs)
}

// A replayed backlog sheds hundreds of events for one DID; that must produce one
// booking, not one per event.
func TestCoalescerBatchesOneDIDIntoOneJob(t *testing.T) {
	c, manager := newCoalescer(t)
	for index := 0; index < 500; index++ {
		c.observe(coalesceDID1, 1)
	}
	c.flush(t.Context())

	var booked []jobs.Job
	for _, job := range manager.List() {
		if job.Reason == jobs.ReasonRepositoryRecovery {
			booked = append(booked, job)
		}
	}
	require.Len(t, booked, 1)
	require.Equal(t, []string{coalesceDID1}, booked[0].RecoveryDIDs)
}

func TestCoalescerGroupsDIDsByPDS(t *testing.T) {
	c, manager := newCoalescer(t)
	c.observe(coalesceDID1, 1)
	c.observe(coalesceDID2, 1)
	c.flush(t.Context())

	var booked []jobs.Job
	for _, job := range manager.List() {
		if job.Reason == jobs.ReasonRepositoryRecovery {
			booked = append(booked, job)
		}
	}
	require.Len(t, booked, 1, "DIDs on one PDS share a booking")
	require.Equal(t, []string{coalesceDID1, coalesceDID2}, booked[0].RecoveryDIDs)
}

// An unattributable DID must be skipped, not booked against a guessed PDS.
func TestCoalescerSkipsUnresolvableDID(t *testing.T) {
	c, manager := newCoalescer(t)
	c.resolve = func(context.Context, string) string { return "" }
	c.observe(coalesceDID1, 1)
	c.flush(t.Context())

	for _, job := range manager.List() {
		require.NotEqual(t, jobs.ReasonRepositoryRecovery, job.Reason)
	}
}

// observe is called from the verifier's drop path on another goroutine.
func TestCoalescerObserveIsConcurrencySafe(t *testing.T) {
	c, _ := newCoalescer(t)
	var wg sync.WaitGroup
	for index := 0; index < 64; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.observe(coalesceDID1, 1)
		}()
	}
	wg.Wait()

	c.mu.Lock()
	defer c.mu.Unlock()
	require.Len(t, c.pending, 1, "the same DID collapses to one pending entry")
}

// A DID already under repair must not be booked again on every flush.
func TestCoalescerSkipsDIDAlreadyRepairing(t *testing.T) {
	c, manager := newCoalescer(t)
	c.observe(coalesceDID1, 1)
	c.flush(t.Context())
	c.observe(coalesceDID1, 1)
	c.flush(t.Context())

	count := 0
	for _, job := range manager.List() {
		if job.Reason == jobs.ReasonRepositoryRecovery {
			count++
		}
	}
	require.Equal(t, 1, count, "an in-flight repair suppresses a duplicate booking")
}
