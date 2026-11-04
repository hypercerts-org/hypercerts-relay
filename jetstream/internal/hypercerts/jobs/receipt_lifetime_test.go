package jobs

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReceiptAttemptsReleaseContextsAndWatchers(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	m.receiptDone = make(chan struct{})
	defer close(m.receiptDone)
	watcherCount := func() int {
		stack := make([]byte, 1<<20)
		return strings.Count(string(stack[:runtime.Stack(stack, true)]), "(*Manager).flushReceipts.func1.")
	}
	before := watcherCount()
	revision := uint64(0)
	for _, fail := range []bool{false, true} {
		for attempt := 0; attempt < 12; attempt++ {
			revision++
			job, err := m.AddSourceWithRevision("https://receipt.example", revision)
			require.NoError(t, err)
			m.mu.Lock()
			job.State, job.ReceiptPending = Complete, true
			job.ReceiptRetryAt = time.Time{}
			m.data.Jobs[job.ID] = job
			m.mu.Unlock()
			var received context.Context
			m.SetReceiptSender(func(ctx context.Context, _ Job) error {
				received = ctx
				if fail {
					return errors.New("unavailable")
				}
				return nil
			})
			var receipts sync.WaitGroup
			require.NoError(t, m.flushReceipts(t.Context(), &receipts))
			receipts.Wait()
			require.NotNil(t, received)
			require.Eventually(t, func() bool { return received.Err() != nil && watcherCount() == before }, time.Second, time.Millisecond)
		}
	}
}

func TestReceiptShutdownCancelsBlockedSender(t *testing.T) {
	for _, parentCancellation := range []bool{false, true} {
		t.Run(map[bool]string{false: "shutdown", true: "parent"}[parentCancellation], func(t *testing.T) {
			m, db := newManager(t, t.TempDir())
			defer db.Close()
			m.receiptDone = make(chan struct{})
			job, err := m.AddSourceWithRevision("https://receipt.example", 1)
			require.NoError(t, err)
			job.State, job.ReceiptPending = Complete, true
			m.data.Jobs[job.ID] = job
			started := make(chan struct{})
			m.SetReceiptSender(func(ctx context.Context, _ Job) error {
				close(started)
				<-ctx.Done()
				return ctx.Err()
			})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var receipts sync.WaitGroup
			require.NoError(t, m.flushReceipts(ctx, &receipts))
			<-started
			if parentCancellation {
				cancel()
			} else {
				close(m.receiptDone)
			}
			finished := make(chan struct{})
			go func() { receipts.Wait(); close(finished) }()
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Fatal("receipt did not stop")
			}
			require.True(t, m.data.Jobs[job.ID].ReceiptPending)
		})
	}
}
