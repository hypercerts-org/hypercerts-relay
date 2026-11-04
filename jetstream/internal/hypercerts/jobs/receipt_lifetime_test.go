package jobs

import (
	"context"
	"errors"
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
			require.ErrorIs(t, received.Err(), context.Canceled)
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

func TestReceiptAttemptWatcherExits(t *testing.T) {
	for _, trigger := range []string{"completion", "parent", "shutdown"} {
		t.Run(trigger, func(t *testing.T) {
			parent, cancelParent := context.WithCancel(t.Context())
			defer cancelParent()
			done := make(chan struct{})
			receiptCtx, cancel, exited := receiptAttemptContext(parent, done)
			defer cancel()
			switch trigger {
			case "completion":
				cancel()
			case "parent":
				cancelParent()
			case "shutdown":
				close(done)
			}
			select {
			case <-exited:
			case <-time.After(time.Second):
				t.Fatal("receipt watcher did not exit")
			}
			require.ErrorIs(t, receiptCtx.Err(), context.Canceled)
		})
	}
}
