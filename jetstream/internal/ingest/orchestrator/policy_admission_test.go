package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/policygate"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

type policyAdmissionDrainer struct{}

func (policyAdmissionDrainer) DrainDurability(context.Context) error { return nil }
func (policyAdmissionDrainer) NextSeq() uint64                       { return 1 }

func TestPolicyAdmissionPrecedesRewriteLockWithoutABBA(t *testing.T) {
	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	manager, err := selection.Open(db, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	gate := policygate.New(manager, policyAdmissionDrainer{}, "test")
	o := &Orchestrator{}
	rewriteHeld := make(chan struct{})
	releaseRewrite := make(chan struct{})
	reconcileDone := make(chan error, 1)
	go func() {
		reconcileDone <- gate.Admit(context.Background(), func(selection.Policy) error {
			return o.withRewriteLock(func() error {
				close(rewriteHeld)
				<-releaseRewrite
				return nil
			})
		})
	}()
	select {
	case <-rewriteHeld:
	case <-time.After(2 * time.Second):
		t.Fatal("managed reconciliation did not acquire the rewrite lock")
	}

	adoptStarted := make(chan struct{})
	adoptDone := make(chan error, 1)
	go func() {
		close(adoptStarted)
		adoptDone <- gate.Adopt(context.Background(), "test", func(*uint64) error {
			return o.withRewriteLock(func() error { return nil })
		})
	}()
	<-adoptStarted
	select {
	case err := <-adoptDone:
		t.Fatalf("adoption crossed an active admitted reconciliation: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(releaseRewrite)
	select {
	case err := <-reconcileDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("reconciliation failed to finish")
	}
	select {
	case err := <-adoptDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("adoption deadlocked behind reconciliation")
	}
}
