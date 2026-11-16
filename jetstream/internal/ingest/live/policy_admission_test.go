package live

import (
	"context"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/policygate"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

type livePolicyDrainer struct{}

func (livePolicyDrainer) DrainDurability(context.Context) error { return nil }
func (livePolicyDrainer) NextSeq() uint64                       { return 1 }

func TestPolicyAdmissionKeepsSyncScopeStableAcrossAdoption(t *testing.T) {
	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	policy, err := selection.Open(db, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	gate := policygate.New(policy, livePolicyDrainer{}, "test")

	syncCaptured := make(chan struct{})
	releaseSync := make(chan struct{})
	syncDone := make(chan error, 1)
	var admitted selection.Policy
	go func() {
		syncDone <- gate.Admit(context.Background(), func(snapshotPolicy selection.Policy) error {
			admitted = snapshotPolicy
			close(syncCaptured)
			<-releaseSync
			return nil
		})
	}()
	select {
	case <-syncCaptured:
	case <-time.After(2 * time.Second):
		t.Fatal("sync did not capture its admitted policy")
	}

	adoptDone := make(chan error, 1)
	go func() {
		adoptDone <- gate.Adopt(context.Background(), "test", func(*uint64) error {
			_, updateErr := policy.Update(1, []string{"app.bsky.feed.like", "app.bsky.feed.post"}, nil)
			return updateErr
		})
	}()
	select {
	case err := <-adoptDone:
		t.Fatalf("policy adoption crossed an active sync projection: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(releaseSync)
	select {
	case err := <-syncDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("sync projection failed to finish")
	}
	select {
	case err := <-adoptDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("policy adoption deadlocked behind sync projection")
	}
	require.Equal(t, []string{"app.bsky.feed.post"}, admitted.Collections)
	var next selection.Policy
	require.NoError(t, gate.Admit(context.Background(), func(current selection.Policy) error {
		next = current
		return nil
	}))
	require.Equal(t, uint64(2), next.Revision)
	require.True(t, policyAllows(next.Collections, &segment.Event{Kind: segment.KindCreate, Collection: "app.bsky.feed.like"}), "the next sync must use the adopted collection scope")
}
