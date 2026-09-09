package relay

import (
	"context"
	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/cmd/relay/stream"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"sync"
	"testing"
	"time"
)

func TestRatePolicyPersistsAndEnforcesBothScopes(t *testing.T) {
	r, _ := newSourceRelay(t)
	require.NoError(t, r.loadRatePolicies())
	view, err := r.AddSource(t.Context(), "https://rate.example")
	require.NoError(t, err)
	require.NotNil(t, view)
	_, err = r.SetRatePolicy(t.Context(), "global", 2)
	require.NoError(t, err)
	_, err = r.SetRatePolicy(t.Context(), "https://rate.example", 1)
	require.NoError(t, err)
	require.NoError(t, r.waitRateCapacity(t.Context(), "rate.example"))
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, r.waitRateCapacity(ctx, "rate.example"), context.DeadlineExceeded)
	// A different source still has global capacity, but cannot exceed the global burst.
	require.NoError(t, r.waitRateCapacity(t.Context(), "other.example"))
	ctx2, cancel2 := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel2()
	require.ErrorIs(t, r.waitRateCapacity(ctx2, "other.example"), context.DeadlineExceeded)
	require.NoError(t, r.loadRatePolicies())
	rows, _, err := r.ListRatePolicies(t.Context(), "")
	require.NoError(t, err)
	require.Len(t, rows, 2)
	_, err = r.SetRatePolicy(t.Context(), "global", 0)
	require.ErrorIs(t, err, ErrInvalidRatePolicy)
	rows, _, err = r.ListRatePolicies(t.Context(), "")
	require.NoError(t, err)
	require.Equal(t, int64(2), rows[0].EventsPerSecond)
}

func TestRatePolicyDiskWriteDoesNotBlockAdmission(t *testing.T) {
	r, _ := newSourceRelay(t)
	require.NoError(t, r.loadRatePolicies())
	_, err := r.SetRatePolicy(t.Context(), "global", 100)
	require.NoError(t, err)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	require.NoError(t, r.db.Callback().Create().Before("gorm:create").Register("test:slow_policy", func(db *gorm.DB) {
		if db.Statement.Table == "hypercerts_rate_policy" {
			close(entered)
			<-release
		}
	}))
	writeDone := make(chan error, 1)
	go func() { _, err := r.SetRatePolicy(t.Context(), "global", 200); writeDone <- err }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("policy write did not start")
	}
	admission := make(chan error, 1)
	go func() { admission <- r.waitRateCapacity(t.Context(), "rate.example") }()
	select {
	case err := <-admission:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("disk write blocked event admission")
	}
	once.Do(func() { close(release) })
	require.NoError(t, <-writeDone)
	require.NoError(t, r.db.Callback().Create().Remove("test:slow_policy"))
	var writers sync.WaitGroup
	failures := make(chan error, 10)
	for i := int64(1); i <= 10; i++ {
		writers.Add(1)
		go func(value int64) {
			defer writers.Done()
			_, err := r.SetRatePolicy(t.Context(), "global", value)
			failures <- err
		}(i)
	}
	writers.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	var stored RatePolicy
	require.NoError(t, r.db.First(&stored, "scope = ?", "global").Error)
	r.rates.mu.Lock()
	published := r.rates.buckets["global"].policy
	r.rates.mu.Unlock()
	require.Equal(t, stored.EventsPerSecond, published.EventsPerSecond)
}

func TestRateSchedulerGatesEverySourceFrame(t *testing.T) {
	scheduler := rateScheduler{host: "rate.example", wait: func(_ context.Context, host string) error {
		require.Equal(t, "rate.example", host)
		return context.Canceled
	}}
	for _, event := range []*stream.XRPCStreamEvent{
		{RepoCommit: &comatproto.SyncSubscribeRepos_Commit{}},
		{RepoSync: &comatproto.SyncSubscribeRepos_Sync{}},
		{RepoIdentity: &comatproto.SyncSubscribeRepos_Identity{}},
		{RepoAccount: &comatproto.SyncSubscribeRepos_Account{}},
		{RepoInfo: &comatproto.SyncSubscribeRepos_Info{}},
		{LabelLabels: &comatproto.LabelSubscribeLabels_Labels{}},
		{Error: &stream.ErrorFrame{}},
	} {
		require.ErrorIs(t, scheduler.AddWork(t.Context(), "", event), context.Canceled)
	}
}
