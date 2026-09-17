package relay

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/cmd/relay/relay/models"
	"github.com/bluesky-social/indigo/cmd/relay/stream"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// TestT11RatePolicyPersistsAndEnforcesBothScopes proves the selected unit is a
// raw source frame, both configured scopes are required, and restart restores
// policy with a fresh one-second burst rather than a fictional durable balance.
func TestT11RatePolicyPersistsAndEnforcesBothScopes(t *testing.T) {
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
	// A new process starts with the selected one-second burst allowance.
	require.NoError(t, r.waitRateCapacity(t.Context(), "rate.example"))
	rows, _, err := r.ListRatePolicies(t.Context(), "")
	require.NoError(t, err)
	require.Len(t, rows, 2)
	var perPDS RatePolicyView
	for _, row := range rows {
		if row.Scope == "https://rate.example" {
			perPDS = row
		}
	}
	require.Equal(t, "events/second", perPDS.Unit)
	require.Equal(t, "single_active_relay_process", perPDS.AdmissionScope)
	require.Equal(t, "process_local_since_start", perPDS.MeasurementScope)
	require.EqualValues(t, 1, perPDS.AdmittedFrames)
	require.EqualValues(t, 0, perPDS.WaitedFrames)
	_, err = r.SetRatePolicy(t.Context(), "global", 0)
	require.ErrorIs(t, err, ErrInvalidRatePolicy)
	rows, _, err = r.ListRatePolicies(t.Context(), "")
	require.NoError(t, err)
	require.Equal(t, int64(2), rows[0].EventsPerSecond)
}

func TestT11RateAdmissionFencesTwoRelayLifecycleAndRestart(t *testing.T) {
	_, testDB := newSourceRelay(t)
	ctx := t.Context()
	fixture := newSourceFixture(t)
	newRelay := func() *Relay {
		t.Helper()
		config := DefaultRelayConfig()
		config.RequireRateAdmission = true
		r, err := NewRelay(testDB.DB, nil, nil, config)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, r.Slurper.Shutdown()) })
		return r
	}

	first := newRelay()
	require.NoError(t, first.AcquireRateAdmission(ctx, "first", time.Minute))
	t.Cleanup(func() { require.NoError(t, first.ReleaseRateAdmission(context.Background(), "first")) })
	_, err := first.SetRatePolicy(ctx, "global", 1)
	require.NoError(t, err)
	// Make the source eligible for a socket. The second Relay must fail before
	// ResubscribeAllHosts reaches this row and calls Slurper.Subscribe.
	source, err := first.AddSource(ctx, "http://"+fixture.host)
	require.NoError(t, err)
	require.NoError(t, testDB.DB.Model(&models.Source{}).Where("host_id = ?", source.HostID).Update("validation_status", models.SourceValidationPassed).Error)
	require.NoError(t, first.waitRateCapacity(ctx, fixture.host))
	require.NoError(t, first.ResubscribeAllHosts(ctx))
	firstConnection := fixture.next(t)

	second := newRelay()
	require.ErrorIs(t, second.AcquireRateAdmission(ctx, "second", time.Minute), ErrRateAdmissionHeld)
	require.ErrorIs(t, second.ResubscribeAllHosts(ctx), ErrRateAdmissionHeld)
	require.False(t, second.Slurper.CheckIfSubscribed(fixture.host))
	requireNoSourceConnection(t, fixture)

	// A graceful release fences the old Relay before allowing a newly
	// constructed process to restore persisted policy with zeroed counters.
	require.NoError(t, first.ReleaseRateAdmission(ctx, "first"))
	select {
	case <-firstConnection.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("lease release did not cancel the existing source socket")
	}
	restarted := newRelay()
	require.NoError(t, restarted.AcquireRateAdmission(ctx, "restarted", time.Minute))
	t.Cleanup(func() { require.NoError(t, restarted.ReleaseRateAdmission(context.Background(), "restarted")) })
	policies, _, err := restarted.ListRatePolicies(ctx, "")
	require.NoError(t, err)
	require.Len(t, policies, 1)
	require.Equal(t, int64(1), policies[0].EventsPerSecond)
	require.Zero(t, policies[0].AdmittedFrames)
	require.Zero(t, policies[0].WaitedFrames)
	require.NoError(t, restarted.waitRateCapacity(ctx, fixture.host))
	require.NoError(t, restarted.ReleaseRateAdmission(ctx, "restarted"))

	// Expiry also fences and closes an already-connected old holder before a
	// fresh process can take over.
	expiring := newRelay()
	require.NoError(t, expiring.AcquireRateAdmission(ctx, "expiring", time.Minute))
	require.NoError(t, expiring.ResubscribeAllHosts(ctx))
	expiringConnection := fixture.next(t)
	require.NoError(t, testDB.DB.Model(&rateAdmission{}).Where("name = ?", rateAdmissionName).Update("expires_at", time.Now().UTC().Add(-time.Minute)).Error)
	require.ErrorIs(t, expiring.RenewRateAdmission(ctx, "expiring", time.Minute), ErrRateAdmissionHeld)
	require.ErrorIs(t, expiring.waitRateCapacity(ctx, fixture.host), ErrRateAdmissionHeld)
	require.ErrorIs(t, expiring.ResubscribeAllHosts(ctx), ErrRateAdmissionHeld)
	select {
	case <-expiringConnection.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("expired lease did not cancel the existing source socket")
	}
	replacement := newRelay()
	require.NoError(t, replacement.AcquireRateAdmission(ctx, "replacement", time.Minute))
	t.Cleanup(func() { require.NoError(t, replacement.ReleaseRateAdmission(context.Background(), "replacement")) })
	require.NoError(t, replacement.waitRateCapacity(ctx, fixture.host))
}

func TestT11RateAdmissionUsesConservativeDatabaseDerivedDeadline(t *testing.T) {
	_, testDB := newSourceRelay(t)
	ctx := t.Context()
	config := DefaultRelayConfig()
	config.RequireRateAdmission = true
	first, err := NewRelay(testDB.DB, nil, nil, config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, first.Slurper.Shutdown()) })
	require.NoError(t, first.AcquireRateAdmission(ctx, "skewed-holder", time.Minute))

	// The deadline starts before the DB operation, so it cannot run later than
	// the durable one-minute lease even if the local wall clock is skewed.
	first.admission.mu.Lock()
	deadline := first.admission.deadline
	first.admission.deadline = time.Now().Add(-time.Millisecond)
	first.admission.mu.Unlock()
	require.False(t, deadline.After(time.Now().Add(time.Minute)))
	require.ErrorIs(t, first.requireRateAdmission(ctx), ErrRateAdmissionHeld)

	second, err := NewRelay(testDB.DB, nil, nil, config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Slurper.Shutdown()) })
	// The conservative local deadline does not invalidate the durable lease.
	require.ErrorIs(t, second.AcquireRateAdmission(ctx, "database-clock-holder", time.Minute), ErrRateAdmissionHeld)
	require.NoError(t, testDB.DB.Model(&rateAdmission{}).Where("name = ?", rateAdmissionName).Update("expires_at", time.Now().UTC().Add(-time.Minute)).Error)
	require.NoError(t, second.AcquireRateAdmission(ctx, "database-clock-holder", time.Minute))
	t.Cleanup(func() { require.NoError(t, second.ReleaseRateAdmission(context.Background(), "database-clock-holder")) })
}

func TestT11FrameAdmissionDoesNotWriteLease(t *testing.T) {
	_, testDB := newSourceRelay(t)
	config := DefaultRelayConfig()
	config.RequireRateAdmission = true
	r, err := NewRelay(testDB.DB, nil, nil, config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Slurper.Shutdown()) })
	require.NoError(t, r.AcquireRateAdmission(t.Context(), "local-frame-fence", time.Minute))
	t.Cleanup(func() { require.NoError(t, r.ReleaseRateAdmission(context.Background(), "local-frame-fence")) })
	_, err = r.SetRatePolicy(t.Context(), "global", 1)
	require.NoError(t, err)

	var leaseWrites atomic.Int32
	require.NoError(t, r.db.Callback().Update().After("gorm:update").Register("test:count_rate_admission_writes", func(db *gorm.DB) {
		if db.Statement.Table == "hypercerts_rate_admission" {
			leaseWrites.Add(1)
		}
	}))
	t.Cleanup(func() { require.NoError(t, r.db.Callback().Update().Remove("test:count_rate_admission_writes")) })
	require.NoError(t, r.waitRateCapacity(t.Context(), "rate.example"))
	require.Zero(t, leaseWrites.Load())
}

func TestT11RateAdmissionFencesEveryRedial(t *testing.T) {
	fixture := newSourceFixture(t)
	config := DefaultSlurperConfig()
	config.PersistCursorPeriod = time.Hour
	var admitted atomic.Bool
	admitted.Store(true)
	var checks atomic.Int32
	config.RequireRateAdmission = func(context.Context) error {
		checks.Add(1)
		if admitted.Load() {
			return nil
		}
		return ErrRateAdmissionHeld
	}
	s, err := NewSlurper(func(context.Context, *stream.XRPCStreamEvent, string, uint64) error { return nil }, config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Shutdown()) })
	require.NoError(t, s.Subscribe(&models.Host{ID: 1, Hostname: fixture.host, NoSSL: true}))
	connection := fixture.next(t)

	admitted.Store(false)
	require.NoError(t, connection.conn.Close())
	requireNoSourceConnection(t, fixture)
	require.GreaterOrEqual(t, checks.Load(), int32(2))
}

// TestT12RatePolicyMeasurementsAreBoundedAndPaginated keeps telemetry useful
// without turning the control response into an unbounded per-frame event log.
func TestT12RatePolicyMeasurementsAreBoundedAndPaginated(t *testing.T) {
	r, db := newSourceRelay(t)
	require.NoError(t, r.loadRatePolicies())
	_, err := r.SetRatePolicy(t.Context(), "global", 1)
	require.NoError(t, err)
	require.NoError(t, r.waitRateCapacity(t.Context(), "any.example"))
	limited, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, r.waitRateCapacity(limited, "any.example"), context.DeadlineExceeded)

	rows, _, err := r.ListRatePolicies(t.Context(), "")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.EqualValues(t, 1, rows[0].AdmittedFrames)
	require.EqualValues(t, 1, rows[0].WaitedFrames)

	for i := 0; i < 100; i++ {
		policy := RatePolicy{Scope: fmt.Sprintf("https://telemetry-%03d.example", i), EventsPerSecond: 1, UpdatedAt: time.Now().UTC()}
		require.NoError(t, db.DB.Create(&policy).Error)
	}
	first, next, err := r.ListRatePolicies(t.Context(), "")
	require.NoError(t, err)
	require.Len(t, first, 100)
	require.NotEmpty(t, next)
	second, final, err := r.ListRatePolicies(t.Context(), next)
	require.NoError(t, err)
	require.Len(t, second, 1)
	require.Empty(t, final)
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
