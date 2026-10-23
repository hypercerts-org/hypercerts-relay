package relay

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/cmd/relay/relay/models"
	"github.com/stretchr/testify/require"
)

type blockingHostChecker struct {
	started chan struct{}
	release chan struct{}
	checks  atomic.Int32
	err     error
}

func (c *blockingHostChecker) CheckHost(ctx context.Context, host string) error {
	c.checks.Add(1)
	select {
	case c.started <- struct{}{}:
	default:
	}
	select {
	case <-c.release:
		return c.err
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (*blockingHostChecker) FetchAccountStatus(context.Context, *identity.Identity) (models.AccountStatus, error) {
	return "", nil
}

type delayedCancellationHostChecker struct {
	started chan struct{}
	delay   time.Duration
	checks  atomic.Int32
}

func (c *delayedCancellationHostChecker) CheckHost(ctx context.Context, _ string) error {
	c.checks.Add(1)
	close(c.started)
	time.Sleep(c.delay)
	return ctx.Err()
}

func (*delayedCancellationHostChecker) FetchAccountStatus(context.Context, *identity.Identity) (models.AccountStatus, error) {
	return "", nil
}

func TestDelayedCanceledPublicAdmissionPersistsFailureAndCooldown(t *testing.T) {
	r, db := newSourceRelay(t)
	host := &models.Host{Hostname: "delayed-cancelled.example.com", Status: models.HostStatusActive}
	require.NoError(t, db.Create(host))
	require.NoError(t, db.Create(&models.Source{HostID: host.ID, State: models.SourceStateEnabled, Revision: 1, ValidationStatus: models.SourceValidationPending, RecoveryRequired: true}))
	checker := &delayedCancellationHostChecker{started: make(chan struct{}), delay: 6 * time.Second}
	r.HostChecker = checker
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- r.AdmitPublicSource(ctx, "https://delayed-cancelled.example.com") }()
	<-checker.started
	cancel()
	require.ErrorIs(t, <-result, ErrSourceValidationFailed)

	var failed models.Source
	require.NoError(t, db.Where("host_id = ?", host.ID).First(&failed).Error)
	require.Equal(t, models.SourceValidationFailed, failed.ValidationStatus)
	require.NotNil(t, failed.ValidationCheckedAt)
	require.ErrorIs(t, r.AdmitPublicSource(context.Background(), "https://delayed-cancelled.example.com"), ErrSourceValidationFailed)
	require.EqualValues(t, 1, checker.checks.Load())
}

func TestPublicAdmissionConcurrentConflictAndDisable(t *testing.T) {
	r, db := newSourceRelay(t)
	host := &models.Host{Hostname: "admission.example.com", Status: models.HostStatusActive}
	require.NoError(t, db.Create(host))
	require.NoError(t, db.Create(&models.Source{HostID: host.ID, State: models.SourceStateEnabled, Revision: 1, ValidationStatus: models.SourceValidationPending, RecoveryRequired: true}))
	checker := &blockingHostChecker{started: make(chan struct{}, 1), release: make(chan struct{})}
	r.HostChecker = checker
	first := make(chan error, 1)
	go func() { first <- r.AdmitPublicSource(context.Background(), "https://admission.example.com") }()
	<-checker.started
	require.ErrorIs(t, r.AdmitPublicSource(context.Background(), "https://admission.example.com"), ErrSourceRevisionConflict)
	// While the host check is blocked, unrelated source policy operations remain available.
	other, err := r.AddSource(context.Background(), "https://unrelated.example.com")
	require.NoError(t, err)
	_, err = r.SetSourceState(context.Background(), other.HostID, other.Revision, models.SourceStateDisabled)
	require.NoError(t, err)
	_, err = r.SetSourceState(context.Background(), host.ID, 1, models.SourceStateDisabled)
	require.NoError(t, err)
	close(checker.release)
	require.ErrorIs(t, <-first, ErrSourceRevisionConflict)
	require.EqualValues(t, 1, checker.checks.Load())
	var source models.Source
	require.NoError(t, db.Where("host_id = ?", host.ID).First(&source).Error)
	require.Equal(t, models.SourceStateDisabled, source.State)
	require.Equal(t, uint64(2), source.Revision)
	require.Equal(t, models.SourceValidationPending, source.ValidationStatus)
}

func TestPublicCreationLimiterRejectsWithoutDurableRows(t *testing.T) {
	r, db := newSourceRelay(t)
	r.HostPerDayLimiter = perDayLimiter(0)
	_, err := r.AddPublicSource(context.Background(), "https://limited.example.com")
	require.ErrorIs(t, err, ErrNewHostsDisabled)
	var hosts, sources int64
	require.NoError(t, db.Model(&models.Host{}).Where("hostname = ?", "limited.example.com").Count(&hosts).Error)
	require.NoError(t, db.Model(&models.Source{}).Count(&sources).Error)
	require.Zero(t, hosts)
	require.Zero(t, sources)
}

func TestPublicCreationLimiterExemptsExistingSources(t *testing.T) {
	r, _ := newSourceRelay(t)
	_, err := r.AddSource(context.Background(), "https://existing.example.com")
	require.NoError(t, err)
	r.HostPerDayLimiter = perDayLimiter(0)
	_, err = r.AddPublicSource(context.Background(), "https://existing.example.com")
	require.NoError(t, err)

	r2, _ := newSourceRelay(t)
	r2.HostPerDayLimiter = perDayLimiter(0)
	host := &models.Host{Hostname: "pending.example.com", Status: models.HostStatusActive}
	db := sourceTestDB{DB: r2.db}
	require.NoError(t, db.Create(host))
	require.NoError(t, db.Create(&models.Source{HostID: host.ID, State: models.SourceStateEnabled, Revision: 1, ValidationStatus: models.SourceValidationPending}))
	_, err = r2.AddPublicSource(context.Background(), "https://pending.example.com")
	require.NoError(t, err)
}

func TestPublicAdmissionCanceledCheckPersistsFailureAndCooldown(t *testing.T) {
	r, db := newSourceRelay(t)
	host := &models.Host{Hostname: "cancelled.example.com", Status: models.HostStatusActive}
	require.NoError(t, db.Create(host))
	require.NoError(t, db.Create(&models.Source{HostID: host.ID, State: models.SourceStateEnabled, Revision: 1, ValidationStatus: models.SourceValidationPending, RecoveryRequired: true}))
	checker := &blockingHostChecker{started: make(chan struct{}, 1), release: make(chan struct{})}
	r.HostChecker = checker
	ctx, cancel := context.WithCancel(context.Background())
	cancelled := make(chan error, 1)
	go func() { cancelled <- r.AdmitPublicSource(ctx, "https://cancelled.example.com") }()
	<-checker.started
	cancel()
	require.ErrorIs(t, <-cancelled, ErrSourceValidationFailed)

	var failed models.Source
	require.NoError(t, db.Where("host_id = ?", host.ID).First(&failed).Error)
	require.Equal(t, models.SourceValidationFailed, failed.ValidationStatus)
	require.NotNil(t, failed.ValidationCheckedAt)
	require.Equal(t, uint64(2), failed.Revision)
	require.ErrorIs(t, r.AdmitPublicSource(context.Background(), "https://cancelled.example.com"), ErrSourceValidationFailed)
	require.EqualValues(t, 1, checker.checks.Load())
}

func TestPublicAdmissionFailureCooldownExpiresAndPersists(t *testing.T) {
	r, db := newSourceRelay(t)
	host := &models.Host{Hostname: "cooldown.example.com", Status: models.HostStatusActive}
	require.NoError(t, db.Create(host))
	require.NoError(t, db.Create(&models.Source{HostID: host.ID, State: models.SourceStateEnabled, Revision: 4, ValidationStatus: models.SourceValidationPending, RecoveryRequired: true}))
	checker := &blockingHostChecker{started: make(chan struct{}, 1), release: make(chan struct{}), err: errors.New("untrusted diagnostic")}
	// Release immediately for the failed check.
	close(checker.release)
	r.HostChecker = checker
	require.ErrorIs(t, r.AdmitPublicSource(context.Background(), "https://cooldown.example.com"), ErrSourceValidationFailed)
	var failed models.Source
	require.NoError(t, db.Where("host_id = ?", host.ID).First(&failed).Error)
	require.Equal(t, uint64(5), failed.Revision)
	require.Equal(t, models.SourceValidationFailed, failed.ValidationStatus)
	require.NotNil(t, failed.ValidationCheckedAt)
	require.ErrorIs(t, r.AdmitPublicSource(context.Background(), "https://cooldown.example.com"), ErrSourceValidationFailed)
	require.EqualValues(t, 1, checker.checks.Load())
	// Reconstruction proves the one-minute gate is persisted, not process-local.
	r2, _ := newSourceRelay(t)
	r2.db = db.DB
	r2.HostChecker = checker
	require.ErrorIs(t, r2.AdmitPublicSource(context.Background(), "https://cooldown.example.com"), ErrSourceValidationFailed)
	require.EqualValues(t, 1, checker.checks.Load())
	require.NoError(t, db.Model(&models.Source{}).Where("host_id = ?", host.ID).Update("validation_checked_at", time.Now().Add(-2*publicValidationCooldown)).Error)
	require.ErrorIs(t, r.AdmitPublicSource(context.Background(), "https://cooldown.example.com"), ErrSourceValidationFailed)
	require.EqualValues(t, 2, checker.checks.Load())
	var retried models.Source
	require.NoError(t, db.Where("host_id = ?", host.ID).First(&retried).Error)
	require.Equal(t, uint64(6), retried.Revision)
}
