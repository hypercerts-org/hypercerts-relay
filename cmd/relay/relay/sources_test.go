package relay

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/cmd/relay/relay/models"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type sourceTestDB struct {
	*gorm.DB
}

func (db sourceTestDB) Create(value interface{}) error {
	return db.DB.Create(value).Error
}

func newSourceRelay(t *testing.T) (*Relay, sourceTestDB) {
	t.Helper()
	r, db := testRelayWithHostDB(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	require.NoError(t, db.AutoMigrate(
		&models.DomainBan{},
		&models.Source{},
		&models.Account{},
		&models.AccountSourceObservation{},
	))
	r.Config = *DefaultRelayConfig()
	r.HostChecker = NewMockHostChecker()
	return r, sourceTestDB{DB: db}
}

func TestListSourcesIncludesQuietHostsAndBoundsPage(t *testing.T) {
	ctx := context.Background()
	r, db := newSourceRelay(t)

	for i := 0; i <= maxSourcePageLimit; i++ {
		host := &models.Host{Hostname: fmt.Sprintf("quiet-%04d.example.com", i), LastSeq: -1}
		require.NoError(t, db.Create(host))
		require.NoError(t, db.Create(&models.Source{
			HostID:           host.ID,
			State:            models.SourceStateEnabled,
			Revision:         1,
			ValidationStatus: models.SourceValidationPending,
			RecoveryRequired: true,
		}))
	}

	page, err := r.ListSources(ctx, 0, maxSourcePageLimit+1)
	require.NoError(t, err)
	require.Len(t, page.Sources, maxSourcePageLimit)
	require.NotZero(t, page.NextAfterHostID)
	require.Equal(t, int64(-1), page.Sources[0].LastDurableCursor)

	next, err := r.ListSources(ctx, page.NextAfterHostID, maxSourcePageLimit+1)
	require.NoError(t, err)
	require.Len(t, next.Sources, 1)
	require.Zero(t, next.NextAfterHostID)
}

func TestSetSourceStateUsesRevisionAndPreservesHost(t *testing.T) {
	ctx := context.Background()
	r, db := newSourceRelay(t)
	host := &models.Host{Hostname: "state.example.com", Status: models.HostStatusActive, LastSeq: 42}
	require.NoError(t, db.Create(host))
	require.NoError(t, db.Create(&models.Source{
		HostID:           host.ID,
		State:            models.SourceStateEnabled,
		Revision:         1,
		ValidationStatus: models.SourceValidationPassed,
		RecoveryRequired: true,
	}))

	disabled, err := r.SetSourceState(ctx, host.ID, 1, models.SourceStateDisabled)
	require.NoError(t, err)
	require.Equal(t, uint64(2), disabled.Revision)
	require.Equal(t, models.SourceStateDisabled, disabled.DesiredState)

	duplicate, err := r.SetSourceState(ctx, host.ID, 1, models.SourceStateDisabled)
	require.NoError(t, err)
	require.Equal(t, disabled.Revision, duplicate.Revision)

	_, err = r.SetSourceState(ctx, host.ID, 1, models.SourceStateRemoved)
	require.ErrorIs(t, err, ErrSourceRevisionConflict)

	removed, err := r.SetSourceState(ctx, host.ID, 2, models.SourceStateRemoved)
	require.NoError(t, err)
	require.Equal(t, models.SourceStateRemoved, removed.DesiredState)

	var storedHost models.Host
	require.NoError(t, db.Where("id = ?", host.ID).First(&storedHost).Error)
	require.Equal(t, int64(42), storedHost.LastSeq)
	var storedSource models.Source
	require.NoError(t, db.Where("host_id = ?", host.ID).First(&storedSource).Error)
	require.Equal(t, models.SourceStateRemoved, storedSource.State)
}

func TestAddSourceIsIdempotentWithoutTLSDowngrade(t *testing.T) {
	ctx := context.Background()
	r, db := newSourceRelay(t)

	first, err := r.AddSource(ctx, "https://source.example.com")
	require.NoError(t, err)
	second, err := r.AddSource(ctx, "http://source.example.com")
	require.NoError(t, err)
	require.Equal(t, first.HostID, second.HostID)
	require.Equal(t, uint64(1), second.Revision)
	require.False(t, second.NoSSL)

	var hosts, sources int64
	require.NoError(t, db.Model(&models.Host{}).Count(&hosts).Error)
	require.NoError(t, db.Model(&models.Source{}).Count(&sources).Error)
	require.Equal(t, int64(1), hosts)
	require.Equal(t, int64(1), sources)
}

func TestValidateSourcePersistsSafeFailure(t *testing.T) {
	ctx := context.Background()
	r, db := newSourceRelay(t)
	host := &models.Host{Hostname: "invalid.example.com"}
	require.NoError(t, db.Create(host))
	require.NoError(t, db.Create(&models.Source{
		HostID:           host.ID,
		State:            models.SourceStateEnabled,
		Revision:         1,
		ValidationStatus: models.SourceValidationPending,
		RecoveryRequired: true,
	}))

	view, err := r.ValidateSource(ctx, host.ID, 1)
	require.ErrorIs(t, err, ErrSourceValidationFailed)
	require.Equal(t, models.SourceValidationFailed, view.Validation.Status)
	require.Equal(t, models.SourceValidationReasonHostCheckFailed, view.Validation.Reason)
	require.NotNil(t, view.Validation.CheckedAt)
	require.Equal(t, uint64(2), view.Revision)

	retry, err := r.ValidateSource(ctx, host.ID, 1)
	require.ErrorIs(t, err, ErrSourceValidationFailed)
	require.Equal(t, view.Revision, retry.Revision)
}

func TestObserveAccountSourceKeepsUnadmittedTargetSeparate(t *testing.T) {
	ctx := context.Background()
	r, db := newSourceRelay(t)
	host := &models.Host{Hostname: "former.example.com", AccountLimit: 1, AccountCount: 1}
	require.NoError(t, db.Create(host))
	require.NoError(t, db.Create(&models.Source{
		HostID:           host.ID,
		State:            models.SourceStateEnabled,
		Revision:         1,
		ValidationStatus: models.SourceValidationPassed,
		RecoveryRequired: true,
	}))
	did := "did:plc:abcdefghijklmnopqrstuvwx"
	require.NoError(t, db.Create(&models.Account{
		DID:            did,
		HostID:         host.ID,
		Status:         models.AccountStatusHostThrottled,
		UpstreamStatus: models.AccountStatusActive,
	}))

	require.NoError(t, r.ObserveAccountSource(ctx, did, host.ID, "https://new-pds.example.com"))
	page, err := r.ListSourceAccounts(ctx, host.ID, "", 1)
	require.NoError(t, err)
	require.Len(t, page.Accounts, 1)
	observation := page.Accounts[0]
	require.Equal(t, "new-pds.example.com", observation.ResolvedHostname)
	require.Equal(t, admissionReasonHostAccountLimit, observation.AdmissionReason)
	require.True(t, observation.RecoveryRequired)
	require.Equal(t, host.ID, observation.CurrentHostID)

	var targets int64
	require.NoError(t, db.Model(&models.Host{}).Where("hostname = ?", "new-pds.example.com").Count(&targets).Error)
	require.Zero(t, targets)
}

func TestListSourceAccountsBoundsThrottledAdmissions(t *testing.T) {
	ctx := context.Background()
	r, db := newSourceRelay(t)
	host := &models.Host{Hostname: "admission.example.com"}
	require.NoError(t, db.Create(host))
	require.NoError(t, db.Create(&models.Source{HostID: host.ID, State: models.SourceStateEnabled, Revision: 1, RecoveryRequired: true}))

	for i := 0; i <= maxSourcePageLimit; i++ {
		did := fmt.Sprintf("did:plc:%024d", i)
		require.NoError(t, db.Create(&models.Account{
			DID:            did,
			HostID:         host.ID,
			Status:         models.AccountStatusHostThrottled,
			UpstreamStatus: models.AccountStatusActive,
		}))
		require.NoError(t, db.Create(&models.AccountSourceObservation{
			DID:              did,
			ObservedHostID:   host.ID,
			ResolvedHostname: "unadmitted.example.com",
			ObservedAt:       timeNow(),
			AdmissionReason:  admissionReasonHostAccountLimit,
		}))
	}

	page, err := r.ListSourceAccounts(ctx, host.ID, "", maxSourcePageLimit+1)
	require.NoError(t, err)
	require.Len(t, page.Accounts, maxSourcePageLimit)
	require.NotEmpty(t, page.NextAfterDID)
	require.Equal(t, admissionReasonHostAccountLimit, page.Accounts[0].AdmissionReason)
}

func timeNow() time.Time {
	return time.Now().UTC()
}
