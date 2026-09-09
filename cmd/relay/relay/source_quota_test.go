package relay

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/bluesky-social/indigo/cmd/relay/relay/models"
	"github.com/bluesky-social/indigo/cmd/relay/stream/eventmgr"
	"github.com/stretchr/testify/require"
)

func TestSourceAccountQuotaUpdatesRetryAndConflicts(t *testing.T) {
	ctx := context.Background()
	r, _ := newSourceRelay(t)
	r.Logger = slog.Default()
	view, err := r.AddSource(ctx, "https://quota.example")
	require.NoError(t, err)
	original := view.AccountQuota.Limit
	for _, requested := range []int64{original + 10, 0, 50} {
		updated, err := r.SetSourceAccountQuota(ctx, "https://quota.example", original, requested)
		require.NoError(t, err)
		require.Equal(t, requested, updated.AccountQuota.Limit)
		require.Equal(t, view.Revision, updated.Revision)
		require.Equal(t, models.SourceStateEnabled, updated.DesiredState)
		_, err = r.SetSourceAccountQuota(ctx, "https://quota.example", original, requested)
		require.NoError(t, err, "retry must acknowledge the same value")
		original = requested
	}
	_, err = r.SetSourceAccountQuota(ctx, "https://quota.example", 0, 25)
	require.ErrorIs(t, err, ErrSourceRevisionConflict)
	for _, invalid := range []int64{-1, 9_007_199_254_740_992} {
		_, err = r.SetSourceAccountQuota(ctx, "https://quota.example", original, invalid)
		require.ErrorIs(t, err, ErrInvalidAccountQuota)
	}
	_, err = r.SetSourceAccountQuota(ctx, "https://unknown.example", 0, 25)
	require.ErrorIs(t, err, ErrSourceNotFound)
	observed, err := r.InspectSource(ctx, "https://quota.example")
	require.NoError(t, err)
	require.Equal(t, int64(50), observed.AccountQuota.Limit)
}

func TestQuotaRetryFinishesActivationAfterPersistenceFailures(t *testing.T) {
	for _, failure := range []string{"event", "metadata"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			r, db := newSourceRelay(t)
			r.Logger = slog.Default()
			view, err := r.AddSource(ctx, "https://quota.example")
			require.NoError(t, err)
			require.NoError(t, db.Model(&models.Host{}).Where("id = ?", view.HostID).Updates(map[string]any{"account_limit": 0, "account_count": 1}).Error)
			account := &models.Account{DID: "did:plc:abcdefghijklmnopqrstuvwx", HostID: view.HostID, Status: models.AccountStatusHostThrottled, UpstreamStatus: models.AccountStatusActive}
			require.NoError(t, db.Create(account))
			persist := &testEventPersistence{}
			r.Events = eventmgr.NewEventManager(persist)
			if failure == "event" {
				persist.persistErr = errors.New("fixture event failure")
			} else {
				require.NoError(t, db.Exec("CREATE TRIGGER fail_activation BEFORE UPDATE OF status ON account BEGIN SELECT RAISE(FAIL, 'fixture metadata failure'); END").Error)
			}
			_, err = r.SetSourceAccountQuota(ctx, "https://quota.example", 0, 1)
			require.Error(t, err)
			observed, err := r.InspectSource(ctx, "https://quota.example")
			require.NoError(t, err)
			require.Equal(t, int64(1), observed.AccountQuota.Limit)
			require.NoError(t, db.First(account, account.UID).Error)
			require.Equal(t, models.AccountStatusHostThrottled, account.Status)
			persist.persistErr = nil
			if failure == "metadata" {
				require.NoError(t, db.Exec("DROP TRIGGER fail_activation").Error)
			}
			_, err = r.SetSourceAccountQuota(ctx, "https://quota.example", 0, 1)
			require.NoError(t, err)
			require.NoError(t, db.First(account, account.UID).Error)
			require.Equal(t, models.AccountStatusActive, account.Status)
			require.NotEmpty(t, persist.events)
			require.True(t, persist.events[len(persist.events)-1].RepoAccount.Active)
			count := len(persist.events)
			_, err = r.SetSourceAccountQuota(ctx, "https://quota.example", 0, 1)
			require.NoError(t, err)
			require.Len(t, persist.events, count, "completed retries must not emit more markers")
		})
	}
}
