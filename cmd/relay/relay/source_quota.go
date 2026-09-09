package relay

import (
	"context"
	"errors"
	"fmt"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/cmd/relay/relay/models"
	"github.com/bluesky-social/indigo/cmd/relay/stream"
	"gorm.io/gorm"
)

var ErrInvalidAccountQuota = errors.New("account quota must be a nonnegative safe integer")

// SetSourceAccountQuota changes an existing source's account admission limit.
// Repeated values are safe; a stale form cannot replace a different current limit.
func (r *Relay) SetSourceAccountQuota(ctx context.Context, rawURL string, expected, limit int64) (*SourceView, error) {
	const maxSafeInteger = 9_007_199_254_740_991
	if expected < 0 || limit < 0 || expected > maxSafeInteger || limit > maxSafeInteger {
		return nil, ErrInvalidAccountQuota
	}
	hostname, noSSL, err := ParseHostname(rawURL)
	if err != nil {
		return nil, ErrInvalidSourceURL
	}
	r.sourcesLk.Lock()
	defer r.sourcesLk.Unlock()
	var host models.Host
	if err := r.db.WithContext(ctx).Where("hostname = ?", hostname).First(&host).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrSourceNotFound
		}
		return nil, err
	}
	source, _, err := r.sourceAndHostLocked(ctx, host.ID)
	if err != nil {
		return nil, err
	}
	if host.NoSSL != noSSL {
		return nil, ErrInvalidSourceURL
	}
	if host.AccountLimit != expected && host.AccountLimit != limit {
		return nil, ErrSourceRevisionConflict
	}
	if err := r.UpdateHostAccountLimit(ctx, host.ID, limit); err != nil {
		return nil, err
	}
	host.AccountLimit = limit
	return r.sourceViewLocked(ctx, source, &host)
}

func (r *Relay) reactivateQuotaAccounts(ctx context.Context, hostID uint64, limit int64) error {
	var reserved int64
	// Accounts not eligible for quota recovery still consume admission capacity.
	if err := r.db.WithContext(ctx).Model(&models.Account{}).
		Where("host_id = ? AND (status <> ? OR upstream_status <> ?)", hostID, models.AccountStatusHostThrottled, models.AccountStatusActive).
		Count(&reserved).Error; err != nil {
		return err
	}
	available := limit - reserved
	if available <= 0 {
		return nil
	}
	var accounts []models.Account
	if err := r.db.WithContext(ctx).Where("host_id = ? AND status = ? AND upstream_status = ?", hostID, models.AccountStatusHostThrottled, models.AccountStatusActive).
		Order("uid ASC").Limit(int(available)).Find(&accounts).Error; err != nil {
		return err
	}
	for _, account := range accounts {
		if err := r.reactivateQuotaAccount(ctx, hostID, account.DID); err != nil {
			return err
		}
	}
	return nil
}

func (r *Relay) reactivateQuotaAccount(ctx context.Context, hostID uint64, did string) error {
	lock := r.accountEventLock(did)
	lock.Lock()
	defer lock.Unlock()
	var account models.Account
	if err := r.db.WithContext(ctx).Where("did = ?", did).First(&account).Error; err != nil {
		return err
	}
	if account.HostID != hostID || account.Status != models.AccountStatusHostThrottled || account.UpstreamStatus != models.AccountStatusActive {
		return nil
	}
	// Emit before changing eligibility, outside any metadata transaction (the
	// event persister uses this database too). A failed write remains retryable;
	// a later metadata failure may replay an idempotent active marker on retry.
	if err := r.Events.AddEvent(ctx, &stream.XRPCStreamEvent{
		RepoAccount: &comatproto.SyncSubscribeRepos_Account{Active: true, Did: did, Time: syntax.DatetimeNow().String()},
		PrivUid:     account.UID,
	}); err != nil {
		return err
	}
	result := r.db.WithContext(ctx).Model(&models.Account{}).
		Where("uid = ? AND host_id = ? AND status = ? AND upstream_status = ?", account.UID, hostID, models.AccountStatusHostThrottled, models.AccountStatusActive).
		Update("status", models.AccountStatusActive)
	if r.accountCache != nil {
		r.accountCache.Remove(did)
	}
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("quota recovery account changed during activation")
	}
	return nil
}
