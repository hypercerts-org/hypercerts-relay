package relay

import (
	"context"
	"time"

	"github.com/bluesky-social/indigo/cmd/relay/relay/models"
	"gorm.io/gorm"
)

const publicValidationCooldown = time.Minute

// hypercerts: Public admission validates synchronously; disabled policy stays authoritative.
func (r *Relay) SubscribeToHost(ctx context.Context, hostname string, noSSL, adminForce bool) error {
	origin, err := r.sourceURL(hostname, noSSL)
	if err != nil {
		return err
	}
	if adminForce {
		source, err := r.AddSource(ctx, origin)
		if err != nil {
			return err
		}
		if source.DesiredState != models.SourceStateEnabled {
			return ErrSourceDisabled
		}
		if source.Validation.Status != models.SourceValidationPassed {
			validated, err := r.ValidateSource(ctx, source.HostID, source.Revision)
			if err != nil {
				return err
			}
			if validated.DesiredState != models.SourceStateEnabled {
				return ErrSourceDisabled
			}
			if validated.Validation.Status != models.SourceValidationPassed {
				return ErrSourceValidationFailed
			}
		}
		return nil
	}
	if _, err := r.AddPublicSource(ctx, origin); err != nil {
		return err
	}
	return r.AdmitPublicSource(ctx, origin)
}

// AdmitPublicSource validates outside sourcesLk so unrelated policy changes are not blocked by network IO.
func (r *Relay) AdmitPublicSource(ctx context.Context, rawURL string) error {
	hostname, noSSL, err := ParseHostname(rawURL)
	if err != nil {
		return ErrInvalidSourceURL
	}
	if _, err := r.sourceURL(hostname, noSSL); err != nil {
		return err
	}
	source, host, err := r.preparePublicSourceCheck(ctx, hostname)
	if err != nil || source == nil {
		return err
	}
	revision, scheme := source.Revision, host.NoSSL
	defer r.endSourceCheck(host.ID)

	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	checkErr := error(nil)
	checkerMissing := r.HostChecker == nil
	if checkerMissing {
		checkErr = ErrSourceValidationFailed
	} else {
		origin, err := r.sourceURL(hostname, scheme)
		if err != nil {
			checkErr = err
		} else {
			checkErr = r.HostChecker.CheckHost(checkCtx, origin)
		}
	}

	return r.finalizePublicSourceCheck(host.ID, hostname, revision, scheme, checkErr, checkerMissing)
}

func (r *Relay) preparePublicSourceCheck(ctx context.Context, hostname string) (*models.Source, *models.Host, error) {
	r.sourcesLk.Lock()
	defer r.sourcesLk.Unlock()
	banned, err := r.sourceDomainIsBanned(ctx, hostname)
	if err != nil {
		return nil, nil, err
	}
	if banned {
		return nil, nil, ErrSourceDomainBanned
	}
	host, err := r.GetHost(ctx, hostname)
	if err != nil {
		return nil, nil, err
	}
	source, host, err := r.sourceAndHostLocked(ctx, host.ID)
	if err != nil {
		return nil, nil, err
	}
	if source.State != models.SourceStateEnabled {
		return nil, nil, ErrSourceDisabled
	}
	if source.ValidationStatus == models.SourceValidationPassed {
		err = r.reconcileSourceLocked(ctx, source, host)
		return nil, nil, err
	}
	if source.ValidationStatus == models.SourceValidationFailed && source.ValidationCheckedAt != nil && time.Since(*source.ValidationCheckedAt) < publicValidationCooldown {
		return nil, nil, ErrSourceValidationFailed
	}
	if err := r.beginSourceCheck(host.ID); err != nil {
		return nil, nil, err
	}
	return source, host, nil
}

func (r *Relay) finalizePublicSourceCheck(hostID uint64, hostname string, revision uint64, scheme bool, checkErr error, checkerMissing bool) error {
	finalizeCtx, finalizeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer finalizeCancel()
	r.sourcesLk.Lock()
	defer r.sourcesLk.Unlock()
	banned, err := r.sourceDomainIsBanned(finalizeCtx, hostname)
	if err != nil {
		return err
	}
	if banned {
		return ErrSourceDomainBanned
	}
	source, host, err := r.sourceAndHostLocked(finalizeCtx, hostID)
	if err != nil {
		return err
	}
	if source.Revision != revision || source.State != models.SourceStateEnabled || host.NoSSL != scheme {
		return ErrSourceRevisionConflict
	}
	status, reason := models.SourceValidationPassed, models.SourceValidationReasonNone
	if checkErr != nil {
		status, reason = models.SourceValidationFailed, models.SourceValidationReasonHostCheckFailed
		if checkerMissing {
			reason = models.SourceValidationReasonCheckerMissing
		}
	}
	now := time.Now().UTC()
	updatedRevision := revision + 1
	err = r.db.WithContext(finalizeCtx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&models.Source{}).Where("host_id = ? AND revision = ? AND state = ?", source.HostID, revision, models.SourceStateEnabled).Updates(map[string]any{
			"validation_status": status, "validation_checked_at": now, "validation_reason": reason,
			"revision": updatedRevision, "last_operation": "validate", "recovery_required": true,
		})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrSourceRevisionConflict
		}
		if status == models.SourceValidationPassed {
			return enqueueSourceHandoff(tx, host, updatedRevision, models.SourceStateEnabled)
		}
		return enqueueSourceHandoff(tx, host, updatedRevision, models.SourceStateDisabled)
	})
	if err != nil {
		return err
	}
	source.ValidationStatus, source.ValidationCheckedAt, source.ValidationReason = status, &now, reason
	source.Revision, source.LastOperation, source.RecoveryRequired = updatedRevision, "validate", true
	if err := r.reconcileSourceLocked(finalizeCtx, source, host); err != nil {
		return err
	}
	if checkErr != nil {
		return ErrSourceValidationFailed
	}
	return nil
}

// ResubscribeAllHosts reconnects durable enabled sources from their saved cursors.
func (r *Relay) ResubscribeAllHosts(ctx context.Context) error {
	if err := r.requireRateAdmission(ctx); err != nil {
		return err
	}
	var after uint64
	for {
		var sources []models.Source
		if err := r.db.WithContext(ctx).Where("host_id > ?", after).Order("host_id").Limit(defaultSourcePageLimit).Find(&sources).Error; err != nil {
			return err
		}
		if len(sources) == 0 {
			return nil
		}
		for _, row := range sources {
			r.sourcesLk.Lock()
			source, host, err := r.sourceAndHostLocked(ctx, row.HostID)
			if err == nil {
				err = r.reconcileSourceLocked(ctx, source, host)
			}
			r.sourcesLk.Unlock()
			if err != nil {
				return err
			}
			after = row.HostID
		}
	}
}
