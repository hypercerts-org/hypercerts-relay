package relay

import (
	"context"

	"github.com/bluesky-social/indigo/cmd/relay/relay/models"
)

// hypercerts: Only administrative admission can create a managed source.
func (r *Relay) SubscribeToHost(ctx context.Context, hostname string, noSSL, adminForce bool) error {
	if adminForce {
		url := "https://" + hostname
		if noSSL {
			url = "http://" + hostname
		}
		source, err := r.AddSource(ctx, url)
		if err != nil {
			return err
		}
		if source.DesiredState != models.SourceStateEnabled {
			return ErrSourceDisabled
		}
		if source.Validation.Status != models.SourceValidationPassed {
			_, err = r.ValidateSource(ctx, source.HostID, source.Revision)
			return err
		}
		return nil
	}
	r.sourcesLk.Lock()
	defer r.sourcesLk.Unlock()
	host, err := r.GetHost(ctx, hostname)
	if err != nil {
		return err
	}
	source, host, err := r.sourceAndHostLocked(ctx, host.ID)
	if err != nil {
		return err
	}
	if source.State != models.SourceStateEnabled {
		return ErrSourceDisabled
	}
	if source.ValidationStatus != models.SourceValidationPassed {
		return ErrSourceValidationFailed
	}
	return r.reconcileSourceLocked(ctx, source, host)
}

// hypercerts: Restart from durable policy, including quiet and previously unavailable sources.
func (r *Relay) ResubscribeAllHosts(ctx context.Context) error {
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
