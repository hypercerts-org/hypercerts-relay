package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/cmd/relay/relay/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const sourceHandoffPath = "/hypercerts/v1/sources"

// enqueueSourceHandoff atomically replaces the pending desired-state delivery.
func enqueueSourceHandoff(tx *gorm.DB, host *models.Host, revision uint64, state models.SourceState) error {
	pds := sourceHostURL(host)
	var existing models.SourceHandoff
	if err := tx.Where("host_id = ?", host.ID).First(&existing).Error; err == nil && existing.SourceRevision == revision && existing.State == string(state) {
		return nil
	}
	row := models.SourceHandoff{HostID: host.ID, PDS: pds, SourceRevision: revision, State: string(state), NextRetryAt: time.Now().UTC()}
	return tx.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "host_id"}},
		DoUpdates: clause.Assignments(map[string]any{"pds": row.PDS, "source_revision": row.SourceRevision, "state": row.State,
			"attempts": 0, "next_retry_at": row.NextRetryAt, "updated_at": time.Now().UTC()}),
	}).Create(&row).Error
}

type sourceHandoffSender struct {
	url, token string
	client     *http.Client
}

func newSourceHandoffSender(targetURL, token string) *sourceHandoffSender {
	return &sourceHandoffSender{url: strings.TrimRight(targetURL, "/") + sourceHandoffPath, token: token, client: &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}

func normalizeHandoffState(state string) string {
	if state == string(models.SourceStateEnabled) {
		return string(models.SourceStateEnabled)
	}
	return string(models.SourceStateDisabled)
}

func (s *sourceHandoffSender) send(ctx context.Context, row models.SourceHandoff) error {
	payload, err := json.Marshal(struct {
		PDS            string `json:"pds"`
		SourceRevision uint64 `json:"sourceRevision"`
	}{row.PDS, row.SourceRevision})
	if err != nil {
		return err
	}
	method := http.MethodPost
	if normalizeHandoffState(row.State) != string(models.SourceStateEnabled) {
		method = http.MethodDelete
	}
	req, err := http.NewRequestWithContext(ctx, method, s.url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if normalizeHandoffState(row.State) == string(models.SourceStateEnabled) {
		if resp.StatusCode != http.StatusAccepted {
			return fmt.Errorf("source handoff returned HTTP %d, expected 202", resp.StatusCode)
		}
		var ack struct {
			PDS            string `json:"pds"`
			SourceRevision uint64 `json:"sourceRevision"`
			State          string `json:"state"`
			PolicyRevision uint64 `json:"policyRevision"`
			Policy         struct {
				Revision uint64 `json:"revision"`
			} `json:"policy"`
			JobID  string `json:"id"`
			Reason string `json:"reason"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&ack); err != nil {
			return fmt.Errorf("decode source handoff acknowledgment: %w", err)
		}
		if ack.PolicyRevision == 0 {
			ack.PolicyRevision = ack.Policy.Revision
		}
		switch ack.State {
		case "running", "complete", "canceled", "incomplete", "pending", "failed":
		default:
			return fmt.Errorf("source handoff acknowledgment has invalid job state")
		}
		if ack.PDS != row.PDS || ack.SourceRevision != row.SourceRevision || ack.PolicyRevision == 0 || ack.JobID == "" || ack.Reason != "source_added" {
			return fmt.Errorf("source handoff acknowledgment does not match accepted PDS and lifecycle coordinate")
		}
		return nil
	}
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("source handoff returned HTTP %d, expected 204", resp.StatusCode)
	}
	return nil
}

// ProcessSourceHandoffs sends due desired state without holding the source lock during network IO.
func (r *Relay) ProcessSourceHandoffs(ctx context.Context) error {
	if r.Config.JetstreamControlURL == "" || r.Config.JetstreamControlToken == "" {
		return nil
	}
	var due []models.SourceHandoff
	if err := r.db.WithContext(ctx).Order("next_retry_at, host_id").Limit(100).Find(&due).Error; err != nil {
		return err
	}
	sender := newSourceHandoffSender(r.Config.JetstreamControlURL, r.Config.JetstreamControlToken)
	for _, queued := range due {
		if err := ctx.Err(); err != nil {
			return err
		}
		r.sourcesLk.Lock()
		var current models.SourceHandoff
		err := r.db.WithContext(ctx).Where("host_id = ? AND source_revision = ? AND state = ? AND attempts = ?", queued.HostID, queued.SourceRevision, queued.State, queued.Attempts).First(&current).Error
		if err == nil {
			var source models.Source
			var host models.Host
			sourceErr := r.db.WithContext(ctx).First(&source, "host_id = ?", current.HostID).Error
			hostErr := r.db.WithContext(ctx).First(&host, current.HostID).Error
			if errors.Is(sourceErr, gorm.ErrRecordNotFound) {
				// Legacy rows have no managed source record to compare.
			} else if sourceErr != nil {
				err = sourceErr
			} else if hostErr != nil {
				err = hostErr
			} else {
				effectiveState := models.SourceStateDisabled
				if source.State == models.SourceStateEnabled && source.ValidationStatus == models.SourceValidationPassed && host.Status != models.HostStatusBanned {
					effectiveState = models.SourceStateEnabled
				}
				if source.Revision != current.SourceRevision || normalizeHandoffState(string(effectiveState)) != normalizeHandoffState(current.State) {
					_ = r.db.WithContext(ctx).Delete(&models.SourceHandoff{}, "host_id = ? AND source_revision = ?", current.HostID, current.SourceRevision).Error
					err = gorm.ErrRecordNotFound
				}
			}
		}
		if err == nil && current.NextRetryAt.After(time.Now().UTC()) {
			r.sourcesLk.Unlock()
			continue
		}
		r.sourcesLk.Unlock()
		if err != nil {
			if err == gorm.ErrRecordNotFound {
				continue
			}
			return err
		}
		sendErr := sender.send(ctx, current)
		r.sourcesLk.Lock()
		query := r.db.WithContext(ctx).Model(&models.SourceHandoff{}).Where("host_id = ? AND source_revision = ? AND state = ? AND attempts = ?", current.HostID, current.SourceRevision, current.State, current.Attempts)
		if sendErr == nil {
			var latest models.Source
			var host models.Host
			sourceErr := r.db.WithContext(ctx).First(&latest, "host_id = ?", current.HostID).Error
			hostErr := r.db.WithContext(ctx).First(&host, current.HostID).Error
			if errors.Is(sourceErr, gorm.ErrRecordNotFound) {
				// Pre-managed sources do not have an authoritative source row.
			} else if sourceErr != nil {
				r.sourcesLk.Unlock()
				return sourceErr
			} else if hostErr != nil {
				r.sourcesLk.Unlock()
				return hostErr
			} else {
				effectiveState := models.SourceStateDisabled
				if latest.State == models.SourceStateEnabled && latest.ValidationStatus == models.SourceValidationPassed && host.Status != models.HostStatusBanned {
					effectiveState = models.SourceStateEnabled
				}
				if latest.Revision != current.SourceRevision || normalizeHandoffState(string(effectiveState)) != normalizeHandoffState(current.State) {
					sendErr = fmt.Errorf("source handoff superseded during delivery")
				}
			}
		}
		if sendErr == nil {
			if err := query.Delete(&models.SourceHandoff{}).Error; err != nil {
				r.sourcesLk.Unlock()
				return err
			}
		} else {
			attempts := current.Attempts + 1
			shift := attempts - 1
			if shift > 10 {
				shift = 10
			}
			delay := time.Second * time.Duration(1<<shift)
			if delay > 15*time.Minute {
				delay = 15 * time.Minute
			}
			if err := query.Updates(map[string]any{"attempts": attempts, "next_retry_at": time.Now().UTC().Add(delay)}).Error; err != nil {
				r.sourcesLk.Unlock()
				return err
			}
		}
		r.sourcesLk.Unlock()
	}
	return nil
}

func (r *Relay) RunSourceHandoffWorker(ctx context.Context) {
	if r.Config.JetstreamControlURL == "" || r.Config.JetstreamControlToken == "" {
		return
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		_ = r.ProcessSourceHandoffs(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
