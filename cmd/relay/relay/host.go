package relay

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/cmd/relay/relay/models"

	"gorm.io/gorm"
)

func (r *Relay) GetHost(ctx context.Context, hostname string) (*models.Host, error) {
	ctx, span := tracer.Start(ctx, "getHost")
	defer span.End()

	var host models.Host
	if err := r.db.WithContext(ctx).Model(models.Host{}).Where("hostname = ?", hostname).First(&host).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrHostNotFound
		}
		return nil, err
	}

	// TODO: is this further check needed?
	if host.ID == 0 {
		return nil, ErrHostNotFound
	}

	return &host, nil
}

func (r *Relay) GetHostByID(ctx context.Context, hostID uint64) (*models.Host, error) {
	ctx, span := tracer.Start(ctx, "getHostByID")
	defer span.End()

	var host models.Host
	if err := r.db.WithContext(ctx).First(&host, hostID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrHostNotFound
		}
		return nil, err
	}

	// TODO: is this further check needed?
	if host.ID == 0 {
		return nil, ErrHostNotFound
	}

	return &host, nil
}

// If `everActive` flag is true, then only hosts which have ever had a message received (seq > 0) are returned.
func (r *Relay) ListHosts(ctx context.Context, cursor int64, limit int, everActive bool) ([]*models.Host, error) {

	hosts := []*models.Host{}

	// filters for accounts which have seen at least one event
	// TODO: also filter based on status?
	clause := "id > ?"
	if everActive {
		clause = "id > ? AND last_seq > 0"
	}

	if err := r.db.WithContext(ctx).Model(&models.Host{}).Where(clause, cursor).Order("id").Limit(limit).Find(&hosts).Error; err != nil {
		return nil, err
	}
	return hosts, nil
}

func (r *Relay) UpdateHostStatus(ctx context.Context, hostID uint64, status models.HostStatus) error {
	// hypercerts: Security bans also survive source-policy restarts.
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if status == models.HostStatusBanned {
			if err := tx.Model(&models.Source{}).Where("host_id = ? AND state = ?", hostID, models.SourceStateEnabled).
				Updates(map[string]any{"state": models.SourceStateDisabled, "revision": gorm.Expr("revision + 1"), "last_operation": "ban"}).Error; err != nil {
				return err
			}
		}
		return tx.Model(models.Host{}).Where("id = ?", hostID).Update("status", status).Error
	})
}

func (r *Relay) UpdateHostAccountLimit(ctx context.Context, hostID uint64, accountLimit int64) error {

	if accountLimit < 0 {
		return fmt.Errorf("negative account limit")
	}

	host, err := r.GetHostByID(ctx, hostID)
	if err != nil {
		return err
	}

	delta := accountLimit - host.AccountLimit
	r.Logger.Info("updating host account limit", "host", host.Hostname, "accountLimit", accountLimit, "previousAccountLimit", host.AccountLimit)

	if err := r.db.WithContext(ctx).Model(models.Host{}).Where("id = ?", hostID).Update("account_limit", accountLimit).Error; err != nil {
		return err
	}

	// manage accounts marked as "host-throttled" when host-level account limits change. Note that this isn't in a transaction: there is a small chance of race-conditions.
	if delta > 0 {
		// if limit increased: potentially mark some "host-throttled" accounts as "active" (ordered by UID ascending)
		// fetch accounts and update individually. this ensures that account cache is cleared for each (as well as any future code around account status changes)
		var accounts []models.Account
		if err := r.db.WithContext(ctx).Model(models.Account{}).Where("status = ? AND upstream_status = ? AND host_id = ?", models.AccountStatusHostThrottled, models.AccountStatusActive, host.ID).Order("uid ASC").Limit(int(delta)).Find(&accounts).Error; err != nil {
			return err
		}
		r.Logger.Info("marking host-throttled accounts as active", "count", len(accounts), "delta", delta, "accountLimit", accountLimit, "host", host.Hostname)
		for _, acc := range accounts {
			// defensive double-check
			if acc.Status != models.AccountStatusHostThrottled || acc.HostID != host.ID {
				continue
			}
			if err := r.UpdateAccountLocalStatus(ctx, syntax.DID(acc.DID), models.AccountStatusActive, true); err != nil {
				return err
			}
		}
	}
	// TODO: If limit decreased: potentially mark some "active" accounts as "host-throttled" (ordered by UID descending?)

	if r.Slurper.CheckIfSubscribed(host.Hostname) {
		return r.Slurper.UpdateLimiters(host.Hostname, accountLimit, host.Trusted)
	}

	return nil
}

// hypercerts: Save cursors atomically without changing source policy or reducing saved progress.
func (r *Relay) PersistHostCursors(ctx context.Context, cursors *[]HostCursor) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, cur := range *cursors {
			if cur.LastSeq <= 0 {
				continue
			}
			if err := tx.Model(models.Host{}).Where("id = ? AND last_seq < ?", cur.HostID, cur.LastSeq).UpdateColumn("last_seq", cur.LastSeq).Error; err != nil {
				return fmt.Errorf("persisting host cursor: %w", err)
			}
		}
		return nil
	})
}

// parses, normalizes, and validates a raw URL (HTTP or WebSocket) in to a hostname for subscriptions
//
// Hostnames must be DNS names, not IP addresses.
func ParseHostname(raw string) (hostname string, noSSL bool, err error) {

	// handle case of bare hostname
	if !strings.Contains(raw, "://") {
		if strings.HasPrefix(raw, "localhost:") {
			raw = "http://" + raw
		} else {
			raw = "https://" + raw
		}
	}

	u, err := url.Parse(raw)
	if err != nil {
		return "", false, fmt.Errorf("not a valid host URL: %w", err)
	}
	// hypercerts: Source admission accepts origins without credentials or request parameters.
	if u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return "", false, fmt.Errorf("host URL must contain only an origin")
	}
	noSSL = false

	switch u.Scheme {
	case "https", "wss":
		// pass
	case "http", "ws":
		noSSL = true
	default:
		return "", false, fmt.Errorf("unsupported URL scheme: %s", u.Scheme)
	}

	// 'localhost' (exact string) is allowed *with* a required port number; SSL is optional
	if u.Hostname() == "localhost" {
		if u.Port() == "" || !strings.HasPrefix(u.Host, "localhost:") {
			return "", false, fmt.Errorf("port number is required for localhost")
		}
		return u.Host, noSSL, nil
	}

	// port numbers not allowed otherwise
	if u.Port() != "" {
		return "", false, fmt.Errorf("port number not allowed for non-local names")
	}

	// check it is a real hostname (eg, not IP address or single-word alias)
	// TODO: more SSRF protection here? eg disallow '.local'
	h, err := syntax.ParseHandle(u.Host)
	if err != nil {
		return "", false, fmt.Errorf("not a public hostname")
	}
	// lower-case in response
	return h.Normalize().String(), noSSL, nil
}

func IsTrustedHostname(hostname string, domains []string) bool {
	for _, d := range domains {
		if hostname == d {
			return true
		}
		if strings.HasPrefix(d, "*") && strings.HasSuffix(hostname, d[1:]) {
			return true
		}
	}
	return false
}
