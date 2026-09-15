package relay

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/cmd/relay/relay/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrSourceNotFound         = errors.New("unknown managed source")
	ErrSourceRevisionConflict = errors.New("source revision conflict")
	ErrInvalidSourceState     = errors.New("invalid source state")
	ErrInvalidSourceURL       = errors.New("invalid source URL")
	ErrSourceDomainBanned     = errors.New("source hostname is banned")
	ErrSourceValidationFailed = errors.New("source validation failed")
	ErrSourceDisabled         = errors.New("source acquisition is disabled")
)

const (
	sourceHostIDPredicate = "host_id = ?"

	defaultSourcePageLimit = 100
	maxSourcePageLimit     = 1_000

	admissionReasonHostAccountLimit = "host-account-limit"
)

// SourceValidation is the latest explicit admission check. Reason is always a
// fixed code and never an upstream error string.
type SourceValidation struct {
	Status    models.SourceValidationStatus
	CheckedAt *time.Time
	Reason    models.SourceValidationReason
}

// SourceAccountQuota reports admission capacity only. It deliberately does not
// report or derive stream limiter values.
type SourceAccountQuota struct {
	Limit int64
	Count int64
}

// SourceView separates durable desired source state from the legacy Host.Status
// and the current Slurper connection state.
type SourceView struct {
	HostID       uint64
	Hostname     string
	NoSSL        bool
	DesiredState models.SourceState
	Revision     uint64

	Validation       SourceValidation
	RecoveryRequired bool

	HostStatus        models.HostStatus
	RuntimeState      string
	LastDurableCursor int64
	AccountQuota      SourceAccountQuota
}

type SourcePage struct {
	Sources         []*SourceView
	NextAfterHostID uint64
}

// SourceAccountView keeps historical source observation distinct from current
// account placement. A zero CurrentHostID means no current account row exists.
type SourceAccountView struct {
	DID                    string
	ObservedSourceHostID   uint64
	ObservedSourceHostname string
	ResolvedHostname       string
	ObservedAt             time.Time
	ResolvedAt             time.Time
	TargetCoverage         string
	AdmissionReason        string

	CurrentHostID         uint64
	CurrentHostname       string
	CurrentStatus         models.AccountStatus
	CurrentUpstreamStatus models.AccountStatus

	RecoveryRequired bool
}

type SourceAccountsPage struct {
	Accounts     []*SourceAccountView
	NextAfterDID string
}

// AddSource records a new administrative source without retaining raw URL
// material. A repeat for the same normalized hostname is a no-op that still
// reconciles its desired socket state.
func (r *Relay) AddSource(ctx context.Context, rawURL string) (*SourceView, error) {
	hostname, noSSL, err := ParseHostname(rawURL)
	if err != nil {
		return nil, ErrInvalidSourceURL
	}

	r.sourcesLk.Lock()
	defer r.sourcesLk.Unlock()

	banned, err := r.sourceDomainIsBanned(ctx, hostname)
	if err != nil {
		return nil, fmt.Errorf("checking source domain: %w", err)
	}
	if banned {
		return nil, ErrSourceDomainBanned
	}

	var host models.Host
	var source models.Source
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := r.ensureSourceHost(tx, &host, hostname, noSSL); err != nil {
			return err
		}

		err := tx.Where(sourceHostIDPredicate, host.ID).First(&source).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("loading source: %w", err)
		}
		if errors.Is(err, gorm.ErrRecordNotFound) {
			state := models.SourceStateEnabled
			if host.Status == models.HostStatusBanned {
				state = models.SourceStateDisabled
			}
			source = models.Source{
				HostID:           host.ID,
				State:            state,
				Revision:         1,
				ValidationStatus: models.SourceValidationPending,
				ValidationReason: models.SourceValidationReasonNone,
				RecoveryRequired: true,
				LastOperation:    "add",
			}
			if err := tx.Create(&source).Error; err != nil {
				return fmt.Errorf("creating source: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	view, viewErr := r.sourceViewLocked(ctx, &source, &host)
	if viewErr != nil {
		return nil, viewErr
	}
	if err := r.reconcileSourceLocked(ctx, &source, &host); err != nil {
		return view, err
	}
	return r.sourceViewLocked(ctx, &source, &host)
}

// ensureSourceHost loads or creates the host within the caller's source transaction.
func (r *Relay) ensureSourceHost(tx *gorm.DB, host *models.Host, hostname string, noSSL bool) error {
	err := tx.Where("hostname = ?", hostname).First(host).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("loading source host: %w", err)
	}

	if errors.Is(err, gorm.ErrRecordNotFound) {
		*host = models.Host{
			Hostname:     hostname,
			NoSSL:        noSSL,
			Status:       models.HostStatusActive,
			Trusted:      IsTrustedHostname(hostname, r.Config.TrustedDomains),
			AccountLimit: r.Config.DefaultRepoLimit,
		}
		if host.Trusted {
			host.AccountLimit = r.Config.TrustedRepoLimit
		}
		if err := tx.Create(host).Error; err != nil {
			return fmt.Errorf("creating source host: %w", err)
		}
	}
	return nil
}

// ValidateSource performs one explicit host check against the stored scheme and
// hostname. It never retries with an insecure scheme and persists only a stable
// failure code.
func (r *Relay) ValidateSource(ctx context.Context, hostID, expectedRevision uint64) (*SourceView, error) {
	r.sourcesLk.Lock()
	defer r.sourcesLk.Unlock()

	source, host, err := r.sourceAndHostLocked(ctx, hostID)
	if err != nil {
		return nil, err
	}
	if source.Revision != expectedRevision {
		if source.Revision == expectedRevision+1 && source.LastOperation == "validate" {
			if err := r.reconcileSourceLocked(ctx, source, host); err != nil {
				return nil, err
			}
			view, err := r.sourceViewLocked(ctx, source, host)
			if err != nil {
				return nil, err
			}
			if source.ValidationStatus == models.SourceValidationFailed {
				return view, ErrSourceValidationFailed
			}
			return view, nil
		}
		return nil, ErrSourceRevisionConflict
	}

	status := models.SourceValidationPassed
	reason := models.SourceValidationReasonNone
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if r.HostChecker == nil {
		status = models.SourceValidationFailed
		reason = models.SourceValidationReasonCheckerMissing
	} else if err := r.HostChecker.CheckHost(checkCtx, sourceHostURL(host)); err != nil {
		status = models.SourceValidationFailed
		reason = models.SourceValidationReasonHostCheckFailed
	}

	now := time.Now().UTC()
	updatedRevision := source.Revision + 1
	result := r.db.WithContext(ctx).Model(&models.Source{}).
		Where("host_id = ? AND revision = ?", source.HostID, source.Revision).
		Updates(map[string]interface{}{
			"validation_status":     status,
			"validation_checked_at": now,
			"validation_reason":     reason,
			"revision":              updatedRevision,
			"last_operation":        "validate",
		})
	if result.Error != nil {
		return nil, fmt.Errorf("saving source validation: %w", result.Error)
	}
	if result.RowsAffected != 1 {
		return nil, ErrSourceRevisionConflict
	}

	source.ValidationStatus = status
	source.ValidationCheckedAt = &now
	source.ValidationReason = reason
	source.Revision = updatedRevision
	source.LastOperation = "validate"
	view, err := r.sourceViewLocked(ctx, source, host)
	if err != nil {
		return nil, err
	}
	if err := r.reconcileSourceLocked(ctx, source, host); err != nil {
		return view, err
	}
	if status == models.SourceValidationFailed {
		return view, ErrSourceValidationFailed
	}
	return r.sourceViewLocked(ctx, source, host)
}

// SetSourceState records desired state before starting or stopping a socket. A
// repeat of the already desired state is idempotent and reconciles the socket.
func (r *Relay) SetSourceState(ctx context.Context, hostID, expectedRevision uint64, state models.SourceState) (*SourceView, error) {
	if !validSourceState(state) {
		return nil, ErrInvalidSourceState
	}

	r.sourcesLk.Lock()
	defer r.sourcesLk.Unlock()

	source, host, err := r.sourceAndHostLocked(ctx, hostID)
	if err != nil {
		return nil, err
	}

	operation := "state:" + string(state)
	if source.Revision != expectedRevision && !(source.Revision == expectedRevision+1 && source.LastOperation == operation) {
		return nil, ErrSourceRevisionConflict
	}
	if state == models.SourceStateEnabled {
		if err := r.checkSourceEnablement(ctx, source, host); err != nil {
			return nil, err
		}
	}
	if source.State != state {
		updates := map[string]interface{}{
			"state":          state,
			"revision":       source.Revision + 1,
			"last_operation": operation,
		}
		if state == models.SourceStateEnabled {
			updates["recovery_required"] = true
		}
		result := r.db.WithContext(ctx).Model(&models.Source{}).
			Where("host_id = ? AND revision = ?", source.HostID, source.Revision).
			Updates(updates)
		if result.Error != nil {
			return nil, fmt.Errorf("saving source state: %w", result.Error)
		}
		if result.RowsAffected != 1 {
			return nil, ErrSourceRevisionConflict
		}
		source.State = state
		source.Revision++
		source.LastOperation = operation
		if state == models.SourceStateEnabled {
			source.RecoveryRequired = true
		}
	}

	view, viewErr := r.sourceViewLocked(ctx, source, host)
	if viewErr != nil {
		return nil, viewErr
	}
	if err := r.reconcileSourceLocked(ctx, source, host); err != nil {
		return view, err
	}
	host, err = r.GetHostByID(ctx, source.HostID)
	if err != nil {
		return nil, err
	}
	return r.sourceViewLocked(ctx, source, host)
}

// checkSourceEnablement enforces validation and bans before acquisition is enabled.
func (r *Relay) checkSourceEnablement(ctx context.Context, source *models.Source, host *models.Host) error {
	if source.ValidationStatus != models.SourceValidationPassed {
		return ErrSourceValidationFailed
	}
	banned, err := r.sourceDomainIsBanned(ctx, host.Hostname)
	if err != nil {
		return err
	}
	if banned || host.Status == models.HostStatusBanned {
		return ErrSourceDomainBanned
	}
	return nil
}

// ListSources returns a deterministic, bounded page including quiet sources.
func (r *Relay) ListSources(ctx context.Context, afterHostID uint64, limit int) (*SourcePage, error) {
	r.sourcesLk.Lock()
	defer r.sourcesLk.Unlock()

	limit = sourcePageLimit(limit)
	sources := make([]*models.Source, 0, limit+1)
	if err := r.db.WithContext(ctx).
		Where("host_id > ?", afterHostID).
		Order("host_id ASC").
		Limit(limit + 1).
		Find(&sources).Error; err != nil {
		return nil, fmt.Errorf("listing sources: %w", err)
	}

	page := &SourcePage{Sources: make([]*SourceView, 0, min(limit, len(sources)))}
	if len(sources) > limit {
		page.NextAfterHostID = sources[limit-1].HostID
		sources = sources[:limit]
	}

	hosts, err := r.hostsByID(ctx, sourceHostIDs(sources))
	if err != nil {
		return nil, err
	}
	for _, source := range sources {
		host, ok := hosts[source.HostID]
		if !ok {
			return nil, fmt.Errorf("managed source %d has no host row", source.HostID)
		}
		view, err := r.sourceViewLocked(ctx, source, host)
		if err != nil {
			return nil, err
		}
		page.Sources = append(page.Sources, view)
	}
	return page, nil
}

// ObserveAccountSource records a DID's observation on a managed source. The
// resolved host is attribution only; it never creates or subscribes a source.
func (r *Relay) ObserveAccountSource(ctx context.Context, didStr string, sourceHostID uint64, resolvedHost string) error {
	did, err := syntax.ParseDID(didStr)
	if err != nil {
		return ErrInvalidSourceURL
	}
	resolvedHostname, _, err := ParseHostname(resolvedHost)
	if err != nil {
		return ErrInvalidSourceURL
	}

	var source models.Source
	if err := r.db.WithContext(ctx).First(&source, sourceHostIDPredicate, sourceHostID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrSourceNotFound
		}
		return fmt.Errorf("loading observed source: %w", err)
	}

	normalizedDID := NormalizeDID(did).String()
	admissionReason := ""
	var account models.Account
	if err := r.db.WithContext(ctx).Where("did = ?", normalizedDID).First(&account).Error; err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("loading observed account: %w", err)
		}
	} else if account.Status == models.AccountStatusHostThrottled {
		admissionReason = admissionReasonHostAccountLimit
	}

	observation := &models.AccountSourceObservation{
		DID:              normalizedDID,
		ObservedHostID:   sourceHostID,
		ResolvedHostname: resolvedHostname,
		ObservedAt:       time.Now().UTC(),
		AdmissionReason:  admissionReason,
	}
	observation.ResolvedAt = observation.ObservedAt
	// Retain each source observation while refreshing the separately resolved placement.
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&models.AccountSourceObservation{}).Where("did = ?", normalizedDID).
			Updates(map[string]any{"resolved_hostname": resolvedHostname, "resolved_at": observation.ResolvedAt}).Error; err != nil {
			return err
		}
		return tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "did"}, {Name: "observed_host_id"}},
			DoUpdates: clause.Assignments(map[string]interface{}{
				"resolved_hostname": observation.ResolvedHostname,
				"observed_at":       observation.ObservedAt,
				"resolved_at":       observation.ResolvedAt,
				"admission_reason":  observation.AdmissionReason,
			}),
		}).Create(observation).Error
	})
	if err != nil {
		return fmt.Errorf("saving source observation: %w", err)
	}
	return nil
}

// ListSourceAccounts returns bounded, deterministic historical observations for
// a source. Current account placement is included only as a separate snapshot.
func (r *Relay) ListSourceAccounts(ctx context.Context, sourceHostID uint64, afterDID string, limit int) (*SourceAccountsPage, error) {
	r.sourcesLk.Lock()
	defer r.sourcesLk.Unlock()

	var source models.Source
	if err := r.db.WithContext(ctx).First(&source, sourceHostIDPredicate, sourceHostID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrSourceNotFound
		}
		return nil, fmt.Errorf("loading source accounts: %w", err)
	}

	limit = sourcePageLimit(limit)
	observations := make([]*models.AccountSourceObservation, 0, limit+1)
	if err := r.db.WithContext(ctx).
		Where("observed_host_id = ? AND did > ?", sourceHostID, afterDID).
		Order("did ASC").
		Limit(limit + 1).
		Find(&observations).Error; err != nil {
		return nil, fmt.Errorf("listing source observations: %w", err)
	}

	page := &SourceAccountsPage{Accounts: make([]*SourceAccountView, 0, min(limit, len(observations)))}
	if len(observations) > limit {
		page.NextAfterDID = observations[limit-1].DID
		observations = observations[:limit]
	}

	accounts, err := r.accountsByDID(ctx, observationDIDs(observations))
	if err != nil {
		return nil, err
	}
	hostIDs := make([]uint64, 0, len(accounts)+1)
	hostIDs = append(hostIDs, sourceHostID)
	for _, account := range accounts {
		hostIDs = append(hostIDs, account.HostID)
	}
	hosts, err := r.hostsByID(ctx, hostIDs)
	if err != nil {
		return nil, err
	}
	resolvedNames := make([]string, 0, len(observations))
	for _, observation := range observations {
		resolvedNames = append(resolvedNames, observation.ResolvedHostname)
	}
	var admittedTargets []string
	if len(resolvedNames) > 0 {
		if err := r.db.WithContext(ctx).Table("host").Joins("JOIN source ON source.host_id = host.id").
			Where("host.hostname IN ? AND source.state = ? AND source.validation_status = ? AND host.status <> ?", resolvedNames, models.SourceStateEnabled, models.SourceValidationPassed, models.HostStatusBanned).
			Pluck("host.hostname", &admittedTargets).Error; err != nil {
			return nil, err
		}
	}
	admitted := make(map[string]bool, len(admittedTargets))
	for _, hostname := range admittedTargets {
		admitted[hostname] = true
	}

	for _, observation := range observations {
		view := sourceAccountView(observation, accounts[observation.DID], hosts, admitted[observation.ResolvedHostname], source.RecoveryRequired)
		page.Accounts = append(page.Accounts, view)
	}
	return page, nil
}

// sourceAccountView keeps historical observation and current placement separate.
func sourceAccountView(observation *models.AccountSourceObservation, account *models.Account, hosts map[uint64]*models.Host, targetAdmitted, recoveryRequired bool) *SourceAccountView {
	view := &SourceAccountView{
		DID:                  observation.DID,
		ObservedSourceHostID: observation.ObservedHostID,
		ResolvedHostname:     observation.ResolvedHostname,
		ObservedAt:           observation.ObservedAt,
		ResolvedAt:           observation.ResolvedAt,
		TargetCoverage:       "unknown",
		AdmissionReason:      observation.AdmissionReason,
		RecoveryRequired:     recoveryRequired,
	}
	if targetAdmitted {
		view.TargetCoverage = "incomplete"
	}
	if observedHost := hosts[observation.ObservedHostID]; observedHost != nil {
		view.ObservedSourceHostname = observedHost.Hostname
	}
	if account != nil {
		view.CurrentHostID = account.HostID
		view.CurrentStatus = account.Status
		view.CurrentUpstreamStatus = account.UpstreamStatus
		if currentHost := hosts[account.HostID]; currentHost != nil {
			view.CurrentHostname = currentHost.Hostname
		}
		if account.Status == models.AccountStatusHostThrottled {
			view.AdmissionReason = admissionReasonHostAccountLimit
		}
	}
	return view
}

func (r *Relay) sourceAndHostLocked(ctx context.Context, hostID uint64) (*models.Source, *models.Host, error) {
	var source models.Source
	if err := r.db.WithContext(ctx).First(&source, sourceHostIDPredicate, hostID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil, ErrSourceNotFound
		}
		return nil, nil, fmt.Errorf("loading source: %w", err)
	}

	var host models.Host
	if err := r.db.WithContext(ctx).First(&host, hostID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil, fmt.Errorf("managed source %d has no host row", hostID)
		}
		return nil, nil, fmt.Errorf("loading source host: %w", err)
	}
	return &source, &host, nil
}

func (r *Relay) sourceViewLocked(ctx context.Context, source *models.Source, host *models.Host) (*SourceView, error) {
	if source == nil || host == nil || source.HostID != host.ID {
		return nil, errors.New("invalid source view")
	}
	runtimeState := "configured"
	if r.Slurper != nil {
		runtimeState = r.Slurper.SourceState(host.Hostname)
	}
	if source.State != models.SourceStateEnabled {
		runtimeState = "disabled"
	} else if runtimeState == "configured" && (source.ValidationStatus == models.SourceValidationFailed || host.Status == models.HostStatusOffline || host.Status == models.HostStatusBanned) {
		runtimeState = "failing"
	}
	return &SourceView{
		HostID:       host.ID,
		Hostname:     host.Hostname,
		NoSSL:        host.NoSSL,
		DesiredState: source.State,
		Revision:     source.Revision,
		Validation: SourceValidation{
			Status:    source.ValidationStatus,
			CheckedAt: source.ValidationCheckedAt,
			Reason:    source.ValidationReason,
		},
		RecoveryRequired:  source.RecoveryRequired,
		HostStatus:        host.Status,
		RuntimeState:      runtimeState,
		LastDurableCursor: host.LastSeq,
		AccountQuota: SourceAccountQuota{
			Limit: host.AccountLimit,
			Count: host.AccountCount,
		},
	}, nil
}

func (r *Relay) reconcileSourceLocked(ctx context.Context, source *models.Source, host *models.Host) error {
	if r.Slurper == nil {
		return nil
	}
	switch source.State {
	case models.SourceStateEnabled:
		banned, err := r.sourceDomainIsBanned(ctx, host.Hostname)
		if err != nil {
			return err
		}
		if source.ValidationStatus != models.SourceValidationPassed || banned || host.Status == models.HostStatusBanned {
			return r.Slurper.StopSource(ctx, host.Hostname)
		}
		if err := r.Slurper.Subscribe(host); err != nil {
			return fmt.Errorf("starting source: %w", err)
		}
	case models.SourceStateDisabled, models.SourceStateRemoved:
		if err := r.Slurper.StopSource(ctx, host.Hostname); err != nil {
			return fmt.Errorf("stopping source: %w", err)
		}
	default:
		return ErrInvalidSourceState
	}
	return nil
}

func (r *Relay) hostsByID(ctx context.Context, ids []uint64) (map[uint64]*models.Host, error) {
	hosts := make([]*models.Host, 0, len(ids))
	if len(ids) == 0 {
		return map[uint64]*models.Host{}, nil
	}
	if err := r.db.WithContext(ctx).Where("id IN ?", ids).Find(&hosts).Error; err != nil {
		return nil, fmt.Errorf("loading source hosts: %w", err)
	}
	byID := make(map[uint64]*models.Host, len(hosts))
	for _, host := range hosts {
		byID[host.ID] = host
	}
	return byID, nil
}

func (r *Relay) accountsByDID(ctx context.Context, dids []string) (map[string]*models.Account, error) {
	accounts := make([]*models.Account, 0, len(dids))
	if len(dids) == 0 {
		return map[string]*models.Account{}, nil
	}
	if err := r.db.WithContext(ctx).Where("did IN ?", dids).Find(&accounts).Error; err != nil {
		return nil, fmt.Errorf("loading source accounts: %w", err)
	}
	byDID := make(map[string]*models.Account, len(accounts))
	for _, account := range accounts {
		byDID[account.DID] = account
	}
	return byDID, nil
}

func (r *Relay) sourceDomainIsBanned(ctx context.Context, hostname string) (bool, error) {
	if strings.HasPrefix(hostname, "localhost:") {
		return false, nil
	}
	return r.DomainIsBanned(ctx, hostname)
}

func sourceHostURL(host *models.Host) string {
	if host.NoSSL {
		return "http://" + host.Hostname
	}
	return "https://" + host.Hostname
}

// SetSourceBlocked delegates legacy block actions to the durable source policy.
func (r *Relay) SetSourceBlocked(ctx context.Context, hostname string, blocked bool) error {
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
	state, status := models.SourceStateEnabled, models.HostStatusActive
	if blocked {
		state, status = models.SourceStateDisabled, models.HostStatusBanned
	} else {
		banned, err := r.sourceDomainIsBanned(ctx, hostname)
		if err != nil {
			return err
		}
		if banned {
			return ErrSourceDomainBanned
		}
		if source.ValidationStatus != models.SourceValidationPassed {
			return ErrSourceValidationFailed
		}
	}
	if source.State == state && host.Status == status {
		return r.reconcileSourceLocked(ctx, source, host)
	}
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&models.Host{}).Where("id = ?", host.ID).Update("status", status).Error; err != nil {
			return err
		}
		return tx.Model(&models.Source{}).Where(sourceHostIDPredicate, host.ID).Updates(map[string]any{
			"state": state, "revision": gorm.Expr("revision + 1"), "last_operation": "block", "recovery_required": true,
		}).Error
	})
	if err != nil {
		return err
	}
	source.State, host.Status = state, status
	return r.reconcileSourceLocked(ctx, source, host)
}

func sourcePageLimit(limit int) int {
	if limit <= 0 {
		return defaultSourcePageLimit
	}
	if limit > maxSourcePageLimit {
		return maxSourcePageLimit
	}
	return limit
}

func validSourceState(state models.SourceState) bool {
	switch state {
	case models.SourceStateEnabled, models.SourceStateDisabled, models.SourceStateRemoved:
		return true
	default:
		return false
	}
}

func sourceHostIDs(sources []*models.Source) []uint64 {
	ids := make([]uint64, 0, len(sources))
	for _, source := range sources {
		ids = append(ids, source.HostID)
	}
	return ids
}

func observationDIDs(observations []*models.AccountSourceObservation) []string {
	dids := make([]string, 0, len(observations))
	for _, observation := range observations {
		dids = append(dids, observation.DID)
	}
	return dids
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// InspectSource returns current durable and runtime state without reconciling it.
func (r *Relay) InspectSource(ctx context.Context, rawURL string) (*SourceView, error) {
	hostname, _, err := ParseHostname(rawURL)
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
	source, storedHost, err := r.sourceAndHostLocked(ctx, host.ID)
	if err != nil {
		return nil, err
	}
	return r.sourceViewLocked(ctx, source, storedHost)
}
