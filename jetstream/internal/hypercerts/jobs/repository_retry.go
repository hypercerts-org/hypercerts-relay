package jobs

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
)

const (
	repositoryRetryPrefix = "hypercerts/backfill-repo-retry/"
	pdsCooldownPrefix     = "hypercerts/backfill-pds-cooldown/"

	maxRepositoryAttempts    = 3
	repositoryRetryBaseDelay = time.Second
	repositoryRetryMaxDelay  = 15 * time.Minute
)

// RepositoryRetryState is the derived or persisted state of one repository in
// a frozen job inventory. Ready and complete are derived rather than stored:
// absence means ready unless Job.CompletedRepos already checkpoints the DID.
type RepositoryRetryState string

const (
	RepositoryRetryReady      RepositoryRetryState = "ready"
	RepositoryRetryInFlight   RepositoryRetryState = "in_flight"
	RepositoryRetryWait       RepositoryRetryState = "retry_wait"
	RepositoryRetryUnresolved RepositoryRetryState = "unresolved"
	RepositoryRetryComplete   RepositoryRetryState = "complete"
)

// RepositoryFailureCategory is deliberately bounded; raw transport errors and
// response bodies are never retained in repository retry state.
type RepositoryFailureCategory string

const (
	RepositoryFailureTimeout     RepositoryFailureCategory = "timeout"
	RepositoryFailureTransport   RepositoryFailureCategory = "transport"
	RepositoryFailureBodyRead    RepositoryFailureCategory = "body_read"
	RepositoryFailureHTTP        RepositoryFailureCategory = "http"
	RepositoryFailureUnknown     RepositoryFailureCategory = "unknown"
	RepositoryFailureInterrupted RepositoryFailureCategory = "interrupted"
	RepositoryFailureRejected    RepositoryFailureCategory = "rejected"
)

// RepositoryFailureStage limits stored failure location to direct getRepo
// request/body processing, not arbitrary caller-provided strings.
type RepositoryFailureStage string

const (
	RepositoryFailureGetRepoRequest RepositoryFailureStage = "getRepo/request"
	RepositoryFailureGetRepoBody    RepositoryFailureStage = "getRepo/body"
)

// RepositoryRetryFailure is the sanitized failure classification stored with a
// retry record. Its timestamp is stored on RepositoryRetry and set by the manager.
type RepositoryRetryFailure struct {
	Category   RepositoryFailureCategory `json:"category"`
	HTTPStatus int                       `json:"httpStatus,omitempty"`
	Stage      RepositoryFailureStage    `json:"stage"`
	Code       string                    `json:"code,omitempty"`
}

// RepositoryRetry is the repository-local attempt state for one job's frozen
// DID/revision coordinate. Attempts count only attempts begun for this job and
// repository. Complete is synthesized from Job.CompletedRepos.
type RepositoryRetry struct {
	JobID          string                  `json:"jobId"`
	DID            string                  `json:"did"`
	ListedRevision string                  `json:"listedRevision"`
	State          RepositoryRetryState    `json:"state"`
	Attempts       int                     `json:"attempts"`
	RetryAt        time.Time               `json:"retryAt,omitempty"`
	Failure        *RepositoryRetryFailure `json:"failure,omitempty"`
	FailedAt       time.Time               `json:"failedAt,omitempty"`
}

// PDSCooldown is a PDS-origin-wide deadline, independent of any job's
// per-repository attempt record.
type PDSCooldown struct {
	PDS   string    `json:"pds"`
	Until time.Time `json:"until"`
}

var (
	ErrRepositoryCoolingDown    = errors.New("PDS is in repository retry cooldown")
	ErrRepositoryRetryNotDue    = errors.New("repository retry deadline has not elapsed")
	ErrRepositoryRetryExhausted = errors.New("repository retry attempt budget is exhausted")
)

func repositoryRetryJobPrefix(jobID string) []byte {
	return []byte(repositoryRetryPrefix + jobID + "/")
}

func repositoryRetryKey(jobID, did string) []byte {
	return []byte(string(repositoryRetryJobPrefix(jobID)) + base64.RawURLEncoding.EncodeToString([]byte(did)))
}

func pdsCooldownKey(pds string) []byte {
	return []byte(pdsCooldownPrefix + base64.RawURLEncoding.EncodeToString([]byte(pds)))
}

// GetRepositoryRetry returns one frozen repository coordinate. A missing
// retry row is ready unless the existing CompletedRepos checkpoint marks it
// complete. Stored attempts for a different listed revision are rejected.
func (m *Manager) GetRepositoryRetry(jobID, did string) (RepositoryRetry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job, listedRevision, err := m.repositoryCoordinateLocked(jobID, did)
	if err != nil {
		return RepositoryRetry{}, err
	}
	return m.repositoryRetryLocked(job, did, listedRevision)
}

// ListRepositoryRetries lists only explicit, persisted retry rows for a job.
// Implicit ready repositories and CompletedRepos checkpoints are not expanded
// into an inventory-sized result.
func (m *Manager) ListRepositoryRetries(jobID string) ([]RepositoryRetry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job, err := m.repositoryRetryJobLocked(jobID)
	if err != nil {
		return nil, err
	}
	if !job.TotalReposKnown {
		return nil, ErrConflict
	}
	inv, err := m.readInventory(jobID)
	if err != nil {
		return nil, err
	}
	out, err := m.listFrozenRepositoryRetryRowsLocked(jobID, job, inv.Entries)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(out, func(a, b RepositoryRetry) int { return strings.Compare(a.DID, b.DID) })
	return out, nil
}

func (m *Manager) repositoryRetryJobLocked(jobID string) (Job, error) {
	job, ok := m.data.Jobs[jobID]
	if !ok {
		if jobID == "" {
			return Job{}, ErrInvalidInput
		}
		return Job{}, ErrNotFound
	}
	return job, nil
}

func decodePersistedRepositoryRetry(jobID string, prefix, key, value []byte) (RepositoryRetry, error) {
	var retry RepositoryRetry
	if err := json.Unmarshal(value, &retry); err != nil {
		return RepositoryRetry{}, errors.New("invalid persisted repository retry state")
	}
	keyDID, err := decodeRepositoryRetryDID(prefix, key)
	if err != nil || keyDID != retry.DID || retry.JobID != jobID || !validPersistedRepositoryRetry(retry) {
		return RepositoryRetry{}, errors.New("invalid persisted repository retry state")
	}
	return retry, nil
}

func (m *Manager) listFrozenRepositoryRetryRowsLocked(jobID string, job Job, entries map[string]string) ([]RepositoryRetry, error) {
	prefix := repositoryRetryJobPrefix(jobID)
	iter, err := m.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: store.PrefixUpperBound(prefix)})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()
	return listFrozenRepositoryRetryRows(iter, jobID, job, entries)
}

func listFrozenRepositoryRetryRows(iter *pebble.Iterator, jobID string, job Job, entries map[string]string) ([]RepositoryRetry, error) {
	prefix := repositoryRetryJobPrefix(jobID)
	out := make([]RepositoryRetry, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		retry, err := decodePersistedRepositoryRetry(jobID, prefix, iter.Key(), iter.Value())
		if err != nil {
			return nil, err
		}
		listedRevision, exists := entries[retry.DID]
		if !exists || listedRevision != retry.ListedRevision {
			// A frozen inventory can be discarded by the existing failed-job
			// retry path. Never expose its old attempts as work for a new listing.
			continue
		}
		if completedRevisionAtLeast(job.CompletedRepos[retry.DID], listedRevision) {
			continue
		}
		out = append(out, retry)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return out, nil
}

// BeginRepositoryAttempt durably consumes one job/repository attempt before
// external work begins. It requires a running, current job and an exact frozen
// inventory entry; an interrupted in_flight row remains in_flight after reopen.
func (m *Manager) BeginRepositoryAttempt(jobID, did string) (RepositoryRetry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job, listedRevision, err := m.repositoryCoordinateLocked(jobID, did)
	if err != nil {
		return RepositoryRetry{}, err
	}
	if !m.active(jobID) {
		return RepositoryRetry{}, ErrConflict
	}
	current, err := m.repositoryRetryLocked(job, did, listedRevision)
	if err != nil {
		return RepositoryRetry{}, err
	}
	if current.State == RepositoryRetryComplete || current.State == RepositoryRetryInFlight || current.State == RepositoryRetryUnresolved {
		return RepositoryRetry{}, ErrConflict
	}
	if current.Attempts >= maxRepositoryAttempts {
		return RepositoryRetry{}, ErrRepositoryRetryExhausted
	}
	if current.State == RepositoryRetryWait && current.RetryAt.After(time.Now()) {
		return RepositoryRetry{}, ErrRepositoryRetryNotDue
	}
	cooldown, found, err := m.readPDSCooldownLocked(job.PDS)
	if err != nil {
		return RepositoryRetry{}, err
	}
	now := time.Now().UTC()
	if found && cooldown.Until.After(now) {
		return RepositoryRetry{}, ErrRepositoryCoolingDown
	}
	if current.Attempts == int(^uint(0)>>1) {
		return RepositoryRetry{}, ErrInvalidInput
	}
	current.State = RepositoryRetryInFlight
	current.Attempts++
	current.RetryAt = time.Time{}
	current.Failure = nil
	current.FailedAt = time.Time{}
	if err := m.commitRepositoryRetryBatch(m.data, false, &current, nil, nil); err != nil {
		return RepositoryRetry{}, err
	}
	return current, nil
}

// RecordRepositoryFailure atomically records a sanitized failure and, when
// supplied, extends the PDS-wide cooldown. A nil retryAt marks the repository
// unresolved; otherwise it enters retry_wait until that deadline.
func (m *Manager) RecordRepositoryFailure(jobID, did string, failure RepositoryRetryFailure, retryAt, pdsCooldownUntil *time.Time) (RepositoryRetry, error) {
	if !validRepositoryFailure(failure) || (retryAt != nil && !validRetryDeadline(*retryAt)) || (pdsCooldownUntil != nil && !validRetryDeadline(*pdsCooldownUntil)) {
		return RepositoryRetry{}, ErrInvalidInput
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	job, listedRevision, err := m.repositoryCoordinateLocked(jobID, did)
	if err != nil {
		return RepositoryRetry{}, err
	}
	if !m.active(jobID) {
		return RepositoryRetry{}, ErrConflict
	}
	current, err := m.repositoryRetryLocked(job, did, listedRevision)
	if err != nil {
		return RepositoryRetry{}, err
	}
	if current.State != RepositoryRetryInFlight {
		return RepositoryRetry{}, ErrConflict
	}
	current.Failure = &failure
	current.FailedAt = time.Now().UTC()
	if retryAt == nil {
		current.State = RepositoryRetryUnresolved
		current.RetryAt = time.Time{}
	} else {
		current.State = RepositoryRetryWait
		current.RetryAt = retryAt.UTC()
	}

	cooldown, err := m.repositoryFailureCooldownLocked(job.PDS, pdsCooldownUntil)
	if err != nil {
		return RepositoryRetry{}, err
	}
	if err := m.commitRepositoryRetryBatch(m.data, false, &current, nil, cooldown); err != nil {
		return RepositoryRetry{}, err
	}
	return current, nil
}

func (m *Manager) repositoryFailureCooldownLocked(pds string, pdsCooldownUntil *time.Time) (*PDSCooldown, error) {
	if pdsCooldownUntil == nil {
		return nil, nil
	}
	until := pdsCooldownUntil.UTC()
	if !until.After(time.Now().UTC()) {
		return nil, ErrInvalidInput
	}
	existing, found, err := m.readPDSCooldownLocked(pds)
	if err != nil {
		return nil, err
	}
	if !found || until.After(existing.Until) {
		return &PDSCooldown{PDS: pds, Until: until}, nil
	}
	return nil, nil
}

// CheckpointRepository records archive-backed completion after the caller's
// archive write succeeds. listedRevision must match the frozen inventory;
// completedRevision may be newer and is stored in CompletedRepos. The job
// checkpoint and retry-row deletion commit in one synced batch, with
// CompletedRepos remaining the only completion authority.
func (m *Manager) CheckpointRepository(jobID, did, listedRevision, completedRevision, cursor string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	job, frozenRevision, err := m.repositoryCoordinateLocked(jobID, did)
	if err != nil {
		return err
	}
	if !m.active(jobID) {
		return ErrConflict
	}
	if listedRevision != frozenRevision {
		return ErrConflict
	}
	listedTID, err := atmos.ParseTID(frozenRevision)
	if err != nil {
		return errors.New("invalid persisted job inventory")
	}
	completedTID, err := atmos.ParseTID(completedRevision)
	if err != nil {
		return ErrInvalidInput
	}
	if completedTID.Integer() < listedTID.Integer() {
		return ErrConflict
	}
	current, err := m.repositoryRetryLocked(job, did, frozenRevision)
	if err != nil {
		return err
	}
	if current.State != RepositoryRetryInFlight {
		return ErrConflict
	}
	next := clone(m.data)
	job = next.Jobs[jobID]
	job.CompletedRepos[did] = completedRevision
	job.Cursor = cursor
	next.Jobs[jobID] = job
	return m.commitRepositoryRetryBatch(next, true, nil, [][]byte{repositoryRetryKey(jobID, did)}, nil)
}

func (m *Manager) repositoryRetryDeleteKeysLocked(jobID string, unresolvedOnly bool) ([][]byte, error) {
	prefix := repositoryRetryJobPrefix(jobID)
	var inv inventory
	var err error
	var job Job
	if unresolvedOnly {
		job = m.data.Jobs[jobID]
		inv, err = m.readInventory(jobID)
		if err != nil {
			return nil, err
		}
	}
	iter, err := m.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: store.PrefixUpperBound(prefix)})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()
	var deletes [][]byte
	for iter.First(); iter.Valid(); iter.Next() {
		var retry RepositoryRetry
		if err := json.Unmarshal(iter.Value(), &retry); err != nil {
			return nil, errors.New("invalid persisted repository retry state")
		}
		key := append([]byte(nil), iter.Key()...)
		keyDID, err := decodeRepositoryRetryDID(prefix, key)
		if err != nil || keyDID != retry.DID || retry.JobID != jobID || !validPersistedRepositoryRetry(retry) {
			return nil, errors.New("invalid persisted repository retry state")
		}
		if unresolvedOnly {
			listedRevision, exists := inv.Entries[retry.DID]
			if !exists || listedRevision != retry.ListedRevision || retry.State != RepositoryRetryUnresolved {
				continue
			}
			if _, complete := job.CompletedRepos[retry.DID]; complete {
				continue
			}
		}
		deletes = append(deletes, key)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return deletes, nil
}

// ResetUnresolvedRepositoryRetries deletes only unresolved rows for an eligible
// non-running job. Deletion restores the implicit ready state and zeroes this
// job/repository's attempt count; checkpointed repositories are never touched.
func (m *Manager) ResetUnresolvedRepositoryRetries(jobID string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job, err := m.repositoryRetryJobLocked(jobID)
	if err != nil {
		return 0, err
	}
	if !m.repositoryResetEligibleLocked(job) || !job.TotalReposKnown {
		return 0, ErrConflict
	}
	deletes, err := m.repositoryRetryDeleteKeysLocked(jobID, true)
	if err != nil {
		return 0, err
	}
	if len(deletes) == 0 {
		return 0, nil
	}
	if err := m.commitRepositoryRetryBatch(m.data, false, nil, deletes, nil); err != nil {
		return 0, err
	}
	return len(deletes), nil
}

// RecordPDSCooldown monotonically extends the active job's canonical PDS-origin
// deadline without creating a repository retry row. The cooldown is synced
// through the retry-state batch so all jobs for that origin observe it.
func (m *Manager) RecordPDSCooldown(jobID string, until time.Time) error {
	until = until.UTC()
	if jobID == "" || !validRetryDeadline(until) || !until.After(time.Now().UTC()) {
		return ErrInvalidInput
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	job, ok := m.data.Jobs[jobID]
	if !ok {
		return ErrNotFound
	}
	if !m.active(jobID) {
		return ErrConflict
	}
	canonical, err := normalizeSource(job.PDS)
	if err != nil || canonical != job.PDS {
		return errors.New("invalid persisted job source")
	}
	existing, found, err := m.readPDSCooldownLocked(canonical)
	if err != nil {
		return err
	}
	if found && !until.After(existing.Until) {
		return nil
	}
	cooldown := PDSCooldown{PDS: canonical, Until: until}
	return m.commitRepositoryRetryBatch(m.data, false, nil, nil, &cooldown)
}

// GetPDSCooldown returns the canonical origin's persisted cooldown, including
// an expired deadline. Callers decide whether Until is still in the future.
func (m *Manager) GetPDSCooldown(pds string) (PDSCooldown, bool, error) {
	canonical, err := normalizeSource(pds)
	if err != nil {
		return PDSCooldown{}, false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.readPDSCooldownLocked(canonical)
}

func (m *Manager) repositoryCoordinateLocked(jobID, did string) (Job, string, error) {
	if jobID == "" {
		return Job{}, "", ErrInvalidInput
	}
	job, ok := m.data.Jobs[jobID]
	if !ok {
		return Job{}, "", ErrNotFound
	}
	if _, err := atmos.ParseDID(did); err != nil {
		return Job{}, "", ErrInvalidInput
	}
	if !job.TotalReposKnown {
		return Job{}, "", ErrConflict
	}
	inv, err := m.readInventory(jobID)
	if err != nil {
		return Job{}, "", err
	}
	listedRevision, ok := inv.Entries[did]
	if !ok {
		return Job{}, "", ErrConflict
	}
	if _, err := atmos.ParseTID(listedRevision); err != nil {
		return Job{}, "", errors.New("invalid persisted job inventory")
	}
	return job, listedRevision, nil
}

func (m *Manager) repositoryRetryLocked(job Job, did, listedRevision string) (RepositoryRetry, error) {
	base := RepositoryRetry{JobID: job.ID, DID: did, ListedRevision: listedRevision, State: RepositoryRetryReady}
	if _, complete := job.CompletedRepos[did]; complete {
		base.State = RepositoryRetryComplete
		return base, nil
	}
	value, closer, err := m.db.Get(repositoryRetryKey(job.ID, did))
	if errors.Is(err, store.ErrNotFound) {
		return base, nil
	}
	if err != nil {
		return RepositoryRetry{}, err
	}
	defer closer.Close()
	var retry RepositoryRetry
	if err := json.Unmarshal(value, &retry); err != nil || !validPersistedRepositoryRetry(retry) || retry.JobID != job.ID || retry.DID != did {
		return RepositoryRetry{}, errors.New("invalid persisted repository retry state")
	}
	if retry.ListedRevision != listedRevision {
		return RepositoryRetry{}, ErrConflict
	}
	return retry, nil
}

func (m *Manager) readPDSCooldownLocked(pds string) (PDSCooldown, bool, error) {
	value, closer, err := m.db.Get(pdsCooldownKey(pds))
	if errors.Is(err, store.ErrNotFound) {
		return PDSCooldown{}, false, nil
	}
	if err != nil {
		return PDSCooldown{}, false, err
	}
	defer closer.Close()
	var cooldown PDSCooldown
	if err := json.Unmarshal(value, &cooldown); err != nil || cooldown.PDS != pds || !validRetryDeadline(cooldown.Until) {
		return PDSCooldown{}, false, errors.New("invalid persisted PDS retry cooldown")
	}
	return cooldown, true, nil
}

func (m *Manager) repositoryResetEligibleLocked(job Job) bool {
	if job.State == Running || job.State == Complete || job.State == Canceled || !m.data.Sources[job.PDS] {
		return false
	}
	if job.Policy.Revision != m.policy.Current().Revision {
		return false
	}
	return m.data.SourceRevisions[job.PDS] == 0 || job.SourceRevision == m.data.SourceRevisions[job.PDS]
}

func (m *Manager) pendingJobDueLocked(job Job, now time.Time) (time.Time, bool, error) {
	cooldown, found, err := m.readPDSCooldownLocked(job.PDS)
	if err != nil {
		return time.Time{}, false, err
	}
	coolingDown := found && cooldown.Until.After(now)
	if !job.TotalReposKnown {
		return applyPDSCooldown(now, cooldown, coolingDown), true, nil
	}
	inv, err := m.readInventory(job.ID)
	if err != nil {
		return time.Time{}, false, err
	}
	retries, err := m.pendingRepositoryRetryRowsLocked(job, inv.Entries)
	if err != nil {
		return time.Time{}, false, err
	}
	earliest, dueNow, err := pendingRetryDeadline(retries, now)
	if err != nil {
		return time.Time{}, false, err
	}
	if dueNow || pendingInventoryHasImplicitReady(job, inv.Entries, retries) {
		return applyPDSCooldown(now, cooldown, coolingDown), true, nil
	}
	if earliest.IsZero() {
		// Give the processor one pass to persist the terminal outcome for an
		// inventory whose remaining entries are all already resolved.
		return now, true, nil
	}
	return applyPDSCooldown(earliest, cooldown, coolingDown), true, nil
}

func (m *Manager) pendingRepositoryRetryRowsLocked(job Job, entries map[string]string) ([]RepositoryRetry, error) {
	prefix := repositoryRetryJobPrefix(job.ID)
	iter, err := m.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: store.PrefixUpperBound(prefix)})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()
	var retries []RepositoryRetry
	for iter.First(); iter.Valid(); iter.Next() {
		retry, err := decodePersistedRepositoryRetry(job.ID, prefix, iter.Key(), iter.Value())
		if err != nil {
			return nil, err
		}
		listedRevision, exists := entries[retry.DID]
		if !exists {
			continue
		}
		if listedRevision != retry.ListedRevision {
			return nil, ErrConflict
		}
		if completedRevisionAtLeast(job.CompletedRepos[retry.DID], listedRevision) {
			continue
		}
		retries = append(retries, retry)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return retries, nil
}

func pendingRetryDeadline(retries []RepositoryRetry, now time.Time) (time.Time, bool, error) {
	var earliest time.Time
	for _, retry := range retries {
		switch retry.State {
		case RepositoryRetryInFlight:
			return now, true, nil
		case RepositoryRetryWait:
			if !retry.RetryAt.After(now) {
				return now, true, nil
			}
			if earliest.IsZero() || retry.RetryAt.Before(earliest) {
				earliest = retry.RetryAt
			}
		case RepositoryRetryUnresolved:
		default:
			return time.Time{}, false, errors.New("invalid repository retry state")
		}
	}
	return earliest, false, nil
}

func pendingInventoryHasImplicitReady(job Job, entries map[string]string, retries []RepositoryRetry) bool {
	retryDIDs := make(map[string]struct{}, len(retries))
	for _, retry := range retries {
		retryDIDs[retry.DID] = struct{}{}
	}
	for did, listedRevision := range entries {
		if completedRevisionAtLeast(job.CompletedRepos[did], listedRevision) {
			continue
		}
		if _, found := retryDIDs[did]; !found {
			return true
		}
	}
	return false
}

func applyPDSCooldown(deadline time.Time, cooldown PDSCooldown, coolingDown bool) time.Time {
	if coolingDown && cooldown.Until.After(deadline) {
		return cooldown.Until
	}
	return deadline
}

func completedRevisionAtLeast(completed, listed string) bool {
	completedTID, completedErr := atmos.ParseTID(completed)
	listedTID, listedErr := atmos.ParseTID(listed)
	return completedErr == nil && listedErr == nil && completedTID.Integer() >= listedTID.Integer()
}

func (m *Manager) nextWorkDelay() (time.Duration, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	deadline, found, err := m.nextWorkDeadlineLocked(time.Now().UTC())
	if err != nil || !found {
		return 0, found, err
	}
	return max(time.Until(deadline), 0), true, nil
}

func (m *Manager) nextWorkDeadlineLocked(now time.Time) (time.Time, bool, error) {
	var earliest time.Time
	for _, job := range m.data.Jobs {
		if job.State == Pending {
			due, found, err := m.pendingJobDueLocked(job, now)
			if err != nil {
				return time.Time{}, false, err
			}
			if found {
				earliest = earlierDeadline(earliest, due)
			}
		}
		receiptDeadline, ready := receiptRetryDeadline(job, m.receiptSender != nil, now)
		if ready {
			return now, true, nil
		}
		if !receiptDeadline.IsZero() {
			earliest = earlierDeadline(earliest, receiptDeadline)
		}
	}
	return earliest, !earliest.IsZero(), nil
}

func receiptRetryDeadline(job Job, senderConfigured bool, now time.Time) (time.Time, bool) {
	if !senderConfigured || job.State != Complete || !job.ReceiptPending {
		return time.Time{}, false
	}
	if job.ReceiptRetryAt.IsZero() || !job.ReceiptRetryAt.After(now) {
		return now, true
	}
	return job.ReceiptRetryAt, false
}

func earlierDeadline(current, candidate time.Time) time.Time {
	if current.IsZero() || candidate.Before(current) {
		return candidate
	}
	return current
}

func (m *Manager) signalScheduler() {
	if m.wake == nil {
		return
	}
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (m *Manager) commitRepositoryRetryBatch(next data, persistJob bool, retry *RepositoryRetry, deleteRetries [][]byte, cooldown *PDSCooldown) error {
	batch := m.db.NewBatch()
	defer func() { _ = batch.Close() }()
	if persistJob {
		encoded, err := json.Marshal(next)
		if err != nil {
			return err
		}
		if err := batch.Set([]byte(stateKey), encoded, nil); err != nil {
			return err
		}
	}
	if retry != nil {
		encoded, err := json.Marshal(retry)
		if err != nil {
			return err
		}
		if err := batch.Set(repositoryRetryKey(retry.JobID, retry.DID), encoded, nil); err != nil {
			return err
		}
	}
	for _, key := range deleteRetries {
		if err := batch.Delete(key, nil); err != nil {
			return err
		}
	}
	if cooldown != nil {
		encoded, err := json.Marshal(cooldown)
		if err != nil {
			return err
		}
		if err := batch.Set(pdsCooldownKey(cooldown.PDS), encoded, nil); err != nil {
			return err
		}
	}
	if err := m.db.Commit(batch, store.SyncWrites); err != nil {
		return err
	}
	if persistJob {
		m.data = next
	}
	m.signalScheduler()
	return nil
}

func decodeRepositoryRetryDID(prefix, key []byte) (string, error) {
	if !strings.HasPrefix(string(key), string(prefix)) {
		return "", fmt.Errorf("invalid repository retry key")
	}
	encoded := strings.TrimPrefix(string(key), string(prefix))
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || base64.RawURLEncoding.EncodeToString(decoded) != encoded {
		return "", fmt.Errorf("invalid repository retry key")
	}
	return string(decoded), nil
}

func validRepositoryFailure(failure RepositoryRetryFailure) bool {
	categoryValid := failure.Category == RepositoryFailureTimeout || failure.Category == RepositoryFailureTransport || failure.Category == RepositoryFailureBodyRead || failure.Category == RepositoryFailureHTTP || failure.Category == RepositoryFailureUnknown || failure.Category == RepositoryFailureInterrupted || failure.Category == RepositoryFailureRejected
	stageValid := failure.Stage == RepositoryFailureGetRepoRequest || failure.Stage == RepositoryFailureGetRepoBody
	codeValid := len(failure.Code) <= 64 && strings.Trim(failure.Code, "abcdefghijklmnopqrstuvwxyz0123456789_") == ""
	if !categoryValid || !stageValid || !codeValid {
		return false
	}
	if failure.Category == RepositoryFailureHTTP {
		return failure.HTTPStatus >= 100 && failure.HTTPStatus <= 599
	}
	return failure.HTTPStatus == 0
}

func validPersistedRepositoryRetry(retry RepositoryRetry) bool {
	if retry.JobID == "" || retry.Attempts <= 0 || retry.DID == "" {
		return false
	}
	if _, err := atmos.ParseDID(retry.DID); err != nil {
		return false
	}
	if _, err := atmos.ParseTID(retry.ListedRevision); err != nil {
		return false
	}
	switch retry.State {
	case RepositoryRetryInFlight:
		return retry.RetryAt.IsZero() && retry.Failure == nil && retry.FailedAt.IsZero()
	case RepositoryRetryWait:
		return validRetryDeadline(retry.RetryAt) && retry.Failure != nil && validRepositoryFailure(*retry.Failure) && validRetryDeadline(retry.FailedAt)
	case RepositoryRetryUnresolved:
		return retry.RetryAt.IsZero() && retry.Failure != nil && validRepositoryFailure(*retry.Failure) && validRetryDeadline(retry.FailedAt)
	default:
		return false
	}
}

func validRetryDeadline(value time.Time) bool {
	return !value.IsZero() && value.Year() >= 1 && value.Year() <= 9999
}
