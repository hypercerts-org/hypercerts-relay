package jobs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"

	"github.com/jcalabro/atmos"
	atmossync "github.com/jcalabro/atmos/sync"
)

// normalizeRecoveryDIDs validates, deduplicates and sorts the DID set a
// repository-recovery job covers. An empty set or a malformed DID is rejected:
// either would silently widen the job's scope or guarantee it matches nothing.
func normalizeRecoveryDIDs(dids []string) ([]string, error) {
	if len(dids) == 0 {
		return nil, fmt.Errorf("%w: recovery requires at least one DID", ErrInvalidInput)
	}
	seen := make(map[string]struct{}, len(dids))
	out := make([]string, 0, len(dids))
	for _, did := range dids {
		if _, err := atmos.ParseDID(did); err != nil {
			return nil, fmt.Errorf("%w: invalid recovery DID %q", ErrInvalidInput, did)
		}
		if _, dup := seen[did]; dup {
			continue
		}
		seen[did] = struct{}{}
		out = append(out, did)
	}
	slices.Sort(out)
	return out, nil
}

const recoveryScopeLimit = 4096

var ErrRecoveryBusy = errors.New("repository recovery ownership or capacity unavailable")

func recoverySealed(job Job) bool {
	return job.RecoveryScopeSealed == nil || *job.RecoveryScopeSealed || job.State == Running
}

func recoveryOwners(next data) (map[string]string, error) {
	owners := make(map[string]string)
	for id, job := range next.Jobs {
		if job.Reason != ReasonRepositoryRecovery || (job.State != Pending && job.State != Running) {
			continue
		}
		for _, did := range job.RecoveryDIDs {
			if _, exists := owners[did]; exists {
				return nil, fmt.Errorf("ambiguous active repository recovery ownership")
			}
			owners[did] = id
		}
	}
	return owners, nil
}

func addRecoveryJob(next data, predecessor Job, dids []string) (Job, error) {
	job := newJob(predecessor.PDS, predecessor.Policy, ReasonRepositoryRecovery, predecessor.SourceRevision)
	job.RecoveryDIDs = slices.Clone(dids)
	job.RecoveryScopeSealed = new(bool)
	if err := beginAcquisition(next, &job); err != nil {
		return Job{}, err
	}
	baseID := job.ID
	for ordinal := len(next.Jobs); ; ordinal++ {
		if _, exists := next.Jobs[job.ID]; !exists {
			break
		}
		job.ID = distinctRecoveryJobID(baseID, ordinal)
	}
	next.Jobs[job.ID] = job
	return job, nil
}

func (m *Manager) RequestRepositoryRecovery(raw string, dids []string, requestID string) (Job, error) {
	if len(requestID) > 128 || len(dids) > recoveryScopeLimit {
		return Job{}, ErrInvalidInput
	}
	normalized, err := normalizeRecoveryDIDs(dids)
	if err != nil {
		return Job{}, err
	}
	pds, err := normalizeSource(raw)
	if err != nil {
		return Job{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, found, err := m.requestReceipt(pds, ReasonRepositoryRecovery, requestID, 0); found {
		return existing, err
	}
	if !m.data.Sources[pds] {
		return Job{}, ErrConflict
	}
	next := clone(m.data)
	ensureSourceRevisions(&next)
	policy := m.policy.Current()
	owners, err := recoveryOwners(next)
	if err != nil {
		return Job{}, err
	}
	var unowned []string
	representativeID := ""
	for _, did := range normalized {
		id, exists := owners[did]
		if !exists {
			unowned = append(unowned, did)
			continue
		}
		owner := next.Jobs[id]
		if owner.PDS != pds || owner.SourceRevision != next.SourceRevisions[pds] || owner.Policy.Revision != policy.Revision {
			return Job{}, ErrRecoveryBusy
		}
		if recoverySealed(owner) {
			owner.RecoveryFollowupDIDs = mergeRecoveryDIDs(owner.RecoveryFollowupDIDs, []string{did})
			next.Jobs[id] = owner
		}
		if representativeID == "" || id < representativeID {
			representativeID = id
		}
	}
	if len(owners)+len(unowned) > recoveryScopeLimit {
		return Job{}, ErrRecoveryBusy
	}
	if len(unowned) > 0 {
		candidateID := ""
		for id, candidate := range next.Jobs {
			if candidate.PDS == pds && candidate.Reason == ReasonRepositoryRecovery &&
				candidate.SourceRevision == next.SourceRevisions[pds] && candidate.Policy.Revision == policy.Revision &&
				candidate.State == Pending && !recoverySealed(candidate) && len(candidate.RecoveryDIDs)+len(unowned) <= recoveryScopeLimit &&
				(candidateID == "" || id < candidateID) {
				candidateID = id
			}
		}
		if candidateID == "" {
			job, err := addRecoveryJob(next, newJob(pds, policy, ReasonRepositoryRecovery, next.SourceRevisions[pds]), unowned)
			if err != nil {
				return Job{}, err
			}
			candidateID = job.ID
		} else {
			candidate := next.Jobs[candidateID]
			candidate.RecoveryDIDs = mergeRecoveryDIDs(candidate.RecoveryDIDs, unowned)
			next.Jobs[candidateID] = candidate
		}
		if representativeID == "" || candidateID < representativeID {
			representativeID = candidateID
		}
	}
	return m.rememberRequest(next, next.Jobs[representativeID], requestID)
}

func restoreRecoveryJobs(next *data) error {
	for id, job := range next.Jobs {
		if job.Reason != ReasonRepositoryRecovery {
			continue
		}
		normalized, err := normalizeRecoveryDIDs(job.RecoveryDIDs)
		if err != nil || len(normalized) > recoveryScopeLimit || !slices.Equal(normalized, job.RecoveryDIDs) {
			return fmt.Errorf("invalid persisted repository recovery scope")
		}
		if job.RecoveryScopeSealed == nil || job.State == Running || job.Attempts > 0 || !job.StartedAt.IsZero() || job.TotalReposKnown || job.EnumeratedRepos > 0 || len(job.CompletedRepos) > 0 {
			sealed := true
			job.RecoveryScopeSealed = &sealed
		}
		for _, did := range job.RecoveryFollowupDIDs {
			if !slices.Contains(job.RecoveryDIDs, did) || !recoverySealed(job) || (job.State != Pending && job.State != Running) {
				return fmt.Errorf("invalid persisted repository recovery followup")
			}
		}
		next.Jobs[id] = job
	}
	owners, err := recoveryOwners(*next)
	if err != nil {
		return err
	}
	if len(owners) > recoveryScopeLimit {
		return fmt.Errorf("persisted repository recovery capacity exceeded")
	}
	return nil
}

func promoteRecoveryFollowups(next *data, job *Job) error {
	if job.Reason != ReasonRepositoryRecovery || (job.State != Complete && job.State != Failed && job.State != Incomplete) || len(job.RecoveryFollowupDIDs) == 0 {
		return nil
	}
	if _, err := addRecoveryJob(*next, *job, job.RecoveryFollowupDIDs); err != nil {
		return err
	}
	job.RecoveryFollowupDIDs = nil
	return nil
}

// distinctRecoveryJobID derives a fresh job identifier for a recovery request
// whose deterministic identity is already occupied.
func distinctRecoveryJobID(base string, ordinal int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("recovery/%s/%d", base, ordinal)))
	return hex.EncodeToString(sum[:16])
}

// mergeRecoveryDIDs returns the sorted union of two normalized DID sets.
func mergeRecoveryDIDs(existing, added []string) []string {
	seen := make(map[string]struct{}, len(existing)+len(added))
	out := make([]string, 0, len(existing)+len(added))
	for _, did := range append(append([]string{}, existing...), added...) {
		if _, dup := seen[did]; dup {
			continue
		}
		seen[did] = struct{}{}
		out = append(out, did)
	}
	slices.Sort(out)
	return out
}

// seedRecoveryInventory persists a frozen inventory holding exactly the DIDs a
// recovery job covers, so the per-repository ledger can resolve their listing
// coordinates without a listRepos traversal of the whole PDS.
//
// It is re-runnable. Only DIDs absent from the current inventory are submitted:
// CheckpointInventory rejects a DID whose newly fetched revision conflicts with
// one already recorded (`invalid_listing`), so re-submitting a seeded DID would
// fail on the second pass or after any restart.
func (p PDSProcessor) seedRecoveryInventory(ctx context.Context, client *atmossync.Client, job *Job) error {
	seeded := map[string]struct{}{}
	if job.TotalReposKnown {
		existing, err := p.Manager.Inventory(job.ID)
		if err != nil {
			return err
		}
		for _, entry := range existing {
			seeded[entry.DID] = struct{}{}
		}
	}

	entries := make(map[string]string, len(job.RecoveryDIDs))
	for _, did := range job.RecoveryDIDs {
		if _, exists := seeded[did]; exists {
			continue
		}
		// The ledger requires a TID-parseable revision, and the revision recorded
		// at gap detection would already be stale.
		rev, _, err := client.GetLatestCommit(ctx, atmos.DID(did))
		if err != nil {
			// TODO: Checkpoint successful DIDs and retry unavailable revisions independently.
			return &InputError{Code: "recovery_revision_unavailable", Unavailable: true}
		}
		entries[did] = rev
	}
	if len(entries) == 0 {
		return nil
	}
	return p.Manager.CheckpointInventory(job.ID, "", entries, true)
}
