package jobs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

// recoveryScopeLimit bounds the DID set one recovery job may carry. Beyond it a
// new job is created, so a long-lived pending job cannot grow without bound.
const recoveryScopeLimit = 4096

// RequestRepositoryRecovery asks for re-acquisition of specific repositories on
// an enabled source. It is idempotent per requestID and coalesces into an
// existing pending recovery job at the same (PDS, policy) coordinate.
//
// A running job is never extended: its frozen inventory was already seeded from
// the previous DID set, so a DID appended afterwards would have no inventory
// entry and the repository ledger would reject it, silently excluding that DID
// from the repair.
func (m *Manager) RequestRepositoryRecovery(raw string, dids []string, requestID string) (Job, error) {
	if len(requestID) > 128 {
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

	// Reuse only a *pending* recovery job at this coordinate, and only while its
	// scope stays bounded. requestJob would also return a running one, so the
	// state is checked before extending anything.
	for _, candidate := range next.Jobs {
		if candidate.PDS != pds || candidate.Reason != ReasonRepositoryRecovery ||
			candidate.Policy.Revision != policy.Revision || candidate.State != Pending {
			continue
		}
		if merged := mergeRecoveryDIDs(candidate.RecoveryDIDs, normalized); len(merged) <= recoveryScopeLimit {
			candidate.RecoveryDIDs = merged
			next.Jobs[candidate.ID] = candidate
			return m.rememberRequest(next, candidate, requestID)
		}
	}

	job := newJob(pds, policy, ReasonRepositoryRecovery, next.SourceRevisions[pds])
	job.RecoveryDIDs = normalized
	if err := beginAcquisition(next, &job); err != nil {
		return Job{}, err
	}
	if existing, ok := next.Jobs[job.ID]; ok && (existing.State == Pending || existing.State == Running) {
		// A job for this exact coordinate already exists in a non-pending state;
		// book new work under a distinct identity rather than overwrite it.
		job.ID = distinctRecoveryJobID(job.ID, len(next.Jobs))
	}
	next.Jobs[job.ID] = job
	return m.rememberRequest(next, job, requestID)
}

// distinctRecoveryJobID derives a fresh job identifier for a recovery request
// whose deterministic identity is already occupied. Unlike requestJob's re-key
// it is keyed on the existing ID and a per-coordinate counter, never on the
// global job count, so unrelated job churn cannot change it.
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
			return &InputError{Code: "recovery_revision_unavailable", Unavailable: true}
		}
		entries[did] = rev
	}
	if len(entries) == 0 {
		return nil
	}
	return p.Manager.CheckpointInventory(job.ID, "", entries, true)
}
