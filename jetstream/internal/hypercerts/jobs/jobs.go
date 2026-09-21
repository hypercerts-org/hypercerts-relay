// Package jobs owns durable operator-requested PDS/collection backfill work.
package jobs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
)

type State string

const (
	Pending    State = "pending"
	Running    State = "running"
	Failed     State = "failed"
	Canceled   State = "canceled"
	Incomplete State = "incomplete"
	Complete   State = "complete"
)
const stateKey = "hypercerts/backfill-jobs"
const inventoryKeyPrefix = "hypercerts/backfill-job-inventory/"

const (
	receiptRetryBase = time.Second
	receiptRetryMax  = time.Minute
)

var ErrConflict = errors.New("job or source state conflict")
var ErrNotFound = errors.New("job not found")
var ErrInvalidInput = errors.New("invalid job input")
var ErrPolicyMirrorRequired = errors.New("Relay policy mirror is required for a Relay-bound source")
var ErrReceiptStale = errors.New("recovery receipt rejected as stale")
var ErrReceiptSourceMissing = errors.New("recovery receipt source no longer exists")

type Job struct {
	ID     string           `json:"id"`
	PDS    string           `json:"pds"`
	Policy selection.Policy `json:"policy"`
	Reason string           `json:"reason"`
	// SourceRevision is the Relay lifecycle revision this job is allowed to
	// acknowledge after it reaches its durable current-state boundary.
	SourceRevision uint64            `json:"sourceRevision,omitempty"`
	State          State             `json:"state"`
	Attempts       int               `json:"attempts"`
	CompletedRepos map[string]string `json:"completedRepos"`
	// EnumeratedRepos is the durable, non-public subtotal used to resume a
	// single PDS inventory scan without re-counting earlier pages.
	EnumeratedRepos int       `json:"enumeratedRepos"`
	TotalRepos      int       `json:"totalRepos"`
	TotalReposKnown bool      `json:"totalReposKnown"`
	Cursor          string    `json:"cursor"`
	ErrorCode       string    `json:"errorCode,omitempty"`
	CreatedAt       time.Time `json:"createdAt"`
	StartedAt       time.Time `json:"startedAt,omitempty"`
	FinishedAt      time.Time `json:"finishedAt,omitempty"`
	Coverage        string    `json:"coverage"`
	ReceiptPending  bool      `json:"receiptPending,omitempty"`
	ReceiptAttempts int       `json:"receiptAttempts,omitempty"`
	ReceiptRetryAt  time.Time `json:"receiptRetryAt,omitempty"`
	ReceiptError    string    `json:"receiptError,omitempty"`
}

// inventory is kept under a per-job key instead of inside the job-state
// document. It freezes one accepted listRepos traversal before repository
// downloads begin, without making every page rewrite every other job.
type inventory struct {
	Entries map[string]string `json:"entries"`
}

// SnapshotRejection is durable, bounded non-payload metadata for a direct-PDS
// snapshot that is permanently invalid under the listed policy revision.
type SnapshotRejection struct {
	PDS            string    `json:"pds"`
	PolicyRevision uint64    `json:"policyRevision"`
	DID            string    `json:"did"`
	ListedRevision string    `json:"listedRevision"`
	Kind           string    `json:"kind"`
	Code           string    `json:"code"`
	RejectedAt     time.Time `json:"rejectedAt"`
}

const (
	directPDSSnapshotRejectionKind   = "direct_pds_snapshot"
	snapshotRejectionDigestPrefix    = "sha256:"
	maxSnapshotRejectionPDSLength    = 2048
	snapshotRejectionDigestHexLength = sha256.Size * 2
	maxSnapshotRejections            = 1000
)

// hypercerts: Direct-PDS snapshot rejections are separate from job outcomes so
// retries can acknowledge the same permanently invalid listed revision without
// downloading or materializing it again.
type data struct {
	Actions            map[string]string            `json:"actions,omitempty"`
	Requests           map[string]string            `json:"requests,omitempty"`
	Initialized        bool                         `json:"initialized"`
	Sources            map[string]bool              `json:"sources"`
	SourceRevisions    map[string]uint64            `json:"sourceRevisions,omitempty"`
	Jobs               map[string]Job               `json:"jobs"`
	SnapshotRejections map[string]SnapshotRejection `json:"snapshotRejections"`
}
type Manager struct {
	mu             sync.Mutex
	db             *store.Store
	policy         *selection.Manager
	data           data
	cancel         context.CancelFunc
	running        bool
	runningID      string
	receiptSender  ReceiptSender
	policyAdvancer PolicyAdvanceSender
}

// ReceiptSender submits a completed job's bounded recovery coordinate to the
// Relay owner. It is nil when no private Relay control seam is configured.
type ReceiptSender func(context.Context, Job) error

// PolicyAdvanceSender records a new Jetstream policy revision with Relay
// before that policy is allowed to create recovery work locally.
type PolicyAdvanceSender func(context.Context, string, uint64, uint64) error

func Open(db *store.Store, policy *selection.Manager) (*Manager, error) {
	if policy == nil {
		return nil, errors.New("jobs require collection policy")
	}
	m := &Manager{db: db, policy: policy}
	persisted, found, err := readPersistedData(db)
	if err != nil {
		return nil, err
	}
	if !found {
		m.data = emptyData()
		return m, nil
	}
	if err := restorePersistedData(&persisted); err != nil {
		return nil, err
	}
	m.data = persisted
	if err := m.save(m.data); err != nil {
		return nil, err
	}
	return m, nil
}

func emptyData() data {
	return data{
		Sources:            map[string]bool{},
		SourceRevisions:    map[string]uint64{},
		Jobs:               map[string]Job{},
		SnapshotRejections: map[string]SnapshotRejection{},
	}
}

func readPersistedData(db *store.Store) (data, bool, error) {
	b, closer, err := db.Get([]byte(stateKey))
	if errors.Is(err, store.ErrNotFound) {
		return data{}, false, nil
	}
	if err != nil {
		return data{}, false, err
	}
	defer closer.Close()
	persisted := emptyData()
	if err := json.Unmarshal(b, &persisted); err != nil {
		return data{}, false, err
	}
	return persisted, true, nil
}

func restorePersistedData(persisted *data) error {
	if persisted.Sources == nil || persisted.Jobs == nil {
		return errors.New("invalid persisted job state")
	}
	ensureSourceRevisions(persisted)
	// hypercerts: Older job documents predate the direct-PDS rejection ledger.
	if persisted.SnapshotRejections == nil {
		persisted.SnapshotRejections = map[string]SnapshotRejection{}
	}
	if err := validateSnapshotRejectionLedger(persisted.SnapshotRejections); err != nil {
		return err
	}
	trimSnapshotRejections(persisted.SnapshotRejections)
	if err := resumePersistedJobs(persisted.Jobs); err != nil {
		return err
	}
	persisted.migrateActionReceipts()
	return nil
}

func validateSnapshotRejectionLedger(rejections map[string]SnapshotRejection) error {
	for key, rejection := range rejections {
		if !validSnapshotRejection(rejection) || key != snapshotRejectionStoredKey(rejection.PDS, rejection.PolicyRevision, rejection.DID, rejection.ListedRevision, rejection.Kind) {
			return errors.New("invalid persisted snapshot rejection")
		}
	}
	return nil
}

func resumePersistedJobs(jobs map[string]Job) error {
	for id, job := range jobs {
		if job.ID != id || job.CompletedRepos == nil || job.Policy.Revision == 0 {
			return errors.New("invalid persisted job")
		}
		if job.State == Running {
			job.State = Pending
			jobs[id] = job
			continue
		}
		if job.State != Pending && job.State != Complete && job.State != Failed && job.State != Canceled && job.State != Incomplete {
			return errors.New("invalid job status")
		}
	}
	return nil
}

func normalizeSource(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return "", fmt.Errorf("%w: PDS must be an HTTP(S) origin", ErrInvalidInput)
	}
	if u.Scheme != "https" && !(u.Scheme == "http" && (u.Hostname() == "localhost" || net.ParseIP(u.Hostname()).IsLoopback())) {
		return "", fmt.Errorf("%w: PDS requires HTTPS except loopback fixtures", ErrInvalidInput)
	}
	u.Host = strings.ToLower(u.Host)
	u.Path = ""
	return u.String(), nil
}
func clone[T any](value T) T {
	b, _ := json.Marshal(value)
	var out T
	_ = json.Unmarshal(b, &out)
	return out
}

func ensureSourceRevisions(next *data) {
	if next.SourceRevisions == nil {
		next.SourceRevisions = map[string]uint64{}
	}
}
func (m *Manager) save(next data) error {
	b, err := json.Marshal(next)
	if err != nil {
		return err
	}
	return m.db.Set([]byte(stateKey), b, store.SyncWrites)
}
func (m *Manager) commit(next data) error {
	if err := m.save(next); err != nil {
		return err
	}
	m.data = next
	return nil
}

func jobInventoryKey(id string) []byte { return []byte(inventoryKeyPrefix + id) }

func (m *Manager) readInventory(id string) (inventory, error) {
	b, closer, err := m.db.Get(jobInventoryKey(id))
	if errors.Is(err, store.ErrNotFound) {
		return inventory{Entries: map[string]string{}}, nil
	}
	if err != nil {
		return inventory{}, err
	}
	defer closer.Close()
	var out inventory
	if err := json.Unmarshal(b, &out); err != nil || out.Entries == nil {
		return inventory{}, errors.New("invalid persisted job inventory")
	}
	return out, nil
}

func (m *Manager) commitInventory(next data, id string, inv inventory) error {
	encodedState, err := json.Marshal(next)
	if err != nil {
		return err
	}
	encodedInventory, err := json.Marshal(inv)
	if err != nil {
		return err
	}
	batch := m.db.NewBatch()
	defer batch.Close()
	if err := batch.Set([]byte(stateKey), encodedState, nil); err != nil {
		return err
	}
	if err := batch.Set(jobInventoryKey(id), encodedInventory, nil); err != nil {
		return err
	}
	if err := m.db.Commit(batch, store.SyncWrites); err != nil {
		return err
	}
	m.data = next
	return nil
}
func newJob(pds string, policy selection.Policy, reason string, sourceRevision uint64) Job {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\n%d\n%s\n%d", pds, policy.Revision, reason, sourceRevision)))
	return Job{ID: hex.EncodeToString(sum[:16]), PDS: pds, Policy: policy, Reason: reason, SourceRevision: sourceRevision, State: Pending, CompletedRepos: map[string]string{}, CreatedAt: time.Now().UTC(), Coverage: "current_state"}
}

// SeedSources applies CLI configuration once; stale environment cannot revive a removed source.
func (m *Manager) SeedSources(sources []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.data.Initialized {
		return nil
	}
	next := clone(m.data)
	ensureSourceRevisions(&next)
	for _, raw := range sources {
		pds, err := normalizeSource(raw)
		if err != nil {
			return err
		}
		next.Sources[pds] = true
		j := newJob(pds, m.policy.Current(), "source_added", next.SourceRevisions[pds])
		next.Jobs[j.ID] = j
	}
	next.Initialized = true
	return m.commit(next)
}

// AddSource admits an explicit direct-PDS acquisition target and schedules its
// current-state job without waiting for any live relay event.
func (m *Manager) AddSource(raw string) (Job, error) {
	return m.AddSourceWithRevision(raw, 0)
}

// AddSourceWithRevision remembers the Relay lifecycle coordinate that made an
// explicitly admitted source eligible for a recovery acknowledgement.
func (m *Manager) AddSourceWithRevision(raw string, sourceRevision uint64) (Job, error) {
	pds, err := normalizeSource(raw)
	if err != nil {
		return Job{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	next := clone(m.data)
	ensureSourceRevisions(&next)
	if sourceRevision > 0 && sourceRevision < next.SourceRevisions[pds] {
		return Job{}, ErrConflict
	}
	advanced := sourceRevision > 0 && sourceRevision > next.SourceRevisions[pds]
	if sourceRevision > 0 {
		next.SourceRevisions[pds] = sourceRevision
	}
	if advanced {
		m.cancelStaleSourceJobs(&next, pds, sourceRevision)
	}
	job := newJob(pds, m.policy.Current(), "source_added", next.SourceRevisions[pds])
	if existing, ok := next.Jobs[job.ID]; ok && next.Sources[pds] {
		return clone(existing), nil
	}
	next.Sources[pds] = true
	next.Jobs[job.ID] = job
	if err := m.commit(next); err != nil {
		return Job{}, err
	}
	if advanced && m.cancel != nil && m.runningID != "" && m.data.Jobs[m.runningID].PDS == pds && m.data.Jobs[m.runningID].SourceRevision != sourceRevision {
		m.cancel()
	}
	return clone(job), nil
}
func (m *Manager) RemoveSource(raw string) error {
	pds, err := normalizeSource(raw)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.data.Sources[pds]; !ok {
		return ErrNotFound
	}
	next := clone(m.data)
	next.Sources[pds] = false
	for id, j := range next.Jobs {
		if j.PDS == pds && (j.State == Pending || j.State == Running) {
			j.State = Canceled
			j.ErrorCode = "source_removed"
			j.FinishedAt = time.Now().UTC()
			next.Jobs[id] = j
		}
	}
	if err := m.commit(next); err != nil {
		return err
	}
	if m.cancel != nil && m.data.Jobs[m.runningID].PDS == pds {
		m.cancel()
	}
	return nil
}
func (m *Manager) SetPolicy(ctx context.Context, expected uint64, collections []string) (selection.Policy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	collections, err := selection.Normalize(collections)
	if err != nil {
		return selection.Policy{}, err
	}
	current := m.policy.Current()
	if expected != current.Revision {
		return selection.Policy{}, selection.ErrRevision
	}
	if slices.Equal(collections, current.Collections) {
		return current, nil
	}
	if expected == ^uint64(0) {
		return selection.Policy{}, errors.New("collection policy revision exhausted")
	}
	for _, pds := range orderedEnabledSources(m.data.Sources) {
		sourceRevision := m.data.SourceRevisions[pds]
		if sourceRevision == 0 {
			continue
		}
		if m.policyAdvancer == nil {
			return selection.Policy{}, ErrPolicyMirrorRequired
		}
		if err := m.policyAdvancer(ctx, pds, sourceRevision, expected+1); err != nil {
			return selection.Policy{}, fmt.Errorf("advance Relay recovery policy for %s: %w", pds, err)
		}
	}
	next := clone(m.data)
	ensureSourceRevisions(&next)
	policy, err := m.policy.Update(expected, collections, func(policy selection.Policy, b *pebble.Batch) error {
		for id, j := range next.Jobs {
			if j.State == Pending || j.State == Running {
				j.State = Canceled
				j.ErrorCode = "policy_changed"
				j.FinishedAt = time.Now().UTC()
				next.Jobs[id] = j
			}
		}
		for pds, enabled := range next.Sources {
			if enabled {
				j := newJob(pds, policy, "policy_changed", next.SourceRevisions[pds])
				next.Jobs[j.ID] = j
			}
		}
		encoded, err := json.Marshal(next)
		if err != nil {
			return err
		}
		return b.Set([]byte(stateKey), encoded, nil)
	})
	if err != nil {
		return selection.Policy{}, err
	}
	m.data = next
	if m.cancel != nil && m.data.Jobs[m.runningID].Policy.Revision != policy.Revision {
		m.cancel()
	}
	return policy, nil
}
func (m *Manager) Request(raw, reason string) (Job, error) { return m.RequestOnce(raw, reason, "") }

// RequestOnce durably deduplicates control-plane retries, including terminal jobs.
func (m *Manager) RequestOnce(raw, reason, requestID string) (Job, error) {
	return m.RequestOnceWithSourceRevision(raw, reason, requestID, 0)
}

// RequestOnceWithSourceRevision binds lifecycle/quota intent to the Relay
// source version observed by the control plane.
func (m *Manager) RequestOnceWithSourceRevision(raw, reason, requestID string, sourceRevision uint64) (Job, error) {
	if err := validateRequest(reason, requestID); err != nil {
		return Job{}, err
	}
	pds, err := normalizeSource(raw)
	if err != nil {
		return Job{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if sourceRevision > 0 && sourceRevision < m.data.SourceRevisions[pds] {
		return Job{}, ErrConflict
	}
	if existing, found, err := m.requestReceipt(pds, reason, requestID, sourceRevision); found {
		return existing, err
	}
	if !m.data.Sources[pds] {
		return Job{}, ErrConflict
	}
	next := clone(m.data)
	advanced := advanceSourceRevision(&next, pds, sourceRevision)
	if advanced {
		m.cancelStaleSourceJobs(&next, pds, sourceRevision)
	}
	job := requestJob(next, pds, reason, m.policy.Current(), next.SourceRevisions[pds])
	remembered, err := m.rememberRequest(next, job, requestID)
	if err != nil {
		return Job{}, err
	}
	if advanced {
		m.cancelStaleRunningSourceJob(pds, sourceRevision)
	}
	return remembered, nil
}

func validateRequest(reason, requestID string) error {
	if len(requestID) > 128 {
		return ErrInvalidInput
	}
	if reason != "quota_recovery" && reason != "backfill" {
		return fmt.Errorf("%w: unsupported job reason", ErrInvalidInput)
	}
	return nil
}

func (m *Manager) requestReceipt(pds, reason, requestID string, sourceRevision uint64) (Job, bool, error) {
	if requestID == "" {
		return Job{}, false, nil
	}
	id, ok := m.data.Requests[requestID]
	if !ok {
		return Job{}, false, nil
	}
	existing := m.data.Jobs[id]
	if existing.PDS != pds || existing.Reason != reason || sourceRevisionConflict(existing.SourceRevision, sourceRevision) {
		return Job{}, true, ErrConflict
	}
	return clone(existing), true, nil
}

func advanceSourceRevision(next *data, pds string, sourceRevision uint64) bool {
	ensureSourceRevisions(next)
	advanced := sourceRevision > next.SourceRevisions[pds]
	if sourceRevision > 0 {
		next.SourceRevisions[pds] = sourceRevision
	}
	return advanced
}

func (m *Manager) cancelStaleRunningSourceJob(pds string, sourceRevision uint64) {
	if m.cancel == nil || m.runningID == "" {
		return
	}
	running := m.data.Jobs[m.runningID]
	if running.PDS == pds && running.SourceRevision != sourceRevision {
		m.cancel()
	}
}

func sourceRevisionConflict(stored, requested uint64) bool {
	return stored != 0 && requested != 0 && stored != requested
}

func (m *Manager) cancelStaleSourceJobs(next *data, pds string, sourceRevision uint64) {
	for id, job := range next.Jobs {
		if job.PDS != pds || (job.State != Pending && job.State != Running) || job.SourceRevision == sourceRevision {
			continue
		}
		job.State = Canceled
		job.ErrorCode = "source_revision_changed"
		job.FinishedAt = time.Now().UTC()
		next.Jobs[id] = job
	}
}

func requestJob(next data, pds, reason string, policy selection.Policy, sourceRevision uint64) Job {
	for _, existing := range next.Jobs {
		if existing.PDS == pds && existing.Policy.Revision == policy.Revision && existing.Reason == reason && (existing.State == Pending || existing.State == Running) {
			return existing
		}
	}
	j := newJob(pds, policy, reason, sourceRevision)
	if existing, ok := next.Jobs[j.ID]; ok {
		if existing.State == Pending || existing.State == Running {
			return existing
		}
		// A later recovery gap is new work, preserving the previous result.
		sum := sha256.Sum256([]byte(fmt.Sprintf("%s/%d", j.ID, len(next.Jobs))))
		j.ID = hex.EncodeToString(sum[:16])
	}
	next.Jobs[j.ID] = j
	return j
}
func (m *Manager) Cancel(id string) error                  { return m.transition(id, Canceled) }
func (m *Manager) Retry(id string) error                   { return m.transition(id, Pending) }
func (m *Manager) transition(id string, state State) error { return m.TransitionOnce(id, state, "") }

// TransitionOnce records command receipts atomically with cancellation/retry.
func (m *Manager) TransitionOnce(id string, state State, requestID string) error {
	if len(requestID) > 128 || (state != Pending && state != Canceled) {
		return ErrInvalidInput
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.data.Actions[requestID]; requestID != "" && ok {
		if existing != string(state)+":"+id {
			return ErrConflict
		}
		return nil
	}
	j, ok := m.data.Jobs[id]
	if !ok {
		return ErrNotFound
	}
	if !m.canTransition(j, state, requestID != "") {
		return ErrConflict
	}
	return m.commitTransition(j, state, requestID)
}

// commitTransition persists the receipt with the resulting state before stopping
// acquisition. The caller holds mu and has already validated the transition.
func (m *Manager) commitTransition(j Job, state State, requestID string) error {
	next := clone(m.data)
	if requestID != "" {
		if next.Actions == nil {
			next.Actions = map[string]string{}
		}
		next.Actions[requestID] = string(state) + ":" + j.ID
	}
	if requestID != "" && (j.State == state || (state == Pending && j.State == Running)) {
		return m.commit(next)
	}
	resetInventory := state == Pending && j.State == Failed
	j.State = state
	j.ErrorCode = ""
	j.FinishedAt = time.Time{}
	if resetInventory {
		// A permanent listing/snapshot verdict may become valid only when the
		// PDS exposes a changed revision. An explicit retry therefore starts a
		// new frozen inventory rather than treating an old denominator as live.
		j.Cursor = ""
		j.EnumeratedRepos = 0
		j.TotalRepos = 0
		j.TotalReposKnown = false
		j.CompletedRepos = map[string]string{}
	}
	if state == Canceled {
		j.FinishedAt = time.Now().UTC()
	}
	next.Jobs[j.ID] = j
	if resetInventory {
		return m.commitInventory(next, j.ID, inventory{Entries: map[string]string{}})
	}
	if err := m.commit(next); err != nil {
		return err
	}
	if state == Canceled && m.cancel != nil && m.runningID == j.ID {
		m.cancel()
	}
	return nil
}
func (m *Manager) List() []Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Job, 0, len(m.data.Jobs))
	for _, j := range m.data.Jobs {
		out = append(out, clone(j))
	}
	slices.SortFunc(out, func(a, b Job) int { return strings.Compare(a.ID, b.ID) })
	return out
}

// Get returns one durable job snapshot.
func (m *Manager) Get(id string) (Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job, ok := m.data.Jobs[id]
	if !ok {
		return Job{}, ErrNotFound
	}
	return clone(job), nil
}
func (m *Manager) Sources() map[string]bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return clone(m.data.Sources)
}

// SetReceiptSender configures the private Relay acknowledgement seam. The
// sender is intentionally optional: standalone Jetstream remains usable, but
// only jobs carrying a Relay source revision create a receipt obligation.
func (m *Manager) SetReceiptSender(sender ReceiptSender) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.receiptSender = sender
}

// SetPolicyAdvanceSender configures the private Relay mirror which must
// acknowledge a newer policy before Jetstream commits it.
func (m *Manager) SetPolicyAdvanceSender(sender PolicyAdvanceSender) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.policyAdvancer = sender
}

func orderedEnabledSources(sources map[string]bool) []string {
	result := make([]string, 0, len(sources))
	for pds, enabled := range sources {
		if enabled {
			result = append(result, pds)
		}
	}
	slices.Sort(result)
	return result
}

// snapshotRejectionPosition bounds untrusted source/listing coordinates before
// they enter the durable rejection ledger. Canonical PDS origins, DIDs, and
// TIDs remain exact for operator correlation; every other value becomes a
// fixed digest, never retained raw.
func snapshotRejectionPosition(pds, did, listedRevision string) (string, string, string) {
	return snapshotRejectionPDSPosition(pds), snapshotRejectionDIDPosition(did), snapshotRejectionRevisionPosition(listedRevision)
}

func snapshotRejectionPDSPosition(value string) string {
	normalized, err := normalizeSource(value)
	if err == nil && normalized == value && len(value) <= maxSnapshotRejectionPDSLength {
		return value
	}
	return snapshotRejectionDigest(value)
}

func snapshotRejectionDIDPosition(value string) string {
	if _, err := atmos.ParseDID(value); err == nil {
		return value
	}
	return snapshotRejectionDigest(value)
}

func snapshotRejectionRevisionPosition(value string) string {
	if _, err := atmos.ParseTID(value); err == nil {
		return value
	}
	return snapshotRejectionDigest(value)
}

func snapshotRejectionDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return snapshotRejectionDigestPrefix + hex.EncodeToString(sum[:])
}

func validSnapshotRejectionDigest(value string) bool {
	if len(value) != len(snapshotRejectionDigestPrefix)+snapshotRejectionDigestHexLength || !strings.HasPrefix(value, snapshotRejectionDigestPrefix) {
		return false
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, snapshotRejectionDigestPrefix))
	return err == nil && hex.EncodeToString(decoded) == strings.TrimPrefix(value, snapshotRejectionDigestPrefix)
}

func validSnapshotRejectionPDSPosition(value string) bool {
	if validSnapshotRejectionDigest(value) {
		return true
	}
	normalized, err := normalizeSource(value)
	return err == nil && normalized == value && len(value) <= maxSnapshotRejectionPDSLength
}

func validSnapshotRejectionDIDPosition(value string) bool {
	if validSnapshotRejectionDigest(value) {
		return true
	}
	_, err := atmos.ParseDID(value)
	return err == nil
}

func validSnapshotRejectionRevisionPosition(value string) bool {
	if validSnapshotRejectionDigest(value) {
		return true
	}
	_, err := atmos.ParseTID(value)
	return err == nil
}

func snapshotRejectionStoredKey(pds string, policyRevision uint64, did, listedRevision, kind string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\n%d\n%s\n%s\n%s", pds, policyRevision, did, listedRevision, kind)))
	return hex.EncodeToString(sum[:])
}

func validSnapshotRejection(rejection SnapshotRejection) bool {
	return validSnapshotRejectionPDSPosition(rejection.PDS) && rejection.PolicyRevision != 0 && validSnapshotRejectionDIDPosition(rejection.DID) && validSnapshotRejectionRevisionPosition(rejection.ListedRevision) && rejection.Kind == directPDSSnapshotRejectionKind && len(rejection.Code) > 0 && len(rejection.Code) <= 64 && !rejection.RejectedAt.IsZero()
}

// lookupSnapshotRejection returns only an exact listed snapshot decision. A
// changed listed revision deliberately requires a fresh download and verdict.
func (m *Manager) lookupSnapshotRejection(pds string, policyRevision uint64, did, listedRevision, kind string) (SnapshotRejection, bool) {
	pds, did, listedRevision = snapshotRejectionPosition(pds, did, listedRevision)
	m.mu.Lock()
	defer m.mu.Unlock()
	rejection, ok := m.data.SnapshotRejections[snapshotRejectionStoredKey(pds, policyRevision, did, listedRevision, kind)]
	return rejection, ok
}

// ListSnapshotRejections provides deterministic, bounded non-payload
// inspection for private control-plane callers. It returns value copies, never
// the durable ledger map.
func (m *Manager) ListSnapshotRejections() []SnapshotRejection {
	m.mu.Lock()
	defer m.mu.Unlock()
	type keyedRejection struct {
		key       string
		rejection SnapshotRejection
	}
	keyed := make([]keyedRejection, 0, len(m.data.SnapshotRejections))
	for key, rejection := range m.data.SnapshotRejections {
		keyed = append(keyed, keyedRejection{key: key, rejection: rejection})
	}
	slices.SortFunc(keyed, func(a, b keyedRejection) int {
		return strings.Compare(a.key, b.key)
	})
	out := make([]SnapshotRejection, len(keyed))
	for i, item := range keyed {
		out[i] = item.rejection
	}
	return out
}

// recordSnapshotRejection synchronously persists an exact permanent verdict.
// Repeating the same key returns its original bounded metadata unchanged.
func (m *Manager) recordSnapshotRejection(pds string, policyRevision uint64, did, listedRevision, kind, code string) (SnapshotRejection, error) {
	pds, did, listedRevision = snapshotRejectionPosition(pds, did, listedRevision)
	rejection := SnapshotRejection{PDS: pds, PolicyRevision: policyRevision, DID: did, ListedRevision: listedRevision, Kind: kind, Code: code, RejectedAt: time.Now().UTC()}
	if !validSnapshotRejection(rejection) {
		return SnapshotRejection{}, ErrInvalidInput
	}
	key := snapshotRejectionStoredKey(pds, policyRevision, did, listedRevision, kind)
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.data.SnapshotRejections[key]; ok {
		return existing, nil
	}
	next := m.data
	next.SnapshotRejections = cloneSnapshotRejections(m.data.SnapshotRejections)
	next.SnapshotRejections[key] = rejection
	trimSnapshotRejections(next.SnapshotRejections)
	if err := m.commit(next); err != nil {
		return SnapshotRejection{}, err
	}
	return rejection, nil
}

func cloneSnapshotRejections(source map[string]SnapshotRejection) map[string]SnapshotRejection {
	cloned := make(map[string]SnapshotRejection, len(source)+1)
	for key, rejection := range source {
		cloned[key] = rejection
	}
	return cloned
}

// trimSnapshotRejections retains recent durable verdicts without allowing a
// single JSON state document to grow with every rejected listing. Equal
// timestamps use the stored-key order so eviction is deterministic.
func trimSnapshotRejections(rejections map[string]SnapshotRejection) {
	for len(rejections) > maxSnapshotRejections {
		delete(rejections, oldestSnapshotRejectionKey(rejections))
	}
}

func oldestSnapshotRejectionKey(rejections map[string]SnapshotRejection) string {
	var oldestKey string
	var oldest SnapshotRejection
	for key, rejection := range rejections {
		if oldestKey == "" || rejection.RejectedAt.Before(oldest.RejectedAt) || (rejection.RejectedAt.Equal(oldest.RejectedAt) && key < oldestKey) {
			oldestKey = key
			oldest = rejection
		}
	}
	return oldestKey
}

func (m *Manager) active(id string) bool {
	j, ok := m.data.Jobs[id]
	return ok && j.State == Running && m.data.Sources[j.PDS] && j.Policy.Revision == m.policy.Current().Revision && (m.data.SourceRevisions[j.PDS] == 0 || j.SourceRevision == m.data.SourceRevisions[j.PDS])
}

// Apply holds the policy/source cancellation boundary through the archive write.
func (m *Manager) Apply(id string, write func() error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.active(id) {
		return ErrConflict
	}
	return write()
}

// SetTotalRepos records a known PDS snapshot size. PDSProcessor normally uses
// CheckpointEnumeration so retries never need to re-count earlier pages.
func (m *Manager) SetTotalRepos(id string, total int) error {
	if total < 0 {
		return ErrInvalidInput
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.active(id) {
		return ErrConflict
	}
	next := clone(m.data)
	job := next.Jobs[id]
	job.TotalRepos = total
	job.TotalReposKnown = true
	next.Jobs[id] = job
	return m.commit(next)
}

// CheckpointEnumeration atomically advances a completed inventory page and its
// active-repository subtotal. A retry begins at the saved cursor, so each page
// contributes to the eventual total at most once.
func (m *Manager) CheckpointEnumeration(id, cursor string, active int, complete bool) error {
	if active < 0 {
		return ErrInvalidInput
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.active(id) {
		return ErrConflict
	}
	next := clone(m.data)
	job := next.Jobs[id]
	job.Cursor = cursor
	job.EnumeratedRepos += active
	if complete {
		job.TotalRepos = job.EnumeratedRepos
		job.TotalReposKnown = true
	}
	next.Jobs[id] = job
	return m.commit(next)
}

// CheckpointInventory records one validated listRepos page and advances its
// cursor atomically with the job state. The caller must finish this one scan
// before it starts repository downloads, making the published denominator a
// frozen current-state snapshot rather than a moving PDS census.
func (m *Manager) CheckpointInventory(id, cursor string, entries map[string]string, complete bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.active(id) {
		return ErrConflict
	}
	inv, err := m.readInventory(id)
	if err != nil {
		return err
	}
	for did, rev := range entries {
		if existing, ok := inv.Entries[did]; ok && existing != rev {
			return &InputError{Code: "invalid_listing"}
		}
		inv.Entries[did] = rev
	}
	next := clone(m.data)
	job := next.Jobs[id]
	job.Cursor = cursor
	job.EnumeratedRepos = len(inv.Entries)
	if complete {
		job.TotalRepos = len(inv.Entries)
		job.TotalReposKnown = true
	}
	next.Jobs[id] = job
	return m.commitInventory(next, id, inv)
}

// Inventory returns the frozen, active listing in DID order. It is available
// only after the terminal listRepos page is durable.
func (m *Manager) Inventory(id string) ([]ListedRepository, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job, ok := m.data.Jobs[id]
	if !ok {
		return nil, ErrNotFound
	}
	if !job.TotalReposKnown {
		return nil, ErrConflict
	}
	inv, err := m.readInventory(id)
	if err != nil {
		return nil, err
	}
	out := make([]ListedRepository, 0, len(inv.Entries))
	for did, rev := range inv.Entries {
		out = append(out, ListedRepository{DID: did, Revision: rev})
	}
	slices.SortFunc(out, func(a, b ListedRepository) int { return strings.Compare(a.DID, b.DID) })
	return out, nil
}

// ListedRepository is a validated direct-PDS snapshot coordinate frozen for a
// single job. It deliberately excludes CAR data and listing-only metadata.
type ListedRepository struct {
	DID      string
	Revision string
}

func (m *Manager) Checkpoint(id, did, rev, cursor string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.active(id) {
		return ErrConflict
	}
	next := clone(m.data)
	j := next.Jobs[id]
	if did != "" {
		j.CompletedRepos[did] = rev
	}
	j.Cursor = cursor
	next.Jobs[id] = j
	return m.commit(next)
}

type Processor func(context.Context, Job) error

// InputError is safe, bounded outcome metadata, never a payload or raw error.
type InputError struct {
	Code        string
	Unavailable bool
}

func (e *InputError) Error() string { return e.Code }

func (m *Manager) Run(ctx context.Context, process Processor) error {
	m.mu.Lock()
	if m.running {
		m.mu.Unlock()
		return ErrConflict
	}
	m.running = true
	m.mu.Unlock()
	defer func() { m.mu.Lock(); m.running = false; m.mu.Unlock() }()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := m.flushReceipts(ctx); err != nil {
			return err
		}
		worked, err := m.runNext(ctx, process)
		if err != nil {
			return err
		}
		if !worked {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}
		}
	}
}

// Claim and register cancellation under the same lock so removal/policy changes
// cannot slip between the durable Running transition and the active worker.
func (m *Manager) runNext(ctx context.Context, process Processor) (bool, error) {
	m.mu.Lock()
	job, err := m.claimNextLocked()
	if err != nil || job.ID == "" {
		m.mu.Unlock()
		return false, err
	}
	jobCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.runningID = job.ID
	m.mu.Unlock()
	processErr := process(jobCtx, clone(job))
	cancel()
	return true, m.finishJob(ctx, job.ID, processErr)
}

func (m *Manager) claimNextLocked() (Job, error) {
	for _, job := range m.data.Jobs {
		if job.State != Pending {
			continue
		}
		if current := m.data.SourceRevisions[job.PDS]; current > 0 && job.SourceRevision != current {
			next := clone(m.data)
			job.State = Canceled
			job.ErrorCode = "source_revision_changed"
			job.FinishedAt = time.Now().UTC()
			next.Jobs[job.ID] = job
			if err := m.commit(next); err != nil {
				return Job{}, err
			}
			continue
		}
		next := clone(m.data)
		job.State = Running
		job.Attempts++
		job.StartedAt = time.Now().UTC()
		next.Jobs[job.ID] = job
		if err := m.commit(next); err != nil {
			return Job{}, err
		}
		return job, nil
	}
	return Job{}, nil
}

func (m *Manager) finishJob(ctx context.Context, id string, processErr error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cancel = nil
	m.runningID = ""
	if err := ctx.Err(); err != nil {
		return err
	} // Persisted Running resumes on Open.
	if !m.active(id) {
		return nil
	}
	state, code, err := jobOutcome(processErr)
	if err != nil {
		return err
	} // Local persistence/invariant failures stop the runtime.
	next := clone(m.data)
	job := next.Jobs[id]
	job.FinishedAt = time.Now().UTC()
	job.State = state
	if state == Complete && job.SourceRevision != 0 {
		// This flag is written in the same durable job record as Complete, so a
		// process loss after local success replays the acknowledgement on restart.
		job.ReceiptPending = true
	}
	if code != "" {
		job.ErrorCode = code
	}
	next.Jobs[id] = job
	return m.commit(next)
}

func (m *Manager) flushReceipts(ctx context.Context) error {
	m.mu.Lock()
	sender := m.receiptSender
	pending := make([]Job, 0)
	now := time.Now().UTC()
	if sender != nil {
		currentPolicyRevision := m.policy.Current().Revision
		next := clone(m.data)
		changed := false
		for _, job := range m.data.Jobs {
			if job.State == Complete && job.ReceiptPending {
				if job.Policy.Revision != currentPolicyRevision {
					// A policy change has already created current-policy work. Do not
					// let its superseded predecessor acknowledge Relay recovery.
					job.ReceiptPending = false
					job.ReceiptRetryAt = time.Time{}
					job.ReceiptError = "policy_superseded"
					next.Jobs[job.ID] = job
					changed = true
					continue
				}
				if !job.ReceiptRetryAt.After(now) {
					pending = append(pending, clone(job))
				}
			}
		}
		if changed {
			if err := m.commit(next); err != nil {
				m.mu.Unlock()
				return err
			}
		}
	}
	m.mu.Unlock()
	for _, job := range pending {
		if err := sender(ctx, job); err != nil {
			if err := m.recordReceiptFailure(job, err); err != nil {
				return err
			}
			continue
		}
		m.mu.Lock()
		next := clone(m.data)
		current, ok := next.Jobs[job.ID]
		if ok && current.State == Complete && current.ReceiptPending && current.SourceRevision == job.SourceRevision {
			current.ReceiptPending = false
			current.ReceiptRetryAt = time.Time{}
			current.ReceiptError = ""
			next.Jobs[job.ID] = current
			if err := m.commit(next); err != nil {
				m.mu.Unlock()
				return err
			}
		}
		m.mu.Unlock()
	}
	return nil
}

func (m *Manager) recordReceiptFailure(job Job, receiptErr error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	next := clone(m.data)
	current, ok := next.Jobs[job.ID]
	if !ok || current.State != Complete || !current.ReceiptPending || current.SourceRevision != job.SourceRevision {
		return nil
	}
	if errors.Is(receiptErr, ErrReceiptStale) {
		current.ReceiptPending = false
		current.ReceiptRetryAt = time.Time{}
		current.ReceiptError = "stale_source_revision"
	} else if errors.Is(receiptErr, ErrReceiptSourceMissing) {
		current.ReceiptPending = false
		current.ReceiptRetryAt = time.Time{}
		current.ReceiptError = "source_not_found"
	} else {
		current.ReceiptAttempts++
		current.ReceiptRetryAt = time.Now().UTC().Add(receiptBackoff(current.ReceiptAttempts))
		current.ReceiptError = "relay_unavailable"
	}
	next.Jobs[job.ID] = current
	return m.commit(next)
}

func receiptBackoff(attempts int) time.Duration {
	delay := receiptRetryBase
	for i := 1; i < attempts && delay < receiptRetryMax; i++ {
		delay *= 2
	}
	if delay > receiptRetryMax {
		return receiptRetryMax
	}
	return delay
}

func jobOutcome(err error) (State, string, error) {
	if err == nil {
		return Complete, "", nil
	}
	var input *InputError
	if errors.As(err, &input) {
		if input.Unavailable {
			return Incomplete, input.Code, nil
		}
		return Failed, input.Code, nil
	}
	if errors.Is(err, context.Canceled) {
		return Pending, "", nil
	}
	return "", "", err
}

// Previous versions mixed action receipts into Requests using an action/ prefix.
// Recognize their typed value, not only their key: a job request ID may itself
// start with action/. Keep every old receipt so journal retries remain safe.
func (d *data) migrateActionReceipts() {
	for key, value := range d.Requests {
		state, id, ok := strings.Cut(value, ":")
		if !strings.HasPrefix(key, "action/") || !ok || (State(state) != Pending && State(state) != Canceled) {
			continue
		}
		if _, exists := d.Jobs[id]; !exists {
			continue
		}
		if d.Actions == nil {
			d.Actions = map[string]string{}
		}
		d.Actions[strings.TrimPrefix(key, "action/")] = value
		delete(d.Requests, key)
	}
}
func (m *Manager) rememberRequest(next data, job Job, requestID string) (Job, error) {
	if requestID != "" {
		if next.Requests == nil {
			next.Requests = map[string]string{}
		}
		next.Requests[requestID] = job.ID
	}
	if err := m.commit(next); err != nil {
		return Job{}, err
	}
	return clone(job), nil
}
func (m *Manager) canTransition(j Job, state State, hasReceipt bool) bool {
	if state == Pending {
		return m.data.Sources[j.PDS] && j.Policy.Revision == m.policy.Current().Revision && (j.State != Running || hasReceipt)
	}
	return j.State == Pending || j.State == Running || (hasReceipt && j.State == Canceled)
}
