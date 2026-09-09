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

var ErrConflict = errors.New("job or source state conflict")
var ErrNotFound = errors.New("job not found")
var ErrInvalidInput = errors.New("invalid job input")

type Job struct {
	ID              string            `json:"id"`
	PDS             string            `json:"pds"`
	Policy          selection.Policy  `json:"policy"`
	Reason          string            `json:"reason"`
	State           State             `json:"state"`
	Attempts        int               `json:"attempts"`
	CompletedRepos  map[string]string `json:"completedRepos"`
	Cursor          string            `json:"cursor"`
	ErrorCode       string            `json:"errorCode,omitempty"`
	CreatedAt       time.Time         `json:"createdAt"`
	StartedAt       time.Time         `json:"startedAt,omitempty"`
	FinishedAt      time.Time         `json:"finishedAt,omitempty"`
	Coverage        string            `json:"coverage"`
	HistoryComplete bool              `json:"historyComplete"`
}
type data struct {
	Requests    map[string]string `json:"requests,omitempty"`
	Initialized bool              `json:"initialized"`
	Sources     map[string]bool   `json:"sources"`
	Jobs        map[string]Job    `json:"jobs"`
}
type Manager struct {
	mu        sync.Mutex
	db        *store.Store
	policy    *selection.Manager
	data      data
	cancel    context.CancelFunc
	running   bool
	runningID string
}

func Open(db *store.Store, policy *selection.Manager) (*Manager, error) {
	if policy == nil {
		return nil, errors.New("jobs require collection policy")
	}
	m := &Manager{db: db, policy: policy, data: data{Sources: map[string]bool{}, Jobs: map[string]Job{}}}
	b, closer, err := db.Get([]byte(stateKey))
	if errors.Is(err, store.ErrNotFound) {
		return m, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	if err := json.Unmarshal(b, &m.data); err != nil {
		return nil, err
	}
	if m.data.Sources == nil || m.data.Jobs == nil {
		return nil, errors.New("invalid persisted job state")
	}
	for id, job := range m.data.Jobs {
		if job.ID != id || job.CompletedRepos == nil || job.Policy.Revision == 0 {
			return nil, errors.New("invalid persisted job")
		}
		switch job.State {
		case Running:
			job.State = Pending
			m.data.Jobs[id] = job
		case Pending, Complete, Failed, Canceled, Incomplete:
		default:
			return nil, errors.New("invalid job status")
		}
	}
	if err := m.save(m.data); err != nil {
		return nil, err
	}
	return m, nil
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
func newJob(pds string, policy selection.Policy, reason string) Job {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\n%d\n%s", pds, policy.Revision, reason)))
	return Job{ID: hex.EncodeToString(sum[:16]), PDS: pds, Policy: policy, Reason: reason, State: Pending, CompletedRepos: map[string]string{}, CreatedAt: time.Now().UTC(), Coverage: "current_state", HistoryComplete: false}
}

// SeedSources applies CLI configuration once; stale environment cannot revive a removed source.
func (m *Manager) SeedSources(sources []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.data.Initialized {
		return nil
	}
	next := clone(m.data)
	for _, raw := range sources {
		pds, err := normalizeSource(raw)
		if err != nil {
			return err
		}
		next.Sources[pds] = true
		j := newJob(pds, m.policy.Current(), "source_added")
		next.Jobs[j.ID] = j
	}
	next.Initialized = true
	return m.commit(next)
}

// AddSource admits an explicit direct-PDS acquisition target and schedules its
// current-state job without waiting for any live relay event.
func (m *Manager) AddSource(raw string) (Job, error) {
	pds, err := normalizeSource(raw)
	if err != nil {
		return Job{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	next := clone(m.data)
	job := newJob(pds, m.policy.Current(), "source_added")
	if existing, ok := next.Jobs[job.ID]; ok && next.Sources[pds] {
		return clone(existing), nil
	}
	next.Sources[pds] = true
	next.Jobs[job.ID] = job
	if err := m.commit(next); err != nil {
		return Job{}, err
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
func (m *Manager) SetPolicy(expected uint64, collections []string) (selection.Policy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	next := clone(m.data)
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
				j := newJob(pds, policy, "policy_changed")
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
	if len(requestID) > 128 {
		return Job{}, ErrInvalidInput
	}
	if reason != "quota_recovery" && reason != "backfill" {
		return Job{}, fmt.Errorf("%w: unsupported job reason", ErrInvalidInput)
	}
	pds, err := normalizeSource(raw)
	if err != nil {
		return Job{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if id, ok := m.data.Requests[requestID]; requestID != "" && ok {
		existing := m.data.Jobs[id]
		if existing.PDS != pds || existing.Reason != reason {
			return Job{}, ErrConflict
		}
		return clone(existing), nil
	}
	if !m.data.Sources[pds] {
		return Job{}, ErrConflict
	}
	next := clone(m.data)
	remember := func(job Job) (Job, error) {
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
	policy := m.policy.Current()
	for _, existing := range next.Jobs {
		if existing.PDS == pds && existing.Policy.Revision == policy.Revision && existing.Reason == reason && (existing.State == Pending || existing.State == Running) {
			return remember(existing)
		}
	}
	j := newJob(pds, policy, reason)
	if existing, ok := next.Jobs[j.ID]; ok {
		if existing.State == Pending || existing.State == Running {
			return remember(existing)
		}
		// A later recovery gap is new work, preserving the previous result.
		sum := sha256.Sum256([]byte(fmt.Sprintf("%s/%d", j.ID, len(next.Jobs))))
		j.ID = hex.EncodeToString(sum[:16])
	}
	next.Jobs[j.ID] = j
	return remember(j)
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
	receipt := "action/" + requestID
	if existing, ok := m.data.Requests[receipt]; requestID != "" && ok {
		if existing != string(state)+":"+id {
			return ErrConflict
		}
		return nil
	}
	j, ok := m.data.Jobs[id]
	if !ok {
		return ErrNotFound
	}
	if state == Pending && (!m.data.Sources[j.PDS] || j.Policy.Revision != m.policy.Current().Revision || (j.State == Running && requestID == "")) {
		return ErrConflict
	}
	if state == Canceled && j.State != Pending && j.State != Running && !(requestID != "" && j.State == Canceled) {
		return ErrConflict
	}
	next := clone(m.data)
	if requestID != "" {
		if next.Requests == nil {
			next.Requests = map[string]string{}
		}
		next.Requests[receipt] = string(state) + ":" + id
	}
	if requestID != "" && (j.State == state || (state == Pending && j.State == Running)) {
		return m.commit(next)
	}
	j.State = state
	j.ErrorCode = ""
	j.FinishedAt = time.Time{}
	if state == Canceled {
		j.FinishedAt = time.Now().UTC()
	}
	next.Jobs[id] = j
	if err := m.commit(next); err != nil {
		return err
	}
	if state == Canceled && m.cancel != nil && m.runningID == id {
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
func (m *Manager) Sources() map[string]bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return clone(m.data.Sources)
}
func (m *Manager) active(id string) bool {
	j, ok := m.data.Jobs[id]
	return ok && j.State == Running && m.data.Sources[j.PDS] && j.Policy.Revision == m.policy.Current().Revision
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
	if code != "" {
		job.ErrorCode = code
	}
	next.Jobs[id] = job
	return m.commit(next)
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
