package history

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/cockroachdb/pebble"
)

const keyPolicyState = metaPrefix + "capture-policy-state"

type PolicyInterval struct {
	Collection             string  `json:"collection"`
	Revision               uint64  `json:"revision"`
	FromJournalSequence    uint64  `json:"fromJournalSequence"`
	ThroughJournalSequence *uint64 `json:"throughJournalSequence,omitempty"`
	OpenedAt               int64   `json:"openedAt,omitempty"`
	ClosedAt               int64   `json:"closedAt,omitempty"`
	RemovalRevision        uint64  `json:"removalRevision,omitempty"`
	SourceEvidence         string  `json:"sourceEvidence"`
}

type PolicyTransition struct {
	Collection        string `json:"collection"`
	RemovalRevision   uint64 `json:"removalRevision"`
	ReaddRevision     uint64 `json:"readdRevision"`
	EffectiveAfterSeq uint64 `json:"effectiveAfterSeq"`
}

type PolicyAdoption struct {
	Policy                   Policy  `json:"policy"`
	EffectiveAfterJournalSeq *uint64 `json:"effectiveAfterJournalSeq"`
}

type PolicyState struct {
	StreamID                 string             `json:"streamId"`
	CaptureGeneration        string             `json:"captureGeneration"`
	Policy                   Policy             `json:"policy"`
	History                  []Policy           `json:"history"`
	Adoptions                []PolicyAdoption   `json:"adoptions,omitempty"`
	AdoptedAtJournalSequence uint64             `json:"adoptedAtJournalSequence"`
	Intervals                []PolicyInterval   `json:"intervals"`
	Transitions              []PolicyTransition `json:"transitions"`
}

var ErrPolicyRevision = errors.New("history: policy revision must increase")

func (s *Store) EnsurePolicy(policy Policy, adoptedAt int64) (PolicyState, error) {
	if s == nil {
		return PolicyState{}, ErrInvalid
	}
	state, ok, err := s.ReadPolicyState()
	if err != nil {
		return PolicyState{}, err
	}
	if ok && state.Policy.Revision == policy.Revision {
		collections := append([]string(nil), policy.Collections...)
		sort.Strings(collections)
		if equalPolicyCollections(state.Policy.Collections, collections) {
			return state, nil
		}
		return PolicyState{}, ErrPolicyRevision
	}
	if ok && policy.Revision < state.Policy.Revision {
		return PolicyState{}, ErrPolicyRevision
	}
	return s.AdoptPolicy(policy, adoptedAt)
}

func equalPolicyCollections(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func clonePolicy(p Policy) Policy {
	return Policy{Revision: p.Revision, Collections: append([]string(nil), p.Collections...)}
}

func cloneAdoptions(in []PolicyAdoption) []PolicyAdoption {
	out := make([]PolicyAdoption, len(in))
	for i, adoption := range in {
		out[i] = PolicyAdoption{Policy: clonePolicy(adoption.Policy)}
		if adoption.EffectiveAfterJournalSeq != nil {
			n := *adoption.EffectiveAfterJournalSeq
			out[i].EffectiveAfterJournalSeq = &n
		}
	}
	return out
}

func (s *Store) AdoptPolicy(policy Policy, adoptedAt int64) (PolicyState, error) {
	return s.AdoptPolicyAfterCommit(policy, adoptedAt, nil)
}

func (s *Store) AdoptPolicyAfterCommit(policy Policy, adoptedAt int64, afterCommit func()) (PolicyState, error) {
	if s == nil || policy.Revision == 0 || policy.Collections == nil {
		return PolicyState{}, ErrInvalid
	}
	collections := append([]string(nil), policy.Collections...)
	sort.Strings(collections)
	for i, collection := range collections {
		if collection == "" || (i > 0 && collection == collections[i-1]) {
			return PolicyState{}, ErrInvalid
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	batch := s.db.NewBatch()
	defer batch.Close()
	state, err := s.stagePolicy(batch, Policy{Revision: policy.Revision, Collections: collections}, adoptedAt)
	if err != nil {
		return PolicyState{}, err
	}
	if err := s.db.Commit(batch, store.SyncWrites); err != nil {
		return PolicyState{}, fmt.Errorf("history: persist adopted policy: %w", err)
	}
	if afterCommit != nil {
		afterCommit()
	}
	notifyCommitted(s.db)
	return state, nil
}

func (s *Store) StagePolicy(batch *pebble.Batch, policy Policy, adoptedAt int64) (PolicyState, error) {
	return s.StagePolicyAfterCommit(batch, policy, adoptedAt, nil)
}

func (s *Store) StagePolicyAfterCommit(batch *pebble.Batch, policy Policy, adoptedAt int64, afterCommit func()) (PolicyState, error) {
	if s == nil || batch == nil || policy.Revision == 0 || policy.Collections == nil {
		return PolicyState{}, ErrInvalid
	}
	collections := append([]string(nil), policy.Collections...)
	sort.Strings(collections)
	for i, collection := range collections {
		if collection == "" || (i > 0 && collection == collections[i-1]) {
			return PolicyState{}, ErrInvalid
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.stagePolicy(batch, Policy{Revision: policy.Revision, Collections: collections}, adoptedAt)
	if err != nil {
		return PolicyState{}, err
	}
	if afterCommit != nil {
		batch.SetDeferred(0, 0)
	}
	return state, nil
}

func (s *Store) stagePolicy(batch *pebble.Batch, policy Policy, adoptedAt int64) (PolicyState, error) {
	tip, err := readTip(s.db)
	if err != nil {
		return PolicyState{}, err
	}
	state := PolicyState{StreamID: s.stream, CaptureGeneration: s.CaptureGeneration(), Policy: clonePolicy(policy), AdoptedAtJournalSequence: tip, Intervals: []PolicyInterval{}, Transitions: []PolicyTransition{}}
	value, closer, getErr := s.db.Get([]byte(keyPolicyState))
	if getErr == nil {
		var old PolicyState
		decodeErr, closeErr := json.Unmarshal(value, &old), closer.Close()
		if decodeErr != nil {
			return PolicyState{}, decodeErr
		}
		if closeErr != nil {
			return PolicyState{}, closeErr
		}
		if old.StreamID != s.stream || old.CaptureGeneration != s.CaptureGeneration() || policy.Revision <= old.Policy.Revision {
			return PolicyState{}, ErrPolicyRevision
		}
		state.Intervals = append(state.Intervals, old.Intervals...)
		state.Transitions = append(state.Transitions, old.Transitions...)
		state.History = append(state.History, old.History...)
		if len(state.History) == 0 {
			state.History = append(state.History, clonePolicy(old.Policy))
		}
		state.Adoptions = cloneAdoptions(old.Adoptions)
		if len(state.Adoptions) == 0 {
			for i, prior := range state.History {
				var known *uint64
				if i == len(state.History)-1 {
					n := old.AdoptedAtJournalSequence
					known = &n
				}
				state.Adoptions = append(state.Adoptions, PolicyAdoption{Policy: clonePolicy(prior), EffectiveAfterJournalSeq: known})
			}
		}
		boundary := tip
		state.Adoptions = append(state.Adoptions, PolicyAdoption{Policy: clonePolicy(policy), EffectiveAfterJournalSeq: &boundary})
		state.History = append(state.History, clonePolicy(policy))
		included := make(map[string]bool, len(policy.Collections))
		for _, c := range policy.Collections {
			included[c] = true
		}
		removed := make(map[string]PolicyInterval)
		for _, interval := range state.Intervals {
			if interval.ClosedAt != 0 && interval.RemovalRevision != 0 {
				removed[interval.Collection] = interval
			}
		}
		for i := range state.Intervals {
			interval := &state.Intervals[i]
			if !included[interval.Collection] && interval.ThroughJournalSequence == nil && interval.ClosedAt == 0 {
				interval.RemovalRevision = policy.Revision
				end := tip
				interval.ThroughJournalSequence = &end
				interval.ClosedAt = adoptedAt
				removed[interval.Collection] = *interval
			}
		}
		prior := make(map[string]bool, len(old.Policy.Collections))
		for _, c := range old.Policy.Collections {
			prior[c] = true
		}
		for _, c := range policy.Collections {
			if !prior[c] {
				if closed, ok := removed[c]; ok {
					state.Transitions = append(state.Transitions, PolicyTransition{Collection: c, RemovalRevision: closed.RemovalRevision, ReaddRevision: policy.Revision, EffectiveAfterSeq: tip})
				}
				state.Intervals = append(state.Intervals, PolicyInterval{Collection: c, Revision: policy.Revision, FromJournalSequence: tip + 1, OpenedAt: adoptedAt, SourceEvidence: "unknown"})
				continue
			}
			active := false
			for _, interval := range state.Intervals {
				if interval.Collection == c && interval.ThroughJournalSequence == nil && interval.ClosedAt == 0 {
					active = true
					break
				}
			}
			if !active {
				state.Intervals = append(state.Intervals, PolicyInterval{Collection: c, Revision: policy.Revision, FromJournalSequence: tip + 1, OpenedAt: adoptedAt, SourceEvidence: "unknown"})
			}
		}
	} else if !errors.Is(getErr, store.ErrNotFound) {
		return PolicyState{}, getErr
	} else {
		boundary := tip
		state.History = []Policy{clonePolicy(policy)}
		state.Adoptions = []PolicyAdoption{{Policy: clonePolicy(policy), EffectiveAfterJournalSeq: &boundary}}
		for _, c := range policy.Collections {
			state.Intervals = append(state.Intervals, PolicyInterval{Collection: c, Revision: policy.Revision, FromJournalSequence: tip + 1, OpenedAt: adoptedAt, SourceEvidence: "unknown"})
		}
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		return PolicyState{}, err
	}
	if err := batch.Set([]byte(keyPolicyState), encoded, nil); err != nil {
		return PolicyState{}, err
	}
	return state, nil
}

func (s *Store) ReadPolicyAdoptions() ([]PolicyAdoption, error) {
	state, ok, err := s.ReadPolicyState()
	if err != nil || !ok {
		return nil, err
	}
	if len(state.Adoptions) == 0 {
		policies := state.History
		if len(policies) == 0 {
			policies = []Policy{state.Policy}
		}
		out := make([]PolicyAdoption, len(policies))
		for i, policy := range policies {
			out[i].Policy = clonePolicy(policy)
			if i == len(policies)-1 {
				n := state.AdoptedAtJournalSequence
				out[i].EffectiveAfterJournalSeq = &n
			}
		}
		return out, nil
	}
	return cloneAdoptions(state.Adoptions), nil
}

func (s *Store) ReadPolicyHistory() ([]Policy, error) {
	state, ok, err := s.ReadPolicyState()
	if err != nil || !ok {
		return nil, err
	}
	if len(state.History) == 0 {
		return []Policy{clonePolicy(state.Policy)}, nil
	}
	out := make([]Policy, len(state.History))
	for i, policy := range state.History {
		out[i] = clonePolicy(policy)
	}
	return out, nil
}

func (s *Store) ReadPolicyState() (PolicyState, bool, error) {
	value, closer, err := s.db.Get([]byte(keyPolicyState))
	if errors.Is(err, store.ErrNotFound) {
		return PolicyState{}, false, nil
	}
	if err != nil {
		return PolicyState{}, false, err
	}
	defer closer.Close()
	var state PolicyState
	if err := json.Unmarshal(value, &state); err != nil {
		return PolicyState{}, false, err
	}
	if state.StreamID != s.stream || state.CaptureGeneration != s.CaptureGeneration() {
		return PolicyState{}, false, ErrInvalid
	}
	return state, true, nil
}
