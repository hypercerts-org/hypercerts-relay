// Package selection owns Hypercerts' durable collection-selection policy.
package selection

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
)

const key = "hypercerts/collection-policy"

// Policy is a global exact-NSID allowlist. Empty never means all collections.
type Policy struct {
	Revision    uint64   `json:"revision"`
	Collections []string `json:"collections"`
}

type Manager struct {
	mu     sync.RWMutex
	db     *store.Store
	policy Policy
}

// Open reuses persisted policy. Initial is used only for a new data directory.
func Open(db *store.Store, initial []string) (*Manager, error) {
	m := &Manager{db: db}
	value, closer, err := db.Get([]byte(key))
	if err == nil {
		defer closer.Close()
		if err := json.Unmarshal(value, &m.policy); err != nil {
			return nil, fmt.Errorf("collection policy: %w", err)
		}
		if m.policy.Revision == 0 {
			return nil, errors.New("collection policy: invalid persisted revision")
		}
		if _, err := Normalize(m.policy.Collections); err != nil {
			return nil, err
		}
		return m, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	collections, err := Normalize(initial)
	if err != nil {
		return nil, err
	}
	m.policy = Policy{Revision: 1, Collections: collections}
	data, err := json.Marshal(m.policy)
	if err != nil {
		return nil, err
	}
	if err := db.Set([]byte(key), data, store.SyncWrites); err != nil {
		return nil, err
	}
	return m, nil
}

func Normalize(collections []string) ([]string, error) {
	result := slices.Clone(collections)
	for _, collection := range result {
		if _, err := atmos.ParseNSID(collection); err != nil {
			return nil, fmt.Errorf("invalid exact collection NSID %q", collection)
		}
	}
	slices.Sort(result)
	return slices.Compact(result), nil
}

func (m *Manager) Current() Policy {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return Policy{Revision: m.policy.Revision, Collections: slices.Clone(m.policy.Collections)}
}

var ErrRevision = errors.New("collection policy revision conflict")

// Update atomically commits a policy and its dependent jobs. The caller owns
// the job-manager lock; no callback may call back into this selection manager.
func (m *Manager) Update(expected uint64, collections []string, stage func(Policy, *pebble.Batch) error) (Policy, error) {
	collections, err := Normalize(collections)
	if err != nil {
		return Policy{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if expected != m.policy.Revision {
		return Policy{}, ErrRevision
	}
	if slices.Equal(collections, m.policy.Collections) {
		return Policy{Revision: m.policy.Revision, Collections: slices.Clone(m.policy.Collections)}, nil
	}
	if expected == ^uint64(0) {
		return Policy{}, errors.New("collection policy revision exhausted")
	}
	next := Policy{Revision: expected + 1, Collections: collections}
	data, err := json.Marshal(next)
	if err != nil {
		return Policy{}, err
	}
	b := m.db.NewBatch()
	defer b.Close()
	if err := b.Set([]byte(key), data, nil); err != nil {
		return Policy{}, err
	}
	if stage != nil {
		if err := stage(next, b); err != nil {
			return Policy{}, err
		}
	}
	if err := m.db.Commit(b, store.SyncWrites); err != nil {
		return Policy{}, err
	}
	m.policy = next
	return Policy{Revision: next.Revision, Collections: slices.Clone(next.Collections)}, nil
}

// Allows preserves protocol markers while filtering every record operation.
func (m *Manager) Allows(ev *segment.Event) bool {
	// Nil retains the upstream embedding contract for internal tools/tests.
	// The serve command always enables a persisted manager, even for an empty list.
	if m == nil || !ev.Kind.IsCommit() {
		return true
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return slices.Contains(m.policy.Collections, ev.Collection)
}
