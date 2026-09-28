package archivekeys

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/cockroachdb/pebble"
)

const stateKey = "hypercerts/archive-keys/v1"

var (
	ErrNotFound       = errors.New("archive key not found")
	ErrInvalid        = errors.New("invalid archive key input")
	ErrUnauthorized   = errors.New("invalid archive key")
	ErrRequestLimited = errors.New("archive key request limit exceeded")
	ErrByteLimited    = errors.New("archive key byte limit exceeded")
)

type Key struct {
	ID                        string     `json:"id"`
	Name                      string     `json:"name"`
	Owner                     string     `json:"owner"`
	RequestsPerMinute         int        `json:"requestsPerMinute"`
	ArchiveMegabytesPerMinute int        `json:"archiveMegabytesPerMinute"`
	CreatedAt                 time.Time  `json:"createdAt"`
	RevokedAt                 *time.Time `json:"revokedAt,omitempty"`
}
type record struct {
	Key
	Hash [32]byte `json:"hash"`
}
type persisted struct {
	Keys []record `json:"keys"`
}
type Manager struct {
	db      *store.Store
	mu      sync.Mutex
	keys    []record
	windows map[string]*window
}
type window struct {
	at       time.Time
	requests int
	bytes    int64
}

const megabyte = int64(1_000_000)

func Open(db *store.Store) (*Manager, error) {
	m := &Manager{db: db, windows: map[string]*window{}}
	v, c, err := db.Get([]byte(stateKey))
	if errors.Is(err, pebble.ErrNotFound) {
		return m, nil
	}
	if err != nil {
		return nil, err
	}
	defer c.Close()
	var p persisted
	if err = json.Unmarshal(v, &p); err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(p.Keys))
	for _, k := range p.Keys {
		if k.ID == "" || seen[k.ID] || !valid(k.Name) || !valid(k.Owner) ||
			k.RequestsPerMinute < 1 || k.RequestsPerMinute > 100000 ||
			k.ArchiveMegabytesPerMinute < 1 || k.ArchiveMegabytesPerMinute > 100000 ||
			k.CreatedAt.IsZero() || k.Hash == ([32]byte{}) {
			return nil, fmt.Errorf("invalid persisted archive key %q", k.ID)
		}
		seen[k.ID] = true
	}
	m.keys = p.Keys
	return m, nil
}
func valid(s string) bool { return len(strings.TrimSpace(s)) > 0 && len(s) <= 120 }
func (m *Manager) Create(name, owner string, rpm, mb int) (Key, string, error) {
	if !valid(name) || !valid(owner) || rpm < 1 || rpm > 100000 || mb < 1 || mb > 100000 {
		return Key{}, "", ErrInvalid
	}
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return Key{}, "", err
	}
	id := base64.RawURLEncoding.EncodeToString(b[:9])
	token := "hck_" + id + "_" + base64.RawURLEncoding.EncodeToString(b)
	k := record{Key: Key{ID: id, Name: strings.TrimSpace(name), Owner: strings.TrimSpace(owner), RequestsPerMinute: rpm, ArchiveMegabytesPerMinute: mb, CreatedAt: time.Now().UTC()}, Hash: sha256.Sum256([]byte(token))}
	m.mu.Lock()
	defer m.mu.Unlock()
	next := append(append([]record(nil), m.keys...), k)
	if err := m.save(next); err != nil {
		return Key{}, "", err
	}
	m.keys = next
	return k.Key, token, nil
}
func (m *Manager) List() []Key {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Key, len(m.keys))
	for i := range m.keys {
		out[i] = m.keys[i].Key
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out
}
func (m *Manager) Revoke(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.keys {
		if m.keys[i].ID == id {
			if m.keys[i].RevokedAt == nil {
				n := time.Now().UTC()
				next := append([]record(nil), m.keys...)
				next[i].RevokedAt = &n
				if err := m.save(next); err != nil {
					return err
				}
				m.keys = next
				delete(m.windows, id)
				return nil
			}
			return nil
		}
	}
	return ErrNotFound
}
func (m *Manager) Authorize(token string) error {
	_, err := m.Authenticate(token)
	return err
}

// Authenticate verifies a bearer secret without spending quota. The caller
// admits the successful response with AdmitResponse before writing its body.
func (m *Manager) Authenticate(token string) (string, error) {
	h := sha256.Sum256([]byte(token))
	m.mu.Lock()
	defer m.mu.Unlock()
	var found *record
	for i := range m.keys {
		if subtle.ConstantTimeCompare(h[:], m.keys[i].Hash[:]) == 1 && m.keys[i].RevokedAt == nil {
			found = &m.keys[i]
		}
	}
	if found == nil {
		return "", ErrUnauthorized
	}
	return found.ID, nil
}

// AdmitResponse reserves one successful request and its archive bytes as one
// atomic operation. A failed reservation spends neither budget.
func (m *Manager) AdmitResponse(id string, n int64) error {
	if n < 0 {
		return ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, k := range m.keys {
		if k.ID == id && k.RevokedAt == nil {
			w := m.currentWindow(id, time.Now().UTC())
			if w.requests >= k.RequestsPerMinute {
				return ErrRequestLimited
			}
			limit := int64(k.ArchiveMegabytesPerMinute) * megabyte
			if n > limit-w.bytes {
				return ErrByteLimited
			}
			w.requests++
			w.bytes += n
			return nil
		}
	}
	return ErrUnauthorized
}

func (m *Manager) currentWindow(id string, now time.Time) *window {
	minute := now.Truncate(time.Minute)
	w := m.windows[id]
	if w == nil || !w.at.Equal(minute) {
		w = &window{at: minute}
		m.windows[id] = w
	}
	return w
}

func RetryAfter(now time.Time) int {
	d := now.Truncate(time.Minute).Add(time.Minute).Sub(now)
	return int((d + time.Second - 1) / time.Second)
}

func (m *Manager) save(keys []record) error {
	b, e := json.Marshal(persisted{Keys: keys})
	if e != nil {
		return e
	}
	return m.db.Set([]byte(stateKey), b, pebble.Sync)
}
