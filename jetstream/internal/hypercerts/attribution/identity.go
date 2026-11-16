package attribution

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/cockroachdb/pebble"
)

const selectedStreamIdentityKey = "hypercerts/selected-stream/v1/identity"

var identityLocks sync.Map

type identityLockKey struct{ db *store.Store }

// OpenSelectedStreamIdentity atomically loads or initializes the persistent
// identity of the selected event stream. A corrupt value is never regenerated.
func OpenSelectedStreamIdentity(db *store.Store) (string, error) {
	return openIdentity(db, selectedStreamIdentityKey, "selected stream")
}

func openIdentity(db *store.Store, key, name string) (string, error) {
	if db == nil {
		return "", fmt.Errorf("%w: metadata store required", ErrInvalid)
	}
	lock, _ := identityLocks.LoadOrStore(identityLockKey{db}, &sync.Mutex{})
	mu := lock.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	value, closer, err := db.Get([]byte(key))
	if err == nil {
		defer closer.Close()
		id := string(value)
		decoded, decodeErr := base64.RawURLEncoding.DecodeString(id)
		if decodeErr != nil || len(decoded) != 32 || base64.RawURLEncoding.EncodeToString(decoded) != id {
			return "", fmt.Errorf("%w: corrupt %s identity", ErrInvalid, name)
		}
		return id, nil
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return "", fmt.Errorf("read %s identity: %w", name, err)
	}

	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("generate %s identity: %w", name, err)
	}
	id := base64.RawURLEncoding.EncodeToString(random[:])
	if err := db.Set([]byte(key), []byte(id), store.SyncWrites); err != nil {
		return "", fmt.Errorf("persist %s identity: %w", name, err)
	}
	return id, nil
}
