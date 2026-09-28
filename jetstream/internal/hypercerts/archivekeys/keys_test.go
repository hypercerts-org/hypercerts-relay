package archivekeys

import (
	"errors"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestKeySurvivesRestart(t *testing.T) {
	fs := vfs.NewMem()
	db, e := store.Open("/keys", nil, store.WithFS(fs))
	if e != nil {
		t.Fatal(e)
	}
	m, e := Open(db)
	if e != nil {
		t.Fatal(e)
	}
	_, token, e := m.Create("consumer", "team", 60, 100)
	if e != nil {
		t.Fatal(e)
	}
	if e = db.Close(); e != nil {
		t.Fatal(e)
	}
	db, e = store.Open("/keys", nil, store.WithFS(fs))
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	m, e = Open(db)
	if e != nil {
		t.Fatal(e)
	}
	if e = m.Authorize(token); e != nil {
		t.Fatal(e)
	}
}

func TestRequestAndByteBudgets(t *testing.T) {
	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	m, err := Open(db)
	require.NoError(t, err)
	key, token, err := m.Create("consumer", "same owner", 2, 1)
	require.NoError(t, err)
	require.Equal(t, ErrUnauthorized, m.Authorize("wrong"))
	id, err := m.Authenticate(token)
	require.NoError(t, err)
	require.Equal(t, key.ID, id)
	_, err = m.Authenticate(token)
	require.NoError(t, err)
	require.NoError(t, m.AdmitResponse(id, 600_000))
	require.ErrorIs(t, m.AdmitResponse(id, 400_001), ErrByteLimited)
	require.NoError(t, m.AdmitResponse(id, 400_000))
	require.ErrorIs(t, m.AdmitResponse(id, 0), ErrRequestLimited)
	require.NoError(t, m.Revoke(id))
	require.ErrorIs(t, m.Authorize(token), ErrUnauthorized)
	require.ErrorIs(t, m.AdmitResponse(id, 0), ErrUnauthorized)
}

func TestFailedWritesDoNotChangeLiveKeys(t *testing.T) {
	writeErr := errors.New("injected write failure")
	fault := &store.KeyPrefixFault{Prefix: []byte(stateKey), Op: store.WriteOpSet, Ordinal: 2, Err: writeErr}
	db, err := store.Open(t.TempDir(), nil, store.WithFaultInjector(fault))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	m, err := Open(db)
	require.NoError(t, err)
	key, token, err := m.Create("first", "owner", 10, 1)
	require.NoError(t, err)
	_, _, err = m.Create("second", "owner", 10, 1)
	require.ErrorIs(t, err, writeErr)
	require.Equal(t, []Key{key}, m.List())
	require.NoError(t, m.Authorize(token))
	// The third write succeeds, so the surviving key may be revoked.
	require.NoError(t, m.Revoke(key.ID))
	require.ErrorIs(t, m.Authorize(token), ErrUnauthorized)
}

func TestFailedRevokeKeepsKeyActive(t *testing.T) {
	writeErr := errors.New("injected revoke failure")
	fault := &store.KeyPrefixFault{Prefix: []byte(stateKey), Op: store.WriteOpSet, Ordinal: 2, Err: writeErr}
	db, err := store.Open(t.TempDir(), nil, store.WithFaultInjector(fault))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	m, err := Open(db)
	require.NoError(t, err)
	key, token, err := m.Create("first", "owner", 10, 1)
	require.NoError(t, err)
	require.ErrorIs(t, m.Revoke(key.ID), writeErr)
	require.Nil(t, m.List()[0].RevokedAt)
	require.NoError(t, m.Authorize(token))
}

func TestValidationAndRetryAfter(t *testing.T) {
	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	m, err := Open(db)
	require.NoError(t, err)
	for _, input := range []struct {
		name, owner string
		rpm, mb     int
	}{
		{"", "owner", 10, 1}, {"name", " ", 10, 1}, {"name", "owner", 0, 1}, {"name", "owner", 1, 0},
		{"name", "owner", 100001, 1}, {"name", "owner", 1, 100001},
	} {
		_, _, err := m.Create(input.name, input.owner, input.rpm, input.mb)
		require.ErrorIs(t, err, ErrInvalid)
	}
	require.Empty(t, m.List())
	require.Equal(t, 1, RetryAfter(time.Date(2026, 1, 1, 0, 0, 59, 500_000_000, time.UTC)))
	require.Equal(t, 60, RetryAfter(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)))
}
