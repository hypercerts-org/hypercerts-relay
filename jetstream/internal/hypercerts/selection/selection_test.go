package selection

import (
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDurablePolicyEmptyAndRestart(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(dir, nil)
	require.NoError(t, err)
	m, err := Open(db, nil)
	require.NoError(t, err)
	require.False(t, m.Allows(&segment.Event{Kind: segment.KindCreate, Collection: "app.bsky.feed.post"}))
	for _, kind := range []segment.Kind{segment.KindSync, segment.KindAccount, segment.KindIdentity} {
		require.True(t, m.Allows(&segment.Event{Kind: kind}))
	}
	require.NoError(t, db.Close())
	db, err = store.Open(dir, nil)
	require.NoError(t, err)
	defer db.Close()
	m, err = Open(db, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	require.Equal(t, uint64(1), m.Current().Revision)
	require.Empty(t, m.Current().Collections, "startup configuration cannot overwrite a durable policy")
}

func TestRejectsPrefixesAndCorruption(t *testing.T) {
	_, err := Normalize([]string{"app.bsky.*"})
	require.Error(t, err)
	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.Set([]byte(key), []byte(`{"revision":0}`), store.SyncWrites))
	_, err = Open(db, nil)
	require.Error(t, err)
}

func TestBundledSeedCanBeClearedAndStaysCleared(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(dir, nil)
	require.NoError(t, err)
	seed := DefaultCollections()
	normalized, err := Normalize(seed)
	require.NoError(t, err)
	require.Equal(t, normalized, seed)
	require.Contains(t, seed, "org.hypercerts.claim.activity")
	require.Contains(t, seed, "app.certified.actor.profile")
	require.NotContains(t, seed, "org.hypercerts.defs")
	require.NotContains(t, seed, "app.certified.authWrite")
	require.NotContains(t, seed, "org.hypercerts.workscope.cel")
	m, err := Open(db, seed)
	require.NoError(t, err)
	require.True(t, m.Allows(&segment.Event{Kind: segment.KindCreate, Collection: "org.hypercerts.claim.activity"}))
	require.False(t, m.Allows(&segment.Event{Kind: segment.KindCreate, Collection: "app.bsky.feed.post"}))
	_, err = m.Update(1, nil, nil)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	db, err = store.Open(dir, nil)
	require.NoError(t, err)
	defer db.Close()
	m, err = Open(db, DefaultCollections())
	require.NoError(t, err)
	require.Empty(t, m.Current().Collections)
	require.Equal(t, uint64(2), m.Current().Revision)
}
