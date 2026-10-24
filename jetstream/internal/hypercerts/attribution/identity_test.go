package attribution

import (
	"encoding/base64"
	"testing"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

func TestOpenSelectedStreamIdentityPersistsAndRejectsCorruption(t *testing.T) {
	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	first, err := OpenSelectedStreamIdentity(db)
	require.NoError(t, err)
	decoded, err := base64.RawURLEncoding.DecodeString(first)
	require.NoError(t, err)
	require.Len(t, decoded, 32)
	second, err := OpenSelectedStreamIdentity(db)
	require.NoError(t, err)
	require.Equal(t, first, second)

	require.NoError(t, db.Set([]byte(selectedStreamIdentityKey), []byte("corrupt"), store.SyncWrites))
	_, err = OpenSelectedStreamIdentity(db)
	require.ErrorIs(t, err, ErrInvalid)
}
