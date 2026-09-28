package control

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/bluesky-social/jetstream/internal/hypercerts/archivekeys"
	"github.com/bluesky-social/jetstream/internal/hypercerts/jobs"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

func TestArchiveKeyControlLifecycleAndFlag(t *testing.T) {
	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	policy, err := selection.Open(db, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	jobsManager, err := jobs.Open(db, policy)
	require.NoError(t, err)
	keys, err := archivekeys.Open(db)
	require.NoError(t, err)
	disabled, err := New(testToken, jobsManager, policy)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, request(disabled, "GET", "/archive-keys", "", testToken).Code)

	h, err := NewWithArchiveKeys(testToken, jobsManager, policy, keys)
	require.NoError(t, err)
	require.Equal(t, http.StatusUnauthorized, request(h, "GET", "/archive-keys", "", "").Code)
	require.Equal(t, http.StatusBadRequest, request(h, "POST", "/archive-keys", `{}`, testToken).Code)
	created := request(h, "POST", "/archive-keys", `{"name":"fixture","owner":"team","requestsPerMinute":2,"archiveMegabytesPerMinute":1}`, testToken)
	require.Equal(t, http.StatusCreated, created.Code)
	require.Equal(t, "no-store", created.Header().Get("Cache-Control"))
	var response struct {
		Key   archivekeys.Key `json:"key"`
		Token string          `json:"token"`
	}
	require.NoError(t, json.Unmarshal(created.Body.Bytes(), &response))
	require.NotEmpty(t, response.Token)
	require.Equal(t, "team", response.Key.Owner)
	require.NoError(t, keys.Authorize(response.Token))
	listed := request(h, "GET", "/archive-keys", "", testToken)
	require.Equal(t, http.StatusOK, listed.Code)
	require.NotContains(t, listed.Body.String(), response.Token)
	require.Equal(t, http.StatusNoContent, request(h, "DELETE", "/archive-keys/"+response.Key.ID, "", testToken).Code)
	require.Equal(t, http.StatusNoContent, request(h, "DELETE", "/archive-keys/"+response.Key.ID, "", testToken).Code)
	require.ErrorIs(t, keys.Authorize(response.Token), archivekeys.ErrUnauthorized)
	require.Equal(t, http.StatusNotFound, request(h, "DELETE", "/archive-keys/unknown", "", testToken).Code)
}
