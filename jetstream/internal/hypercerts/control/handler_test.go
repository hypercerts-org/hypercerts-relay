package control

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/jobs"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

const testToken = "fixture-service-credential-32-bytes-minimum"

func setup(t *testing.T) (*Handler, *jobs.Manager) {
	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	policy, err := selection.Open(db, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	manager, err := jobs.Open(db, policy)
	require.NoError(t, err)
	handler, err := New(testToken, manager, policy)
	require.NoError(t, err)
	return handler, manager
}
func request(h http.Handler, method, path, body, token string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, Prefix+path, strings.NewReader(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func TestPrivateAuthenticationAndInputValidation(t *testing.T) {
	h, m := setup(t)
	for _, path := range []string{"/policy", "/sources", "/jobs", "/jobs/unknown", "/jobs/unknown/cancel", "/jobs/unknown/retry"} {
		for _, method := range []string{"GET", "POST", "PUT", "DELETE"} {
			w := request(h, method, path, `{}`, "")
			require.Equal(t, 401, w.Code)
			require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
		}
	}
	require.Empty(t, m.List())
	for _, body := range []string{`{}`, `{"collections":null}`, `{"expectedRevision":1,"collections":["app.bsky.*"]}`, `{"expectedRevision":1,"collections":[],"unknown":1}`, `{"expectedRevision":1,"collections":[]} {}`, strings.Repeat(" ", 64<<10) + `{}`} {
		require.Equal(t, 400, request(h, "PUT", "/policy", body, testToken).Code)
	}
	require.Equal(t, 409, request(h, "PUT", "/policy", `{"expectedRevision":9,"collections":[]}`, testToken).Code)
	require.Equal(t, 400, request(h, "POST", "/sources", `{"pds":"http://outside.example"}`, testToken).Code)
	require.Equal(t, 400, request(h, "GET", "/jobs?limit=999", "", testToken).Code)
	require.Equal(t, 404, request(h, "GET", "/jobs/missing", "", testToken).Code)
	_, err := New("", m, h.policy)
	require.Error(t, err)
}
func TestPrivateLifecycleAndCoverage(t *testing.T) {
	h, m := setup(t)
	w := request(h, "POST", "/sources", `{"pds":"https://pds.example"}`, testToken)
	require.Equal(t, 202, w.Code)
	var pending jobView
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &pending))
	require.Equal(t, jobs.Pending, pending.State)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	started := make(chan struct{})
	release := make(chan struct{})
	go func() {
		done <- m.Run(ctx, func(ctx context.Context, j jobs.Job) error {
			close(started)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
				return &jobs.InputError{Code: "source_unavailable", Unavailable: true}
			}
		})
	}()
	<-started
	var running jobView
	require.NoError(t, json.Unmarshal(request(h, "GET", "/jobs/"+pending.ID, "", testToken).Body.Bytes(), &running))
	require.Equal(t, jobs.Running, running.State)
	require.Equal(t, 1, running.Attempts)
	close(release)
	require.Eventually(t, func() bool { return m.List()[0].State == jobs.Incomplete }, time.Second, time.Millisecond)
	cancel()
	<-done
	var incomplete jobView
	require.NoError(t, json.Unmarshal(request(h, "GET", "/jobs/"+pending.ID, "", testToken).Body.Bytes(), &incomplete))
	require.Equal(t, jobs.Incomplete, incomplete.State)
	require.Equal(t, "source_unavailable", incomplete.ErrorCode)
	require.False(t, incomplete.HistoryComplete)
	require.Equal(t, "https://pds.example", incomplete.PDS)
	require.Equal(t, uint64(1), incomplete.Policy.Revision)
	require.Equal(t, 200, request(h, "POST", "/jobs/"+pending.ID+"/retry", "", testToken).Code)
	require.Equal(t, 200, request(h, "POST", "/jobs/"+pending.ID+"/cancel", "", testToken).Code)
	require.Equal(t, jobs.Canceled, m.List()[0].State)
	require.Equal(t, 200, request(h, "PUT", "/policy", `{"expectedRevision":1,"collections":[]}`, testToken).Code)
	require.Len(t, m.List(), 2)
	page := request(h, "GET", "/jobs?limit=1", "", testToken)
	require.Equal(t, 200, page.Code)
	var result struct {
		Jobs       []jobView `json:"jobs"`
		NextCursor string    `json:"nextCursor"`
	}
	require.NoError(t, json.Unmarshal(page.Body.Bytes(), &result))
	require.Len(t, result.Jobs, 1)
	require.NotEmpty(t, result.NextCursor)
	require.Equal(t, 204, request(h, "DELETE", "/sources", `{"pds":"https://pds.example"}`, testToken).Code)
}
func TestPrivateCompleteAndFailedResults(t *testing.T) {
	for _, state := range []jobs.State{jobs.Complete, jobs.Failed} {
		t.Run(string(state), func(t *testing.T) {
			h, m := setup(t)
			j, err := m.AddSource("https://pds.example")
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() {
				done <- m.Run(ctx, func(ctx context.Context, j jobs.Job) error {
					if state == jobs.Failed {
						return &jobs.InputError{Code: "invalid_repository"}
					}
					return m.Checkpoint(j.ID, "did:plc:fixture", "3l3qo2vutsw2b", "")
				})
			}()
			require.Eventually(t, func() bool { return m.List()[0].State == state }, time.Second, time.Millisecond)
			cancel()
			<-done
			var got jobView
			require.NoError(t, json.Unmarshal(request(h, "GET", "/jobs/"+j.ID, "", testToken).Body.Bytes(), &got))
			require.Equal(t, state, got.State)
			require.Equal(t, j.Policy, got.Policy)
			require.Equal(t, j.PDS, got.PDS)
			require.False(t, got.HistoryComplete)
			if state == jobs.Complete {
				require.Equal(t, 1, got.CompletedRepos)
				require.Equal(t, "current_state", got.Coverage)
			}
		})
	}
}

func TestPrivatePersistenceFailureDoesNotAcknowledgeOrLeak(t *testing.T) {
	fault := &store.KeyPrefixFault{Prefix: []byte("hypercerts/backfill-jobs"), Ordinal: 1, Err: errors.New("fixture-private-database-detail")}
	db, err := store.Open(t.TempDir(), nil, store.WithFaultInjector(fault))
	require.NoError(t, err)
	defer db.Close()
	policy, err := selection.Open(db, nil)
	require.NoError(t, err)
	manager, err := jobs.Open(db, policy)
	require.NoError(t, err)
	h, err := New(testToken, manager, policy)
	require.NoError(t, err)
	w := request(h, "POST", "/sources", `{"pds":"https://pds.example"}`, testToken)
	require.Equal(t, 500, w.Code)
	require.NotContains(t, w.Body.String(), "fixture-private-database-detail")
	require.Empty(t, manager.Sources())
	require.Empty(t, manager.List())
	require.Equal(t, 202, request(h, "POST", "/sources", `{"pds":"https://pds.example"}`, testToken).Code)
}

func TestPrivatePolicyAndJobsRemainAtomicOnWriteFailure(t *testing.T) {
	fault := &store.KeyPrefixFault{Prefix: []byte("hypercerts/backfill-jobs"), Ordinal: 2, Err: errors.New("fixture write failure")}
	db, err := store.Open(t.TempDir(), nil, store.WithFaultInjector(fault))
	require.NoError(t, err)
	defer db.Close()
	policy, err := selection.Open(db, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	manager, err := jobs.Open(db, policy)
	require.NoError(t, err)
	_, err = manager.AddSource("https://pds.example")
	require.NoError(t, err)
	h, err := New(testToken, manager, policy)
	require.NoError(t, err)
	require.Equal(t, 500, request(h, "PUT", "/policy", `{"expectedRevision":1,"collections":[]}`, testToken).Code)
	require.Equal(t, uint64(1), policy.Current().Revision)
	require.Len(t, manager.List(), 1)
	require.Equal(t, jobs.Pending, manager.List()[0].State)
	restored, err := selection.Open(db, nil)
	require.NoError(t, err)
	require.Equal(t, policy.Current(), restored.Current())
	require.Equal(t, 200, request(h, "PUT", "/policy", `{"expectedRevision":1,"collections":[]}`, testToken).Code)
}
