package control

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/jobs"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
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
	for _, path := range []string{"/policy", "/sources", "/jobs", "/snapshot-rejections", "/coverage", "/jobs/unknown", "/jobs/unknown/cancel", "/jobs/unknown/retry"} {
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
func seedSnapshotRejections(t *testing.T, manager *jobs.Manager, dids []string, nextPolicy bool) string {
	t.Helper()
	pds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.listRepos":
			w.Header().Set("Content-Type", "application/json")
			entries := make([]string, 0, len(dids))
			for _, did := range dids {
				entries = append(entries, `{"did":"`+did+`","rev":"3l3qo2vutsw2b","head":"head","active":true}`)
			}
			_, _ = w.Write([]byte(`{"repos":[` + strings.Join(entries, ",") + `]}`))
		case "/xrpc/com.atproto.sync.getRepo":
			_, _ = w.Write([]byte{1, 0xff})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(pds.Close)
	job, err := manager.AddSource(pds.URL)
	require.NoError(t, err)
	directory := &identity.Directory{Cache: identity.NewLRUCache(len(dids), time.Hour)}
	for _, did := range dids {
		directory.Cache.Set(t.Context(), "did:"+did, &identity.Identity{DID: atmos.DID(did), Services: map[string]identity.ServiceEndpoint{"atproto_pds": {URL: pds.URL}}})
	}
	processor := jobs.PDSProcessor{Manager: manager, HTTPClient: pds.Client(), Directory: directory}
	run := func(job jobs.Job) {
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- manager.Run(ctx, processor.Run) }()
		require.Eventually(t, func() bool {
			current, getErr := manager.Get(job.ID)
			return getErr == nil && current.State == jobs.Failed
		}, time.Second, time.Millisecond)
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
	}
	run(job)
	if nextPolicy {
		_, err = manager.SetPolicy(t.Context(), 1, []string{})
		require.NoError(t, err)
		var replacement jobs.Job
		for _, candidate := range manager.List() {
			if candidate.PDS == pds.URL && candidate.Policy.Revision == 2 {
				replacement = candidate
				break
			}
		}
		require.NotEmpty(t, replacement.ID)
		run(replacement)
	}
	return pds.URL
}

func TestPrivateSnapshotRejectionsView(t *testing.T) {
	h, manager := setup(t)
	pdsA := seedSnapshotRejections(t, manager, []string{"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"}, true)
	pdsB := seedSnapshotRejections(t, manager, []string{"did:plc:cccccccccccccccccccccccc"}, false)
	require.Len(t, manager.ListSnapshotRejections(), 3)

	first := request(h, "GET", "/snapshot-rejections?limit=1&pds="+url.QueryEscape(pdsA), "", testToken)
	require.Equal(t, 200, first.Code)
	var page struct {
		Rejections []rejectionView `json:"rejections"`
		NextCursor string          `json:"nextCursor"`
	}
	require.NoError(t, json.Unmarshal(first.Body.Bytes(), &page))
	require.Len(t, page.Rejections, 1)
	require.NotEmpty(t, page.NextCursor)
	require.Equal(t, pdsA, page.Rejections[0].PDS)
	require.Equal(t, uint64(1), page.Rejections[0].PolicyRevision)
	require.Equal(t, "direct_pds_snapshot", page.Rejections[0].Kind)
	require.Equal(t, "invalid_repository", page.Rejections[0].Code)
	require.NotContains(t, first.Body.String(), "snapshotRejections")
	require.NotContains(t, first.Body.String(), "completedRepos")

	second := request(h, "GET", "/snapshot-rejections?limit=1&pds="+url.QueryEscape(pdsA)+"&after="+url.QueryEscape(page.NextCursor), "", testToken)
	require.Equal(t, 200, second.Code)
	var next struct {
		Rejections []rejectionView `json:"rejections"`
		NextCursor string          `json:"nextCursor"`
	}
	require.NoError(t, json.Unmarshal(second.Body.Bytes(), &next))
	require.Len(t, next.Rejections, 1)
	require.Empty(t, next.NextCursor)
	require.NotEqual(t, page.Rejections[0].PolicyRevision, next.Rejections[0].PolicyRevision)

	filtered := request(h, "GET", "/snapshot-rejections?pds="+url.QueryEscape(pdsB), "", testToken)
	require.Equal(t, 200, filtered.Code)
	var result struct {
		Rejections []rejectionView `json:"rejections"`
	}
	require.NoError(t, json.Unmarshal(filtered.Body.Bytes(), &result))
	require.Len(t, result.Rejections, 1)
	require.Equal(t, pdsB, result.Rejections[0].PDS)
	require.Equal(t, 400, request(h, "GET", "/snapshot-rejections?after=not-a-cursor", "", testToken).Code)
}

func TestPrivateSnapshotRejectionQueryBounds(t *testing.T) {
	h, _ := setup(t)
	validPDS := strings.Repeat("p", maxSnapshotRejectionPDSLength)
	require.Equal(t, 200, request(h, "GET", "/snapshot-rejections?pds="+url.QueryEscape(validPDS), "", testToken).Code)
	require.Equal(t, 400, request(h, "GET", "/snapshot-rejections?pds="+url.QueryEscape(validPDS+"p"), "", testToken).Code)

	atLimit := url.Values{}
	for range maxSnapshotRejectionPDSFilters {
		atLimit.Add("pds", "https://pds.example")
	}
	require.Equal(t, 200, request(h, "GET", "/snapshot-rejections?"+atLimit.Encode(), "", testToken).Code)
	atLimit.Add("pds", "https://one-too-many.example")
	require.Equal(t, 400, request(h, "GET", "/snapshot-rejections?"+atLimit.Encode(), "", testToken).Code)

	overlongAfter := strings.Repeat("A", maxSnapshotRejectionAfterLength+1)
	_, ok := parseRejectionCursor(overlongAfter)
	require.False(t, ok, "oversized encoded cursors must be rejected before decoding")
	require.Equal(t, 400, request(h, "GET", "/snapshot-rejections?after="+overlongAfter, "", testToken).Code)
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
	require.Equal(t, "current_state", incomplete.Coverage)
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
	coverage := request(h, "GET", "/coverage", "", testToken)
	require.Equal(t, 200, coverage.Code)
	var coveragePage struct {
		Items []coverageView `json:"items"`
	}
	require.NoError(t, json.Unmarshal(coverage.Body.Bytes(), &coveragePage))
	require.Len(t, coveragePage.Items, 1)
	require.Equal(t, "https://pds.example", coveragePage.Items[0].PDS)
	require.Equal(t, uint64(2), coveragePage.Items[0].Policy.Revision)
	require.Equal(t, "policy_changed", coveragePage.Items[0].Reason)
	_, err := m.AddSource("https://other.example")
	require.NoError(t, err)
	filtered := request(h, "GET", "/coverage?summary=1&pds=https%3A%2F%2Fpds.example&pds=https%3A%2F%2Fother.example", "", testToken)
	require.Equal(t, 200, filtered.Code)
	var summaries struct {
		Items []coverageSummaryView `json:"items"`
	}
	require.NoError(t, json.Unmarshal(filtered.Body.Bytes(), &summaries))
	require.Len(t, summaries.Items, 2)
	require.Equal(t, "https://other.example", summaries.Items[0].PDS)
	require.Equal(t, "https://pds.example", summaries.Items[1].PDS)
	require.NotContains(t, filtered.Body.String(), "policy")
	require.Equal(t, 204, request(h, "DELETE", "/sources", `{"pds":"https://pds.example"}`, testToken).Code)
}

func TestCoverageSelectionPaginationAndSummary(t *testing.T) {
	created := time.Date(2026, time.September, 15, 12, 0, 0, 0, time.UTC)
	jobsList := []jobs.Job{
		{ID: "a-old", PDS: "https://a.example", CreatedAt: created},
		{ID: "a-new", PDS: "https://a.example", CreatedAt: created.Add(time.Second)},
		{ID: "b-a", PDS: "https://b.example", CreatedAt: created},
		{ID: "b-z", PDS: "https://b.example", CreatedAt: created},
		{ID: "c-only", PDS: "https://c.example", CreatedAt: created},
	}

	options, ok := parseCoverageOptions(httptest.NewRequest("GET", Prefix+"/coverage?limit=1&pds=https%3A%2F%2Fa.example&pds=https%3A%2F%2Fb.example&summary=1", nil))
	require.True(t, ok)
	require.True(t, options.summary)
	selected := latestCoverageJobs(jobsList, options.requestedPDS)
	require.Equal(t, []string{"a-new", "b-z"}, []string{selected[0].ID, selected[1].ID})

	page, next := pageCoverage(selected, options.after, options.limit)
	require.Len(t, page, 1)
	require.Equal(t, "https://a.example", page[0].PDS)
	require.Equal(t, "https://a.example", next)
	require.Equal(t, "a-new", coverageSummaryPage(page, next).Items[0].JobID)

	options, ok = parseCoverageOptions(httptest.NewRequest("GET", Prefix+"/coverage?limit=0", nil))
	require.False(t, ok)
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
			require.Equal(t, "current_state", got.Coverage)
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

// Exposes the real persistent management contract to the Node acceptance harness.
func TestControlPlaneAcceptanceFixture(t *testing.T) {
	ready := os.Getenv("CONTROL_ACCEPTANCE_READY")
	if ready == "" {
		t.Skip("cross-language fixture")
	}
	handler, manager := setup(t)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- manager.Run(ctx, func(context.Context, jobs.Job) error {
			return &jobs.InputError{Code: "source_unavailable", Unavailable: true}
		})
	}()
	defer func() { cancel(); <-done }()
	server := httptest.NewServer(handler)
	defer server.Close()
	require.NoError(t, os.WriteFile(ready, []byte(server.URL), 0600))
	deadline := time.NewTimer(2 * time.Minute)
	defer deadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline.C:
			t.Fatal("acceptance harness did not shut down")
		case <-ticker.C:
			if _, err := os.Stat(ready + ".stop"); err == nil {
				return
			}
		}
	}
}
