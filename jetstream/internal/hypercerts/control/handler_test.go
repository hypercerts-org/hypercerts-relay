package control

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/jobs"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/jcalabro/atmos"
	atmoscrypto "github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/mst"
	"github.com/jcalabro/atmos/repo"
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

func TestLegacyKnownTotalJobWithoutInventoryRemainsReadable(t *testing.T) {
	const (
		legacyDID = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
		revision  = "3l3qo2vutsw2b"
	)
	dir := t.TempDir()
	db, err := store.Open(dir, nil)
	require.NoError(t, err)
	policy, err := selection.Open(db, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	manager, err := jobs.Open(db, policy)
	require.NoError(t, err)
	legacyJob, err := manager.AddSource("https://legacy.example")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- manager.Run(ctx, func(_ context.Context, running jobs.Job) error {
			if running.ID != legacyJob.ID {
				return jobs.ErrConflict
			}
			if err := manager.SetTotalRepos(running.ID, 3); err != nil {
				return err
			}
			if err := manager.Checkpoint(running.ID, legacyDID, revision, ""); err != nil {
				return err
			}
			return &jobs.InputError{Code: "source_unavailable", Unavailable: true}
		})
	}()
	require.Eventually(t, func() bool {
		current, getErr := manager.Get(legacyJob.ID)
		return getErr == nil && current.State == jobs.Incomplete
	}, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.NoError(t, db.Close())

	db, err = store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	policy, err = selection.Open(db, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	manager, err = jobs.Open(db, policy)
	require.NoError(t, err)
	h, err := New(testToken, manager, policy)
	require.NoError(t, err)

	detail := request(h, "GET", "/jobs/"+legacyJob.ID, "", testToken)
	require.Equal(t, http.StatusOK, detail.Code)
	var detailView jobView
	require.NoError(t, json.Unmarshal(detail.Body.Bytes(), &detailView))
	require.Equal(t, 1, detailView.CompletedRepos)
	require.Equal(t, 3, detailView.TotalRepos)
	require.True(t, detailView.TotalReposKnown)
	require.Equal(t, jobs.JobExecutionStopped, detailView.Diagnostics.Execution)
	require.Nil(t, detailView.Diagnostics.UnresolvedRepos)
	require.Nil(t, detailView.Diagnostics.RetryingRepos)

	listing := request(h, "GET", "/jobs?limit=10", "", testToken)
	require.Equal(t, http.StatusOK, listing.Code)
	var listPage struct {
		Jobs []jobView `json:"jobs"`
	}
	require.NoError(t, json.Unmarshal(listing.Body.Bytes(), &listPage))
	require.Len(t, listPage.Jobs, 1)
	require.Equal(t, 1, listPage.Jobs[0].CompletedRepos)
	require.Equal(t, 3, listPage.Jobs[0].TotalRepos)
	require.True(t, listPage.Jobs[0].TotalReposKnown)
	require.Nil(t, listPage.Jobs[0].Diagnostics.UnresolvedRepos)

	coverage := request(h, "GET", "/coverage", "", testToken)
	require.Equal(t, http.StatusOK, coverage.Code)
	var coveragePage struct {
		Items []coverageView `json:"items"`
	}
	require.NoError(t, json.Unmarshal(coverage.Body.Bytes(), &coveragePage))
	require.Len(t, coveragePage.Items, 1)
	require.Equal(t, 1, coveragePage.Items[0].CompletedRepos)
	require.Equal(t, 3, coveragePage.Items[0].TotalRepos)
	require.True(t, coveragePage.Items[0].TotalReposKnown)
	require.Nil(t, coveragePage.Items[0].Diagnostics.UnresolvedRepos)

	repositoryDetails := request(h, "GET", "/jobs/"+legacyJob.ID+"/repositories?limit=10", "", testToken)
	require.Equal(t, http.StatusConflict, repositoryDetails.Code)

	// An absent legacy inventory is readable, but a present inventory whose
	// count contradicts the durable total remains a persistence error.
	require.NoError(t, db.Set([]byte("hypercerts/backfill-job-inventory/"+legacyJob.ID), []byte(`{"entries":{}}`), store.SyncWrites))
	corrupt := request(h, "GET", "/jobs/"+legacyJob.ID, "", testToken)
	require.Equal(t, http.StatusInternalServerError, corrupt.Code)
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
	require.Equal(t, jobs.JobExecutionQueued, summaries.Items[0].Diagnostics.Execution)
	require.Nil(t, summaries.Items[0].Diagnostics.UnresolvedRepos)
	require.Equal(t, "https://pds.example", summaries.Items[1].PDS)
	require.NotContains(t, filtered.Body.String(), "policy")
	require.Equal(t, 204, request(h, "DELETE", "/sources", `{"pds":"https://pds.example"}`, testToken).Code)
}

// TestT10CoverageTruthfulness proves a configured source is not silently
// promoted to complete: it has no coverage before a job, and an unavailable
// current-state acquisition stays explicitly incomplete. Historical provenance
// is intentionally not inferred by this owner.
func TestJobDiagnosticsUseSchedulerEligibilityAndTerminalState(t *testing.T) {
	h, manager := setup(t)
	unknown, err := manager.AddSource("https://unknown.example")
	require.NoError(t, err)
	unknownResponse := request(h, "GET", "/jobs/"+unknown.ID, "", testToken)
	require.Equal(t, http.StatusOK, unknownResponse.Code)
	var unknownView map[string]any
	require.NoError(t, json.Unmarshal(unknownResponse.Body.Bytes(), &unknownView))
	unknownDiagnostics, ok := unknownView["diagnostics"].(map[string]any)
	require.True(t, ok, "job view must include manager-owned diagnostics")
	require.Equal(t, "queued", unknownDiagnostics["execution"])
	require.NotContains(t, unknownDiagnostics, "unresolvedRepos", "an unfinished inventory is unknown, not zero")
	unknownCoverage := request(h, "GET", "/coverage?pds=https%3A%2F%2Funknown.example", "", testToken)
	require.Equal(t, http.StatusOK, unknownCoverage.Code)
	var unknownCoveragePage struct {
		Items []coverageView `json:"items"`
	}
	require.NoError(t, json.Unmarshal(unknownCoverage.Body.Bytes(), &unknownCoveragePage))
	require.Len(t, unknownCoveragePage.Items, 1)
	require.Equal(t, jobs.JobExecutionQueued, unknownCoveragePage.Items[0].Diagnostics.Execution)
	require.Nil(t, unknownCoveragePage.Items[0].Diagnostics.UnresolvedRepos)
	require.Equal(t, http.StatusConflict, request(h, "GET", "/jobs/"+unknown.ID+"/repositories", "", testToken).Code)
	require.NoError(t, manager.Cancel(unknown.ID))

	listing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/xrpc/com.atproto.sync.listRepos" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(listing.Close)
	listingJob, err := manager.AddSource(listing.URL)
	require.NoError(t, err)
	processor := jobs.PDSProcessor{
		Manager:    manager,
		HTTPClient: listing.Client(),
		Directory:  &identity.Directory{Cache: identity.NewLRUCache(1, time.Hour)},
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx, processor.Run) }()
	require.Eventually(t, func() bool {
		current, getErr := manager.Get(listingJob.ID)
		return getErr == nil && current.State == jobs.Incomplete
	}, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	cooldown, found, err := manager.GetPDSCooldown(listing.URL)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, cooldown.Until.After(time.Now()))

	cooldownJob, err := manager.Request(listing.URL, "quota_recovery")
	require.NoError(t, err)
	cooldownResponse := request(h, "GET", "/jobs/"+cooldownJob.ID, "", testToken)
	require.Equal(t, http.StatusOK, cooldownResponse.Code)
	var cooldownView map[string]any
	require.NoError(t, json.Unmarshal(cooldownResponse.Body.Bytes(), &cooldownView))
	cooldownDiagnostics, ok := cooldownView["diagnostics"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "waiting", cooldownDiagnostics["execution"])
	require.Equal(t, "pds_cooldown", cooldownDiagnostics["reason"])
	require.NotContains(t, cooldownDiagnostics, "unresolvedRepos", "listing 429 did not produce a frozen inventory")
	require.NotEmpty(t, cooldownDiagnostics["pdsCooldownUntil"])

	mixed, err := manager.AddSource("https://mixed.example")
	require.NoError(t, err)
	const (
		retryingDID   = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
		readyDID      = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"
		rejectedDID   = "did:plc:cccccccccccccccccccccccc"
		exhaustedDID  = "did:plc:dddddddddddddddddddddddd"
		completedDID  = "did:plc:eeeeeeeeeeeeeeeeeeeeeeee"
		revision      = "3l3qo2vutsw2b"
		newerRevision = "3l3qo2vutsw2c"
	)
	ctx, cancel = context.WithCancel(t.Context())
	done = make(chan error, 1)
	go func() {
		done <- manager.Run(ctx, func(_ context.Context, job jobs.Job) error {
			if job.ID != mixed.ID {
				return &jobs.InputError{Code: "source_unavailable", Unavailable: true}
			}
			if err := manager.CheckpointInventory(job.ID, "", map[string]string{
				retryingDID: revision, readyDID: revision, rejectedDID: revision,
				exhaustedDID: revision, completedDID: revision,
			}, true); err != nil {
				return err
			}
			if _, err := manager.BeginRepositoryAttempt(job.ID, retryingDID); err != nil {
				return err
			}
			retryAt := time.Now().UTC().Add(time.Hour)
			if _, err := manager.RecordRepositoryFailure(job.ID, retryingDID, jobs.RepositoryRetryFailure{
				Category:   jobs.RepositoryFailureHTTP,
				HTTPStatus: http.StatusServiceUnavailable,
				Stage:      jobs.RepositoryFailureGetRepoRequest,
				Code:       "source_unavailable",
			}, &retryAt, nil); err != nil {
				return err
			}
			if _, err := manager.BeginRepositoryAttempt(job.ID, completedDID); err != nil {
				return err
			}
			if err := manager.CheckpointRepository(job.ID, completedDID, revision, newerRevision, ""); err != nil {
				return err
			}
			if _, err := manager.BeginRepositoryAttempt(job.ID, rejectedDID); err != nil {
				return err
			}
			if _, err := manager.RecordRepositoryFailure(job.ID, rejectedDID, jobs.RepositoryRetryFailure{
				Category: jobs.RepositoryFailureRejected,
				Stage:    jobs.RepositoryFailureGetRepoBody,
				Code:     "verification_failed",
			}, nil, nil); err != nil {
				return err
			}
			for attempt := 1; attempt <= 3; attempt++ {
				if _, err := manager.BeginRepositoryAttempt(job.ID, exhaustedDID); err != nil {
					return err
				}
				var nextRetry *time.Time
				if attempt < 3 {
					past := time.Now().UTC().Add(-time.Second)
					nextRetry = &past
				}
				if _, err := manager.RecordRepositoryFailure(job.ID, exhaustedDID, jobs.RepositoryRetryFailure{
					Category:   jobs.RepositoryFailureHTTP,
					HTTPStatus: http.StatusServiceUnavailable,
					Stage:      jobs.RepositoryFailureGetRepoRequest,
					Code:       "source_unavailable",
				}, nextRetry, nil); err != nil {
					return err
				}
			}
			return &jobs.InputError{Code: "source_unavailable", Unavailable: true}
		})
	}()
	require.Eventually(t, func() bool {
		current, getErr := manager.Get(mixed.ID)
		return getErr == nil && current.State == jobs.Incomplete
	}, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	stoppedResponse := request(h, "GET", "/jobs/"+mixed.ID, "", testToken)
	require.Equal(t, http.StatusOK, stoppedResponse.Code)
	var stoppedView map[string]any
	require.NoError(t, json.Unmarshal(stoppedResponse.Body.Bytes(), &stoppedView))
	stoppedDiagnostics, ok := stoppedView["diagnostics"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "stopped", stoppedDiagnostics["execution"])
	require.EqualValues(t, 1, stoppedView["completedRepos"], "a newer checkpoint counts as complete for its frozen coordinate")
	require.EqualValues(t, 5, stoppedView["totalRepos"])
	require.EqualValues(t, 4, stoppedDiagnostics["unresolvedRepos"], "a newer completed checkpoint satisfies its older listing")
	require.EqualValues(t, 0, stoppedDiagnostics["retryingRepos"], "terminal jobs do not present persisted wait rows as active retries")

	firstDetails := request(h, "GET", "/jobs/"+mixed.ID+"/repositories?limit=1", "", testToken)
	require.Equal(t, http.StatusOK, firstDetails.Code)
	var repositoryPage struct {
		Job struct {
			State          jobs.State          `json:"state"`
			CompletedRepos int                 `json:"completedRepos"`
			TotalRepos     int                 `json:"totalRepos"`
			Diagnostics    jobs.JobDiagnostics `json:"diagnostics"`
		} `json:"job"`
		Repositories []map[string]any `json:"repositories"`
		NextCursor   string           `json:"nextCursor"`
	}
	require.NoError(t, json.Unmarshal(firstDetails.Body.Bytes(), &repositoryPage))
	require.Equal(t, jobs.Incomplete, repositoryPage.Job.State)
	require.Equal(t, 1, repositoryPage.Job.CompletedRepos)
	require.Equal(t, 5, repositoryPage.Job.TotalRepos)
	require.NotNil(t, repositoryPage.Job.Diagnostics.UnresolvedRepos)
	require.Equal(t, 4, *repositoryPage.Job.Diagnostics.UnresolvedRepos)
	require.Equal(t, repositoryPage.Job.TotalRepos, repositoryPage.Job.CompletedRepos+*repositoryPage.Job.Diagnostics.UnresolvedRepos)
	require.Len(t, repositoryPage.Repositories, 1)
	require.Equal(t, retryingDID, repositoryPage.Repositories[0]["did"])
	require.Equal(t, revision, repositoryPage.Repositories[0]["listedRevision"])
	require.Equal(t, "retry_wait", repositoryPage.Repositories[0]["state"])
	require.EqualValues(t, 1, repositoryPage.Repositories[0]["attempts"])
	failure, ok := repositoryPage.Repositories[0]["failure"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "http", failure["category"])
	require.EqualValues(t, http.StatusServiceUnavailable, failure["httpStatus"])
	require.Equal(t, "getRepo/request", failure["stage"])
	require.Equal(t, "source_unavailable", failure["code"])
	require.NotContains(t, firstDetails.Body.String(), "responseBody")
	require.NotEmpty(t, repositoryPage.NextCursor)
	forgedCursor, err := base64.RawURLEncoding.DecodeString(repositoryPage.NextCursor)
	require.NoError(t, err)
	binary.BigEndian.PutUint32(forgedCursor[16:], 2)
	forgedCursorValue := base64.RawURLEncoding.EncodeToString(forgedCursor)
	require.Equal(t, http.StatusBadRequest, request(h, "GET", "/jobs/"+mixed.ID+"/repositories?limit=1&after="+url.QueryEscape(forgedCursorValue), "", testToken).Code)
	require.Equal(t, http.StatusBadRequest, request(h, "GET", "/jobs/"+unknown.ID+"/repositories?limit=1&after="+url.QueryEscape(repositoryPage.NextCursor), "", testToken).Code)
	secondDetails := request(h, "GET", "/jobs/"+mixed.ID+"/repositories?limit=1&after="+url.QueryEscape(repositoryPage.NextCursor), "", testToken)
	require.Equal(t, http.StatusOK, secondDetails.Code)
	var nextRepositoryPage struct {
		Repositories []map[string]any `json:"repositories"`
		NextCursor   string           `json:"nextCursor"`
	}
	require.NoError(t, json.Unmarshal(secondDetails.Body.Bytes(), &nextRepositoryPage))
	require.Len(t, nextRepositoryPage.Repositories, 1)
	require.Equal(t, readyDID, nextRepositoryPage.Repositories[0]["did"])
	require.Equal(t, "ready", nextRepositoryPage.Repositories[0]["state"])
	require.EqualValues(t, 0, nextRepositoryPage.Repositories[0]["attempts"])
	remainingDetails := request(h, "GET", "/jobs/"+mixed.ID+"/repositories?limit=200&after="+url.QueryEscape(nextRepositoryPage.NextCursor), "", testToken)
	require.Equal(t, http.StatusOK, remainingDetails.Code)
	var remainingPage struct {
		Repositories []map[string]any `json:"repositories"`
	}
	require.NoError(t, json.Unmarshal(remainingDetails.Body.Bytes(), &remainingPage))
	require.Len(t, remainingPage.Repositories, 2, "the newer checkpoint is excluded from unresolved details")
	require.Equal(t, rejectedDID, remainingPage.Repositories[0]["did"])
	require.Equal(t, "unresolved", remainingPage.Repositories[0]["state"])
	require.EqualValues(t, 1, remainingPage.Repositories[0]["attempts"])
	rejectedFailure := remainingPage.Repositories[0]["failure"].(map[string]any)
	require.Equal(t, "rejected", rejectedFailure["category"])
	require.Equal(t, "getRepo/body", rejectedFailure["stage"])
	require.Equal(t, "verification_failed", rejectedFailure["code"])
	require.Equal(t, exhaustedDID, remainingPage.Repositories[1]["did"])
	require.EqualValues(t, 3, remainingPage.Repositories[1]["attempts"])
	require.Equal(t, http.StatusBadRequest, request(h, "GET", "/jobs/"+mixed.ID+"/repositories?limit=201", "", testToken).Code)

	require.NoError(t, manager.Retry(mixed.ID))
	mixedResponse := request(h, "GET", "/jobs/"+mixed.ID, "", testToken)
	require.Equal(t, http.StatusOK, mixedResponse.Code)
	var mixedView map[string]any
	require.NoError(t, json.Unmarshal(mixedResponse.Body.Bytes(), &mixedView))
	mixedDiagnostics, ok := mixedView["diagnostics"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "queued", mixedDiagnostics["execution"], "ready repository work is due now despite another future retry")
	require.EqualValues(t, 4, mixedDiagnostics["unresolvedRepos"])
	require.EqualValues(t, 1, mixedDiagnostics["retryingRepos"])
	require.NotEmpty(t, mixedDiagnostics["retryAt"])
	require.EqualValues(t, 3, mixedDiagnostics["maxRepositoryAttempts"])

	require.NoError(t, manager.Cancel(mixed.ID))
	lingering, err := manager.ListRepositoryRetries(mixed.ID)
	require.NoError(t, err)
	require.Len(t, lingering, 1)
	terminalResponse := request(h, "GET", "/jobs/"+mixed.ID, "", testToken)
	require.Equal(t, http.StatusOK, terminalResponse.Code)
	var terminalView map[string]any
	require.NoError(t, json.Unmarshal(terminalResponse.Body.Bytes(), &terminalView))
	terminalDiagnostics, ok := terminalView["diagnostics"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "stopped", terminalDiagnostics["execution"], "a stored retry deadline cannot make a canceled job active")
	terminalCoverage := request(h, "GET", "/coverage?pds=https%3A%2F%2Fmixed.example", "", testToken)
	require.Equal(t, http.StatusOK, terminalCoverage.Code)
	var terminalCoveragePage struct {
		Items []coverageView `json:"items"`
	}
	require.NoError(t, json.Unmarshal(terminalCoverage.Body.Bytes(), &terminalCoveragePage))
	require.Len(t, terminalCoveragePage.Items, 1)
	require.Equal(t, jobs.JobExecutionStopped, terminalCoveragePage.Items[0].Diagnostics.Execution)
	require.Equal(t, 1, terminalCoveragePage.Items[0].CompletedRepos)
	require.Equal(t, 5, terminalCoveragePage.Items[0].TotalRepos)
	require.Equal(t, 4, *terminalCoveragePage.Items[0].Diagnostics.UnresolvedRepos)
	require.Equal(t, terminalCoveragePage.Items[0].TotalRepos, terminalCoveragePage.Items[0].CompletedRepos+*terminalCoveragePage.Items[0].Diagnostics.UnresolvedRepos)
}

func TestRepositoryDetailsOversizedSnapshotReturnsPayloadTooLarge(t *testing.T) {
	h, manager := setup(t)
	job, err := manager.AddSource("https://large-inventory.example")
	require.NoError(t, err)
	entries := make(map[string]string, jobs.MaxRepositoryDetailsSnapshotRows+1)
	for i := 0; i < jobs.MaxRepositoryDetailsSnapshotRows+1; i++ {
		var suffix [24]byte
		for j := range suffix {
			suffix[j] = 'a'
		}
		value := i
		for j := len(suffix) - 1; value > 0; j-- {
			suffix[j] = 'a' + byte(value%26)
			value /= 26
		}
		entries["did:plc:"+string(suffix[:])] = "3l3qo2vutsw2b"
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- manager.Run(ctx, func(_ context.Context, current jobs.Job) error {
			if current.ID != job.ID {
				return &jobs.InputError{Code: "unexpected_job"}
			}
			if err := manager.CheckpointInventory(current.ID, "", entries, true); err != nil {
				return err
			}
			return &jobs.InputError{Code: "source_unavailable", Unavailable: true}
		})
	}()
	require.Eventually(t, func() bool {
		current, getErr := manager.Get(job.ID)
		return getErr == nil && current.State == jobs.Incomplete && current.TotalReposKnown
	}, 10*time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)

	response := request(h, "GET", "/jobs/"+job.ID+"/repositories?limit=200", "", testToken)
	require.Equal(t, http.StatusRequestEntityTooLarge, response.Code)
	var body struct {
		Error string `json:"error"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.Equal(t, "repository_snapshot_too_large", body.Error)
}

func TestRepositoryDetailsCursorExpiresAfterManagerRestart(t *testing.T) {
	const (
		didA = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
		didB = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"
		rev  = "3l3qo2vutsw2b"
	)
	dir := t.TempDir()
	db, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	policy, err := selection.Open(db, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	manager, err := jobs.Open(db, policy)
	require.NoError(t, err)
	h, err := New(testToken, manager, policy)
	require.NoError(t, err)
	job, err := manager.AddSource("https://restart.example")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- manager.Run(ctx, func(ctx context.Context, current jobs.Job) error {
			if current.ID != job.ID {
				return &jobs.InputError{Code: "unexpected_job"}
			}
			if err := manager.CheckpointInventory(current.ID, "", map[string]string{didA: rev, didB: rev}, true); err != nil {
				return err
			}
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	require.Eventually(t, func() bool {
		current, getErr := manager.Get(job.ID)
		return getErr == nil && current.State == jobs.Running && current.TotalReposKnown
	}, time.Second, time.Millisecond)

	first := request(h, "GET", "/jobs/"+job.ID+"/repositories?limit=1", "", testToken)
	require.Equal(t, http.StatusOK, first.Code)
	var page struct {
		NextCursor string `json:"nextCursor"`
	}
	require.NoError(t, json.Unmarshal(first.Body.Bytes(), &page))
	require.NotEmpty(t, page.NextCursor)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.NoError(t, db.Close())

	db, err = store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	policy, err = selection.Open(db, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	manager, err = jobs.Open(db, policy)
	require.NoError(t, err)
	h, err = New(testToken, manager, policy)
	require.NoError(t, err)
	response := request(h, "GET", "/jobs/"+job.ID+"/repositories?limit=1&after="+url.QueryEscape(page.NextCursor), "", testToken)
	require.Equal(t, http.StatusGone, response.Code)
	var expired struct {
		Error string `json:"error"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &expired))
	require.Equal(t, "repository_snapshot_expired", expired.Error)
}

func TestT10CoverageTruthfulness(t *testing.T) {
	h, m := setup(t)
	job, err := m.AddSource("https://offline.example")
	require.NoError(t, err)

	before := request(h, "GET", "/coverage", "", testToken)
	require.Equal(t, http.StatusOK, before.Code)
	var empty struct {
		Items []coverageView `json:"items"`
	}
	require.NoError(t, json.Unmarshal(before.Body.Bytes(), &empty))
	require.Len(t, empty.Items, 1)
	require.Equal(t, jobs.Pending, empty.Items[0].State)
	require.Equal(t, "current_state", empty.Items[0].Coverage)
	require.NotEqual(t, jobs.Complete, empty.Items[0].State, "a configured source without a completed observation is unknown, not complete")

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- m.Run(ctx, func(context.Context, jobs.Job) error {
			return &jobs.InputError{Code: "source_unavailable", Unavailable: true}
		})
	}()
	require.Eventually(t, func() bool {
		current, getErr := m.Get(job.ID)
		return getErr == nil && current.State == jobs.Incomplete
	}, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)

	after := request(h, "GET", "/coverage", "", testToken)
	require.Equal(t, http.StatusOK, after.Code)
	var page struct {
		Items []coverageView `json:"items"`
	}
	require.NoError(t, json.Unmarshal(after.Body.Bytes(), &page))
	require.Len(t, page.Items, 1)
	require.Equal(t, "https://offline.example", page.Items[0].PDS)
	require.Equal(t, jobs.Incomplete, page.Items[0].State)
	require.Equal(t, "current_state", page.Items[0].Coverage)
	require.Equal(t, "source_unavailable", page.Items[0].ErrorCode)
	require.NotContains(t, after.Body.String(), "historical")
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

func TestAuthorizedRetryAfterRestartPreservesCompletedAcquisition(t *testing.T) {
	const (
		didA = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
		didB = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"
	)
	keyA, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)
	carA, revisionA := controlPDSCAR(t, atmos.DID(didA), keyA)
	keyB, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)
	carB, revisionB := controlPDSCAR(t, atmos.DID(didB), keyB)

	var listReposRequests atomic.Int64
	var getRepoARequests atomic.Int64
	var getRepoBRequests atomic.Int64
	var allowBSuccess atomic.Bool
	var reconciled atomic.Int64
	pds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.listRepos":
			listReposRequests.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"repos":[{"did":"` + didA + `","rev":"` + revisionA + `","head":"a","active":true},{"did":"` + didB + `","rev":"` + revisionB + `","head":"b","active":true}]}`))
		case "/xrpc/com.atproto.sync.getRepo":
			switch r.URL.Query().Get("did") {
			case didA:
				getRepoARequests.Add(1)
				w.Header().Set("Content-Type", "application/vnd.ipld.car")
				_, _ = w.Write(carA)
			case didB:
				getRepoBRequests.Add(1)
				if !allowBSuccess.Load() {
					http.Error(w, "temporary PDS failure", http.StatusServiceUnavailable)
					return
				}
				w.Header().Set("Content-Type", "application/vnd.ipld.car")
				_, _ = w.Write(carB)
			default:
				http.NotFound(w, r)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer pds.Close()

	directory := &identity.Directory{
		Resolver: controlSnapshotResolver{documents: map[string]*identity.DIDDocument{
			didA: controlPDSDocument(atmos.DID(didA), keyA, pds.URL),
			didB: controlPDSDocument(atmos.DID(didB), keyB, pds.URL),
		}},
		Cache: identity.NewLRUCache(2, time.Hour),
	}
	for did := range map[string]struct{}{didA: {}, didB: {}} {
		directory.Cache.Set(t.Context(), "did:"+did, &identity.Identity{
			DID:      atmos.DID(did),
			Services: map[string]identity.ServiceEndpoint{"atproto_pds": {URL: pds.URL}},
		})
	}

	dbDir := t.TempDir()
	var db *store.Store
	t.Cleanup(func() {
		if db != nil {
			require.NoError(t, db.Close())
		}
	})
	openManager := func() (*jobs.Manager, *Handler) {
		var openErr error
		db, openErr = store.Open(dbDir, nil)
		require.NoError(t, openErr)
		policy, openErr := selection.Open(db, []string{"app.bsky.feed.post"})
		require.NoError(t, openErr)
		manager, openErr := jobs.Open(db, policy)
		require.NoError(t, openErr)
		handler, openErr := New(testToken, manager, policy)
		require.NoError(t, openErr)
		return manager, handler
	}
	processorFor := func(manager *jobs.Manager) jobs.PDSProcessor {
		return jobs.PDSProcessor{
			Manager: manager, HTTPClient: pds.Client(), Directory: directory,
			Reconcile: func(context.Context, ingest.Snapshot) error {
				reconciled.Add(1)
				return nil
			},
		}
	}

	manager, _ := openManager()
	job, err := manager.AddSource(pds.URL)
	require.NoError(t, err)
	runControlManagerUntilState(t, manager, processorFor(manager), job.ID, jobs.Incomplete)
	beforeRestart, err := manager.Get(job.ID)
	require.NoError(t, err)
	require.Equal(t, map[string]string{didA: revisionA}, beforeRestart.CompletedRepos)
	require.Equal(t, int64(1), getRepoARequests.Load())
	require.Equal(t, int64(3), getRepoBRequests.Load())
	require.Equal(t, int64(1), reconciled.Load())
	require.NoError(t, db.Close())
	db = nil

	manager, handler := openManager()
	restored, err := manager.Get(job.ID)
	require.NoError(t, err)
	require.Equal(t, jobs.Incomplete, restored.State)
	require.Equal(t, map[string]string{didA: revisionA}, restored.CompletedRepos,
		"Pebble reopen must retain the successful repository checkpoint")
	require.Equal(t, http.StatusUnauthorized, request(handler, "POST", "/jobs/"+job.ID+"/retry", "", "invalid-token").Code)
	require.Equal(t, jobs.Incomplete, mustGetControlJob(t, manager, job.ID).State,
		"an unauthorized retry must not create another acquisition cycle")

	allowBSuccess.Store(true)
	retryResponse := request(handler, "POST", "/jobs/"+job.ID+"/retry", "", testToken)
	require.Equal(t, http.StatusOK, retryResponse.Code)
	var retryView jobView
	require.NoError(t, json.Unmarshal(retryResponse.Body.Bytes(), &retryView))
	require.Equal(t, jobs.Pending, retryView.State)
	require.True(t, retryView.TotalReposKnown, "incomplete retry resumes the frozen inventory")
	require.Equal(t, 1, retryView.CompletedRepos)
	reset, err := manager.GetRepositoryRetry(job.ID, didB)
	require.NoError(t, err)
	require.Equal(t, jobs.RepositoryRetryReady, reset.State)
	require.Zero(t, reset.Attempts)

	runControlManagerUntilState(t, manager, processorFor(manager), job.ID, jobs.Complete)
	completed, err := manager.Get(job.ID)
	require.NoError(t, err)
	require.Equal(t, map[string]string{didA: revisionA, didB: revisionB}, completed.CompletedRepos)
	require.Equal(t, int64(1), listReposRequests.Load(), "the authorized retry uses the durable frozen inventory")
	require.Equal(t, int64(1), getRepoARequests.Load(), "a checkpointed repository must not be downloaded a second time")
	require.Equal(t, int64(4), getRepoBRequests.Load(), "the unresolved coordinate receives a fresh retry cycle")
	require.Equal(t, int64(2), reconciled.Load())
}

type controlSnapshotResolver struct {
	documents map[string]*identity.DIDDocument
}

func (r controlSnapshotResolver) ResolveDID(_ context.Context, did atmos.DID) (*identity.DIDDocument, error) {
	document := r.documents[string(did)]
	if document == nil {
		return nil, errors.New("fixture DID document not found")
	}
	return document, nil
}

func (controlSnapshotResolver) ResolveHandle(context.Context, atmos.Handle) (atmos.DID, error) {
	return "", errors.New("fixture resolver does not resolve handles")
}

func controlPDSDocument(did atmos.DID, key atmoscrypto.PrivateKey, pds string) *identity.DIDDocument {
	return &identity.DIDDocument{
		ID: string(did),
		VerificationMethod: []identity.VerificationMethod{{
			ID:                 string(did) + "#atproto",
			Type:               "Multikey",
			Controller:         string(did),
			PublicKeyMultibase: key.PublicKey().Multibase(),
		}},
		Service: []identity.Service{{
			ID:              "#atproto_pds",
			Type:            "AtprotoPersonalDataServer",
			ServiceEndpoint: pds,
		}},
	}
}

func controlPDSCAR(t *testing.T, did atmos.DID, key atmoscrypto.PrivateKey) ([]byte, string) {
	t.Helper()
	blockStore := mst.NewMemBlockStore()
	snapshot := &repo.Repo{DID: did, Clock: atmos.NewTIDClock(0), Store: blockStore, Tree: mst.NewTree(blockStore)}
	require.NoError(t, snapshot.Create("app.bsky.feed.post", "3l3qo2vutsw2b", map[string]any{"text": string(did)}))
	var car bytes.Buffer
	require.NoError(t, snapshot.ExportCAR(&car, key))
	_, commit, err := repo.LoadCompleteFromCAR(bytes.NewReader(car.Bytes()))
	require.NoError(t, err)
	return car.Bytes(), commit.Rev
}

func runControlManagerUntilState(t *testing.T, manager *jobs.Manager, processor jobs.PDSProcessor, jobID string, want jobs.State) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx, processor.Run) }()
	joined := false
	defer func() {
		if !joined {
			cancel()
			<-done
		}
	}()
	require.Eventually(t, func() bool {
		current, err := manager.Get(jobID)
		return err == nil && current.State == want
	}, 10*time.Second, 5*time.Millisecond)
	cancel()
	err := <-done
	joined = true
	require.ErrorIs(t, err, context.Canceled)
}

func mustGetControlJob(t *testing.T, manager *jobs.Manager, id string) jobs.Job {
	t.Helper()
	job, err := manager.Get(id)
	require.NoError(t, err)
	return job
}

// Exposes the real persistent management contract to the Node acceptance harness.
func TestControlPlaneAcceptanceFixture(t *testing.T) {
	ready := os.Getenv("CONTROL_ACCEPTANCE_READY")
	if ready == "" {
		t.Skip("cross-language fixture")
	}
	relayURL := os.Getenv("CONTROL_ACCEPTANCE_RELAY_URL")
	if relayURL == "" {
		t.Fatal("cross-language fixture requires a Relay control URL")
	}
	handler, manager := setup(t)
	manager.SetPolicyAdvanceSender((jobs.RelayReceiptSender{
		URL:   relayURL,
		Token: testToken,
	}).AdvancePolicy)
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
