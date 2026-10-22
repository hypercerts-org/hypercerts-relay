package web_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/status"
	"github.com/bluesky-social/jetstream/internal/web"
	"github.com/stretchr/testify/require"
)

type immutableBrowserSnapshot struct{ snap status.Snapshot }

func (s immutableBrowserSnapshot) SnapshotForRequest(_ context.Context, req status.Request) (*status.Snapshot, error) {
	snapshotCopy := s.snap
	snapshotCopy.Request = req
	if snapshotCopy.Request.Tab == "" {
		snapshotCopy.Request.Tab = "summary"
	}
	return &snapshotCopy, nil
}

func browserCoverage(state string, scanned, matching, noMatch, unresolved, records *int) *status.CoverageInfo {
	count := func(v *int) status.CoverageCount {
		if v == nil {
			return status.CoverageCount{}
		}
		return status.CoverageCount{Known: true, Value: *v}
	}
	return &status.CoverageInfo{Scope: "enabled_sources_current_policy", Policy: status.CoveragePolicy{Revision: 2, Collections: []string{"app.certified.actor.profile"}},
		Aggregate: status.CoverageAggregate{Scope: "enabled_sources_current_policy", PolicyRevision: 2, EnabledSources: 1, State: state},
		Sources: []status.CoverageSource{{PDS: "https://pds.example", SourceRevision: 3, PolicyRevision: 2, Collections: []string{"app.certified.actor.profile"}, MatchingJob: state != "unknown", State: state, ProgressScope: "job_inventory",
			Counts: status.CoverageCounts{InitialInventory: count(intPtr(14541)), Scanned: count(scanned), Matching: count(matching), NoMatch: count(noMatch), Unresolved: count(unresolved), AttributableRecords: count(records)}, HistoryState: "unknown", LiveState: "unknown",
			FailureCategory: map[bool]string{true: "source_unavailable", false: ""}[state == "stopped_incomplete"], Attempts: 1, RetryAction: map[bool]string{true: "explicit_retry", false: "unknown"}[state == "stopped_incomplete"]}}}
}
func intPtr(v int) *int       { return &v }
func int64Ptr(v int64) *int64 { return &v }

func publicStatusScenarios(now time.Time) map[string]status.Snapshot {
	stale := now.Add(-24 * time.Hour)
	future := now.Add(time.Hour)
	scenarios := map[string]status.Snapshot{}
	add := func(name, state string, scanned, matching, noMatch, unresolved, records *int) {
		scenarios[name] = status.Snapshot{GeneratedAt: now, Request: status.Request{Tab: "summary"}, Coverage: browserCoverage(state, scanned, matching, noMatch, unresolved, records)}
	}
	add("sparse", "complete", intPtr(14541), intPtr(2), intPtr(14539), intPtr(0), intPtr(3))
	add("failed", "failed", intPtr(5980), intPtr(3000), intPtr(0), intPtr(8561), intPtr(3))
	add("waiting", "retry_waiting", nil, nil, nil, nil, nil)
	add("throttled", "stopped_incomplete", intPtr(5980), nil, nil, nil, nil)
	add("unknown", "unknown", nil, nil, nil, nil, nil)
	add("empty-unknown", "unknown", nil, nil, nil, nil, nil)
	emptyUnknown := scenarios["empty-unknown"]
	emptyCoverage := *emptyUnknown.Coverage
	emptyCoverage.Sources = nil
	emptyCoverage.Aggregate.EnabledSources = 0
	emptyUnknown.Coverage = &emptyCoverage
	scenarios["empty-unknown"] = emptyUnknown
	add("zero", "complete", intPtr(0), intPtr(0), intPtr(0), intPtr(0), intPtr(0))
	add("stale", "complete", intPtr(1), intPtr(1), intPtr(0), intPtr(0), intPtr(1))
	staleSnapshot := scenarios["stale"]
	staleSnapshot.Live.LastSeenUpstreamEventAt = stale
	staleSnapshot.Live.LastSeenUpstreamEventAgeSeconds = int64Ptr(86400)
	scenarios["stale"] = staleSnapshot
	add("future", "complete", intPtr(1), intPtr(1), intPtr(0), intPtr(0), intPtr(1))
	futureSnapshot := scenarios["future"]
	futureSnapshot.Live.LastSeenUpstreamEventAt = future
	scenarios["future"] = futureSnapshot
	// absent reporter is represented separately from an empty current-policy projection.
	scenarios["unavailable"] = status.Snapshot{GeneratedAt: now, Request: status.Request{Tab: "summary"}}
	return scenarios
}

func newPublicStatusFixture(t *testing.T, now time.Time) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	for name, snap := range publicStatusScenarios(now) {
		h, err := web.New(web.Options{Snapshotter: immutableBrowserSnapshot{snap: snap}, Now: func() time.Time { return now }})
		require.NoError(t, err)
		mux.Handle("/"+name+"/", http.StripPrefix("/"+name, h))
	}
	return httptest.NewServer(mux)
}

func assertPublicStatusPrivacy(t *testing.T, body string) {
	t.Helper()
	for _, sentinel := range []string{"private-job-sentinel", "private-error-sentinel", "did:plc:private-sentinel", "receiptRetryAt", "jobId"} {
		require.NotContains(t, body, sentinel)
	}
}

func TestPublicStatusBrowserFixture(t *testing.T) {
	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	server := newPublicStatusFixture(t, now)
	t.Cleanup(server.Close)
	for name := range publicStatusScenarios(now) {
		assertPublicStatusHTTP(t, server.URL, name)
	}
	if os.Getenv("PUBLIC_STATUS_BROWSER") == "1" {
		runPublicStatusBrowser(t, server.URL)
	}
}

func assertPublicStatusHTTP(t *testing.T, baseURL, name string) {
	t.Helper()
	resp, err := http.Get(baseURL + "/" + name + "/status")
	require.NoError(t, err)
	bodyBytes, readErr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	require.NoError(t, readErr)
	body := string(bodyBytes)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, body, "Current-policy acquisition")
	assertPublicStatusScenario(t, name, body)
	assertPublicStatusPrivacy(t, body)
}

func assertPublicStatusScenario(t *testing.T, name, body string) {
	t.Helper()
	switch name {
	case "unavailable":
		require.Contains(t, body, "Current-policy acquisition evidence unavailable.")
	case "unknown":
		require.Contains(t, body, "unknown")
	case "empty-unknown":
		require.Contains(t, body, "Current-policy acquisition is unknown: no enabled sources.")
	case "throttled":
		require.Contains(t, body, "source_unavailable")
		require.NotContains(t, body, "429")
		require.Contains(t, body, "no retry timing is known")
	case "failed":
		require.Contains(t, body, "5980")
		require.Contains(t, body, "14541")
		require.Contains(t, body, "job_inventory")
	case "sparse":
		require.Contains(t, body, "14539")
		require.Contains(t, body, "unknown")
	}
}

func runPublicStatusBrowser(t *testing.T, serverURL string) {
	t.Helper()
	root, err := filepath.Abs("../../..")
	require.NoError(t, err)
	script := filepath.Join(root, "administration/tests/browser/public-status.spec.ts")
	require.FileExists(t, script)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "node", script)
	cmd.Dir = filepath.Join(root, "administration")
	cmd.Env = append(os.Environ(), "PUBLIC_STATUS_FIXTURE_URL="+serverURL+"/")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, fmt.Sprintf("Node browser fixture: %s", strings.TrimSpace(string(output))))
	t.Log(string(output))
}
