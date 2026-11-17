package main

// hypercerts: crawl registration, outcome history and status reporting used by
// the administration control plane.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/util/ssrf"
	"github.com/bluesky-social/indigo/xrpc"

	"github.com/labstack/echo/v4"
)

const crawlTestToken = "crawl-status-test-admin-token"

// fakePDS serves listRepos and describeRepo for a fixed repository list.
type fakePDS struct {
	dids            []string
	listStatus      int
	failDescribeFor string
	release         chan struct{}
	listRequests    atomic.Int32
	server          *httptest.Server
}

func newFakePDS(t *testing.T, pds *fakePDS) *fakePDS {
	t.Helper()
	pds.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.listRepos":
			pds.listRequests.Add(1)
			if pds.release != nil {
				select {
				case <-pds.release:
				case <-r.Context().Done():
					return
				}
			}
			if pds.listStatus != 0 {
				w.WriteHeader(pds.listStatus)
				_, _ = io.WriteString(w, `{"error":"InternalServerError","message":"upstream-private-detail"}`)
				return
			}
			repos := make([]map[string]any, 0, len(pds.dids))
			for _, did := range pds.dids {
				repos = append(repos, map[string]any{"did": did, "head": "bafyhead", "rev": "rev"})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"repos": repos})
		case "/xrpc/com.atproto.repo.describeRepo":
			did := r.URL.Query().Get("repo")
			if did == pds.failDescribeFor {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, `{"error":"RepoNotFound","message":"missing"}`)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"did": did, "handle": "user.example", "didDoc": map[string]any{},
				"collections": []string{"org.hypercerts.claim"}, "handleIsCorrect": true,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(pds.server.Close)
	return pds
}

func newCrawlTestServer(t *testing.T, client *http.Client) (*collectionServer, *echo.Echo) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cs := &collectionServer{
		ctx:                ctx,
		cancel:             cancel,
		log:                slog.New(slog.NewTextHandler(io.Discard, nil)),
		ingestCrawl:        make(chan DidCollection, 16),
		PerPDSCrawlQPS:     1000,
		ExepctedAuthHeader: "Bearer " + crawlTestToken,
		processStart:       time.Now(),
		crawlHTTPClient:    client,
	}
	var drained sync.WaitGroup
	drained.Add(1)
	go func() {
		defer drained.Done()
		for range cs.ingestCrawl {
		}
	}()
	t.Cleanup(func() {
		cancel()
		cs.producerWG.Wait()
		close(cs.ingestCrawl)
		drained.Wait()
	})
	e := echo.New()
	e.POST("/admin/pds/requestCrawl", cs.crawlPds)
	e.GET("/admin/crawlStatus", cs.crawlStatus)
	return cs, e
}

func crawlRequest(t *testing.T, e *echo.Echo, method, path, body, token string) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	request := httptest.NewRequest(method, path, reader)
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, request)
	return recorder
}

func submitCrawl(t *testing.T, e *echo.Echo, host string) CrawlRequestHost {
	t.Helper()
	recorder := crawlRequest(t, e, http.MethodPost, "/admin/pds/requestCrawl", fmt.Sprintf(`{"hosts":[%q]}`, host), crawlTestToken)
	if recorder.Code != http.StatusOK {
		t.Fatalf("requestCrawl: %d %s", recorder.Code, recorder.Body)
	}
	var reply struct {
		Message string                      `json:"message"`
		Hosts   map[string]CrawlRequestHost `json:"hosts"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &reply); err != nil {
		t.Fatal(err)
	}
	if reply.Message != "ok" {
		t.Fatalf("legacy message changed: %q", reply.Message)
	}
	outcome, ok := reply.Hosts[host]
	if !ok {
		t.Fatalf("reply has no entry for %s: %s", host, recorder.Body)
	}
	return outcome
}

type statusReply struct {
	HostStarts   map[string]map[string]any  `json:"host_starts"`
	ServerTime   string                     `json:"server_time"`
	ProcessStart string                     `json:"process_start"`
	HostResults  map[string]HostCrawlResult `json:"host_results"`
	raw          string
}

func crawlStatusReply(t *testing.T, e *echo.Echo) statusReply {
	t.Helper()
	recorder := crawlRequest(t, e, http.MethodGet, "/admin/crawlStatus", "", crawlTestToken)
	if recorder.Code != http.StatusOK {
		t.Fatalf("crawlStatus: %d %s", recorder.Code, recorder.Body)
	}
	var reply statusReply
	if err := json.Unmarshal(recorder.Body.Bytes(), &reply); err != nil {
		t.Fatal(err)
	}
	reply.raw = recorder.Body.String()
	return reply
}

func waitForResult(t *testing.T, e *echo.Echo, host string) (statusReply, HostCrawlResult) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		reply := crawlStatusReply(t, e)
		if result, ok := reply.HostResults[host]; ok {
			return reply, result
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("crawl of %s did not end", host)
	return statusReply{}, HostCrawlResult{}
}

func TestCrawlFinishedMovesToResults(t *testing.T) {
	pds := newFakePDS(t, &fakePDS{dids: []string{"did:plc:one", "did:plc:two"}})
	_, e := newCrawlTestServer(t, pds.server.Client())
	outcome := submitCrawl(t, e, pds.server.URL)
	if outcome.State != "started" || outcome.Start == "" {
		t.Fatalf("unexpected submission outcome %+v", outcome)
	}
	reply, result := waitForResult(t, e, pds.server.URL)
	if _, active := reply.HostStarts[pds.server.URL]; active {
		t.Fatal("finished crawl is still reported as active")
	}
	if result.State != "finished" || result.ReposDescribed != 2 || result.DescribeErrors != 0 || result.Start != outcome.Start || result.End == "" {
		t.Fatalf("unexpected result %+v", result)
	}
	if strings.Contains(reply.raw, "error_category") || strings.Contains(reply.raw, "http_status") {
		t.Fatalf("finished result carries failure fields: %s", reply.raw)
	}
	if reply.ServerTime == "" || reply.ProcessStart == "" {
		t.Fatalf("status omits server_time or process_start: %s", reply.raw)
	}
}

func TestCrawlListReposFailureIsCategorized(t *testing.T) {
	pds := newFakePDS(t, &fakePDS{listStatus: http.StatusInternalServerError})
	_, e := newCrawlTestServer(t, pds.server.Client())
	submitCrawl(t, e, pds.server.URL)
	reply, result := waitForResult(t, e, pds.server.URL)
	if result.State != "failed" || result.ErrorCategory != "http" || result.HTTPStatus != http.StatusInternalServerError {
		t.Fatalf("unexpected result %+v", result)
	}
	if strings.Contains(reply.raw, "upstream-private-detail") || strings.Contains(reply.raw, "InternalServerError") {
		t.Fatalf("status exposes raw upstream error text: %s", reply.raw)
	}
}

func TestCrawlDescribeFailuresAreCounted(t *testing.T) {
	pds := newFakePDS(t, &fakePDS{dids: []string{"did:plc:one", "did:plc:two"}, failDescribeFor: "did:plc:two"})
	_, e := newCrawlTestServer(t, pds.server.Client())
	submitCrawl(t, e, pds.server.URL)
	_, result := waitForResult(t, e, pds.server.URL)
	if result.State != "finished" || result.ReposDescribed != 1 || result.DescribeErrors != 1 {
		t.Fatalf("unexpected result %+v", result)
	}
}

func TestCrawlDuplicateReportsActiveCrawl(t *testing.T) {
	pds := newFakePDS(t, &fakePDS{dids: []string{"did:plc:one"}, release: make(chan struct{})})
	_, e := newCrawlTestServer(t, pds.server.Client())
	first := submitCrawl(t, e, pds.server.URL)
	second := submitCrawl(t, e, pds.server.URL)
	if first.State != "started" || second.State != "already_active" || second.Start != first.Start {
		t.Fatalf("unexpected outcomes %+v then %+v", first, second)
	}
	reply := crawlStatusReply(t, e)
	active, ok := reply.HostStarts[pds.server.URL]
	if !ok || active["start"] != first.Start {
		t.Fatalf("active crawl not reported: %s", reply.raw)
	}
	for _, key := range []string{"start", "seen", "describe_errors"} {
		if _, ok := active[key]; !ok {
			t.Fatalf("active crawl omits %q: %s", key, reply.raw)
		}
	}
	close(pds.release)
	_, result := waitForResult(t, e, pds.server.URL)
	if result.Start != first.Start {
		t.Fatalf("result start %s, want %s", result.Start, first.Start)
	}
	if got := pds.listRequests.Load(); got != 1 {
		t.Fatalf("PDS received %d listRepos requests, want 1", got)
	}
}

func TestCrawlResultsEvictOldest(t *testing.T) {
	cs := &collectionServer{}
	for i := 0; i <= maxCrawlResults; i++ {
		host := fmt.Sprintf("https://pds%d.example", i)
		ok, start, stats := cs.recordCrawlStart(host, time.Now())
		if !ok {
			t.Fatal("unexpected duplicate")
		}
		cs.finishCrawl(host, start, stats, nil)
		time.Sleep(time.Microsecond)
	}
	if len(cs.crawlResults) != maxCrawlResults || len(cs.activeCrawls) != 0 {
		t.Fatalf("retained %d results and %d active crawls", len(cs.crawlResults), len(cs.activeCrawls))
	}
	if _, ok := cs.crawlResults["https://pds0.example"]; ok {
		t.Fatal("oldest result was not evicted")
	}
	if _, ok := cs.crawlResults[fmt.Sprintf("https://pds%d.example", maxCrawlResults)]; !ok {
		t.Fatal("newest result was evicted")
	}
}

func TestCrawlEndpointsRequireAdminToken(t *testing.T) {
	pds := newFakePDS(t, &fakePDS{})
	cs, e := newCrawlTestServer(t, pds.server.Client())
	body := fmt.Sprintf(`{"hosts":[%q]}`, pds.server.URL)
	for _, token := range []string{"", "wrong-token"} {
		if got := crawlRequest(t, e, http.MethodPost, "/admin/pds/requestCrawl", body, token).Code; got != http.StatusForbidden {
			t.Fatalf("requestCrawl with %q token: %d", token, got)
		}
		if got := crawlRequest(t, e, http.MethodGet, "/admin/crawlStatus", "", token).Code; got != http.StatusBadRequest {
			t.Fatalf("crawlStatus with %q token: %d", token, got)
		}
	}
	if len(cs.activeCrawls) != 0 || pds.listRequests.Load() != 0 {
		t.Fatal("unauthenticated request started a crawl")
	}
}

func TestClassifyCrawlError(t *testing.T) {
	refused, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refusedURL := "http://" + refused.Addr().String()
	refused.Close()
	_, transportErr := http.Get(refusedURL)
	local := httptest.NewServer(http.NotFoundHandler())
	defer local.Close()
	_, blockedErr := (&http.Client{Transport: ssrf.PublicOnlyTransport()}).Get(local.URL)
	if transportErr == nil || blockedErr == nil {
		t.Fatalf("fixtures did not fail: transport %v, blocked %v", transportErr, blockedErr)
	}
	for name, tc := range map[string]struct {
		err      error
		category string
		status   int
	}{
		"canceled":  {fmt.Errorf("wrapped: %w", context.Canceled), "canceled", 0},
		"deadline":  {fmt.Errorf("wrapped: %w", context.DeadlineExceeded), "timeout", 0},
		"http":      {fmt.Errorf("sync repos: %w", &xrpc.Error{StatusCode: 502, Wrapped: errors.New("bad gateway")}), "http", 502},
		"blocked":   {fmt.Errorf("sync repos: %w", blockedErr), "blocked_address", 0},
		"transport": {fmt.Errorf("sync repos: %w", transportErr), "transport", 0},
		"unknown":   {errors.New("unexpected"), "unknown", 0},
	} {
		category, status := classifyCrawlError(tc.err)
		if category != tc.category || status != tc.status {
			t.Errorf("%s: got %s %d, want %s %d", name, category, status, tc.category, tc.status)
		}
	}
}
