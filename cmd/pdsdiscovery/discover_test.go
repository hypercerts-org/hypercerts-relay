package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
)

const collection = "org.hypercerts.claim.activity"

type fakeAdmin struct {
	mu      sync.Mutex
	enabled []string
	limits  []string
	err     error
}

func (f *fakeAdmin) EnableSource(_ context.Context, pds string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.enabled = append(f.enabled, pds)
	return nil
}

func (f *fakeAdmin) RaiseAccountLimit(_ context.Context, pds string, expected, limit int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.limits = append(f.limits, fmt.Sprintf("%s %d->%d", pds, expected, limit))
	return nil
}

type fakeHosts struct {
	status map[string]*HostStatus
	err    error
	calls  int
}

func (f *fakeHosts) HostStatus(_ context.Context, hostname string) (*HostStatus, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	if s, ok := f.status[hostname]; ok {
		return s, nil
	}
	return nil, ErrHostNotFound
}

func count(n int64) *int64 { return &n }

type harness struct {
	d     *Discoverer
	admin *fakeAdmin
	hosts *fakeHosts
	dir   *identity.MockDirectory
	n     int
}

func newHarness(t *testing.T, cfg Config) *harness {
	t.Helper()
	if cfg.AccountThreshold == 0 {
		cfg.AccountThreshold = 100
	}
	dir := identity.NewMockDirectory()
	h := &harness{admin: &fakeAdmin{}, hosts: &fakeHosts{status: map[string]*HostStatus{}}, dir: dir}
	d, err := NewDiscoverer(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), h.dir, h.admin, h.hosts)
	if err != nil {
		t.Fatal(err)
	}
	h.d = d
	h.setSources()
	return h
}

// setSources publishes an administration state with the retained collection,
// the default maximum account limit and the given sources.
func (h *harness) setSources(sources ...DiscoverySource) {
	h.d.SetState(&DiscoveryState{Collections: []string{collection}, Sources: sources, MaxAccountLimit: 100_000})
}

func enabledSource(pds string, limit int64) DiscoverySource {
	return DiscoverySource{PDS: pds, State: "enabled", AccountLimit: count(limit)}
}

// account registers a new DID hosted on endpoint and returns it.
func (h *harness) account(endpoint string) string {
	h.n++
	did := syntax.DID(fmt.Sprintf("did:plc:test%020d", h.n))
	h.dir.Insert(identity.Identity{
		DID:    did,
		Handle: syntax.HandleInvalid,
		Services: map[string]identity.ServiceEndpoint{
			"atproto_pds": {Type: "AtprotoPersonalDataServer", URL: endpoint},
		},
	})
	return did.String()
}

func TestObserveEnablesNewPDSOnce(t *testing.T) {
	h := newHarness(t, Config{})
	ctx := context.Background()
	h.d.Observe(ctx, h.account("https://PDS.Example.com:443/"), collection)
	h.d.Observe(ctx, h.account("https://pds.example.com"), collection)
	if got := strings.Join(h.admin.enabled, ","); got != "https://pds.example.com" {
		t.Fatalf("enabled = %q", got)
	}
}

func TestObserveSkips(t *testing.T) {
	cases := map[string]struct {
		endpoint   string
		collection string
		sources    []DiscoverySource
		deny       []string
	}{
		"other collection":       {endpoint: "https://pds.example.com", collection: "app.bsky.feed.post"},
		"known enabled":          {endpoint: "https://pds.example.com", sources: []DiscoverySource{{PDS: "https://pds.example.com", State: "enabled"}}},
		"operator removed":       {endpoint: "https://pds.example.com", sources: []DiscoverySource{{PDS: "https://pds.example.com", State: "removed"}}},
		"operator disabled":      {endpoint: "https://pds.example.com", sources: []DiscoverySource{{PDS: "https://PDS.example.com:443", State: "disabled"}}},
		"denied suffix":          {endpoint: "https://morel.us-east.host.bsky.network", deny: []string{"*.host.bsky.network"}},
		"denied exact":           {endpoint: "https://bad.example.com", deny: []string{"bad.example.com"}},
		"denied exact with port": {endpoint: "https://bad.example.com:8443", deny: []string{"bad.example.com"}},
		"http endpoint":          {endpoint: "http://pds.example.com"},
		"ip endpoint":            {endpoint: "https://192.0.2.1"},
		"localhost endpoint":     {endpoint: "https://localhost"},
		"reserved tld":           {endpoint: "https://pds.internal"},
		"endpoint with path":     {endpoint: "https://pds.example.com/xrpc"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, Config{DenyHosts: tc.deny})
			h.setSources(tc.sources...)
			c := tc.collection
			if c == "" {
				c = collection
			}
			h.d.Observe(context.Background(), h.account(tc.endpoint), c)
			if len(h.admin.enabled) != 0 {
				t.Fatalf("enabled %v", h.admin.enabled)
			}
		})
	}
}

func TestDenyPatternDoesNotMatchApex(t *testing.T) {
	h := newHarness(t, Config{DenyHosts: []string{"*.host.bsky.network"}})
	h.d.Observe(context.Background(), h.account("https://host.bsky.network"), collection)
	if len(h.admin.enabled) != 1 {
		t.Fatalf("enabled %v", h.admin.enabled)
	}
}

func TestNoBuiltInDenylistOrHourlyCap(t *testing.T) {
	h := newHarness(t, Config{})
	ctx := context.Background()
	h.d.Observe(ctx, h.account("https://morel.us-east.host.bsky.network"), collection)
	for i := range 50 {
		h.d.Observe(ctx, h.account(fmt.Sprintf("https://pds%d.example.com", i)), collection)
	}
	if len(h.admin.enabled) != 51 {
		t.Fatalf("enabled %d PDSes, want 51", len(h.admin.enabled))
	}
}

func TestBannedUpstreamHostIsNotAdded(t *testing.T) {
	h := newHarness(t, Config{})
	h.hosts.status["bad.example.com"] = &HostStatus{AccountCount: count(5000), Status: "banned"}
	h.d.Observe(context.Background(), h.account("https://bad.example.com"), collection)
	h.d.Observe(context.Background(), h.account("https://bad.example.com"), collection)
	if len(h.admin.enabled) != 0 || h.hosts.calls != 1 {
		t.Fatalf("enabled %v after %d lookups", h.admin.enabled, h.hosts.calls)
	}
}

func TestHostStatusFailureDefersCandidate(t *testing.T) {
	h := newHarness(t, Config{})
	h.hosts.err = errors.New("timeout")
	did := h.account("https://pds.example.com")
	h.d.Observe(context.Background(), did, collection)
	if len(h.admin.enabled) != 0 {
		t.Fatalf("enabled without a host status: %v", h.admin.enabled)
	}
	h.hosts.err = nil
	h.d.Observe(context.Background(), did, collection)
	if len(h.admin.enabled) != 1 {
		t.Fatalf("enabled %v after the lookup recovered", h.admin.enabled)
	}
}

func TestDryRunSubmitsNothing(t *testing.T) {
	h := newHarness(t, Config{DryRun: true})
	h.hosts.status["pds.example.com"] = &HostStatus{AccountCount: count(1400), Status: "active"}
	h.d.Observe(context.Background(), h.account("https://pds.example.com"), collection)
	h.setSources(enabledSource("https://pds.example.com", 100))
	h.d.Reconcile(context.Background())
	if len(h.admin.enabled) != 0 || len(h.admin.limits) != 0 {
		t.Fatalf("dry run submitted %v %v", h.admin.enabled, h.admin.limits)
	}
	if h.hosts.calls != 1 {
		t.Fatalf("dry run should look up the would-be account limit once, got %d", h.hosts.calls)
	}
}

func TestTransientSubmitFailureRetries(t *testing.T) {
	h := newHarness(t, Config{})
	h.admin.err = errors.New("connection refused")
	did := h.account("https://pds.example.com")
	h.d.Observe(context.Background(), did, collection)
	h.admin.err = nil
	h.d.Observe(context.Background(), did, collection)
	if len(h.admin.enabled) != 1 {
		t.Fatalf("enabled %v", h.admin.enabled)
	}
}

func TestRejectedSubmitIsNotRepeated(t *testing.T) {
	h := newHarness(t, Config{})
	h.admin.err = &RejectedError{Status: 403}
	h.d.Observe(context.Background(), h.account("https://pds.example.com"), collection)
	h.admin.err = nil
	h.d.Observe(context.Background(), h.account("https://pds.example.com"), collection)
	if len(h.admin.enabled) != 0 {
		t.Fatalf("enabled %v", h.admin.enabled)
	}
}

func TestAccountLimitTarget(t *testing.T) {
	cases := []struct {
		count  *int64
		status string
		want   int64
		reason string
	}{
		{count(1400), "active", 2000, "rounded"},
		{count(1000), "active", 1000, "rounded"},
		{count(101), "idle", 1000, "rounded"},
		{count(250_001), "active", 251_000, "rounded"},
		{count(100), "active", 0, "below_threshold"},
		{nil, "active", 0, "below_threshold"},
		{count(5000), "throttled", 0, "upstream_throttled"},
	}
	for _, tc := range cases {
		got, reason := accountLimitTarget(&HostStatus{AccountCount: tc.count, Status: tc.status}, 100)
		if got != tc.want || reason != tc.reason {
			t.Errorf("%v %s: got %d %s, want %d %s", tc.count, tc.status, got, reason, tc.want, tc.reason)
		}
	}
}

func TestReconcileRaisesLimitAfterSourceAppears(t *testing.T) {
	h := newHarness(t, Config{})
	ctx := context.Background()
	h.hosts.status["pds.example.com"] = &HostStatus{AccountCount: count(1400), Status: "active"}
	h.d.Observe(ctx, h.account("https://pds.example.com"), collection)

	// Not yet a source: wait.
	h.d.Reconcile(ctx)
	if len(h.admin.limits) != 0 {
		t.Fatalf("premature limit %v", h.admin.limits)
	}
	h.setSources(enabledSource("https://pds.example.com", 100))
	h.d.Reconcile(ctx)
	h.d.Reconcile(ctx)
	if got := strings.Join(h.admin.limits, ","); got != "https://pds.example.com 100->2000" {
		t.Fatalf("limits = %q", got)
	}
}

func TestReconcileLeavesLimitAlone(t *testing.T) {
	cases := map[string]struct {
		status *HostStatus
		source DiscoverySource
	}{
		"throttled upstream": {&HostStatus{AccountCount: count(1400), Status: "throttled"}, DiscoverySource{State: "enabled", AccountLimit: count(100)}},
		"small PDS":          {&HostStatus{AccountCount: count(40), Status: "active"}, DiscoverySource{State: "enabled", AccountLimit: count(100)}},
		"unknown upstream":   {nil, DiscoverySource{State: "enabled", AccountLimit: count(100)}},
		"limit already high": {&HostStatus{AccountCount: count(1400), Status: "active"}, DiscoverySource{State: "enabled", AccountLimit: count(5000)}},
		"operator disabled":  {&HostStatus{AccountCount: count(1400), Status: "active"}, DiscoverySource{State: "disabled", AccountLimit: count(100)}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, Config{})
			ctx := context.Background()
			if tc.status != nil {
				h.hosts.status["pds.example.com"] = tc.status
			}
			h.d.Observe(ctx, h.account("https://pds.example.com"), collection)
			tc.source.PDS = "https://pds.example.com"
			h.setSources(tc.source)
			h.d.Reconcile(ctx)
			if len(h.admin.limits) != 0 {
				t.Fatalf("limits %v", h.admin.limits)
			}
			if len(h.d.pending) != 0 {
				t.Fatalf("pending not cleared: %v", h.d.pending)
			}
		})
	}
}

func TestReconcileAppliesAdministratorMaximum(t *testing.T) {
	cases := map[string]struct {
		maximum int64
		want    string
	}{
		"capped by default maximum": {100_000, "https://pds.example.com 100->100000"},
		"capped by lower maximum":   {5000, "https://pds.example.com 100->5000"},
		"maximum unset":             {0, ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, Config{})
			ctx := context.Background()
			h.hosts.status["pds.example.com"] = &HostStatus{AccountCount: count(250_001), Status: "active"}
			h.d.Observe(ctx, h.account("https://pds.example.com"), collection)
			h.d.SetState(&DiscoveryState{
				Collections:     []string{collection},
				Sources:         []DiscoverySource{enabledSource("https://pds.example.com", 100)},
				MaxAccountLimit: tc.maximum,
			})
			h.d.Reconcile(ctx)
			if got := strings.Join(h.admin.limits, ","); got != tc.want {
				t.Fatalf("limits = %q, want %q", got, tc.want)
			}
			if len(h.d.pending) != 0 {
				t.Fatalf("pending not cleared: %v", h.d.pending)
			}
		})
	}
}

func TestReconcileWaitsForSourceUntilExpiry(t *testing.T) {
	h := newHarness(t, Config{PendingQuotaTTL: time.Hour})
	ctx := context.Background()
	now := time.Now()
	h.d.now = func() time.Time { return now }
	h.hosts.status["pds.example.com"] = &HostStatus{AccountCount: count(1400), Status: "active"}
	h.d.Observe(ctx, h.account("https://pds.example.com"), collection)
	h.d.Reconcile(ctx)
	if len(h.d.pending) != 1 {
		t.Fatal("a PDS not yet listed as a source should stay pending")
	}
	now = now.Add(2 * time.Hour)
	h.d.Reconcile(ctx)
	if len(h.d.pending) != 0 || len(h.admin.limits) != 0 {
		t.Fatalf("expired check: pending %v limits %v", h.d.pending, h.admin.limits)
	}
}

func TestSetStateReportsCollectionChanges(t *testing.T) {
	h := newHarness(t, Config{})
	if h.d.SetState(&DiscoveryState{Collections: []string{collection}, Sources: []DiscoverySource{{PDS: "https://a.example.com", State: "enabled"}}}) {
		t.Fatal("source-only change should not reload upstream")
	}
	if !h.d.SetState(&DiscoveryState{Collections: []string{collection, "org.hypercerts.claim.rights"}}) {
		t.Fatal("collection change should reload upstream")
	}
	if !h.d.SetState(&DiscoveryState{Collections: []string{"org.hypercerts.claim.rights", "bad nsid"}}) {
		t.Fatal("collection change should reload upstream")
	}
	if got := strings.Join(h.d.Collections(), ","); got != "org.hypercerts.claim.rights" {
		t.Fatalf("collections = %q", got)
	}
}

func TestWantedCollections(t *testing.T) {
	small := []string{"a.b.c", "a.b.d"}
	if got, _ := wantedCollections(small); strings.Join(got, ",") != "a.b.c,a.b.d" {
		t.Fatalf("small = %v", got)
	}
	var many []string
	for i := range 150 {
		many = append(many, fmt.Sprintf("org.hypercerts.group%d.record", i%3))
		many = append(many, fmt.Sprintf("app.certified.x.y%d", i))
	}
	got, err := wantedCollections(many)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "app.certified.x.*,org.hypercerts.group0.*,org.hypercerts.group1.*,org.hypercerts.group2.*" {
		t.Fatalf("collapsed = %v", got)
	}
	var tooMany []string
	for i := range 101 {
		tooMany = append(tooMany, fmt.Sprintf("tld%d.example.record", i))
	}
	if _, err := wantedCollections(tooMany); err == nil {
		t.Fatal("expected an error when prefixes cannot fit")
	}
}

func TestIdempotencyKeyIsDeterministicUUIDv5(t *testing.T) {
	a, b := idempotencyKey("source:enabled:https://pds.example.com"), idempotencyKey("source:enabled:https://pds.example.com")
	if a != b || a == idempotencyKey("source:enabled:https://other.example.com") {
		t.Fatalf("keys %s %s", a, b)
	}
	if len(a) != 36 || a[14] != '5' || !strings.ContainsRune("89ab", rune(a[19])) {
		t.Fatalf("not a v5 UUID: %s", a)
	}
}

// fakeAdminHandler records authorised requests and answers like the
// administration discovery routes.
type fakeAdminHandler struct {
	token    string
	mu       sync.Mutex
	requests []string
}

func (f *fakeAdminHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer "+f.token {
		w.WriteHeader(401)
		return
	}
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.requests = append(f.requests, r.Method+" "+r.URL.Path+" "+r.Header.Get("Idempotency-Key")+" "+string(body))
	f.mu.Unlock()
	switch r.URL.Path {
	case "/api/v1/sources/discovery/state":
		_ = json.NewEncoder(w).Encode(DiscoveryState{PolicyRevision: 3, Collections: []string{collection}, Sources: []DiscoverySource{{PDS: "https://a.example.com", State: "enabled", AccountLimit: count(100)}}})
	case "/api/v1/sources/discovery/operations":
		w.WriteHeader(fakeOperationStatus(string(body)))
	}
}

// fakeOperationStatus rejects one fixture PDS and reports another as busy.
func fakeOperationStatus(body string) int {
	switch {
	case strings.Contains(body, "forbidden.example.com"):
		return 403
	case strings.Contains(body, "busy.example.com"):
		return 503
	default:
		return 202
	}
}

func TestAdminClient(t *testing.T) {
	token := strings.Repeat("t", 40)
	handler := &fakeAdminHandler{token: token}
	srv := httptest.NewServer(handler)
	defer srv.Close()
	c, err := NewAdminClient(srv.URL, token)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	state, err := c.State(ctx)
	if err != nil || state.PolicyRevision != 3 || *state.Sources[0].AccountLimit != 100 {
		t.Fatalf("state %+v %v", state, err)
	}
	if err := c.EnableSource(ctx, "https://pds.example.com"); err != nil {
		t.Fatal(err)
	}
	if err := c.RaiseAccountLimit(ctx, "https://pds.example.com", 100, 2000); err != nil {
		t.Fatal(err)
	}
	var rejected *RejectedError
	if err := c.EnableSource(ctx, "https://forbidden.example.com"); !errors.As(err, &rejected) {
		t.Fatalf("403 should be a rejection, got %v", err)
	}
	if err := c.EnableSource(ctx, "https://busy.example.com"); err == nil || errors.As(err, &rejected) {
		t.Fatalf("503 should be retryable, got %v", err)
	}
	want := []string{
		"GET /api/v1/sources/discovery/state  ",
		"POST /api/v1/sources/discovery/operations " + idempotencyKey("source:enabled:https://pds.example.com") + ` {"kind":"source","pds":"https://pds.example.com","state":"enabled"}`,
		"POST /api/v1/sources/discovery/operations " + idempotencyKey("account_quota:https://pds.example.com:100:2000") + ` {"accountLimit":2000,"expectedLimit":100,"kind":"account_quota","pds":"https://pds.example.com"}`,
	}
	for i, w := range want {
		if handler.requests[i] != w {
			t.Errorf("request %d:\n got %s\nwant %s", i, handler.requests[i], w)
		}
	}
}

func TestAdminClientTransportRules(t *testing.T) {
	token := strings.Repeat("t", 40)
	for _, ok := range []string{"https://admin.example.com", "http://127.0.0.1:3000", "http://localhost:3000", "http://administration.railway.internal:3000"} {
		if _, err := NewAdminClient(ok, token); err != nil {
			t.Errorf("%s: %v", ok, err)
		}
	}
	for _, bad := range []string{"http://admin.example.com", "https://admin.example.com/path", "https://u:p@admin.example.com", "ftp://admin.example.com"} {
		if _, err := NewAdminClient(bad, token); err == nil {
			t.Errorf("%s: expected error", bad)
		}
	}
	if _, err := NewAdminClient("https://admin.example.com", "short"); err == nil {
		t.Error("short token accepted")
	}
}

func TestRelayClientHostStatus(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/xrpc/com.atproto.sync.getHostStatus" {
			w.WriteHeader(404)
			return
		}
		switch r.URL.Query().Get("hostname") {
		case "pds.example.com":
			_, _ = w.Write([]byte(`{"hostname":"pds.example.com","accountCount":1400,"seq":5,"status":"active"}`))
		case "missing.example.com":
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"error":"HostNotFound","message":"host not found"}`))
		default:
			w.WriteHeader(500)
		}
	}))
	defer srv.Close()
	c, err := NewRelayClient(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	c.http = srv.Client()
	ctx := context.Background()
	status, err := c.HostStatus(ctx, "pds.example.com")
	if err != nil || *status.AccountCount != 1400 || status.Status != "active" {
		t.Fatalf("status %+v %v", status, err)
	}
	if _, err := c.HostStatus(ctx, "missing.example.com"); !errors.Is(err, ErrHostNotFound) {
		t.Fatalf("missing: %v", err)
	}
	if _, err := c.HostStatus(ctx, "broken.example.com"); err == nil || errors.Is(err, ErrHostNotFound) {
		t.Fatalf("broken: %v", err)
	}
}
