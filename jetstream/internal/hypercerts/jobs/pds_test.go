package jobs

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	atmoscrypto "github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/mst"
	"github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

type synchronizedLogBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *synchronizedLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *synchronizedLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

var defaultSlogCaptureMu sync.Mutex

// Tests using this helper stay non-parallel because slog.Default is process-global.
func captureDefaultLogs(t *testing.T) *synchronizedLogBuffer {
	t.Helper()
	defaultSlogCaptureMu.Lock()
	previous := slog.Default()
	output := &synchronizedLogBuffer{}
	slog.SetDefault(slog.New(slog.NewJSONHandler(output, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previous)
		defaultSlogCaptureMu.Unlock()
	})
	return output
}

func TestPDSProcessorCountsOneResumableInventory(t *testing.T) {
	const cursor = "page-two"
	const didA = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	const didB = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"
	const revA = "3l3qo2vutsw2b"
	const revB = "3l3qo2vutsw2c"

	logOutput := captureDefaultLogs(t)
	var mu sync.Mutex
	var cursors []string
	pageTwoAttempts := 0
	pds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/xrpc/com.atproto.sync.listRepos", r.URL.Path)
		gotCursor := r.URL.Query().Get("cursor")
		mu.Lock()
		cursors = append(cursors, gotCursor)
		if gotCursor == cursor {
			pageTwoAttempts++
		}
		attempt := pageTwoAttempts
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		switch gotCursor {
		case "":
			_, _ = w.Write([]byte(`{"repos":[{"did":"` + didA + `","rev":"` + revA + `","head":"head-a","active":true}],"cursor":"` + cursor + `"}`))
		case cursor:
			if attempt == 1 {
				http.Error(w, "fixture transient failure", http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte(`{"repos":[{"did":"` + didB + `","rev":"` + revB + `","head":"head-b","active":true}]}`))
		default:
			http.Error(w, "unexpected cursor", http.StatusBadRequest)
		}
	}))
	defer pds.Close()

	dir := t.TempDir()
	m, db := newManager(t, dir)
	job, err := m.AddSource(pds.URL)
	require.NoError(t, err)
	primeCompletedRepos(t, m, job.ID, map[string]string{didA: revA, didB: revB})

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- m.Run(ctx, PDSProcessor{Manager: m, HTTPClient: pds.Client(), Directory: &identity.Directory{}}.Run)
	}()
	require.Eventually(t, func() bool { return m.List()[0].State == Incomplete }, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	incomplete := m.List()[0]
	require.Equal(t, cursor, incomplete.Cursor)
	require.Equal(t, 1, incomplete.EnumeratedRepos)
	require.False(t, incomplete.TotalReposKnown)
	require.NoError(t, db.Close())

	m, db = newManager(t, dir)
	defer db.Close()
	require.NoError(t, m.Retry(job.ID))
	ctx, cancel = context.WithCancel(t.Context())
	done = make(chan error, 1)
	go func() {
		done <- m.Run(ctx, PDSProcessor{Manager: m, HTTPClient: pds.Client(), Directory: &identity.Directory{}}.Run)
	}()
	require.Eventually(t, func() bool { return m.List()[0].State == Complete }, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	complete := m.List()[0]
	require.True(t, complete.TotalReposKnown)
	require.Equal(t, 2, complete.TotalRepos)

	logs := logOutput.String()
	require.Equal(t, 1, strings.Count(logs, `"level":"WARN"`))
	require.Contains(t, logs, `"job_id":"`+job.ID+`"`)
	require.Contains(t, logs, `"stage":"listRepos"`)
	require.Contains(t, logs, `"cause_class":"http"`)
	require.Contains(t, logs, `"http_status":503`)
	require.NotContains(t, logs, `"repository_did"`)
	require.NotContains(t, logs, "fixture transient failure")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"", cursor, cursor}, cursors)
}

func TestPDSProcessorRequiresDirectoryBeforeListing(t *testing.T) {
	var listed atomic.Int64
	pds := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { listed.Add(1) }))
	defer pds.Close()

	m, db := newManager(t, t.TempDir())
	defer db.Close()
	job, err := m.AddSource(pds.URL)
	require.NoError(t, err)
	err = (PDSProcessor{Manager: m, HTTPClient: pds.Client()}).Run(t.Context(), job)
	var input *InputError
	require.ErrorAs(t, err, &input)
	require.Equal(t, "identity_unavailable", input.Code)
	require.True(t, input.Unavailable)
	require.Zero(t, listed.Load(), "a nil directory must fail before listRepos, even for an empty PDS")
}

func TestPDSProcessorCompletesEmptyInventory(t *testing.T) {
	pds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/xrpc/com.atproto.sync.listRepos", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"repos":[]}`))
	}))
	defer pds.Close()

	m, db := newManager(t, t.TempDir())
	defer db.Close()
	job, err := m.AddSource(pds.URL)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- m.Run(ctx, PDSProcessor{Manager: m, HTTPClient: pds.Client(), Directory: &identity.Directory{}}.Run)
	}()
	require.Eventually(t, func() bool { return m.List()[0].State == Complete }, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	complete := m.List()[0]
	require.Equal(t, job.ID, complete.ID)
	require.True(t, complete.TotalReposKnown)
	require.Zero(t, complete.TotalRepos)
}

func TestPDSProcessorDoesNotFollowListRedirects(t *testing.T) {
	var redirected atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirected.Store(true)
	}))
	defer target.Close()
	pds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer pds.Close()

	m, db := newManager(t, t.TempDir())
	defer db.Close()
	_, err := m.AddSource(pds.URL)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- m.Run(ctx, PDSProcessor{Manager: m, HTTPClient: pds.Client(), Directory: &identity.Directory{}}.Run)
	}()
	require.Eventually(t, func() bool { return m.List()[0].State == Incomplete }, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Equal(t, "source_unavailable", m.List()[0].ErrorCode)
	require.False(t, redirected.Load())
}

func TestPDSProcessorVerifySnapshotRefreshClassification(t *testing.T) {
	const revision = "3l3qo2vutsw2b"
	const pds = "https://pds.example"
	did := atmos.DID("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")
	freshKey, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)
	wrongKey, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)

	tests := []struct {
		name        string
		response    snapshotIdentityResponse
		signingKey  atmoscrypto.PrivateKey
		wantCode    string
		unavailable bool
	}{
		{name: "current identity verifies after a stale cache entry", response: snapshotIdentityResponse{doc: snapshotDIDDocument(did, freshKey.PublicKey(), pds)}, signingKey: freshKey},
		{name: "refresh resolution unavailable", response: snapshotIdentityResponse{err: errors.New("refresh unavailable")}, signingKey: freshKey, wantCode: "identity_unavailable", unavailable: true},
		{name: "stale cache cannot hide a migrated source", response: snapshotIdentityResponse{doc: snapshotDIDDocument(did, freshKey.PublicKey(), "https://moved.example")}, signingKey: freshKey, wantCode: "source_changed", unavailable: true},
		{name: "invalid signature against current identity is permanent", response: snapshotIdentityResponse{doc: snapshotDIDDocument(did, freshKey.PublicKey(), pds)}, signingKey: wrongKey, wantCode: "verification_failed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resolver := &snapshotIdentityResolver{responses: []snapshotIdentityResponse{tc.response}}
			directory := &identity.Directory{Resolver: resolver, Cache: identity.NewLRUCache(1, time.Hour)}
			directory.Cache.Set(t.Context(), "did:"+string(did), &identity.Identity{DID: did, Services: map[string]identity.ServiceEndpoint{"atproto_pds": {URL: pds}}})
			processor := PDSProcessor{Directory: directory}
			commit := signedSnapshotCommit(t, did, revision, tc.signingKey)

			err := processor.verifySnapshot(t.Context(), Job{PDS: pds}, atmossync.ListReposEntry{DID: did, Rev: revision}, commit)
			if tc.wantCode == "" {
				require.NoError(t, err)
			} else {
				var input *InputError
				require.ErrorAs(t, err, &input)
				require.Equal(t, tc.wantCode, input.Code)
				require.Equal(t, tc.unavailable, input.Unavailable)
			}
			require.Equal(t, 1, resolver.Calls(), "snapshot verification must resolve a fresh identity without using the cache")
			cached, ok := directory.Cache.Get(t.Context(), "did:"+string(did))
			require.True(t, ok, "direct-PDS verification must not evict the shared directory cache")
			require.Equal(t, pds, cached.PDSEndpoint())
		})
	}
}

func TestPDSProcessorComparesSnapshotPositionsAsTIDs(t *testing.T) {
	const pds = "https://pds.example"
	did := atmos.DID("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")
	key, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)
	for _, tc := range []struct {
		name      string
		commitRev string
		wantCode  string
	}{
		{name: "equal", commitRev: "3l3qo2vutsw2b"},
		{name: "ahead", commitRev: "3l3qo2vutsw2c"},
		{name: "behind", commitRev: "3l3qo2vutsw2a", wantCode: "snapshot_behind_listing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			directory := &identity.Directory{Resolver: &snapshotIdentityResolver{responses: []snapshotIdentityResponse{{doc: snapshotDIDDocument(did, key.PublicKey(), pds)}}}}
			err := (PDSProcessor{Directory: directory}).verifySnapshot(t.Context(), Job{PDS: pds}, atmossync.ListReposEntry{DID: did, Rev: "3l3qo2vutsw2b"}, signedSnapshotCommit(t, did, tc.commitRev, key))
			if tc.wantCode == "" {
				require.NoError(t, err)
				return
			}
			var input *InputError
			require.ErrorAs(t, err, &input)
			require.Equal(t, tc.wantCode, input.Code)
			require.True(t, input.Unavailable)
		})
	}
}

func TestPDSProcessorStaleCacheMigrationIsIncompleteWithoutReconcile(t *testing.T) {
	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	const revision = "3l3qo2vutsw2b"
	key, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)
	store := mst.NewMemBlockStore()
	snapshotRepo := &repo.Repo{DID: atmos.DID(did), Clock: atmos.NewTIDClock(0), Store: store, Tree: mst.NewTree(store)}
	var car bytes.Buffer
	require.NoError(t, snapshotRepo.ExportCAR(&car, key))

	var downloads atomic.Int64
	pds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.listRepos":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"repos":[{"did":"` + did + `","rev":"` + revision + `","head":"head","active":true}]}`))
		case "/xrpc/com.atproto.sync.getRepo":
			downloads.Add(1)
			_, _ = w.Write(car.Bytes())
		default:
			http.NotFound(w, r)
		}
	}))
	defer pds.Close()

	m, db := newManager(t, t.TempDir())
	defer db.Close()
	_, err = m.AddSource(pds.URL)
	require.NoError(t, err)
	resolver := &snapshotIdentityResolver{responses: []snapshotIdentityResponse{{doc: snapshotDIDDocument(atmos.DID(did), key.PublicKey(), "https://moved.example")}}}
	directory := &identity.Directory{Resolver: resolver, Cache: identity.NewLRUCache(1, time.Hour)}
	directory.Cache.Set(t.Context(), "did:"+did, &identity.Identity{DID: atmos.DID(did), Services: map[string]identity.ServiceEndpoint{"atproto_pds": {URL: pds.URL}}})
	var reconciled atomic.Int64
	processor := PDSProcessor{Manager: m, HTTPClient: pds.Client(), Directory: directory, Reconcile: func(context.Context, ingest.Snapshot) error {
		reconciled.Add(1)
		return nil
	}}
	runPDSProcessorUntilIncomplete(t, m, processor)
	require.Equal(t, "source_changed", m.List()[0].ErrorCode)
	require.Equal(t, int64(1), downloads.Load())
	require.Zero(t, reconciled.Load())
	require.Equal(t, 1, resolver.Calls(), "the migrated DID binding must be freshly resolved")
}

func TestPDSProcessorVerifySnapshotWithoutDirectoryIsUnavailable(t *testing.T) {
	did := atmos.DID("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")
	processor := PDSProcessor{}
	for _, err := range []error{
		processor.verifySource(t.Context(), did, "https://pds.example"),
		processor.verifySnapshot(t.Context(), Job{}, atmossync.ListReposEntry{DID: did, Rev: "3l3qo2vutsw2b"}, &repo.Commit{DID: string(did), Rev: "3l3qo2vutsw2b"}),
	} {
		var input *InputError
		require.ErrorAs(t, err, &input)
		require.Equal(t, "identity_unavailable", input.Code)
		require.True(t, input.Unavailable)
	}
}

type snapshotIdentityResponse struct {
	doc *identity.DIDDocument
	err error
}

type snapshotIdentityResolver struct {
	mu        sync.Mutex
	responses []snapshotIdentityResponse
	calls     int
}

func (r *snapshotIdentityResolver) ResolveDID(_ context.Context, _ atmos.DID) (*identity.DIDDocument, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	response := r.responses[min(r.calls, len(r.responses)-1)]
	r.calls++
	return response.doc, response.err
}

func (r *snapshotIdentityResolver) ResolveHandle(_ context.Context, _ atmos.Handle) (atmos.DID, error) {
	return "", errors.New("snapshot identity resolver does not resolve handles")
}

func (r *snapshotIdentityResolver) Calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func snapshotDIDDocument(did atmos.DID, key atmoscrypto.PublicKey, pds string) *identity.DIDDocument {
	return &identity.DIDDocument{
		ID: string(did),
		VerificationMethod: []identity.VerificationMethod{{
			ID:                 string(did) + "#atproto",
			Type:               "Multikey",
			Controller:         string(did),
			PublicKeyMultibase: key.Multibase(),
		}},
		Service: []identity.Service{{
			ID:              "#atproto_pds",
			Type:            "AtprotoPersonalDataServer",
			ServiceEndpoint: pds,
		}},
	}
}

func signedSnapshotCommit(t *testing.T, did atmos.DID, revision string, key atmoscrypto.PrivateKey) *repo.Commit {
	t.Helper()
	commit := &repo.Commit{
		DID:     string(did),
		Version: atmossync.CommitVersion,
		Data:    cbor.ComputeCID(cbor.CodecDagCBOR, []byte("snapshot-root")),
		Rev:     revision,
	}
	require.NoError(t, commit.Sign(key))
	return commit
}

func TestPDSProcessorPersistsPermanentSnapshotRejectionWithoutProgress(t *testing.T) {
	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	var listedRevision atomic.Value
	listedRevision.Store("3l3qo2vutsw2b")
	var downloads atomic.Int64
	pds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.listRepos":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"repos":[{"did":"` + did + `","rev":"` + listedRevision.Load().(string) + `","head":"head","active":true}]}`))
		case "/xrpc/com.atproto.sync.getRepo":
			downloads.Add(1)
			_, _ = w.Write([]byte{1, 0xff})
		default:
			http.NotFound(w, r)
		}
	}))
	defer pds.Close()
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	job, err := m.AddSource(pds.URL)
	require.NoError(t, err)
	directory := &identity.Directory{Cache: identity.NewLRUCache(1, time.Hour)}
	directory.Cache.Set(t.Context(), "did:"+did, &identity.Identity{DID: atmos.DID(did), Services: map[string]identity.ServiceEndpoint{"atproto_pds": {URL: pds.URL}}})
	processor := PDSProcessor{Manager: m, HTTPClient: pds.Client(), Directory: directory}
	runPDSProcessorUntilFailed(t, m, processor)
	require.Equal(t, int64(1), downloads.Load())
	first := m.List()[0]
	require.Equal(t, Failed, first.State)
	require.Equal(t, "invalid_repository", first.ErrorCode)
	require.Empty(t, first.CompletedRepos)
	require.Empty(t, first.Cursor)
	require.True(t, first.TotalReposKnown, "the frozen inventory denominator is published before repository processing")
	require.Len(t, m.ListSnapshotRejections(), 1)

	require.NoError(t, m.Retry(job.ID))
	runPDSProcessorUntilFailed(t, m, processor)
	require.Equal(t, int64(1), downloads.Load(), "a matching durable rejection must not redownload")
	require.Empty(t, m.List()[0].CompletedRepos)
	require.Empty(t, m.List()[0].Cursor)

	listedRevision.Store("3l3qo2vutsw2c")
	require.NoError(t, m.Retry(job.ID))
	runPDSProcessorUntilFailed(t, m, processor)
	require.Equal(t, int64(2), downloads.Load(), "a changed listed revision requires a fresh download")
}

func TestPDSProcessorPermanentRejectionDoesNotAdvanceMultiPageInventory(t *testing.T) {
	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	const next = "second-page"
	var revision atomic.Value
	revision.Store("")
	var downloads atomic.Int64
	var mu sync.Mutex
	var cursors []string
	pds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.listRepos":
			cursor := r.URL.Query().Get("cursor")
			mu.Lock()
			cursors = append(cursors, cursor)
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			switch cursor {
			case "":
				_, _ = w.Write([]byte(`{"repos":[{"did":"` + did + `","rev":"` + revision.Load().(string) + `","head":"head","active":true}],"cursor":"` + next + `"}`))
			case next:
				_, _ = w.Write([]byte(`{"repos":[{"did":"did:plc:bbbbbbbbbbbbbbbbbbbbbbbb","rev":"3l3qo2vutsw2c","head":"head","active":true}]}`))
			default:
				http.Error(w, "unexpected cursor", http.StatusBadRequest)
			}
		case "/xrpc/com.atproto.sync.getRepo":
			downloads.Add(1)
			_, _ = w.Write([]byte{1, 0xff})
		default:
			http.NotFound(w, r)
		}
	}))
	defer pds.Close()

	m, db := newManager(t, t.TempDir())
	defer db.Close()
	job, err := m.AddSource(pds.URL)
	require.NoError(t, err)
	directory := &identity.Directory{Cache: identity.NewLRUCache(1, time.Hour)}
	directory.Cache.Set(t.Context(), "did:"+did, &identity.Identity{DID: atmos.DID(did), Services: map[string]identity.ServiceEndpoint{"atproto_pds": {URL: pds.URL}}})
	processor := PDSProcessor{Manager: m, HTTPClient: pds.Client(), Directory: directory}
	runPDSProcessorUntilFailed(t, m, processor)
	first := m.List()[0]
	require.Equal(t, "invalid_listing_revision", first.ErrorCode)
	require.Empty(t, first.CompletedRepos)
	require.Empty(t, first.Cursor)
	require.Zero(t, first.EnumeratedRepos)
	require.False(t, first.TotalReposKnown)
	require.Zero(t, downloads.Load())
	require.Len(t, m.ListSnapshotRejections(), 1)

	require.NoError(t, m.Retry(job.ID))
	runPDSProcessorUntilFailed(t, m, processor)
	require.Equal(t, "invalid_listing_revision", m.List()[0].ErrorCode)
	require.Zero(t, downloads.Load(), "a matching malformed listing rejection must not download")
	require.Empty(t, m.List()[0].Cursor)
	require.False(t, m.List()[0].TotalReposKnown, "the rejected first page cannot complete through page two")

	revision.Store("3l3qo2vutsw2b")
	require.NoError(t, m.Retry(job.ID))
	runPDSProcessorUntilFailed(t, m, processor)
	require.Equal(t, int64(1), downloads.Load(), "a changed listed position requires a fresh download")
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"", "", "", next}, cursors, "the retry completes the frozen inventory before processing its entries")
}

func TestPDSProcessorRejectsMalformedListingDIDBeforeDownload(t *testing.T) {
	const did = "not-a-did"
	const revision = "3l3qo2vutsw2b"
	const next = "second-page"
	var downloads atomic.Int64
	pds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/xrpc/com.atproto.sync.getRepo", r.URL.Path)
		downloads.Add(1)
		http.Error(w, "getRepo must not be called for a malformed listing DID", http.StatusInternalServerError)
	}))
	defer pds.Close()

	m, db := newManager(t, t.TempDir())
	defer db.Close()
	job, err := m.AddSource(pds.URL)
	require.NoError(t, err)
	processor := PDSProcessor{Manager: m, HTTPClient: pds.Client(), Directory: &identity.Directory{}}
	client := processor.client(job)
	page := atmossync.ListReposPage{
		Entries:    []atmossync.ListReposEntry{{DID: atmos.DID(did), Rev: revision, Active: true}},
		NextCursor: next,
	}

	err = processor.processPage(t.Context(), client, &job, page)
	var input *InputError
	require.ErrorAs(t, err, &input)
	require.Equal(t, "invalid_listing_did", input.Code)
	require.False(t, input.Unavailable)
	require.Empty(t, job.CompletedRepos)
	require.Empty(t, job.Cursor)
	require.Zero(t, job.EnumeratedRepos)
	require.False(t, job.TotalReposKnown)
	require.Zero(t, downloads.Load())
	rejections := m.ListSnapshotRejections()
	require.Len(t, rejections, 1)
	require.Equal(t, "invalid_listing_did", rejections[0].Code)

	err = processor.processPage(t.Context(), client, &job, page)
	require.ErrorAs(t, err, &input)
	require.Equal(t, "invalid_listing_did", input.Code)
	require.Zero(t, downloads.Load(), "a matching malformed DID rejection must not download")
	require.Empty(t, job.Cursor, "the cursor must not advance past the malformed listing DID")
}

func TestPDSProcessorStalledBodyIsIncompleteWithoutRejectionOrProgress(t *testing.T) {
	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	const revision = "3l3qo2vutsw2b"
	logs := captureDefaultLogs(t)
	var downloads atomic.Int64
	pds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.listRepos":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"repos":[{"did":"` + did + `","rev":"` + revision + `","head":"head","active":true}]}`))
		case "/xrpc/com.atproto.sync.getRepo":
			downloads.Add(1)
			w.Header().Set("Content-Type", "application/vnd.ipld.car")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	defer pds.Close()

	m, db := newManager(t, t.TempDir())
	defer db.Close()
	job, err := m.AddSource(pds.URL)
	require.NoError(t, err)
	directory := &identity.Directory{Cache: identity.NewLRUCache(1, time.Hour)}
	directory.Cache.Set(t.Context(), "did:"+did, &identity.Identity{DID: atmos.DID(did), Services: map[string]identity.ServiceEndpoint{"atproto_pds": {URL: pds.URL}}})
	httpClient := pds.Client()
	httpClient.Timeout = 50 * time.Millisecond
	processor := PDSProcessor{Manager: m, HTTPClient: httpClient, Directory: directory}

	runPDSProcessorUntilIncomplete(t, m, processor)
	first := m.List()[0]
	require.NotNil(t, first.Evidence)
	require.Zero(t, first.Evidence.Scanned)
	require.Zero(t, first.Evidence.NoMatch)
	require.Zero(t, first.Evidence.AttributableRecords)
	require.Equal(t, int64(3), downloads.Load())
	require.Empty(t, first.CompletedRepos)
	require.Empty(t, first.Cursor)
	require.Empty(t, m.ListSnapshotRejections())

	require.NoError(t, m.Retry(job.ID))
	runPDSProcessorUntilIncomplete(t, m, processor)
	require.Equal(t, int64(6), downloads.Load(), "a stalled body is retried within each attempt and downloaded again after explicit retry")
	second := m.List()[0]
	require.Empty(t, second.CompletedRepos)
	require.Empty(t, second.Cursor)
	require.Empty(t, m.ListSnapshotRejections())

	output := logs.String()
	require.Equal(t, 2, strings.Count(output, `"level":"WARN"`))
	require.Contains(t, output, `"stage":"getRepo/body"`)
	require.Contains(t, output, `"cause_class":"timeout"`)
	require.Contains(t, output, `"repository_did":"`+did+`"`)
	require.NotContains(t, output, `"http_status"`)
}

func TestPDSProcessorRetriesTransientGetRepoFailureAndCompletes(t *testing.T) {
	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	key, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)
	blockStore := mst.NewMemBlockStore()
	snapshotRepo := &repo.Repo{DID: atmos.DID(did), Clock: atmos.NewTIDClock(0), Store: blockStore, Tree: mst.NewTree(blockStore)}
	var car bytes.Buffer
	require.NoError(t, snapshotRepo.ExportCAR(&car, key))
	_, commit, err := repo.LoadCompleteFromCAR(bytes.NewReader(car.Bytes()))
	require.NoError(t, err)

	var getRepoAttempts atomic.Int64
	pds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.listRepos":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"repos":[{"did":"` + did + `","rev":"` + commit.Rev + `","head":"head","active":true}]}`))
		case "/xrpc/com.atproto.sync.getRepo":
			switch getRepoAttempts.Add(1) {
			case 1:
				http.Error(w, "temporary PDS rate limit", http.StatusTooManyRequests)
				return
			case 2:
				http.Error(w, "temporary PDS failure", http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/vnd.ipld.car")
			_, _ = w.Write(car.Bytes())
		default:
			http.NotFound(w, r)
		}
	}))
	defer pds.Close()

	m, db := newManager(t, t.TempDir())
	defer db.Close()
	_, err = m.AddSource(pds.URL)
	require.NoError(t, err)
	directory := &identity.Directory{
		Resolver: &snapshotIdentityResolver{responses: []snapshotIdentityResponse{{doc: snapshotDIDDocument(atmos.DID(did), key.PublicKey(), pds.URL)}}},
		Cache:    identity.NewLRUCache(1, time.Hour),
	}
	directory.Cache.Set(t.Context(), "did:"+did, &identity.Identity{DID: atmos.DID(did), Services: map[string]identity.ServiceEndpoint{"atproto_pds": {URL: pds.URL}}})
	var reconciled atomic.Int64
	processor := PDSProcessor{
		Manager: m, HTTPClient: pds.Client(), Directory: directory,
		Reconcile: func(context.Context, ingest.Snapshot) error {
			reconciled.Add(1)
			return nil
		},
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx, processor.Run) }()
	require.Eventually(t, func() bool {
		state := m.List()[0].State
		return state == Complete || state == Incomplete || state == Failed
	}, 2*time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)

	completed := m.List()[0]
	require.Equal(t, Complete, completed.State, "a transient getRepo failure should recover within this attempt")
	require.Equal(t, int64(3), getRepoAttempts.Load())
	require.Equal(t, int64(1), reconciled.Load())
	require.Equal(t, commit.Rev, completed.CompletedRepos[did])
}

func TestPDSProcessorRetryExhaustionPreservesEarlierRepositoryProgress(t *testing.T) {
	const didA = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	const didB = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"
	key, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)
	blockStore := mst.NewMemBlockStore()
	snapshotRepo := &repo.Repo{DID: atmos.DID(didA), Clock: atmos.NewTIDClock(0), Store: blockStore, Tree: mst.NewTree(blockStore)}
	var car bytes.Buffer
	require.NoError(t, snapshotRepo.ExportCAR(&car, key))
	_, commit, err := repo.LoadCompleteFromCAR(bytes.NewReader(car.Bytes()))
	require.NoError(t, err)

	var didAAttempts, didBAttempts atomic.Int64
	pds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.listRepos":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"repos":[{"did":"` + didA + `","rev":"` + commit.Rev + `","head":"head-a","active":true},{"did":"` + didB + `","rev":"3l3qo2vutsw2c","head":"head-b","active":true}]}`))
		case "/xrpc/com.atproto.sync.getRepo":
			if r.URL.Query().Get("did") == didA {
				didAAttempts.Add(1)
				w.Header().Set("Content-Type", "application/vnd.ipld.car")
				_, _ = w.Write(car.Bytes())
				return
			}
			didBAttempts.Add(1)
			http.Error(w, "temporary PDS failure", http.StatusServiceUnavailable)
		default:
			http.NotFound(w, r)
		}
	}))
	defer pds.Close()

	m, db := newManager(t, t.TempDir())
	defer db.Close()
	_, err = m.AddSource(pds.URL)
	require.NoError(t, err)
	resolver := &snapshotIdentityResolver{responses: []snapshotIdentityResponse{
		{doc: snapshotDIDDocument(atmos.DID(didA), key.PublicKey(), pds.URL)},
		{doc: snapshotDIDDocument(atmos.DID(didB), key.PublicKey(), pds.URL)},
	}}
	directory := &identity.Directory{Resolver: resolver, Cache: identity.NewLRUCache(2, time.Hour)}
	for _, did := range []string{didA, didB} {
		directory.Cache.Set(t.Context(), "did:"+did, &identity.Identity{DID: atmos.DID(did), Services: map[string]identity.ServiceEndpoint{"atproto_pds": {URL: pds.URL}}})
	}
	var reconciled atomic.Int64
	processor := PDSProcessor{
		Manager: m, HTTPClient: pds.Client(), Directory: directory,
		Reconcile: func(context.Context, ingest.Snapshot) error {
			reconciled.Add(1)
			return nil
		},
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx, processor.Run) }()
	require.Eventually(t, func() bool {
		state := m.List()[0].State
		return state == Complete || state == Incomplete || state == Failed
	}, 2*time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)

	incomplete := m.List()[0]
	require.NotNil(t, incomplete.Evidence)
	require.Equal(t, 1, incomplete.Evidence.Scanned)
	require.Equal(t, 1, incomplete.Evidence.NoMatch)
	require.Zero(t, incomplete.Evidence.Matching)
	require.Zero(t, incomplete.Evidence.AttributableRecords)
	require.Equal(t, Incomplete, incomplete.State)
	require.Equal(t, "repository_unavailable", incomplete.ErrorCode)
	require.True(t, incomplete.TotalReposKnown)
	require.Equal(t, 2, incomplete.TotalRepos)
	require.Equal(t, map[string]string{didA: commit.Rev}, incomplete.CompletedRepos,
		"exhausting retries for the next repository must not erase prior durable progress or acknowledge the failed repository")
	require.Equal(t, int64(1), didAAttempts.Load())
	require.Equal(t, int64(3), didBAttempts.Load())
	require.Equal(t, int64(1), reconciled.Load())
	require.Empty(t, m.ListSnapshotRejections())
}

func TestPDSProcessorLogsGetRepoRequestHTTPFailure(t *testing.T) {
	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	const revision = "3l3qo2vutsw2b"
	const responseBody = `{"error":"fixture_private_code","message":"fixture private response text"}`
	var getRepoAttempts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.listRepos":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"repos":[{"did":"` + did + `","rev":"` + revision + `","head":"head","active":true}]}`))
		case "/xrpc/com.atproto.sync.getRepo":
			getRepoAttempts.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(responseBody))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	logs := captureDefaultLogs(t)
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	job, err := m.AddSource(server.URL)
	require.NoError(t, err)
	directory := &identity.Directory{Cache: identity.NewLRUCache(1, time.Hour)}
	directory.Cache.Set(t.Context(), "did:"+did, &identity.Identity{DID: atmos.DID(did), Services: map[string]identity.ServiceEndpoint{"atproto_pds": {URL: server.URL}}})
	processor := PDSProcessor{Manager: m, HTTPClient: server.Client(), Directory: directory}

	runPDSProcessorUntilIncomplete(t, m, processor)
	output := logs.String()
	require.Equal(t, 1, strings.Count(output, `"level":"WARN"`))
	require.Contains(t, output, `"job_id":"`+job.ID+`"`)
	require.Contains(t, output, `"stage":"getRepo/request"`)
	require.Contains(t, output, `"cause_class":"http"`)
	require.Contains(t, output, `"repository_did":"`+did+`"`)
	require.Equal(t, int64(3), getRepoAttempts.Load(), "503 responses are retried up to three total attempts")
	require.Contains(t, output, `"http_status":503`)
	require.NotContains(t, output, "fixture_private_code")
	require.NotContains(t, output, "fixture private response text")
}

func TestPDSProcessorLogsGetRepoBodyReadFailure(t *testing.T) {
	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	const revision = "3l3qo2vutsw2b"
	car, err := os.ReadFile("../../corpus/testdata/repo.car")
	require.NoError(t, err)
	var getRepoAttempts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.listRepos":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"repos":[{"did":"` + did + `","rev":"` + revision + `","head":"head","active":true}]}`))
		case "/xrpc/com.atproto.sync.getRepo":
			getRepoAttempts.Add(1)
			w.Header().Set("Content-Type", "application/vnd.ipld.car")
			w.Header().Set("Content-Length", "9999999")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(car)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	logs := captureDefaultLogs(t)
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	job, err := m.AddSource(server.URL)
	require.NoError(t, err)
	directory := &identity.Directory{Cache: identity.NewLRUCache(1, time.Hour)}
	directory.Cache.Set(t.Context(), "did:"+did, &identity.Identity{DID: atmos.DID(did), Services: map[string]identity.ServiceEndpoint{"atproto_pds": {URL: server.URL}}})
	processor := PDSProcessor{Manager: m, HTTPClient: server.Client(), Directory: directory}

	runPDSProcessorUntilIncomplete(t, m, processor)
	output := logs.String()
	require.Equal(t, 1, strings.Count(output, `"level":"WARN"`))
	require.Contains(t, output, `"job_id":"`+job.ID+`"`)
	require.Contains(t, output, `"stage":"getRepo/body"`)
	require.Equal(t, int64(3), getRepoAttempts.Load(), "failed response-body reads are retried within the bounded policy")
	require.Contains(t, output, `"cause_class":"body_read"`)
	require.Contains(t, output, `"repository_did":"`+did+`"`)
	require.NotContains(t, output, `"http_status"`)
}

func TestPDSProcessorLogsGetRepoTransportFailure(t *testing.T) {
	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	const revision = "3l3qo2vutsw2b"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/xrpc/com.atproto.sync.listRepos" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"repos":[{"did":"` + did + `","rev":"` + revision + `","head":"head","active":true}]}`))
	}))
	defer server.Close()
	var getRepoAttempts atomic.Int64
	transport := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/xrpc/com.atproto.sync.getRepo" {
			getRepoAttempts.Add(1)
			return nil, errors.New("fixture private transport detail")
		}
		return http.DefaultTransport.RoundTrip(r)
	})

	logs := captureDefaultLogs(t)
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	job, err := m.AddSource(server.URL)
	require.NoError(t, err)
	directory := &identity.Directory{Cache: identity.NewLRUCache(1, time.Hour)}
	directory.Cache.Set(t.Context(), "did:"+did, &identity.Identity{DID: atmos.DID(did), Services: map[string]identity.ServiceEndpoint{"atproto_pds": {URL: server.URL}}})
	processor := PDSProcessor{Manager: m, HTTPClient: &http.Client{Transport: transport}, Directory: directory}

	runPDSProcessorUntilIncomplete(t, m, processor)
	output := logs.String()
	require.Equal(t, 1, strings.Count(output, `"level":"WARN"`))
	require.Contains(t, output, `"job_id":"`+job.ID+`"`)
	require.Contains(t, output, `"stage":"getRepo/request"`)
	require.Equal(t, int64(3), getRepoAttempts.Load(), "transport failures exhaust only the three bounded fetch attempts")
	require.Contains(t, output, `"cause_class":"transport"`)
	require.Contains(t, output, `"repository_did":"`+did+`"`)
	require.NotContains(t, output, `"http_status"`)
	require.NotContains(t, output, "fixture private transport detail")
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestPDSProcessorDoesNotWarnWhenOutcomePersistenceFails(t *testing.T) {
	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	const revision = "3l3qo2vutsw2b"
	injected := errors.New("injected terminal outcome checkpoint failure")
	fault := &store.KeyPrefixFault{Prefix: []byte(stateKey), Op: store.WriteOpSet, Ordinal: 3, Err: injected}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.listRepos":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"repos":[{"did":"` + did + `","rev":"` + revision + `","head":"head","active":true}]}`))
		case "/xrpc/com.atproto.sync.getRepo":
			http.Error(w, "fixture transient failure", http.StatusServiceUnavailable)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	logs := captureDefaultLogs(t)
	m, db := newManagerWithOptions(t, t.TempDir(), store.WithFaultInjector(fault))
	defer db.Close()
	_, err := m.AddSource(server.URL)
	require.NoError(t, err)
	directory := &identity.Directory{Cache: identity.NewLRUCache(1, time.Hour)}
	directory.Cache.Set(t.Context(), "did:"+did, &identity.Identity{DID: atmos.DID(did), Services: map[string]identity.ServiceEndpoint{"atproto_pds": {URL: server.URL}}})
	processor := PDSProcessor{Manager: m, HTTPClient: server.Client(), Directory: directory}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.ErrorIs(t, m.Run(ctx, processor.Run), injected)
	require.Equal(t, Running, m.List()[0].State)
	require.True(t, m.List()[0].TotalReposKnown)
	require.Zero(t, strings.Count(logs.String(), `"level":"WARN"`))
}

func TestPDSProcessorDoesNotWarnForCancellationOrReconciliation(t *testing.T) {
	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	const revision = "3l3qo2vutsw2b"
	for _, action := range []string{"cancel", "source_removed", "policy_changed"} {
		t.Run(action, func(t *testing.T) {
			started := make(chan struct{}, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/xrpc/com.atproto.sync.listRepos":
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"repos":[{"did":"` + did + `","rev":"` + revision + `","head":"head","active":true}]}`))
				case "/xrpc/com.atproto.sync.getRepo":
					w.Header().Set("Content-Type", "application/vnd.ipld.car")
					w.WriteHeader(http.StatusOK)
					w.(http.Flusher).Flush()
					select {
					case started <- struct{}{}:
					default:
					}
					<-r.Context().Done()
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()

			logs := captureDefaultLogs(t)
			m, db := newManager(t, t.TempDir())
			defer db.Close()
			job, err := m.AddSource(server.URL)
			require.NoError(t, err)
			directory := &identity.Directory{Cache: identity.NewLRUCache(1, time.Hour)}
			directory.Cache.Set(t.Context(), "did:"+did, &identity.Identity{DID: atmos.DID(did), Services: map[string]identity.ServiceEndpoint{"atproto_pds": {URL: server.URL}}})
			processor := PDSProcessor{Manager: m, HTTPClient: server.Client(), Directory: directory}

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- m.Run(ctx, processor.Run) }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("getRepo request did not start")
			}
			switch action {
			case "cancel":
				err = m.Cancel(job.ID)
			case "source_removed":
				err = m.RemoveSource(server.URL)
			case "policy_changed":
				_, err = m.SetPolicy(t.Context(), job.Policy.Revision, []string{"app.bsky.feed.like"})
			}
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				current, err := m.Get(job.ID)
				return err == nil && current.State == Canceled
			}, time.Second, time.Millisecond)
			cancel()
			require.ErrorIs(t, <-done, context.Canceled)
			require.Zero(t, strings.Count(logs.String(), `"level":"WARN"`))
		})
	}
}

func TestPDSProcessorRejectionWritePrecedesOutcome(t *testing.T) {
	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	const revision = "3l3qo2vutsw2b"
	var downloads atomic.Int64
	pds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.listRepos":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"repos":[{"did":"` + did + `","rev":"` + revision + `","head":"head","active":true}]}`))
		case "/xrpc/com.atproto.sync.getRepo":
			downloads.Add(1)
			_, _ = w.Write([]byte{1, 0xff})
		default:
			http.NotFound(w, r)
		}
	}))
	defer pds.Close()

	injected := errors.New("injected terminal outcome checkpoint failure")
	fault := &store.KeyPrefixFault{Prefix: []byte(stateKey), Op: store.WriteOpSet, Ordinal: 4, Err: injected}
	dir := t.TempDir()
	m, db := newManagerWithOptions(t, dir, store.WithFaultInjector(fault))
	job, err := m.AddSource(pds.URL)
	require.NoError(t, err)
	directory := &identity.Directory{Cache: identity.NewLRUCache(1, time.Hour)}
	directory.Cache.Set(t.Context(), "did:"+did, &identity.Identity{DID: atmos.DID(did), Services: map[string]identity.ServiceEndpoint{"atproto_pds": {URL: pds.URL}}})
	processor := PDSProcessor{Manager: m, HTTPClient: pds.Client(), Directory: directory}

	require.ErrorIs(t, m.Run(t.Context(), processor.Run), injected)
	require.Equal(t, int64(1), downloads.Load())
	beforeRestart := m.List()[0]
	require.Equal(t, Running, beforeRestart.State)
	require.Empty(t, beforeRestart.CompletedRepos)
	require.Empty(t, beforeRestart.Cursor)
	require.Len(t, m.ListSnapshotRejections(), 1)
	require.NoError(t, db.Close())

	m, db = newManager(t, dir)
	defer db.Close()
	afterRestart := m.List()[0]
	require.Equal(t, Pending, afterRestart.State)
	require.Empty(t, afterRestart.CompletedRepos)
	require.Empty(t, afterRestart.Cursor)
	require.Len(t, m.ListSnapshotRejections(), 1)

	processor.Manager = m
	require.NoError(t, m.Retry(job.ID))
	runPDSProcessorUntilFailed(t, m, processor)
	require.Equal(t, int64(1), downloads.Load(), "the durable rejection must prevent a second download after recovery")
}

func runPDSProcessorUntilFailed(t *testing.T, m *Manager, processor PDSProcessor) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx, processor.Run) }()
	require.Eventually(t, func() bool {
		state := m.List()[0].State
		return state == Failed || state == Incomplete
	}, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Equal(t, Failed, m.List()[0].State, "job=%+v", m.List()[0])
}

func runPDSProcessorUntilIncomplete(t *testing.T, m *Manager, processor PDSProcessor) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx, processor.Run) }()
	require.Eventually(t, func() bool { return m.List()[0].State == Incomplete }, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestFetchRepositoryClassifiesIncompleteCARUnavailable(t *testing.T) {
	raw, err := os.ReadFile("../../corpus/testdata/repo.car")
	require.NoError(t, err)
	pds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/xrpc/com.atproto.sync.getRepo", r.URL.Path)
		w.Header().Set("Content-Type", "application/vnd.ipld.car")
		_, _ = w.Write(raw[:len(raw)/2])
	}))
	defer pds.Close()
	client := atmossync.NewClient(atmossync.Options{Client: &xrpc.Client{
		Host:       pds.URL,
		HTTPClient: gt.Some(pds.Client()),
		Retry:      gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
	}})

	_, _, err = fetchRepository(t.Context(), client, atmos.DID("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"))
	var input *InputError
	require.ErrorAs(t, err, &input)
	require.Equal(t, "repository_incomplete", input.Code)
	require.True(t, input.Unavailable)
}

func primeCompletedRepos(t *testing.T, m *Manager, id string, repos map[string]string) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	next := clone(m.data)
	job := next.Jobs[id]
	job.CompletedRepos = repos
	next.Jobs[id] = job
	require.NoError(t, m.commit(next))
}
