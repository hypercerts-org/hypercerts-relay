package jobs

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

func TestPDSProcessorCountsOneResumableInventory(t *testing.T) {
	const cursor = "page-two"
	const didA = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	const didB = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"

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
			_, _ = w.Write([]byte(`{"repos":[{"did":"` + didA + `","rev":"rev-a","head":"head-a","active":true}],"cursor":"` + cursor + `"}`))
		case cursor:
			if attempt == 1 {
				http.Error(w, "fixture transient failure", http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte(`{"repos":[{"did":"` + didB + `","rev":"rev-b","head":"head-b","active":true}]}`))
		default:
			http.Error(w, "unexpected cursor", http.StatusBadRequest)
		}
	}))
	defer pds.Close()

	dir := t.TempDir()
	m, db := newManager(t, dir)
	job, err := m.AddSource(pds.URL)
	require.NoError(t, err)
	primeCompletedRepos(t, m, job.ID, map[string]string{didA: "rev-a", didB: "rev-b"})

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx, PDSProcessor{Manager: m, HTTPClient: pds.Client()}.Run) }()
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
	go func() { done <- m.Run(ctx, PDSProcessor{Manager: m, HTTPClient: pds.Client()}.Run) }()
	require.Eventually(t, func() bool { return m.List()[0].State == Complete }, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	complete := m.List()[0]
	require.True(t, complete.TotalReposKnown)
	require.Equal(t, 2, complete.TotalRepos)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"", cursor, cursor}, cursors)
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
	go func() { done <- m.Run(ctx, PDSProcessor{Manager: m, HTTPClient: pds.Client()}.Run) }()
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
	go func() { done <- m.Run(ctx, PDSProcessor{Manager: m, HTTPClient: pds.Client()}.Run) }()
	require.Eventually(t, func() bool { return m.List()[0].State == Incomplete }, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Equal(t, "source_unavailable", m.List()[0].ErrorCode)
	require.False(t, redirected.Load())
}

func TestPDSProcessorPersistsPermanentSnapshotRejectionBeforeAcknowledging(t *testing.T) {
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
	require.Equal(t, "3l3qo2vutsw2b", first.CompletedRepos[did])
	require.Len(t, m.listSnapshotRejections(), 1)

	require.NoError(t, m.Retry(job.ID))
	runPDSProcessorUntilFailed(t, m, processor)
	require.Equal(t, int64(1), downloads.Load(), "a matching durable rejection must not redownload")

	listedRevision.Store("3l3qo2vutsw2c")
	require.NoError(t, m.Retry(job.ID))
	runPDSProcessorUntilFailed(t, m, processor)
	require.Equal(t, int64(2), downloads.Load(), "a changed listed revision requires a fresh download")
}

func TestPDSProcessorStalledBodyIsIncompleteWithoutRejectionOrProgress(t *testing.T) {
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
	require.Equal(t, int64(1), downloads.Load())
	require.Empty(t, first.CompletedRepos)
	require.Empty(t, first.Cursor)
	require.Empty(t, m.listSnapshotRejections())

	require.NoError(t, m.Retry(job.ID))
	runPDSProcessorUntilIncomplete(t, m, processor)
	require.Equal(t, int64(2), downloads.Load(), "an incomplete body must be downloaded again on retry")
	second := m.List()[0]
	require.Empty(t, second.CompletedRepos)
	require.Empty(t, second.Cursor)
	require.Empty(t, m.listSnapshotRejections())
}

func TestPDSProcessorRejectionWritePrecedesAcknowledgement(t *testing.T) {
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

	injected := errors.New("injected acknowledgement checkpoint failure")
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
	require.Len(t, m.listSnapshotRejections(), 1)
	require.NoError(t, db.Close())

	m, db = newManager(t, dir)
	defer db.Close()
	afterRestart := m.List()[0]
	require.Equal(t, Pending, afterRestart.State)
	require.Empty(t, afterRestart.CompletedRepos)
	require.Empty(t, afterRestart.Cursor)
	require.Len(t, m.listSnapshotRejections(), 1)

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
