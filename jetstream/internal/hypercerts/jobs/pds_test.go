package jobs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
