package jobs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/jcalabro/atmos"
	atmoscrypto "github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/mst"
	"github.com/jcalabro/atmos/repo"
	"github.com/stretchr/testify/require"
)

func TestRecoveryRepositoryParentDeadlineAndClientTimeout(t *testing.T) {
	for _, parentDeadline := range []bool{true, false} {
		t.Run(map[bool]string{true: "parent_deadline", false: "client_timeout"}[parentDeadline], func(t *testing.T) {
			m, db := newManager(t, t.TempDir())
			defer db.Close()
			source, err := m.AddSource("https://pds.example")
			require.NoError(t, err)
			require.NoError(t, m.Cancel(source.ID))
			job, err := m.RequestRepositoryRecovery(source.PDS, []string{recoveryDID1}, "")
			require.NoError(t, err)
			claimForSeed(t, m, job.ID)
			require.NoError(t, m.CheckpointInventory(job.ID, "", map[string]string{recoveryDID1: atmos.NewTIDClock(0).Next().String()}, true))
			job, err = m.Get(job.ID)
			require.NoError(t, err)
			directory := &identity.Directory{Cache: identity.NewLRUCache(1, time.Hour)}
			directory.Cache.Set(t.Context(), "did:"+recoveryDID1, &identity.Identity{DID: atmos.DID(recoveryDID1), Services: map[string]identity.ServiceEndpoint{"atproto_pds": {URL: job.PDS}}})
			var requests atomic.Int64
			client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				requests.Add(1)
				<-r.Context().Done()
				return nil, r.Context().Err()
			})}
			ctx := t.Context()
			if parentDeadline {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 50*time.Millisecond)
				defer cancel()
			} else {
				client.Timeout = 50 * time.Millisecond
			}
			processor := PDSProcessor{Manager: m, HTTPClient: client, Directory: directory}
			outcome := processor.Run(ctx, job)
			require.Equal(t, int64(1), requests.Load())
			retry, err := m.GetRepositoryRetry(job.ID, recoveryDID1)
			require.NoError(t, err)
			if parentDeadline {
				require.ErrorIs(t, outcome, context.DeadlineExceeded)
				require.Equal(t, RepositoryRetryInFlight, retry.State)
				require.Nil(t, retry.Failure)
			} else {
				require.NoError(t, ctx.Err())
				require.ErrorIs(t, outcome, errJobYield)
				require.Equal(t, RepositoryRetryWait, retry.State)
				require.NotNil(t, retry.Failure)
				require.Equal(t, RepositoryFailureTimeout, retry.Failure.Category)
			}
		})
	}
}

func TestRecoveryDiscoveryFailureAndCooldownAreAtomic(t *testing.T) {
	injected := errors.New("cooldown commit failed")
	fault := &store.KeyPrefixFault{Prefix: []byte(pdsCooldownPrefix), Op: store.WriteOpBatchCommit, Ordinal: 1, Err: injected}
	path := t.TempDir()
	m, db := newManagerWithOptions(t, path, store.WithFaultInjector(fault))
	source, err := m.AddSource("https://pds.example")
	require.NoError(t, err)
	require.NoError(t, m.Cancel(source.ID))
	job, err := m.RequestRepositoryRecovery(source.PDS, []string{recoveryDID1, recoveryDID2}, "")
	require.NoError(t, err)
	claimForSeed(t, m, job.ID)
	until := time.Now().Add(time.Minute).UTC()
	require.ErrorIs(t, m.recordRecoveryDiscoveryFailure(job.ID, recoveryDID1, &until), injected)
	stored, err := m.Get(job.ID)
	require.NoError(t, err)
	require.Empty(t, stored.RecoveryDiscoveryFailures)
	_, found, err := m.GetPDSCooldown(job.PDS)
	require.NoError(t, err)
	require.False(t, found)
	require.NoError(t, db.Close())
	m, db = newManager(t, path)
	stored, err = m.Get(job.ID)
	require.NoError(t, err)
	require.Empty(t, stored.RecoveryDiscoveryFailures)
	_, found, err = m.GetPDSCooldown(job.PDS)
	require.NoError(t, err)
	require.False(t, found)
	claimForSeed(t, m, job.ID)
	require.NoError(t, m.recordRecoveryDiscoveryFailure(job.ID, recoveryDID1, &until))
	shorter := until.Add(-time.Second)
	require.NoError(t, m.recordRecoveryDiscoveryFailure(job.ID, recoveryDID2, &shorter))
	require.NoError(t, db.Close())
	m, db = newManager(t, path)
	defer db.Close()
	stored, err = m.Get(job.ID)
	require.NoError(t, err)
	require.Equal(t, []string{recoveryDID1, recoveryDID2}, stored.RecoveryDiscoveryFailures)
	cooldown, found, err := m.GetPDSCooldown(job.PDS)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, until, cooldown.Until)
}

func TestRecoveryPartialDiscoveryReconcilesAndRetriesAcrossRestart(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		t.Run(map[bool]string{false: "unavailable", true: "malformed"}[malformed], func(t *testing.T) {
			testRecoveryPartialDiscovery(t, malformed)
		})
	}
}

func testRecoveryPartialDiscovery(t *testing.T, malformed bool) {
	logOutput := captureDefaultLogs(t)
	key, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)
	cars := map[string][]byte{}
	revisions := map[string]string{}
	for _, did := range []string{recoveryDID1, recoveryDID2} {
		blocks := mst.NewMemBlockStore()
		repository := &repo.Repo{DID: atmos.DID(did), Clock: atmos.NewTIDClock(0), Store: blocks, Tree: mst.NewTree(blocks)}
		var car bytes.Buffer
		require.NoError(t, repository.ExportCAR(&car, key))
		_, commit, err := repo.LoadCompleteFromCAR(bytes.NewReader(car.Bytes()))
		require.NoError(t, err)
		cars[did], revisions[did] = car.Bytes(), commit.Rev
	}
	var available atomic.Bool
	var discoveries, downloads, reconciled atomic.Int64
	pds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		did := r.URL.Query().Get("did")
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.getLatestCommit":
			discoveries.Add(1)
			if did == recoveryDID1 && !available.Load() {
				if malformed {
					_, _ = w.Write([]byte(`{"cid":"bafyreibrj4xfbvcicomna6bal32tecjzlrjh2k7u7hs7amdamgov4onuiy","rev":"invalid"}`))
				} else {
					http.Error(w, "unavailable", http.StatusNotFound)
				}
				return
			}
			_, _ = w.Write([]byte(`{"cid":"bafyreibrj4xfbvcicomna6bal32tecjzlrjh2k7u7hs7amdamgov4onuiy","rev":"` + revisions[did] + `"}`))
		case "/xrpc/com.atproto.sync.getRepo":
			if downloads.Add(1) == 1 {
				http.Error(w, "retry", http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write(cars[did])
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer pds.Close()
	path := t.TempDir()
	m, db := newManager(t, path)
	source, err := m.AddSource(pds.URL)
	require.NoError(t, err)
	require.NoError(t, m.Cancel(source.ID))
	job, err := m.RequestRepositoryRecovery(pds.URL, []string{recoveryDID1, recoveryDID2}, "")
	require.NoError(t, err)
	resolver := &snapshotIdentityResolver{responses: []snapshotIdentityResponse{
		{doc: snapshotDIDDocument(atmos.DID(recoveryDID2), key.PublicKey(), pds.URL)},
		{doc: snapshotDIDDocument(atmos.DID(recoveryDID1), key.PublicKey(), pds.URL)},
	}}
	directory := &identity.Directory{Resolver: resolver, Cache: identity.NewLRUCache(3, time.Hour)}
	for _, did := range job.RecoveryDIDs {
		directory.Cache.Set(t.Context(), "did:"+did, &identity.Identity{DID: atmos.DID(did), Services: map[string]identity.ServiceEndpoint{"atproto_pds": {URL: pds.URL}}})
	}
	processor := PDSProcessor{Manager: m, HTTPClient: pds.Client(), Directory: directory, Reconcile: func(context.Context, ingest.Snapshot) error {
		reconciled.Add(1)
		return nil
	}}
	claimed := claimForSeed(t, m, job.ID)
	outcome := processor.Run(t.Context(), claimed)
	require.ErrorIs(t, outcome, errJobYield)
	require.NoError(t, m.finishJob(t.Context(), job.ID, outcome))
	pending, err := m.Get(job.ID)
	require.NoError(t, err)
	require.False(t, pending.TotalReposKnown)
	require.Equal(t, []string{recoveryDID1}, pending.RecoveryDiscoveryFailures)
	// The job ends passes later without attempting discovery again, so the pass
	// that observed the failure reports its bounded cause.
	var warning map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logOutput.String()), "\n") {
		var entry map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &entry))
		if entry["msg"] == "repository recovery revision discovery failed" {
			require.Nil(t, warning, "one seeding pass reports once")
			warning = entry
		}
	}
	require.NotNil(t, warning)
	require.Equal(t, job.ID, warning["job_id"])
	require.Equal(t, "getLatestCommit", warning["stage"])
	require.Equal(t, recoveryDID1, warning["repository_did"])
	require.EqualValues(t, 1, warning["failed_dids"])
	if malformed {
		require.Equal(t, pdsCauseUnknown, warning["cause_class"])
		require.NotContains(t, warning, "http_status")
	} else {
		require.Equal(t, pdsCauseHTTP, warning["cause_class"])
		require.EqualValues(t, http.StatusNotFound, warning["http_status"])
	}
	require.NotContains(t, warning, "err")
	retry, err := m.GetRepositoryRetry(job.ID, recoveryDID2)
	require.NoError(t, err)
	now := time.Now()
	m.mu.Lock()
	due, eligible, err := m.pendingJobDueLocked(pending, now)
	m.mu.Unlock()
	require.NoError(t, err)
	require.True(t, eligible)
	require.Equal(t, retry.RetryAt, due)
	require.True(t, due.After(now))
	require.NoError(t, db.Close())
	m, db = newManager(t, path)
	defer db.Close()
	processor.Manager = m
	restored, err := m.Get(job.ID)
	require.NoError(t, err)
	require.Equal(t, pending.RecoveryDiscoveryFailures, restored.RecoveryDiscoveryFailures)
	require.Equal(t, pending.EnumeratedRepos, restored.EnumeratedRepos)
	claimed = claimForSeed(t, m, job.ID)
	cooldown := time.Now().Add(100 * time.Millisecond).UTC()
	require.NoError(t, m.RecordPDSCooldown(job.ID, cooldown))
	outcome = processor.Run(t.Context(), claimed)
	require.ErrorIs(t, outcome, errJobYield)
	require.Equal(t, int64(2), discoveries.Load())
	require.Equal(t, int64(1), downloads.Load())
	require.NoError(t, m.finishJob(t.Context(), job.ID, outcome))
	time.Sleep(time.Until(retry.RetryAt) + 10*time.Millisecond)
	claimed = claimForSeed(t, m, job.ID)
	outcome = processor.Run(t.Context(), claimed)
	require.NoError(t, m.finishJob(t.Context(), job.ID, outcome))
	incomplete, err := m.Get(job.ID)
	require.NoError(t, err)
	require.Equal(t, Incomplete, incomplete.State)
	require.Equal(t, "recovery_revision_unavailable", incomplete.ErrorCode)
	require.False(t, incomplete.TotalReposKnown)
	require.Equal(t, revisions[recoveryDID2], incomplete.CompletedRepos[recoveryDID2])
	require.Equal(t, int64(1), reconciled.Load())
	require.False(t, incomplete.ReceiptPending)
	available.Store(true)
	require.NoError(t, m.Retry(job.ID))
	claimed = claimForSeed(t, m, job.ID)
	require.Empty(t, claimed.RecoveryDiscoveryFailures)
	require.Equal(t, incomplete.CompletedRepos, claimed.CompletedRepos)
	outcome = processor.Run(t.Context(), claimed)
	require.NoError(t, outcome)
	require.NoError(t, m.finishJob(t.Context(), job.ID, outcome))
	complete, err := m.Get(job.ID)
	require.NoError(t, err)
	require.Equal(t, Complete, complete.State)
	require.True(t, complete.TotalReposKnown)
	require.Len(t, complete.CompletedRepos, 2)
	require.Equal(t, int64(3), discoveries.Load())
	require.Equal(t, int64(3), downloads.Load())
	require.Equal(t, int64(2), reconciled.Load())
	require.False(t, complete.ReceiptPending)
	require.Equal(t, 1, strings.Count(logOutput.String(), "repository recovery revision discovery failed"))
}

func TestRecoveryDiscoveryCancellationDoesNotRecordUnavailable(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "canceled", true: "deadline"}[deadline], func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			pds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				cancel()
				<-r.Context().Done()
			}))
			defer pds.Close()
			m, db := newManager(t, t.TempDir())
			defer db.Close()
			source, err := m.AddSource(pds.URL)
			require.NoError(t, err)
			require.NoError(t, m.Cancel(source.ID))
			job, err := m.RequestRepositoryRecovery(pds.URL, []string{recoveryDID1}, "")
			require.NoError(t, err)
			claimed := claimForSeed(t, m, job.ID)
			if deadline {
				var stop context.CancelFunc
				ctx, stop = context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
				defer stop()
			}
			processor := PDSProcessor{Manager: m, HTTPClient: pds.Client(), Directory: &identity.Directory{}}
			err = processor.Run(ctx, claimed)
			require.ErrorIs(t, err, ctx.Err())
			stored, err := m.Get(job.ID)
			require.NoError(t, err)
			require.Empty(t, stored.RecoveryDiscoveryFailures)
			require.False(t, stored.TotalReposKnown)
		})
	}
}

func TestRecoveryDiscoveryCooldownRunAcrossRestart(t *testing.T) {
	var requests atomic.Int64
	pds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Retry-After", "2")
		http.Error(w, "limited", http.StatusTooManyRequests)
	}))
	defer pds.Close()
	path := t.TempDir()
	m, db := newManager(t, path)
	source, err := m.AddSource(pds.URL)
	require.NoError(t, err)
	require.NoError(t, m.Cancel(source.ID))
	job, err := m.RequestRepositoryRecovery(pds.URL, []string{recoveryDID1, recoveryDID2}, "")
	require.NoError(t, err)
	processor := PDSProcessor{Manager: m, HTTPClient: pds.Client(), Directory: &identity.Directory{}}
	claimed := claimForSeed(t, m, job.ID)
	outcome := processor.Run(t.Context(), claimed)
	require.ErrorIs(t, outcome, errJobYield)
	require.NoError(t, m.finishJob(t.Context(), job.ID, outcome))
	require.Equal(t, int64(1), requests.Load())
	cooldown, found, err := m.GetPDSCooldown(pds.URL)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, cooldown.Until.After(time.Now()))
	require.NoError(t, db.Close())
	m, db = newManager(t, path)
	defer db.Close()
	processor.Manager = m
	var calls atomic.Int64
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- m.Run(ctx, func(ctx context.Context, job Job) error {
			calls.Add(1)
			return processor.Run(ctx, job)
		})
	}()
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int64(0), calls.Load())
	require.Equal(t, int64(1), requests.Load())
	require.Eventually(t, func() bool { return requests.Load() == 2 }, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		stored, err := m.Get(job.ID)
		return err == nil && stored.State == Incomplete
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, int64(2), calls.Load())
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	stored, err := m.Get(job.ID)
	require.NoError(t, err)
	require.Contains(t, stored.RecoveryDiscoveryFailures, recoveryDID1)
}

func TestRecoveryFailedRetryDiscoversFreshRevision(t *testing.T) {
	var revision atomic.Value
	revision.Store(atmos.NewTIDClock(0).Next().String())
	pds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"cid":"bafyreibrj4xfbvcicomna6bal32tecjzlrjh2k7u7hs7amdamgov4onuiy","rev":"` + revision.Load().(string) + `"}`))
	}))
	defer pds.Close()
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	source, err := m.AddSource(pds.URL)
	require.NoError(t, err)
	require.NoError(t, m.Cancel(source.ID))
	job, err := m.RequestRepositoryRecovery(pds.URL, []string{recoveryDID1}, "")
	require.NoError(t, err)
	claimed := claimForSeed(t, m, job.ID)
	processor := PDSProcessor{Manager: m, HTTPClient: pds.Client()}
	require.NoError(t, processor.seedRecoveryInventory(t.Context(), processor.client(claimed), &claimed))
	require.NoError(t, m.finishJob(t.Context(), job.ID, &InputError{Code: "snapshot_rejected"}))
	require.NoError(t, m.Retry(job.ID))
	claimed = claimForSeed(t, m, job.ID)
	require.False(t, claimed.TotalReposKnown)
	entries, err := m.Inventory(job.ID)
	require.NoError(t, err)
	require.Empty(t, entries)
	revision.Store(atmos.NewTIDClock(0).Next().String())
	require.NoError(t, processor.seedRecoveryInventory(t.Context(), processor.client(claimed), &claimed))
	entries, err = m.Inventory(job.ID)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, revision.Load().(string), entries[0].Revision)
}
