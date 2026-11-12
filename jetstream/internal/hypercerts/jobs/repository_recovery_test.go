package jobs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"

	"github.com/jcalabro/atmos/xrpc"
)

const (
	recoveryDID1 = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	recoveryDID2 = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"
	recoveryDID3 = "did:plc:cccccccccccccccccccccccc"
)

func TestNormalizeRecoveryDIDs(t *testing.T) {
	got, err := normalizeRecoveryDIDs([]string{recoveryDID2, recoveryDID1, recoveryDID2})
	require.NoError(t, err)
	require.Equal(t, []string{recoveryDID1, recoveryDID2}, got, "deduplicated and sorted")

	_, err = normalizeRecoveryDIDs(nil)
	require.ErrorIs(t, err, ErrInvalidInput, "empty scope is rejected")

	_, err = normalizeRecoveryDIDs([]string{"not-a-did"})
	require.ErrorIs(t, err, ErrInvalidInput, "malformed DID is rejected")

	input := []string{recoveryDID2, recoveryDID1}
	_, err = normalizeRecoveryDIDs(input)
	require.NoError(t, err)
	require.Equal(t, []string{recoveryDID2, recoveryDID1}, input, "caller slice is not mutated")
}

func TestRequestRepositoryRecoveryScopesDIDs(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	_, err := m.AddSourceWithRevision("https://pds.example", 4)
	require.NoError(t, err)

	job, err := m.RequestRepositoryRecovery("https://pds.example", []string{recoveryDID2, recoveryDID1}, "")
	require.NoError(t, err)
	require.Equal(t, ReasonRepositoryRecovery, job.Reason)
	require.Equal(t, []string{recoveryDID1, recoveryDID2}, job.RecoveryDIDs)
	require.Equal(t, Pending, job.State)
}

func TestRepositoryRecoveryDoesNotSendSourceReceipt(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	_, err := m.AddSourceWithRevision("https://pds.example", 4)
	require.NoError(t, err)
	job, err := m.RequestRepositoryRecovery("https://pds.example", []string{recoveryDID1}, "")
	require.NoError(t, err)
	claimForSeed(t, m, job.ID)
	_, _, err = m.persistJobOutcome(t.Context(), job.ID, nil)
	require.NoError(t, err)
	stored, err := m.Get(job.ID)
	require.NoError(t, err)
	require.Equal(t, Complete, stored.State)
	require.Equal(t, uint64(4), stored.SourceRevision)
	require.False(t, stored.ReceiptPending)
	called := false
	m.SetReceiptSender(func(_ context.Context, _ Job) error { called = true; return nil })
	var receipts sync.WaitGroup
	require.NoError(t, m.flushReceipts(t.Context(), &receipts))
	receipts.Wait()
	require.False(t, called)
}

// A second request while the first is still pending must extend that job rather
// than book a second one, so a burst does not create a job per flush tick.
func TestRequestRepositoryRecoveryCoalescesPending(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	_, err := m.AddSourceWithRevision("https://pds.example", 4)
	require.NoError(t, err)

	first, err := m.RequestRepositoryRecovery("https://pds.example", []string{recoveryDID1}, "")
	require.NoError(t, err)

	second, err := m.RequestRepositoryRecovery("https://pds.example", []string{recoveryDID2}, "")
	require.NoError(t, err)
	require.Equal(t, first.ID, second.ID, "the pending job is extended, not duplicated")
	require.Equal(t, []string{recoveryDID1, recoveryDID2}, second.RecoveryDIDs)
}

// A running job's inventory was already seeded from its original DID set, so a
// DID appended afterwards would have no inventory entry and be silently skipped.
// New work must therefore be booked separately.
func TestRequestRepositoryRecoveryDoesNotExtendRunning(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	_, err := m.AddSourceWithRevision("https://pds.example", 4)
	require.NoError(t, err)

	first, err := m.RequestRepositoryRecovery("https://pds.example", []string{recoveryDID1}, "")
	require.NoError(t, err)
	// Move the job to Running as the runner would. It is claimed by id because
	// claimNextLocked may pick up other pending work.
	claimed := claimForSeed(t, m, first.ID)
	require.Equal(t, first.ID, claimed.ID)
	require.Equal(t, Running, claimed.State)

	second, err := m.RequestRepositoryRecovery("https://pds.example", []string{recoveryDID2}, "")
	require.NoError(t, err)
	require.NotEqual(t, first.ID, second.ID, "a running job must not be extended")
	require.Equal(t, []string{recoveryDID2}, second.RecoveryDIDs)

	running, err := m.Get(first.ID)
	require.NoError(t, err)
	require.Equal(t, []string{recoveryDID1}, running.RecoveryDIDs, "the running job keeps its original scope")
}

func TestRequestRepositoryRecoveryPreservesTerminalJobs(t *testing.T) {
	for _, state := range []State{Complete, Failed, Incomplete, Canceled} {
		t.Run(string(state), func(t *testing.T) {
			m, db := newManager(t, t.TempDir())
			defer db.Close()
			_, err := m.AddSourceWithRevision("https://pds.example", 4)
			require.NoError(t, err)
			first, err := m.RequestRepositoryRecovery("https://pds.example", []string{recoveryDID1}, "")
			require.NoError(t, err)
			m.mu.Lock()
			original := m.data.Jobs[first.ID]
			original.State = state
			m.data.Jobs[first.ID] = original
			m.mu.Unlock()
			second, err := m.RequestRepositoryRecovery("https://pds.example", []string{recoveryDID2}, "")
			require.NoError(t, err)
			require.NotEqual(t, first.ID, second.ID)
			stored, err := m.Get(first.ID)
			require.NoError(t, err)
			require.Equal(t, state, stored.State)
			require.Equal(t, []string{recoveryDID1}, stored.RecoveryDIDs)
		})
	}
}

func TestRequestRepositoryRecoveryIsIdempotent(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	_, err := m.AddSourceWithRevision("https://pds.example", 4)
	require.NoError(t, err)

	first, err := m.RequestRepositoryRecovery("https://pds.example", []string{recoveryDID1}, "req-1")
	require.NoError(t, err)
	again, err := m.RequestRepositoryRecovery("https://pds.example", []string{recoveryDID2}, "req-1")
	require.NoError(t, err)
	require.Equal(t, first.ID, again.ID, "the same request id resolves to the same job")
	require.Equal(t, []string{recoveryDID1}, again.RecoveryDIDs, "a replay does not widen the scope")
}

func TestRequestRepositoryRecoveryRejectsUnknownSource(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	_, err := m.RequestRepositoryRecovery("https://pds.example", []string{recoveryDID1}, "")
	require.ErrorIs(t, err, ErrConflict)
}

// The legacy entry points take no DID list, so they must not be able to create a
// recovery job with an empty scope.
func TestRequestOnceRejectsRecoveryReason(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	_, err := m.AddSourceWithRevision("https://pds.example", 4)
	require.NoError(t, err)

	_, err = m.RequestOnce("https://pds.example", ReasonRepositoryRecovery, "")
	require.ErrorIs(t, err, ErrInvalidInput)
}

// Existing job reasons must keep their identity: recovery work must never
// re-key live jobs.
func TestBackfillJobIdentityUnchanged(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	_, err := m.AddSourceWithRevision("https://pds.example", 4)
	require.NoError(t, err)

	job, err := m.RequestOnce("https://pds.example", "backfill", "")
	require.NoError(t, err)
	// The identity is a hash of (pds, policy revision, reason, source revision).
	expected := newJob("https://pds.example", m.policy.Current(), "backfill", job.SourceRevision)
	require.Equal(t, expected.ID, job.ID)
}

// seedRecoveryJob books a pending recovery job directly, so seeding can be
// exercised without running the full processor.
func seedRecoveryJob(t *testing.T, m *Manager, dids []string) Job {
	t.Helper()
	job, err := m.RequestRepositoryRecovery("https://pds.example", dids, "")
	require.NoError(t, err)
	return job
}

// A recovery job must seed a frozen inventory holding exactly its DIDs, which is
// what lets the repository ledger resolve their coordinates without scanning the
// whole PDS.
func TestSeedRecoveryInventoryCoversRequestedDIDs(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	_, err := m.AddSourceWithRevision("https://pds.example", 4)
	require.NoError(t, err)
	job := seedRecoveryJob(t, m, []string{recoveryDID1, recoveryDID2})
	require.False(t, job.TotalReposKnown)

	rev := "3m3b4fywvy22a"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/xrpc/com.atproto.sync.getLatestCommit" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"cid":"bafyreibrj4xfbvcicomna6bal32tecjzlrjh2k7u7hs7amdamgov4onuiy","rev":"` + rev + `"}`))
	}))
	defer server.Close()

	claimed := claimForSeed(t, m, job.ID)
	processor := PDSProcessor{Manager: m, HTTPClient: server.Client()}
	client := atmossync.NewClient(atmossync.Options{
		Client: &xrpc.Client{Host: server.URL, HTTPClient: gt.Some(server.Client())},
	})
	require.NoError(t, processor.seedRecoveryInventory(t.Context(), client, &claimed))

	seeded, err := m.Get(job.ID)
	require.NoError(t, err)
	require.True(t, seeded.TotalReposKnown)
	entries, err := m.Inventory(job.ID)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	for _, entry := range entries {
		require.Equal(t, rev, entry.Revision)
	}
}

// Re-seeding must submit only DIDs absent from the inventory. Re-submitting a
// seeded DID with a newer revision fails with invalid_listing, which would break
// every restart.
func TestSeedRecoveryInventoryIsRerunnable(t *testing.T) {
	m, db := newManager(t, t.TempDir())
	defer db.Close()
	_, err := m.AddSourceWithRevision("https://pds.example", 4)
	require.NoError(t, err)
	job := seedRecoveryJob(t, m, []string{recoveryDID1})

	rev := "3m3b4fywvy22a"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"cid":"bafyreibrj4xfbvcicomna6bal32tecjzlrjh2k7u7hs7amdamgov4onuiy","rev":"` + rev + `"}`))
	}))
	defer server.Close()
	client := atmossync.NewClient(atmossync.Options{
		Client: &xrpc.Client{Host: server.URL, HTTPClient: gt.Some(server.Client())},
	})
	processor := PDSProcessor{Manager: m, HTTPClient: server.Client()}

	claimed := claimForSeed(t, m, job.ID)
	require.NoError(t, processor.seedRecoveryInventory(t.Context(), client, &claimed))

	// The DID set now grows; the second pass must submit only the new DID.
	current, err := m.Get(job.ID)
	require.NoError(t, err)
	current.RecoveryDIDs = append(current.RecoveryDIDs, recoveryDID2)
	require.NoError(t, processor.seedRecoveryInventory(t.Context(), client, &current))

	entries, err := m.Inventory(job.ID)
	require.NoError(t, err)
	require.Len(t, entries, 2, "the second pass added only the missing DID")
}

// claimForSeed moves the named job to Running so CheckpointInventory accepts
// writes, mirroring what the runner does before calling the processor. It claims
// directly rather than via claimNextLocked, which may pick up other pending work.
func claimForSeed(t *testing.T, m *Manager, id string) Job {
	t.Helper()
	m.mu.Lock()
	next := clone(m.data)
	job := next.Jobs[id]
	job.State = Running
	sealed := true
	job.RecoveryScopeSealed = &sealed
	job.StartedAt = time.Now().UTC()
	next.Jobs[id] = job
	err := m.commit(next)
	m.mu.Unlock()
	require.NoError(t, err)

	claimed, err := m.Get(id)
	require.NoError(t, err)
	require.Equal(t, Running, claimed.State)
	return claimed
}
