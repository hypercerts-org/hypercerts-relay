package jobs

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/jcalabro/atmos"
	"github.com/stretchr/testify/require"
)

type managerRun struct {
	cancel context.CancelFunc
	done   chan error
}

func startManagerRun(m *Manager, processor PDSProcessor) *managerRun {
	ctx, cancel := context.WithCancel(context.Background())
	run := &managerRun{cancel: cancel, done: make(chan error, 1)}
	go func() { run.done <- m.Run(ctx, processor.Run) }()
	return run
}

func (r *managerRun) stop(t *testing.T) {
	t.Helper()
	r.cancel()
	require.ErrorIs(t, <-r.done, context.Canceled)
}

func requireJobState(t *testing.T, m *Manager, jobID string, state State, timeout time.Duration) Job {
	t.Helper()
	var job Job
	require.Eventually(t, func() bool {
		current, err := m.Get(jobID)
		if err != nil {
			return false
		}
		job = current
		return current.State == state
	}, timeout, time.Millisecond, "job did not reach %s; last state %s (%s)", state, job.State, job.ErrorCode)
	return job
}

func requireCompleteEvidence(t *testing.T, job Job, repos int) {
	t.Helper()
	require.Equal(t, Complete, job.State)
	require.Len(t, job.CompletedRepos, repos)
	require.NotNil(t, job.Evidence)
	require.Equal(t, repos, job.Evidence.Scanned, "every repository is counted exactly once")
	require.Equal(t, repos, job.Evidence.Matching+job.Evidence.NoMatch)
}

func rateLimitedResponse(request *http.Request, retryAfter string) *http.Response {
	response := latencyFixtureResponse(request, http.StatusTooManyRequests, "application/json", []byte(`{"error":"RateLimitExceeded"}`))
	response.Header.Set("Retry-After", retryAfter)
	return response
}

func TestPDSProcessorRepoWorkersScaleThroughput(t *testing.T) {
	const repos = 120
	runJob := func(t *testing.T, fixture *latencyFixture, workers int) time.Duration {
		t.Helper()
		m, db := openMemoryManager(t, vfs.NewMem())
		defer db.Close()
		job, err := m.AddSource(latencyFixturePDS)
		require.NoError(t, err)
		completed, elapsed := runJobUntil(t, m, fixture.processor(m, workers), job.ID, time.Minute, Complete, Failed, Incomplete)
		requireCompleteEvidence(t, completed, repos)
		return elapsed
	}

	t.Run("network_bound", func(t *testing.T) {
		fixture := newLatencyFixture(t, repos, 5*time.Millisecond, 5*time.Millisecond, 0)
		serial := runJob(t, fixture, 1)
		pooled := runJob(t, fixture, 8)
		t.Logf("workers=1 %s (%.1f repos/s), workers=8 %s (%.1f repos/s), speedup %.2fx",
			serial, repos/serial.Seconds(), pooled, repos/pooled.Seconds(), serial.Seconds()/pooled.Seconds())
		require.GreaterOrEqual(t, serial.Seconds()/pooled.Seconds(), 3.0, "eight workers must overlap PDS and PLC latency")
	})

	t.Run("serial_reconcile_ceiling", func(t *testing.T) {
		const reconcileLatency = 10 * time.Millisecond
		fixture := newLatencyFixture(t, repos, 5*time.Millisecond, 5*time.Millisecond, reconcileLatency)
		pooled := runJob(t, fixture, 8)
		t.Logf("workers=8 with %s serial reconcile: %s (%.1f repos/s)", reconcileLatency, pooled, repos/pooled.Seconds())
		require.GreaterOrEqual(t, pooled, time.Duration(repos)*reconcileLatency, "reconciliation stays serial, so it bounds throughput")
	})
}

func TestPDSProcessorRepoWorkersBoundConcurrentGetRepo(t *testing.T) {
	const repos = 30
	for _, workers := range []int{1, 3} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			fixture := newLatencyFixture(t, repos, 5*time.Millisecond, 0, 0)
			m, db := openMemoryManager(t, vfs.NewMem())
			defer db.Close()
			job, err := m.AddSource(latencyFixturePDS)
			require.NoError(t, err)
			completed, _ := runJobUntil(t, m, fixture.processor(m, workers), job.ID, time.Minute, Complete, Failed, Incomplete)
			requireCompleteEvidence(t, completed, repos)
			peak := int(fixture.getRepoPeak.Load())
			require.LessOrEqual(t, peak, workers, "concurrent getRepo requests never exceed the setting")
			if workers == 1 {
				require.Equal(t, 1, peak)
			} else {
				require.Greater(t, peak, 1, "the pool overlaps downloads")
			}
			require.EqualValues(t, repos, fixture.getRepoTotal.Load())
		})
	}
}

func TestPDSProcessorRepoWorkersRateLimitStopsNewWorkAndHalvesCeiling(t *testing.T) {
	const repos = 40
	const workers = 4
	fixture := newLatencyFixture(t, repos, 50*time.Millisecond, 0, 0)
	limitedDID := fixture.repos[0].did
	// Every worker begins its attempt at once. Siblings are still resolving
	// identities when the first repository is rate limited, so any getRepo
	// they send would follow the 429.
	fixture.setResolveDelay(func(did string) time.Duration {
		if did == limitedDID {
			return 50 * time.Millisecond
		}
		return 300 * time.Millisecond
	})
	var requestMu sync.Mutex
	var limitedAt time.Time
	var requestsAfterLimit []string
	fixture.setGetRepoHook(func(request *http.Request, index int) *http.Response {
		requestMu.Lock()
		defer requestMu.Unlock()
		if !limitedAt.IsZero() {
			requestsAfterLimit = append(requestsAfterLimit, fixture.repos[index].did)
			return nil
		}
		if index == 0 {
			limitedAt = time.Now()
			return rateLimitedResponse(request, "1")
		}
		return nil
	})
	m, db := openMemoryManager(t, vfs.NewMem())
	defer db.Close()
	job, err := m.AddSource(latencyFixturePDS)
	require.NoError(t, err)
	processor := fixture.processor(m, workers)
	processor.Limits = NewPDSWorkerLimits()
	run := startManagerRun(m, processor)
	defer run.stop(t)

	var yielded Job
	require.Eventually(t, func() bool {
		current, err := m.Get(job.ID)
		if err != nil || current.State != Pending || !current.TotalReposKnown {
			return false
		}
		yielded = current
		return true
	}, 5*time.Second, time.Millisecond, "the job yields after the 429")
	requestMu.Lock()
	require.Empty(t, requestsAfterLimit, "no getRepo is sent after the 429")
	requestMu.Unlock()
	require.EqualValues(t, 1, fixture.getRepoTotal.Load())
	require.Empty(t, yielded.CompletedRepos)
	for _, sibling := range fixture.repos[1:workers] {
		retry, err := m.GetRepositoryRetry(job.ID, sibling.did)
		require.NoError(t, err)
		require.Equal(t, RepositoryRetryReady, retry.State, "%s stopped before its request", sibling.did)
		require.Zero(t, retry.Attempts, "%s keeps its attempt budget", sibling.did)
	}
	limitedRetry, err := m.GetRepositoryRetry(job.ID, limitedDID)
	require.NoError(t, err)
	require.Equal(t, RepositoryRetryWait, limitedRetry.State)
	require.Equal(t, 1, limitedRetry.Attempts)
	cooldown, found, err := m.GetPDSCooldown(latencyFixturePDS)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, cooldown.Until.After(time.Now()))
	snapshots, err := m.DiagnosticSnapshots([]string{job.ID})
	require.NoError(t, err)
	require.NotNil(t, snapshots[0].Diagnostics.PDSCooldownUntil)
	require.Equal(t, JobExecutionWaiting, snapshots[0].Diagnostics.Execution)
	require.Equal(t, "pds_cooldown", snapshots[0].Diagnostics.Reason)
	require.Equal(t, workers/2, processor.Limits.Ceiling(latencyFixturePDS, workers), "a 429 halves the PDS ceiling")
	require.Never(t, func() bool { return fixture.getRepoTotal.Load() != 1 }, 300*time.Millisecond, 5*time.Millisecond,
		"nothing new starts while the cooldown lasts")

	fixture.setResolveDelay(nil)
	fixture.getRepoPeak.Store(0)
	completed := requireJobState(t, m, job.ID, Complete, 10*time.Second)
	requireCompleteEvidence(t, completed, repos)
	require.LessOrEqual(t, fixture.getRepoPeak.Load(), int64(workers/2), "the next pass uses the halved ceiling")
	require.EqualValues(t, 2, fixture.getRepoByDID[0].Load(), "the rate-limited repository is retried once")
}

// TestPDSProcessorRateLimitWhileSiblingReconcilesDoesNotStopRuntime answers a
// getRepo with 429 while a sibling holds the serial reconciliation, so the
// failure is recorded only after its fallback cooldown deadline has passed.
func TestPDSProcessorRateLimitWhileSiblingReconcilesDoesNotStopRuntime(t *testing.T) {
	for _, test := range []struct {
		name       string
		retryAfter string
	}{
		{name: "bare_429"},
		{name: "retry_after", retryAfter: "30"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newLatencyFixture(t, 2, 50*time.Millisecond, 0, 2*time.Second)
			var limited atomic.Bool
			fixture.setGetRepoHook(func(request *http.Request, index int) *http.Response {
				if index != 1 || !limited.CompareAndSwap(false, true) {
					return nil
				}
				// Answer while repository 0 holds the serial reconciliation.
				if err := sleepContext(request.Context(), 300*time.Millisecond); err != nil {
					return nil
				}
				response := latencyFixtureResponse(request, http.StatusTooManyRequests, "application/json", []byte(`{"error":"RateLimitExceeded"}`))
				if test.retryAfter != "" {
					response.Header.Set("Retry-After", test.retryAfter)
				}
				return response
			})
			m, db := openMemoryManager(t, vfs.NewMem())
			defer db.Close()
			job, err := m.AddSource(latencyFixturePDS)
			require.NoError(t, err)
			run := startManagerRun(m, fixture.processor(m, 2))
			requireRunning := func() {
				t.Helper()
				select {
				case err := <-run.done:
					t.Fatalf("the runtime stopped: %v", err)
				default:
				}
			}
			if test.retryAfter == "" {
				// The fallback cooldown elapsed while the failure waited
				// behind the reconciliation, so nothing extends it and the
				// next pass retries the repository.
				completed := requireJobState(t, m, job.ID, Complete, 15*time.Second)
				requireRunning()
				requireCompleteEvidence(t, completed, 2)
				require.EqualValues(t, 2, fixture.getRepoByDID[1].Load())
				_, found, err := m.GetPDSCooldown(latencyFixturePDS)
				require.NoError(t, err)
				require.False(t, found, "an elapsed cooldown is not persisted")
			} else {
				var snapshot JobDiagnosticSnapshot
				require.Eventually(t, func() bool {
					snapshots, err := m.DiagnosticSnapshots([]string{job.ID})
					if err != nil || snapshots[0].Job.State != Pending || snapshots[0].Diagnostics.PDSCooldownUntil == nil {
						return false
					}
					snapshot = snapshots[0]
					return true
				}, 10*time.Second, 5*time.Millisecond, "the job yields to the server cooldown")
				requireRunning()
				require.Equal(t, 1, snapshot.CompletedRepos)
				require.Equal(t, JobExecutionWaiting, snapshot.Diagnostics.Execution)
			}
			run.stop(t)
		})
	}
}

type crashPoint string

const (
	crashAfterAttemptBegins     crashPoint = "after_attempt_begins"
	crashAfterReconcile         crashPoint = "after_reconcile_before_checkpoint"
	crashAfterCheckpoint        crashPoint = "after_checkpoint"
	crashRecoveryRepositoryRuns            = 10
)

// checkpointCommitFault fails one repository checkpoint batch after its
// archive write has succeeded, and records which repository it was.
type checkpointCommitFault struct {
	ordinal int64
	err     error
	seen    atomic.Int64
	mu      sync.Mutex
	failed  []byte
}

func (f *checkpointCommitFault) BeforeWrite(op store.WriteOp, keys [][]byte) error {
	if op != store.WriteOpBatchCommit {
		return nil
	}
	for _, key := range keys {
		// The journal range delete starts at the bare prefix; only a
		// checkpoint row has a key below it.
		if len(key) > len(repositoryCompletionPrefix) && bytes.HasPrefix(key, []byte(repositoryCompletionPrefix)) {
			if f.seen.Add(1) == f.ordinal {
				f.mu.Lock()
				f.failed = append([]byte(nil), key...)
				f.mu.Unlock()
				return f.err
			}
			return nil
		}
	}
	return nil
}

func (f *checkpointCommitFault) failedDID(t *testing.T, jobID string) string {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	encoded, ok := strings.CutPrefix(string(f.failed), repositoryCompletionPrefix+jobID+"/")
	require.True(t, ok, "a checkpoint batch failed")
	did, err := base64.RawURLEncoding.DecodeString(encoded)
	require.NoError(t, err)
	return string(did)
}

func TestPDSProcessorRepoWorkersResumeAfterCrash(t *testing.T) {
	const repos = 40
	const workers = 8
	for _, point := range []crashPoint{crashAfterAttemptBegins, crashAfterReconcile, crashAfterCheckpoint} {
		t.Run(string(point), func(t *testing.T) {
			fixture := newLatencyFixture(t, repos, 5*time.Millisecond, 0, 0)
			dir := t.TempDir()
			injected := errors.New("injected checkpoint loss")
			fault := &checkpointCommitFault{ordinal: crashRecoveryRepositoryRuns + 1, err: injected}
			var m *Manager
			var db *store.Store
			if point == crashAfterReconcile {
				// The archive write lands; the checkpoint batch is lost and
				// the runtime stops on the local persistence failure.
				m, db = newManagerWithOptions(t, dir, store.WithFaultInjector(fault))
			} else {
				m, db = newManager(t, dir)
			}
			job, err := m.AddSource(latencyFixturePDS)
			require.NoError(t, err)
			processor := fixture.processor(m, workers)
			run := startManagerRun(m, processor)
			crashed := make(chan struct{})
			var crashOnce sync.Once
			crash := func() { crashOnce.Do(func() { close(crashed) }) }

			switch point {
			case crashAfterAttemptBegins:
				// Requests after the first ten hang until shutdown, leaving
				// begun attempts in flight.
				var blocked atomic.Int64
				fixture.setGetRepoHook(func(request *http.Request, _ int) *http.Response {
					if fixture.getRepoTotal.Load() > crashRecoveryRepositoryRuns {
						if blocked.Add(1) == workers {
							crash()
						}
						<-request.Context().Done()
					}
					return nil
				})
			case crashAfterCheckpoint:
				var reconciles atomic.Int64
				fixture.setReconcileHook(func(context.Context, ingest.Snapshot) error {
					if reconciles.Add(1) == crashRecoveryRepositoryRuns {
						// The checkpoint is not context-bound, so this
						// repository completes durably as shutdown begins.
						run.cancel()
						crash()
					}
					return nil
				})
			}
			if point == crashAfterReconcile {
				select {
				case err := <-run.done:
					require.ErrorIs(t, err, injected)
				case <-time.After(10 * time.Second):
					t.Fatal("the checkpoint fault was not reached")
				}
			} else {
				select {
				case <-crashed:
				case <-time.After(10 * time.Second):
					t.Fatal("crash point was not reached")
				}
				run.cancel()
				require.ErrorIs(t, <-run.done, context.Canceled)
			}
			beforeCrash, err := m.Get(job.ID)
			require.NoError(t, err)
			retries, err := m.ListRepositoryRetries(job.ID)
			require.NoError(t, err)
			interrupted := map[string]bool{}
			for _, retry := range retries {
				require.Equal(t, RepositoryRetryInFlight, retry.State)
				require.Equal(t, 1, retry.Attempts)
				interrupted[retry.DID] = true
			}
			require.NotEmpty(t, interrupted, "the crash interrupts in-flight repositories")
			lostCheckpointDID := ""
			if point == crashAfterReconcile {
				lostCheckpointDID = fault.failedDID(t, job.ID)
				require.NotContains(t, beforeCrash.CompletedRepos, lostCheckpointDID)
				require.True(t, interrupted[lostCheckpointDID], "the lost checkpoint leaves its attempt in flight")
				require.EqualValues(t, 1, fixture.reconciledByDID[fixture.byDID[lostCheckpointDID]].Load(), "the archive write landed before the checkpoint was lost")
			}
			requestsBeforeCrash := make([]int64, repos)
			for index := range requestsBeforeCrash {
				requestsBeforeCrash[index] = fixture.getRepoByDID[index].Load()
			}
			require.NoError(t, db.Close())

			m, db = newManager(t, dir)
			defer db.Close()
			restored, err := m.Get(job.ID)
			require.NoError(t, err)
			require.Equal(t, beforeCrash.CompletedRepos, restored.CompletedRepos, "every journaled checkpoint survives the crash")
			require.Equal(t, beforeCrash.Evidence, restored.Evidence)
			var attemptsMu sync.Mutex
			retriedAttempts := map[string]int{}
			fixture.setReconcileHook(nil)
			fixture.setGetRepoHook(func(request *http.Request, index int) *http.Response {
				did := fixture.repos[index].did
				if interrupted[did] {
					retry, err := m.GetRepositoryRetry(job.ID, did)
					if err == nil {
						attemptsMu.Lock()
						retriedAttempts[did] = retry.Attempts
						attemptsMu.Unlock()
					}
				}
				return nil
			})
			resumed := startManagerRun(m, fixture.processor(m, workers))
			completed := requireJobState(t, m, job.ID, Complete, 15*time.Second)
			resumed.stop(t)
			requireCompleteEvidence(t, completed, repos)
			for index, fixtureRepo := range fixture.repos {
				if _, done := beforeCrash.CompletedRepos[fixtureRepo.did]; done {
					require.Equal(t, requestsBeforeCrash[index], fixture.getRepoByDID[index].Load(), "checkpointed %s is not downloaded again", fixtureRepo.did)
				}
				require.Positive(t, fixture.getRepoByDID[index].Load(), "%s is not skipped", fixtureRepo.did)
			}
			if lostCheckpointDID != "" {
				index := fixture.byDID[lostCheckpointDID]
				require.EqualValues(t, 2, fixture.getRepoByDID[index].Load(), "the repository whose checkpoint was lost is downloaded again")
				require.EqualValues(t, 2, fixture.reconciledByDID[index].Load(), "and reconciled again")
				require.Contains(t, completed.CompletedRepos, lostCheckpointDID)
			}
			attemptsMu.Lock()
			defer attemptsMu.Unlock()
			for did := range interrupted {
				require.Equal(t, 2, retriedAttempts[did], "interrupted %s consumes its attempt and retries once", did)
			}
		})
	}
}

func TestPDSProcessorRepoWorkersProgressInvariants(t *testing.T) {
	const repos = 120
	fixture := newLatencyFixture(t, repos, 2*time.Millisecond, time.Millisecond, 0)
	m, db := openMemoryManager(t, vfs.NewMem())
	defer db.Close()
	job, err := m.AddSource(latencyFixturePDS)
	require.NoError(t, err)
	stopPolling := make(chan struct{})
	pollErr := make(chan error, 1)
	go func() {
		pollErr <- func() error {
			previous := -1
			for {
				select {
				case <-stopPolling:
					return nil
				default:
				}
				snapshots, err := m.DiagnosticSnapshots([]string{job.ID})
				if err != nil {
					return err
				}
				snapshot := snapshots[0]
				if snapshot.Diagnostics.UnresolvedRepos != nil {
					if snapshot.CompletedRepos < previous {
						return fmt.Errorf("completed repositories decreased from %d to %d", previous, snapshot.CompletedRepos)
					}
					previous = snapshot.CompletedRepos
					if snapshot.CompletedRepos+*snapshot.Diagnostics.UnresolvedRepos != snapshot.Job.TotalRepos {
						return fmt.Errorf("completed %d + unresolved %d != total %d", snapshot.CompletedRepos, *snapshot.Diagnostics.UnresolvedRepos, snapshot.Job.TotalRepos)
					}
				}
				current, err := m.Get(job.ID)
				if err != nil {
					return err
				}
				evidence := current.Evidence
				if evidence.Matching+evidence.NoMatch != evidence.Scanned || evidence.Scanned != len(current.CompletedRepos) {
					return fmt.Errorf("evidence %+v disagrees with %d completed repositories", *evidence, len(current.CompletedRepos))
				}
				time.Sleep(time.Millisecond)
			}
		}()
	}()
	completed, _ := runJobUntil(t, m, fixture.processor(m, 8), job.ID, time.Minute, Complete, Failed, Incomplete)
	close(stopPolling)
	require.NoError(t, <-pollErr)
	requireCompleteEvidence(t, completed, repos)
}

func TestPDSProcessorRepoWorkersStopOnCancellation(t *testing.T) {
	const repos = 40
	const workers = 8
	for _, action := range []string{"cancel", "source_removed", "policy_changed"} {
		t.Run(action, func(t *testing.T) {
			fixture := newLatencyFixture(t, repos, 2*time.Millisecond, 0, 0)
			var blocked atomic.Int64
			var acted atomic.Bool
			allBlocked := make(chan struct{})
			fixture.setGetRepoHook(func(request *http.Request, _ int) *http.Response {
				if acted.Load() {
					// A policy change schedules a successor job; keep it from
					// reconciling so only the canceled job is observed.
					return latencyFixtureResponse(request, http.StatusServiceUnavailable, "application/json", []byte(`{"error":"Unavailable"}`))
				}
				if fixture.getRepoTotal.Load() > 5 {
					if blocked.Add(1) == workers {
						close(allBlocked)
					}
					<-request.Context().Done()
					blocked.Add(-1)
				}
				return nil
			})
			m, db := openMemoryManager(t, vfs.NewMem())
			defer db.Close()
			job, err := m.AddSource(latencyFixturePDS)
			require.NoError(t, err)
			run := startManagerRun(m, fixture.processor(m, workers))
			defer run.stop(t)
			select {
			case <-allBlocked:
			case <-time.After(10 * time.Second):
				t.Fatal("workers did not all start downloads")
			}
			switch action {
			case "cancel":
				err = m.Cancel(job.ID)
			case "source_removed":
				err = m.RemoveSource(latencyFixturePDS)
			case "policy_changed":
				acted.Store(true)
				_, err = m.SetPolicy(t.Context(), job.Policy.Revision, []string{"app.bsky.feed.like"})
			}
			require.NoError(t, err)
			acted.Store(true)
			canceled := requireJobState(t, m, job.ID, Canceled, 5*time.Second)
			require.Eventually(t, func() bool { return blocked.Load() == 0 }, 5*time.Second, time.Millisecond, "every worker stops")
			reconciled := fixture.reconciled.Load()
			requests := fixture.getRepoTotal.Load()
			require.Never(t, func() bool {
				current, err := m.Get(job.ID)
				if err != nil || len(current.CompletedRepos) != len(canceled.CompletedRepos) || fixture.reconciled.Load() != reconciled {
					return true
				}
				// Only a policy change schedules successor work for this PDS.
				return action != "policy_changed" && fixture.getRepoTotal.Load() != requests
			}, 200*time.Millisecond, 5*time.Millisecond, "no download, reconcile or checkpoint happens after %s", action)
		})
	}
}

func TestPDSProcessorPacesOnRateLimitHeadersWithoutA429(t *testing.T) {
	const repos = 12
	const workers = 2
	const budget = 8
	fixture := newLatencyFixture(t, repos, 5*time.Millisecond, 0, 0)
	var limiterMu sync.Mutex
	remaining := 0
	var resetAt int64
	var rateLimited atomic.Int64
	fixture.setGetRepoHook(func(request *http.Request, index int) *http.Response {
		limiterMu.Lock()
		now := time.Now()
		if now.Unix() >= resetAt {
			remaining = budget
			resetAt = now.Unix() + 2
		}
		if remaining == 0 {
			reset := resetAt
			limiterMu.Unlock()
			rateLimited.Add(1)
			response := rateLimitedResponse(request, "1")
			response.Header.Set("RateLimit-Reset", strconv.FormatInt(reset, 10))
			return response
		}
		remaining--
		left, reset := remaining, resetAt
		limiterMu.Unlock()
		if err := sleepContext(request.Context(), fixture.pdsLatency); err != nil {
			return nil
		}
		response := latencyFixtureResponse(request, http.StatusOK, "application/vnd.ipld.car", fixture.repos[index].car)
		response.Header.Set("RateLimit-Limit", strconv.Itoa(budget))
		response.Header.Set("RateLimit-Remaining", strconv.Itoa(left))
		response.Header.Set("RateLimit-Reset", strconv.FormatInt(reset, 10))
		return response
	})
	m, db := openMemoryManager(t, vfs.NewMem())
	defer db.Close()
	job, err := m.AddSource(latencyFixturePDS)
	require.NoError(t, err)
	var sawCooldown atomic.Bool
	stopPolling := make(chan struct{})
	polled := make(chan struct{})
	go func() {
		defer close(polled)
		for {
			select {
			case <-stopPolling:
				return
			default:
			}
			snapshots, err := m.DiagnosticSnapshots([]string{job.ID})
			if err == nil && snapshots[0].Diagnostics.Execution == JobExecutionWaiting && snapshots[0].Diagnostics.Reason == "pds_cooldown" && snapshots[0].Diagnostics.PDSCooldownUntil != nil {
				sawCooldown.Store(true)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	completed, _ := runJobUntil(t, m, fixture.processor(m, workers), job.ID, time.Minute, Complete, Failed, Incomplete)
	close(stopPolling)
	<-polled
	requireCompleteEvidence(t, completed, repos)
	require.Zero(t, rateLimited.Load(), "pacing stops new work before the budget runs out")
	for index := range fixture.repos {
		require.EqualValues(t, 1, fixture.getRepoByDID[index].Load(), "no repository attempt fails or repeats")
	}
	_, found, err := m.GetPDSCooldown(latencyFixturePDS)
	require.NoError(t, err)
	require.True(t, found, "the pacing cooldown is persisted")
	require.True(t, sawCooldown.Load(), "diagnostics report the pacing wait")
}

func TestPaceRateLimitIgnoresMissingOrInvalidHeaders(t *testing.T) {
	processor := PDSProcessor{}
	for _, headers := range []http.Header{
		{},
		{"Ratelimit-Remaining": {"0"}},
		{"Ratelimit-Remaining": {"zero"}, "Ratelimit-Reset": {strconv.FormatInt(time.Now().Add(time.Minute).Unix(), 10)}},
		{"Ratelimit-Remaining": {"0"}, "Ratelimit-Reset": {strconv.FormatInt(time.Now().Add(-time.Minute).Unix(), 10)}},
		{"Ratelimit-Remaining": {"100"}, "Ratelimit-Reset": {strconv.FormatInt(time.Now().Add(time.Minute).Unix(), 10)}},
	} {
		// A nil manager would panic if pacing tried to persist a cooldown.
		require.NoError(t, processor.paceRateLimit(Job{}, headers))
	}
}

func TestClassifyRepositoryDeadlineIsRetryableTimeout(t *testing.T) {
	did := atmos.DID(latencyFixtureDID(0))
	err := classifyRepositoryDeadline(fmt.Errorf("reconcile: %w", context.DeadlineExceeded), did)
	var input *InputError
	require.ErrorAs(t, err, &input)
	require.True(t, input.Unavailable)
	require.Equal(t, "repository_unavailable", input.Code)
	var failure *pdsAttemptFailure
	require.ErrorAs(t, err, &failure)
	require.True(t, failure.retryable)
	require.Equal(t, pdsCauseTimeout, failure.causeClass)
	require.Equal(t, string(did), failure.repositoryDID)
	require.Equal(t, RepositoryRetryFailure{Category: RepositoryFailureTimeout, Stage: RepositoryFailureGetRepoRequest, Code: "repository_unavailable"}, repositoryFailureFor(err, false))
	retryAt, cooldown := repositoryRetrySchedule(RepositoryRetry{Attempts: 1}, input, failure)
	require.NotNil(t, retryAt, "an expired deadline schedules an automatic retry")
	require.Nil(t, cooldown)

	local := errors.New("local persistence failure")
	require.Equal(t, local, classifyRepositoryDeadline(local, did), "other local errors still stop the runtime")
	classified := fmt.Errorf("%w: %w", &InputError{Code: "identity_unavailable", Unavailable: true}, context.DeadlineExceeded)
	require.Equal(t, classified, classifyRepositoryDeadline(classified, did), "an existing classification is kept")
}

func TestPDSProcessorReconcileQueueDoesNotExpireAcquisitionDeadline(t *testing.T) {
	previous := repositoryAcquisitionTimeout
	repositoryAcquisitionTimeout = 300 * time.Millisecond
	t.Cleanup(func() { repositoryAcquisitionTimeout = previous })
	const repos = 3
	// Every repository downloads at once, then queues behind one-second
	// reconciliations for far longer than the acquisition deadline.
	fixture := newLatencyFixture(t, repos, 10*time.Millisecond, 0, time.Second)
	m, db := openMemoryManager(t, vfs.NewMem())
	defer db.Close()
	job, err := m.AddSource(latencyFixturePDS)
	require.NoError(t, err)
	run := startManagerRun(m, fixture.processor(m, repos))
	completed := requireJobState(t, m, job.ID, Complete, 15*time.Second)
	select {
	case err := <-run.done:
		t.Fatalf("the runtime stopped: %v", err)
	default:
	}
	run.stop(t)
	requireCompleteEvidence(t, completed, repos)
	for index := range fixture.repos {
		require.EqualValues(t, 1, fixture.getRepoByDID[index].Load(), "queue time must not fail or repeat an attempt")
	}
}

func TestPDSProcessorPacingReconcileQueueDoesNotExpireAcquisitionDeadline(t *testing.T) {
	const acquisitionTimeout = 300 * time.Millisecond
	previous := repositoryAcquisitionTimeout
	repositoryAcquisitionTimeout = acquisitionTimeout
	t.Cleanup(func() { repositoryAcquisitionTimeout = previous })
	const repos = 2
	fixture := newLatencyFixture(t, repos, 0, 0, 0)
	secondDownloadStarted := make(chan struct{})
	reconcileStarted := make(chan struct{})
	pacedResponseReady := make(chan struct{})
	releaseReconcile := make(chan struct{})
	var secondDownloadOnce, pacedResponseOnce, releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseReconcile) }) }
	fixture.setGetRepoHook(func(request *http.Request, index int) *http.Response {
		if index == 0 {
			// Both attempts must start before the first reconciliation takes
			// the manager lock, otherwise the second download cannot begin.
			select {
			case <-secondDownloadStarted:
			case <-request.Context().Done():
			}
			return nil
		}
		secondDownloadOnce.Do(func() { close(secondDownloadStarted) })
		select {
		case <-reconcileStarted:
		case <-request.Context().Done():
			return nil
		}
		response := latencyFixtureResponse(request, http.StatusOK, "application/vnd.ipld.car", fixture.repos[index].car)
		response.Header.Set("RateLimit-Remaining", "0")
		response.Header.Set("RateLimit-Reset", strconv.FormatInt(time.Now().Add(3*time.Second).Unix(), 10))
		pacedResponseOnce.Do(func() { close(pacedResponseReady) })
		return response
	})
	fixture.setReconcileHook(func(ctx context.Context, snapshot ingest.Snapshot) error {
		if snapshot.DID != fixture.repos[0].did {
			return nil
		}
		close(reconcileStarted)
		select {
		case <-releaseReconcile:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	m, db := openMemoryManager(t, vfs.NewMem())
	defer db.Close()
	job, err := m.AddSource(latencyFixturePDS)
	require.NoError(t, err)
	run := startManagerRun(m, fixture.processor(m, repos))
	defer func() {
		release()
		run.stop(t)
	}()
	select {
	case <-pacedResponseReady:
	case <-time.After(5 * time.Second):
		t.Fatal("the pacing response did not arrive during the sibling reconciliation")
	}
	// Keep the manager lock held beyond the second acquisition's deadline.
	// With pacing inside acquisition, its subsequent fresh identity lookup
	// receives an expired context even though the download succeeded.
	time.Sleep(2 * acquisitionTimeout)
	release()
	completed := requireJobState(t, m, job.ID, Complete, 15*time.Second)
	requireCompleteEvidence(t, completed, repos)
	for index := range fixture.repos {
		require.EqualValues(t, 1, fixture.getRepoByDID[index].Load(), "pacing lock wait must not fail or repeat an attempt")
	}
	require.Nil(t, completed.LastFailure, "pacing lock wait must not record a failed attempt")
	_, found, err := m.GetPDSCooldown(latencyFixturePDS)
	require.NoError(t, err)
	require.True(t, found, "the low-budget response still persists the pacing cooldown")
}
