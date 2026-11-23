package jobs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/jcalabro/atmos"
	atmoscrypto "github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/mst"
	"github.com/jcalabro/atmos/repo"
)

const latencyFixturePDS = "https://latency.pds.example"

// latencyFixtureRepo is one pre-built, signed repository served by the
// in-process PDS. The CAR bytes are immutable once built.
type latencyFixtureRepo struct {
	did string
	rev string
	car []byte
}

// latencyFixture serves listRepos/getRepo and DID resolution in-process after
// fixed latencies, so PDS job throughput can be measured without a network.
// Reconcile is serialized by one mutex, like the archive rewrite lock.
type latencyFixture struct {
	repos            []latencyFixtureRepo
	byDID            map[string]int
	key              atmoscrypto.PrivateKey
	pdsLatency       time.Duration
	plcLatency       time.Duration
	reconcileLatency time.Duration

	reconcileMu   sync.Mutex
	getRepoActive atomic.Int64
	getRepoPeak   atomic.Int64
	getRepoTotal  atomic.Int64
	getRepoByDID  []atomic.Int64
	reconciled    atomic.Int64
	// reconciledByDID counts reconciliations that reached the archive write.
	reconciledByDID []atomic.Int64

	hookMu sync.Mutex
	// getRepoHook, when set, may replace a getRepo response. It runs before
	// the PDS latency, while the request counts as in flight; a replacement
	// response is returned without latency.
	getRepoHook func(request *http.Request, index int) *http.Response
	// reconcileHook, when set, runs after the reconcile latency while the
	// serial reconcile lock is held.
	reconcileHook func(context.Context, ingest.Snapshot) error
	// resolveDelay, when set, adds a per-DID delay to every DID resolution.
	resolveDelay func(did string) time.Duration
}

// latencyFixtureDID returns a valid did:plc whose lexical order matches index
// order, so frozen inventories are processed in fixture index order.
func latencyFixtureDID(index int) string {
	const alphabet = "234567abcdefghijklmnopqrstuvwxyz"
	suffix := make([]byte, 24)
	for position := len(suffix) - 1; position >= 0; position-- {
		suffix[position] = alphabet[index%len(alphabet)]
		index /= len(alphabet)
	}
	return "did:plc:" + string(suffix)
}

func newLatencyFixture(tb testing.TB, repos int, pdsLatency, plcLatency, reconcileLatency time.Duration) *latencyFixture {
	tb.Helper()
	key, err := atmoscrypto.GenerateP256()
	if err != nil {
		tb.Fatal(err)
	}
	fixture := &latencyFixture{
		repos:            make([]latencyFixtureRepo, repos),
		byDID:            make(map[string]int, repos),
		key:              key,
		pdsLatency:       pdsLatency,
		plcLatency:       plcLatency,
		reconcileLatency: reconcileLatency,
		getRepoByDID:     make([]atomic.Int64, repos),
		reconciledByDID:  make([]atomic.Int64, repos),
	}
	clock := atmos.NewTIDClock(0)
	for index := range fixture.repos {
		did := latencyFixtureDID(index)
		blockStore := mst.NewMemBlockStore()
		snapshotRepo := &repo.Repo{DID: atmos.DID(did), Clock: clock, Store: blockStore, Tree: mst.NewTree(blockStore)}
		if err := snapshotRepo.Create("app.bsky.feed.post", clock.Next().String(), map[string]any{"text": "latency fixture"}); err != nil {
			tb.Fatal(err)
		}
		var car bytes.Buffer
		if err := snapshotRepo.ExportCAR(&car, key); err != nil {
			tb.Fatal(err)
		}
		_, commit, err := repo.LoadCompleteFromCAR(bytes.NewReader(car.Bytes()))
		if err != nil {
			tb.Fatal(err)
		}
		fixture.repos[index] = latencyFixtureRepo{did: did, rev: commit.Rev, car: car.Bytes()}
		fixture.byDID[did] = index
	}
	return fixture
}

func (f *latencyFixture) setGetRepoHook(hook func(*http.Request, int) *http.Response) {
	f.hookMu.Lock()
	defer f.hookMu.Unlock()
	f.getRepoHook = hook
}

func (f *latencyFixture) setReconcileHook(hook func(context.Context, ingest.Snapshot) error) {
	f.hookMu.Lock()
	defer f.hookMu.Unlock()
	f.reconcileHook = hook
}

func (f *latencyFixture) setResolveDelay(delay func(did string) time.Duration) {
	f.hookMu.Lock()
	defer f.hookMu.Unlock()
	f.resolveDelay = delay
}

func (f *latencyFixture) currentResolveDelay(did string) time.Duration {
	f.hookMu.Lock()
	delay := f.resolveDelay
	f.hookMu.Unlock()
	if delay == nil {
		return 0
	}
	return delay(did)
}

func (f *latencyFixture) currentGetRepoHook() func(*http.Request, int) *http.Response {
	f.hookMu.Lock()
	defer f.hookMu.Unlock()
	return f.getRepoHook
}

func (f *latencyFixture) currentReconcileHook() func(context.Context, ingest.Snapshot) error {
	f.hookMu.Lock()
	defer f.hookMu.Unlock()
	return f.reconcileHook
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func latencyFixtureResponse(request *http.Request, status int, contentType string, body []byte) *http.Response {
	header := http.Header{}
	header.Set("Content-Type", contentType)
	return &http.Response{
		StatusCode:    status,
		Status:        strconv.Itoa(status) + " " + http.StatusText(status),
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        header,
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       request,
	}
}

func (f *latencyFixture) roundTrip(request *http.Request) (*http.Response, error) {
	switch request.URL.Path {
	case "/xrpc/com.atproto.sync.listRepos":
		if err := sleepContext(request.Context(), f.pdsLatency); err != nil {
			return nil, err
		}
		return f.listRepos(request)
	case "/xrpc/com.atproto.sync.getRepo":
		return f.getRepo(request)
	default:
		return latencyFixtureResponse(request, http.StatusNotFound, "application/json", []byte(`{"error":"NotFound"}`)), nil
	}
}

func (f *latencyFixture) listRepos(request *http.Request) (*http.Response, error) {
	start := 0
	if cursor := request.URL.Query().Get("cursor"); cursor != "" {
		parsed, err := strconv.Atoi(cursor)
		if err != nil {
			return latencyFixtureResponse(request, http.StatusBadRequest, "application/json", []byte(`{"error":"InvalidRequest"}`)), nil
		}
		start = parsed
	}
	limit := 100
	if raw := request.URL.Query().Get("limit"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	end := min(start+limit, len(f.repos))
	type listedRepo struct {
		DID    string `json:"did"`
		Rev    string `json:"rev"`
		Head   string `json:"head"`
		Active bool   `json:"active"`
	}
	page := struct {
		Repos  []listedRepo `json:"repos"`
		Cursor string       `json:"cursor,omitempty"`
	}{Repos: make([]listedRepo, 0, end-start)}
	for _, fixtureRepo := range f.repos[start:end] {
		page.Repos = append(page.Repos, listedRepo{DID: fixtureRepo.did, Rev: fixtureRepo.rev, Head: "head", Active: true})
	}
	if end < len(f.repos) {
		page.Cursor = strconv.Itoa(end)
	}
	body, err := json.Marshal(page)
	if err != nil {
		return nil, err
	}
	return latencyFixtureResponse(request, http.StatusOK, "application/json", body), nil
}

func (f *latencyFixture) getRepo(request *http.Request) (*http.Response, error) {
	index, ok := f.byDID[request.URL.Query().Get("did")]
	if !ok {
		return latencyFixtureResponse(request, http.StatusBadRequest, "application/json", []byte(`{"error":"RepoNotFound"}`)), nil
	}
	active := f.getRepoActive.Add(1)
	defer f.getRepoActive.Add(-1)
	for {
		peak := f.getRepoPeak.Load()
		if active <= peak || f.getRepoPeak.CompareAndSwap(peak, active) {
			break
		}
	}
	f.getRepoTotal.Add(1)
	f.getRepoByDID[index].Add(1)
	if hook := f.currentGetRepoHook(); hook != nil {
		if response := hook(request, index); response != nil {
			return response, nil
		}
	}
	if err := sleepContext(request.Context(), f.pdsLatency); err != nil {
		return nil, err
	}
	return latencyFixtureResponse(request, http.StatusOK, "application/vnd.ipld.car", f.repos[index].car), nil
}

// latencyFixtureResolver resolves every fixture DID to the fixture PDS and key.
type latencyFixtureResolver struct{ fixture *latencyFixture }

func (r latencyFixtureResolver) ResolveDID(ctx context.Context, did atmos.DID) (*identity.DIDDocument, error) {
	if err := sleepContext(ctx, r.fixture.plcLatency+r.fixture.currentResolveDelay(string(did))); err != nil {
		return nil, err
	}
	return snapshotDIDDocument(did, r.fixture.key.PublicKey(), latencyFixturePDS), nil
}

func (r latencyFixtureResolver) ResolveHandle(context.Context, atmos.Handle) (atmos.DID, error) {
	return "", fmt.Errorf("latency fixture does not resolve handles")
}

func (f *latencyFixture) reconcile(ctx context.Context, snapshot ingest.Snapshot) error {
	f.reconcileMu.Lock()
	defer f.reconcileMu.Unlock()
	if err := sleepContext(ctx, f.reconcileLatency); err != nil {
		return err
	}
	if hook := f.currentReconcileHook(); hook != nil {
		if err := hook(ctx, snapshot); err != nil {
			return err
		}
	}
	f.reconciled.Add(1)
	f.reconciledByDID[f.byDID[snapshot.DID]].Add(1)
	return nil
}

// processor builds a processor whose HTTP and identity traffic never leaves
// the process. Clients for different workers share one transport.
func (f *latencyFixture) processor(m *Manager, workers int) PDSProcessor {
	return PDSProcessor{
		Manager:     m,
		HTTPClient:  &http.Client{Transport: roundTripperFunc(f.roundTrip)},
		Directory:   &identity.Directory{Resolver: latencyFixtureResolver{fixture: f}},
		Reconcile:   f.reconcile,
		RepoWorkers: workers,
	}
}

func openMemoryManager(tb testing.TB, fs vfs.FS) (*Manager, *store.Store) {
	tb.Helper()
	db, err := store.Open("/jobs", nil, store.WithFS(fs))
	if err != nil {
		tb.Fatal(err)
	}
	policy, err := selection.Open(db, []string{"app.bsky.feed.post"})
	if err != nil {
		tb.Fatal(err)
	}
	m, err := Open(db, policy)
	if err != nil {
		tb.Fatal(err)
	}
	return m, db
}

// runJobUntil runs the manager until the job reaches one of the wanted states
// and returns the elapsed wall time from Run start.
func runJobUntil(tb testing.TB, m *Manager, processor PDSProcessor, jobID string, timeout time.Duration, wanted ...State) (Job, time.Duration) {
	tb.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	startedAt := time.Now()
	go func() { done <- m.Run(ctx, processor.Run) }()
	deadline := time.Now().Add(timeout)
	for {
		job, err := m.Get(jobID)
		if err != nil {
			cancel()
			<-done
			tb.Fatal(err)
		}
		for _, state := range wanted {
			if job.State == state {
				elapsed := time.Since(startedAt)
				cancel()
				if runErr := <-done; runErr != nil && !errors.Is(runErr, context.Canceled) {
					tb.Fatalf("manager run: %v", runErr)
				}
				return job, elapsed
			}
		}
		if time.Now().After(deadline) {
			cancel()
			runErr := <-done
			tb.Fatalf("job %s did not reach %v within %s: state=%s error=%q run=%v", jobID, wanted, timeout, job.State, job.ErrorCode, runErr)
		}
		time.Sleep(time.Millisecond)
	}
}

// startBenchRepositoryWorker freezes an inventory for a running job and keeps
// the job running until the returned stop function is called.
func startBenchRepositoryWorker(tb testing.TB, m *Manager, jobID string, entries map[string]string) func() {
	tb.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan error, 1)
	done := make(chan error, 1)
	go func() {
		done <- m.Run(ctx, func(ctx context.Context, job Job) error {
			if job.ID != jobID {
				return ErrConflict
			}
			started <- m.CheckpointInventory(job.ID, "", entries, true)
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	select {
	case err := <-started:
		if err != nil {
			cancel()
			<-done
			tb.Fatal(err)
		}
	case err := <-done:
		cancel()
		tb.Fatalf("job worker exited before freezing its inventory: %v", err)
	}
	return func() {
		cancel()
		<-done
	}
}

type checkpointBenchJob struct {
	manager *Manager
	db      *store.Store
	jobID   string
	dids    []string
	next    int
	stop    func()
}

// newCheckpointBenchJob builds a running job whose frozen inventory holds
// inventorySize repositories, half of them already checkpointed, so each
// timed checkpoint runs against a realistically sized job state.
func newCheckpointBenchJob(b *testing.B, inventorySize int) *checkpointBenchJob {
	b.Helper()
	const revision = "3l3qo2vutsw2b"
	m, db := openMemoryManager(b, vfs.NewMem())
	job, err := m.AddSource("https://checkpoint.pds.example")
	if err != nil {
		b.Fatal(err)
	}
	entries := make(map[string]string, inventorySize)
	dids := make([]string, inventorySize)
	for index := range dids {
		dids[index] = latencyFixtureDID(index)
		entries[dids[index]] = revision
	}
	stop := startBenchRepositoryWorker(b, m, job.ID, entries)
	half := inventorySize / 2
	for _, did := range dids[:half] {
		if _, err := m.BeginRepositoryAttempt(job.ID, did); err != nil {
			b.Fatal(err)
		}
		if err := m.CheckpointRepositoryAcquisition(job.ID, did, revision, revision, "", 1); err != nil {
			b.Fatal(err)
		}
	}
	return &checkpointBenchJob{manager: m, db: db, jobID: job.ID, dids: dids, next: half, stop: stop}
}

func (j *checkpointBenchJob) close() {
	j.stop()
	_ = j.db.Close()
}

// BenchmarkCheckpointRepositoryAcquisition measures one successful repository
// checkpoint. The attempt start is untimed; only the completion write is.
func BenchmarkCheckpointRepositoryAcquisition(b *testing.B) {
	const revision = "3l3qo2vutsw2b"
	for _, inventorySize := range []int{1_000, 15_000} {
		b.Run(fmt.Sprintf("inventory=%d", inventorySize), func(b *testing.B) {
			b.StopTimer()
			job := newCheckpointBenchJob(b, inventorySize)
			defer func() { job.close() }()
			for range b.N {
				if job.next == len(job.dids) {
					job.close()
					job = newCheckpointBenchJob(b, inventorySize)
				}
				did := job.dids[job.next]
				job.next++
				if _, err := job.manager.BeginRepositoryAttempt(job.jobID, did); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				err := job.manager.CheckpointRepositoryAcquisition(job.jobID, did, revision, revision, "", 1)
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkPDSProcessorFrozenInventory runs whole PDS jobs against the
// in-process latency fixture and reports repositories per second.
func BenchmarkPDSProcessorFrozenInventory(b *testing.B) {
	const repos = 120
	const latency = 5 * time.Millisecond
	// Keep per-pass logs out of benchmark output.
	defaultSlogCaptureMu.Lock()
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.DiscardHandler))
	defer func() {
		slog.SetDefault(previous)
		defaultSlogCaptureMu.Unlock()
	}()
	for _, reconcileLatency := range []time.Duration{0, 10 * time.Millisecond} {
		fixture := newLatencyFixture(b, repos, latency, latency, reconcileLatency)
		for _, workers := range []int{1, 2, 8} {
			b.Run(fmt.Sprintf("reconcile=%s/workers=%d", reconcileLatency, workers), func(b *testing.B) {
				var total time.Duration
				for range b.N {
					b.StopTimer()
					m, db := openMemoryManager(b, vfs.NewMem())
					job, err := m.AddSource(latencyFixturePDS)
					if err != nil {
						b.Fatal(err)
					}
					processor := fixture.processor(m, workers)
					b.StartTimer()
					completed, elapsed := runJobUntil(b, m, processor, job.ID, 5*time.Minute, Complete, Failed, Incomplete)
					b.StopTimer()
					if completed.State != Complete || len(completed.CompletedRepos) != repos {
						b.Fatalf("job ended %s with %d/%d repositories", completed.State, len(completed.CompletedRepos), repos)
					}
					total += elapsed
					_ = db.Close()
				}
				b.ReportMetric(float64(repos*b.N)/total.Seconds(), "repos/s")
			})
		}
	}
}
