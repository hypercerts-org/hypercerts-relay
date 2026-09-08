package jetstreamd

import (
	"context"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	client "github.com/bluesky-social/jetstream"
	"github.com/bluesky-social/jetstream/internal/hypercerts/jobs"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/simulator/fanout"
	simhttp "github.com/bluesky-social/jetstream/internal/simulator/http"
	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

func TestHypercertsQuietPDSAndEnabledCollectionJobs(t *testing.T) {
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.subscribeRepos":
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.CloseNow()
			_, _, _ = conn.Read(r.Context())
		case "/xrpc/com.atproto.sync.listHosts":
			_, _ = io.WriteString(w, `{"hosts":[]}`)
		default:
			_, _ = io.WriteString(w, `{"repos":[]}`)
		}
	}))
	defer relay.Close()
	cfg := world.DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Accounts = 1
	cfg.PDSHosts = 1
	cfg.InitialRecordsMax = 0
	w, err := world.New(t.Context(), cfg)
	require.NoError(t, err)
	defer w.Close()
	_, err = w.EnsureSeed()
	require.NoError(t, err)
	require.NoError(t, w.Bootstrap(t.Context(), slog.New(slog.NewTextHandler(io.Discard, nil))))
	require.NoError(t, w.AttachRuntime(rand.New(rand.NewPCG(5, 6)), fanout.New(64)))
	_, _, err = w.GenerateRecordOpForTest(t.Context(), 0, "create", "app.bsky.feed.post", "post")
	require.NoError(t, err)
	_, _, err = w.GenerateRecordOpForTest(t.Context(), 0, "create", "app.bsky.feed.like", "like")
	require.NoError(t, err)
	pds := httptest.NewServer(nil)
	pds.Config.Handler = simhttp.NewHandler(w, pds.URL)
	defer pds.Close()
	opts := testOptions(t)
	opts.RelayURL = relay.URL
	opts.PLCURL = pds.URL
	opts.CollectionSelection = true
	opts.InitialCollections = []string{"app.bsky.feed.post"}
	opts.InitialPDSSources = []string{pds.URL}
	opts.LogOutput = io.Discard
	writers := make(chan *ingest.Writer, 1)
	opts.OnSteadyStateWriter = func(w *ingest.Writer) { writers <- w }
	rt, err := Build(t.Context(), opts)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()
	defer func() { cancel(); <-done; require.NoError(t, rt.Close(context.Background())) }()
	ready, readyCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer readyCancel()
	require.NoError(t, rt.WaitSteadyState(ready))
	writer := <-writers
	waitJobs := func(count int) {
		require.Eventually(t, func() bool {
			all := rt.BackfillJobs.List()
			if len(all) != count {
				return false
			}
			for _, j := range all {
				if j.State != jobs.Complete {
					return false
				}
			}
			return true
		}, 10*time.Second, 10*time.Millisecond, "jobs: %+v", rt.BackfillJobs.List())
	}
	waitJobs(1)
	_, err = rt.BackfillJobs.SetPolicy(1, []string{"app.bsky.feed.post", "app.bsky.feed.like"})
	require.NoError(t, err)
	waitJobs(2)
	for _, job := range rt.BackfillJobs.List() {
		require.Equal(t, pds.URL, job.PDS)
		require.Equal(t, "current_state", job.Coverage)
		require.Len(t, job.CompletedRepos, 1)
		require.False(t, job.HistoryComplete)
	}
	require.NoError(t, writer.ForceRotate(t.Context()))
	consumer, err := client.Subscribe("http://"+rt.PublicAddr(), client.WithAfterSeq(0), client.WithSnapshotOnly(), client.WithBatchSize(1))
	require.NoError(t, err)
	defer consumer.Close()
	readCtx, readCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer readCancel()
	keys := map[string]bool{}
	for batch, err := range consumer.Events(readCtx) {
		require.NoError(t, err)
		for _, ev := range batch.Events() {
			if ev.Commit != nil {
				keys[ev.Commit.Rkey] = true
			}
		}
	}
	require.True(t, keys["post"])
	require.True(t, keys["like"], "newly enabled collection must backfill without any relay event")
}
