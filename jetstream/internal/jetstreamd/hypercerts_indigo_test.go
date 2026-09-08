package jetstreamd

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	client "github.com/bluesky-social/jetstream"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/live"
	"github.com/bluesky-social/jetstream/internal/simulator/fanout"
	simhttp "github.com/bluesky-social/jetstream/internal/simulator/http"
	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/stretchr/testify/require"
)

// This crosses the real Indigo disk event manager/subscribeRepos handler,
// Jetstream verification/storage/server, and the public archive-to-live client.
func TestHypercertsIndigoArchiveRestartAndLive(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "relay-source")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", bin, "./tests/jetstream-source")
	build.Dir = "../../.."
	out, err := build.CombinedOutput()
	require.NoError(t, err, string(out))
	fixtureDir := t.TempDir()
	addressFile := filepath.Join(fixtureDir, "address")
	proc := exec.CommandContext(t.Context(), bin, "--data-dir", fixtureDir, "--address-file", addressFile)
	proc.Stdout = io.Discard
	proc.Stderr = io.Discard
	require.NoError(t, proc.Start())
	defer func() { _ = proc.Process.Kill(); _ = proc.Wait() }()
	var source string
	require.Eventually(t, func() bool { b, e := os.ReadFile(addressFile); source = string(b); return e == nil && source != "" }, 10*time.Second, 10*time.Millisecond)
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
	require.NoError(t, w.AttachRuntime(rand.New(rand.NewPCG(1, 2)), fanout.New(64)))
	pds := httptest.NewServer(nil)
	pds.Config.Handler = simhttp.NewHandler(w, pds.URL)
	defer pds.Close()
	options := testOptions(t)
	options.RelayURL = source
	options.PLCURL = pds.URL
	options.CollectionSelection = true
	options.InitialCollections = []string{"app.bsky.feed.post"}
	options.LogOutput = io.Discard
	emit := func(key string) {
		frame, _, err := w.GenerateRecordOpForTest(t.Context(), 0, "create", "app.bsky.feed.post", key)
		require.NoError(t, err)
		resp, err := http.Post(source+"/fixture/emit", "application/cbor", bytes.NewReader(frame))
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusNoContent, resp.StatusCode)
	}
	start := func() (*Runtime, *ingest.Writer, func()) {
		writers := make(chan *ingest.Writer, 1)
		options.OnSteadyStateWriter = func(w *ingest.Writer) { writers <- w }
		rt, err := Build(t.Context(), options)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- rt.Run(ctx) }()
		ready, stop := context.WithTimeout(t.Context(), 10*time.Second)
		defer stop()
		require.NoError(t, rt.WaitSteadyState(ready))
		var writer *ingest.Writer
		select {
		case writer = <-writers:
		case <-ready.Done():
			t.Fatal("writer unavailable")
		}
		require.Eventually(t, func() bool { return rt.PublicAddr() != "" }, time.Second, time.Millisecond)
		return rt, writer, func() { cancel(); <-done; require.NoError(t, rt.Close(context.Background())) }
	}
	rt, writer, stop := start()
	emit("archived")
	require.Eventually(t, func() bool { return writer.NextSeq() > 1 }, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, writer.ForceRotate(t.Context()))
	require.NoError(t, writer.DrainDurability(t.Context()))
	require.Eventually(t, func() bool { n, e := live.LoadUpstreamCursor(rt.metaStore, live.CursorKey); return e == nil && n > 0 }, 5*time.Second, 10*time.Millisecond)
	cursor, err := live.LoadUpstreamCursor(rt.metaStore, live.CursorKey)
	require.NoError(t, err)
	stop()
	rt, writer, stop = start()
	defer stop()
	restored, err := live.LoadUpstreamCursor(rt.metaStore, live.CursorKey)
	require.NoError(t, err)
	require.Equal(t, cursor, restored)
	consumer, err := client.Subscribe("http://"+rt.PublicAddr(), client.WithAfterSeq(0), client.WithBatchSize(1))
	require.NoError(t, err)
	defer consumer.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	received := make(chan client.Event, 4)
	errs := make(chan error, 1)
	go func() {
		for batch, err := range consumer.Events(ctx) {
			if err != nil {
				errs <- err
				return
			}
			for _, ev := range batch.Events() {
				received <- ev
			}
		}
	}()
	read := func(key string) client.Event {
		for {
			select {
			case e := <-received:
				if e.Commit != nil && e.Commit.Rkey == key {
					return e
				}
			case err := <-errs:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatalf("missing %s", key)
			}
		}
	}
	archived := read("archived")
	// The client records page completion after yielding its final batch.
	require.Eventually(t, func() bool { return consumer.Stats().Pages > 0 }, time.Second, time.Millisecond, "consumer must read the archive planner")
	emit("live")
	latest := read("live")
	require.Greater(t, latest.Seq, archived.Seq)
	consumer.Close()
	cancel()
	// A new consumer resumes from its saved cursor and receives the next live record.
	resumed, err := client.Subscribe("http://"+rt.PublicAddr(), client.WithLiveCursor(latest.Seq), client.WithBatchSize(1))
	require.NoError(t, err)
	defer resumed.Close()
	emit("reconnected")
	resumeCtx, resumeCancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer resumeCancel()
	found := false
	for batch, err := range resumed.Events(resumeCtx) {
		require.NoError(t, err)
		for _, e := range batch.Events() {
			if e.Commit != nil && e.Commit.Rkey == "reconnected" {
				require.Greater(t, e.Seq, latest.Seq)
				found = true
			}
		}
		if found {
			break
		}
	}
	require.True(t, found)
}
