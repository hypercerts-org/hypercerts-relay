package subscribe_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble"
	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestSelectedHandlerWebsocketCapabilityAndMetadata(t *testing.T) {
	dir := t.TempDir()
	st, w := openWriterAtTip(t, dir, 0)
	defer func() { _ = w.Close(); _ = st.Close() }()
	require.NoError(t, lifecycle.WritePhase(st, lifecycle.PhaseSteadyState, time.Now().UTC()))
	stream := "crypto-identity"
	manager, err := attribution.Open(st, stream, "selected-stream")
	require.NoError(t, err)
	policy := attribution.Policy{Revision: 1, Collections: []string{}}
	policyManager, err := selection.Open(st, []string{})
	require.NoError(t, err)
	w, err = ingest.Open(ingest.Config{SegmentsDir: filepath.Join(dir, "segments"), Store: st, Attribution: manager, AttributionDomain: "selected-stream", CollectionPolicy: policyManager, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	require.NoError(t, lifecycle.WritePhase(st, lifecycle.PhaseSteadyState, time.Now().UTC()))
	require.NoError(t, manager.EnsureInitialPolicyHistory(policy, nil))
	require.NoError(t, w.AppendAttributed(context.Background(), &segment.Event{Seq: 1, Kind: segment.KindAccount, DID: "did:plc:test", Payload: []byte{0xa0}}, ingest.AttributionMetadata{StreamID: stream, Policy: policy, Provenance: attribution.ProvenanceLive}))
	require.NoError(t, w.Flush(context.Background()))
	tail, err := subscribe.New(subscribe.Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, func() uint64 { return w.NextSeq() })
	require.NoError(t, err)
	tail.SetReadLogSource(func() *ingest.ReadableLog { return w.ReadLog() })
	// Cursorless selection starts at the live edge; append a second event after upgrade below.
	provider := subscribe.SelectedConfigProviderFunc(func() (subscribe.SelectedConfig, error) {
		floor := uint64(0)
		return subscribe.SelectedConfig{StreamID: stream, PolicyRevision: 1, Collections: []string{}, ReplayFloor: &floor, CurrentSeq: 2, ReplayEnabled: true, Coverage: "unknown", Gaps: []subscribe.SelectedGap{}}, nil
	})
	server := httptest.NewServer(subscribe.NewHandler(subscribe.Subscription{Tail: tail, Store: st, Writer: w, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: subscribe.NewMetrics(prometheus.NewRegistry()), Lookback: time.Hour, V2: true, SelectedConfig: provider, SelectedAttribution: manager, SelectedStreamID: stream}))
	defer server.Close()
	conn, resp, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(server.URL, "http")+"/?contract="+subscribe.SelectedContract, &websocket.DialOptions{HTTPHeader: http.Header{"Sec-WebSocket-Protocol": []string{subscribe.SelectedContract}}})
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	defer func() { _ = conn.CloseNow() }()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, first, err := conn.Read(ctx)
	require.NoError(t, err)
	var frame map[string]any
	require.NoError(t, json.Unmarshal(first, &frame))
	require.Contains(t, string(first), "SelectedFeed")
	_, second, err := conn.Read(ctx)
	require.NoError(t, err)
	require.Contains(t, string(second), "PolicyAdopted")
}

func TestAbsentSelectedContractDoesNotOptIn(t *testing.T) {
	dir := t.TempDir()
	st, w := openWriterAtTip(t, dir, 0)
	defer func() { _ = w.Close(); _ = st.Close() }()
	require.NoError(t, lifecycle.WritePhase(st, lifecycle.PhaseSteadyState, time.Now().UTC()))
	tail, err := subscribe.New(subscribe.Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)
	provider := subscribe.SelectedConfigProviderFunc(func() (subscribe.SelectedConfig, error) {
		t.Fatal("selected provider must not be consulted")
		return subscribe.SelectedConfig{}, nil
	})
	srv := httptest.NewServer(subscribe.NewHandler(subscribe.Subscription{Tail: tail, Store: st, Writer: w, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: subscribe.NewMetrics(prometheus.NewRegistry()), V2: true, SelectedConfig: provider}))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/?streamId=some-stream&cursor=1")
	require.NoError(t, err)
	require.Equal(t, http.StatusUpgradeRequired, resp.StatusCode)
	_ = resp.Body.Close()
	request, err := http.NewRequest(http.MethodGet, srv.URL+"/", nil)
	require.NoError(t, err)
	request.Header.Set("Sec-WebSocket-Protocol", subscribe.SelectedContract)
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Upgrade", "websocket")
	request.Header.Set("Sec-WebSocket-Version", "13")
	request.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	resp, err = http.DefaultClient.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	_ = resp.Body.Close()
}

func TestSelectedSyncWebsocketDoesNotLeakCARAndKeepsLifecycle(t *testing.T) {
	// This test isolates selected-wire serialization of a sync marker. The
	// upstream signed-sync/verifier path is exercised in the live consumer tests.
	f := newSelectedWebsocketFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn := f.dial(t, "")
	defer conn.CloseNow()
	readSelectedFrame(t, conn, ctx, "SelectedFeed")
	policy, err := f.manager.LookupPolicy(1)
	require.NoError(t, err)
	car := []byte("secret-excluded-CAR-block")
	syncPayload, err := (&comatproto.SyncSubscribeRepos_Sync{DID: "did:plc:test", Rev: "3mmoojp7vgo2f", Seq: 100, Time: time.Now().UTC().Format(time.RFC3339Nano), Blocks: car}).MarshalCBOR()
	require.NoError(t, err)
	require.NoError(t, f.writer.AppendAttributed(ctx, &segment.Event{Kind: segment.KindSync, DID: "did:plc:test", Rev: "3mmoojp7vgo2f", Payload: syncPayload}, ingest.AttributionMetadata{StreamID: f.stream, Policy: policy, Provenance: attribution.ProvenanceLive}))
	require.NoError(t, f.writer.Flush(ctx))
	readSelectedFrame(t, conn, ctx, "PolicyAdopted")
	frame := readSelectedFrame(t, conn, ctx, "#sync")
	require.NotContains(t, string(frame), string(car))
	require.Contains(t, string(frame), `"did":"did:plc:test"`)
	require.Contains(t, string(frame), `"reconciliationRequired":true`)
	require.Contains(t, string(frame), `"collections":["app.example.one"]`)
}

func TestSelectedNegotiationFailsBeforeUpgrade(t *testing.T) {
	dir := t.TempDir()
	st, w := openWriterAtTip(t, dir, 0)
	defer func() { _ = w.Close(); _ = st.Close() }()
	require.NoError(t, lifecycle.WritePhase(st, lifecycle.PhaseSteadyState, time.Now().UTC()))
	tail, err := subscribe.New(subscribe.Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)
	srv := httptest.NewServer(subscribe.NewHandler(subscribe.Subscription{Tail: tail, Store: st, Writer: w, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: subscribe.NewMetrics(prometheus.NewRegistry()), V2: true}))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/?contract=unsupported")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	require.Contains(t, string(body), "UnsupportedContract")
}

// TestSelectedHandlerHistoricalPolicyAdoptionsPrecedeGovernedRows exercises the
// real handler's ordering guarantees for resumed and live policy boundaries.
func TestSelectedHandlerHistoricalPolicyAdoptionsPrecedeGovernedRows(t *testing.T) {
	t.Run("inclusive cursor announces governing history", func(t *testing.T) {
		f := newSelectedWebsocketFixture(t)
		f.addEvent(t, 1, 1)
		f.addEvent(t, 2, 1)
		f.addEvent(t, 3, 1)
		f.adopt(t, 3, 2)
		f.addEvent(t, 4, 2)
		conn := f.dial(t, "streamId="+f.stream+"&cursor=4")
		defer conn.CloseNow()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		readSelectedFrame(t, conn, ctx, "SelectedFeed")
		first := readSelectedFrame(t, conn, ctx, "PolicyAdopted")
		second := readSelectedFrame(t, conn, ctx, "PolicyAdopted")
		require.Contains(t, string(first), `"policyRevision":1`)
		require.Contains(t, string(second), `"policyRevision":2`)
		readSelectedFrame(t, conn, ctx, `"seq":4`)
	})
	t.Run("same-boundary revisions retain durable order", func(t *testing.T) {
		f := newSelectedWebsocketFixture(t)
		f.addEvent(t, 1, 1)
		f.addEvent(t, 2, 1)
		f.addEvent(t, 3, 1)
		f.adopt(t, 3, 2)
		f.adopt(t, 3, 3)
		f.addEvent(t, 4, 3)
		conn := f.dial(t, "streamId="+f.stream+"&cursor=4")
		defer conn.CloseNow()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		readSelectedFrame(t, conn, ctx, "SelectedFeed")
		a := readSelectedFrame(t, conn, ctx, "PolicyAdopted")
		b := readSelectedFrame(t, conn, ctx, "PolicyAdopted")
		c := readSelectedFrame(t, conn, ctx, "PolicyAdopted")
		require.Contains(t, string(a), `"policyRevision":1`)
		require.Contains(t, string(b), `"policyRevision":2`)
		require.Contains(t, string(c), `"policyRevision":3`)
		readSelectedFrame(t, conn, ctx, `"seq":4`)
	})
	t.Run("idle subscriber resumes with persistent reader", func(t *testing.T) {
		f := newSelectedWebsocketFixture(t)
		conn := f.dial(t, "")
		defer conn.CloseNow()
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		readSelectedFrame(t, conn, ctx, "SelectedFeed")
		f.addEvent(t, 1, 1)
		f.addEvent(t, 2, 1)
		readSelectedFrame(t, conn, ctx, "PolicyAdopted")
		readSelectedFrame(t, conn, ctx, `"seq":1`)
		readSelectedFrame(t, conn, ctx, `"seq":2`)
		f.adopt(t, 2, 2)
		frames := make(chan []byte, 4)
		go func() {
			for {
				_, b, err := conn.Read(context.Background())
				if err != nil {
					return
				}
				frames <- b
			}
		}()
		select {
		case <-frames:
			t.Fatal("unexpected idle frame")
		case <-time.After(1200 * time.Millisecond):
		}
		f.addEvent(t, 3, 2)
		require.Contains(t, string(<-frames), "PolicyAdopted")
		require.Contains(t, string(<-frames), `"seq":3`)
	})
}

type selectedWebsocketFixture struct {
	t       *testing.T
	st      *store.Store
	writer  *ingest.Writer
	manager *attribution.Manager
	tail    *subscribe.Tail
	server  *httptest.Server
	stream  string
}

func newSelectedWebsocketFixture(t *testing.T) *selectedWebsocketFixture {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	stream := "selected-websocket-stream"
	manager, err := attribution.Open(st, stream, "selected-websocket")
	require.NoError(t, err)
	first := attribution.Policy{Revision: 1, Collections: []string{"app.example.one"}}
	require.NoError(t, manager.EnsureInitialPolicyHistory(first, nil))
	policyManager, err := selection.Open(st, []string{"app.example.one"})
	require.NoError(t, err)
	w, err := ingest.Open(ingest.Config{SegmentsDir: filepath.Join(dir, "segments"), Store: st, Attribution: manager, AttributionDomain: "selected-websocket", CollectionPolicy: policyManager, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), ReadLogRetentionBytes: 1 << 20})
	require.NoError(t, err)
	tail, err := subscribe.New(subscribe.Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, func() uint64 { return w.NextSeq() })
	require.NoError(t, err)
	tail.SetReadLogSource(func() *ingest.ReadableLog { return w.ReadLog() })
	require.NoError(t, lifecycle.WritePhase(st, lifecycle.PhaseSteadyState, time.Now().UTC()))
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	require.Equal(t, uint64(1), w.NextSeq())
	f := &selectedWebsocketFixture{t: t, st: st, writer: w, manager: manager, tail: tail, stream: stream}
	m := mustOpenManifest(t, filepath.Join(dir, "segments"))
	f.server = httptest.NewServer(subscribe.NewHandler(subscribe.Subscription{Tail: tail, Store: st, Writer: w, Logger: logger, Metrics: subscribe.NewMetrics(prometheus.NewRegistry()), Lookback: time.Hour, V2: true, Manifest: m, SelectedConfig: subscribe.SelectedConfigProviderFunc(func() (subscribe.SelectedConfig, error) {
		history, e := manager.ReadPolicyHistory()
		if e != nil {
			return subscribe.SelectedConfig{}, e
		}
		p := history[len(history)-1]
		return subscribe.SelectedConfig{StreamID: stream, PolicyRevision: p.Revision, PolicyEffectiveAfterSeq: p.EffectiveAfterSeq, Collections: p.Collections, ReplayFloor: func() *uint64 { v := uint64(0); return &v }(), CurrentSeq: w.NextSeq() - 1, ReplayEnabled: true, Coverage: "unknown", Gaps: []subscribe.SelectedGap{}}, nil
	}), SelectedAttribution: manager, SelectedStreamID: stream}))
	t.Cleanup(func() { f.server.Close(); _ = w.Close(); _ = st.Close() })
	return f
}

func (f *selectedWebsocketFixture) adopt(t *testing.T, seq, revision uint64) {
	t.Helper()
	boundary := seq
	p := attribution.Policy{Revision: revision, Collections: []string{"app.example.one"}, EffectiveAfterSeq: &boundary}
	err := f.manager.WithPolicyCommit(p, func(stage func(*pebble.Batch) error) error {
		b := f.st.NewBatch()
		defer b.Close()
		if err := stage(b); err != nil {
			return err
		}
		return f.st.Commit(b, store.SyncWrites)
	})
	require.NoError(t, err)
}

func (f *selectedWebsocketFixture) addEvent(t *testing.T, seq, revision uint64) {
	t.Helper()
	p, err := f.manager.LookupPolicy(revision)
	require.NoError(t, err)
	e := &segment.Event{Seq: seq, Kind: segment.KindCreate, DID: "did:plc:test", Collection: "app.example.one", Rkey: "r", Payload: []byte{0xa0}}
	require.NoError(t, f.writer.AppendAttributed(context.Background(), e, ingest.AttributionMetadata{StreamID: f.stream, Policy: p, Provenance: attribution.ProvenanceLive}))
	require.NoError(t, f.writer.Flush(context.Background()))
}

func TestSelectedHandlerSealedColdReplayPolicyBoundary(t *testing.T) {
	f := newSelectedWebsocketFixture(t)
	f.addEvent(t, 1, 1)
	f.addEvent(t, 2, 1)
	f.addEvent(t, 3, 1)
	f.adopt(t, 3, 2)
	f.addEvent(t, 4, 2)

	conn := f.dial(t, "streamId="+f.stream+"&cursor=1")
	defer conn.CloseNow()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	readSelectedFrame(t, conn, ctx, "SelectedFeed")
	readSelectedFrame(t, conn, ctx, `"policyRevision":1`)
	readSelectedFrame(t, conn, ctx, `"seq":1`)
	readSelectedFrame(t, conn, ctx, `"seq":2`)
	readSelectedFrame(t, conn, ctx, `"seq":3`)
	readSelectedFrame(t, conn, ctx, `"policyRevision":2`)
	readSelectedFrame(t, conn, ctx, `"seq":4`)
}

func (f *selectedWebsocketFixture) dial(t *testing.T, query string) *websocket.Conn {
	t.Helper()
	if query != "" {
		query = "&" + query
	}
	conn, resp, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(f.server.URL, "http")+"/?contract="+subscribe.SelectedContract+query, &websocket.DialOptions{HTTPHeader: http.Header{"Sec-WebSocket-Protocol": []string{subscribe.SelectedContract}}})
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	return conn
}

func readSelectedFrame(t *testing.T, conn *websocket.Conn, ctx context.Context, want string) []byte {
	t.Helper()
	_, body, err := conn.Read(ctx)
	require.NoError(t, err)
	require.Contains(t, string(body), want)
	return body
}
