package subscribe_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/history"
	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/cbor"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

type journalHandlerFixture struct {
	db      *store.Store
	journal *history.Store
	server  *httptest.Server
	stream  string
}

func newJournalHandlerFixture(t *testing.T) *journalHandlerFixture {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "db"), nil)
	require.NoError(t, err)
	journal, err := history.Open(db)
	require.NoError(t, err)
	require.NoError(t, lifecycle.WritePhase(db, lifecycle.PhaseSteadyState, time.Now().UTC()))
	f := &journalHandlerFixture{db: db, journal: journal, stream: journal.StreamID()}
	provider := subscribe.SelectedConfigProviderFunc(func() (subscribe.SelectedConfig, error) {
		tip, err := journal.Tip()
		if err != nil {
			return subscribe.SelectedConfig{}, err
		}
		activation, err := journal.Activation()
		if err != nil {
			return subscribe.SelectedConfig{}, err
		}
		start := activation.JournalStart
		adoptions, err := journal.ReadPolicyAdoptions()
		if err != nil {
			return subscribe.SelectedConfig{}, err
		}
		return subscribe.SelectedConfig{StreamID: f.stream, CurrentSeq: tip, ReplayEnabled: true, ReplayFloor: &start, HistoryMode: "original-events", OriginalHistoryFromSeq: &start, Coverage: "unknown", Collections: []string{"app.example.one"}, PolicyAdoptions: adoptions, Gaps: []subscribe.SelectedGap{}}, nil
	})
	tail, err := subscribe.New(subscribe.Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, func() uint64 { tip, _ := journal.Tip(); return tip + 1 })
	require.NoError(t, err)
	f.server = httptest.NewServer(subscribe.NewHandler(subscribe.Subscription{Tail: tail, Store: db, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: subscribe.NewMetrics(prometheus.NewRegistry()), V2: true, Lookback: time.Hour, SelectedConfig: provider, SelectedHistory: journal, SelectedJournalMode: true}))
	t.Cleanup(func() {
		f.server.CloseClientConnections()
		f.server.Close()
		time.Sleep(10 * time.Millisecond)
		_ = db.Close()
	})
	return f
}

func encodeHistoryRecord(t *testing.T, body string) []byte {
	t.Helper()
	value, err := cbor.FromJSON([]byte(`{"text":` + strconv.Quote(body) + `}`))
	require.NoError(t, err)
	var encoded bytes.Buffer
	require.NoError(t, cbor.NewEncoder(&encoded).WriteValue(value))
	return encoded.Bytes()
}

func (f *journalHandlerFixture) append(t *testing.T, cursor uint64, collection, body string, disposition history.Disposition) uint64 {
	t.Helper()
	data := encodeHistoryRecord(t, body)
	row := history.Append{Source: history.Source{Domain: "relay", Epoch: "epoch-1", Cursor: cursor, Operation: 1, Path: collection + "/r", ContentHash: history.OriginalHash(data), PDS: ""}, Kind: "put", DID: "did:plc:example", Collection: collection, Revision: "rev", Original: data, InputEvent: segment.Event{Kind: segment.KindCreate, DID: "did:plc:example", Collection: collection, Rkey: "r", Rev: "rev", Payload: data}, Policy: history.Policy{Revision: 1, Collections: []string{"app.example.one"}}, Provenance: "live", Disposition: disposition, Boundary: 2, Attribution: history.Attribution{StreamID: f.stream, Policy: history.EffectivePolicy{Revision: 1, Collections: []string{"app.example.one"}}, Provenance: "live"}}
	receipt, err := f.journal.Append(row)
	require.NoError(t, err)
	return receipt.Sequence
}

func (f *journalHandlerFixture) dial(t *testing.T, query string) *websocket.Conn {
	t.Helper()
	target := "ws" + strings.TrimPrefix(f.server.URL, "http") + "/?contract=" + subscribe.SelectedContract
	if query != "" {
		target += "&" + query
	}
	conn, resp, err := websocket.Dial(context.Background(), target, &websocket.DialOptions{})
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	return conn
}

func journalFrame(t *testing.T, conn *websocket.Conn, ctx context.Context) map[string]any {
	t.Helper()
	_, body, err := conn.Read(ctx)
	require.NoError(t, err)
	var frame map[string]any
	require.NoError(t, json.Unmarshal(body, &frame))
	return frame
}

func requireSelectedInfo(t *testing.T, frame map[string]any) {
	t.Helper()
	encoded, err := json.Marshal(frame)
	require.NoError(t, err)
	require.Contains(t, string(encoded), "SelectedFeed")
}

func TestSelectedHistoryHandlerInclusiveCursorAndImmutableMetadata(t *testing.T) {
	f := newJournalHandlerFixture(t)
	f.append(t, 1, "app.example.one", "one", history.HistoricalOnly)
	f.append(t, 2, "app.example.one", "two", history.Apply)
	conn := f.dial(t, "streamId="+f.stream+"&cursor=1")
	defer conn.CloseNow()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	requireSelectedInfo(t, journalFrame(t, conn, ctx))
	adopted := journalFrame(t, conn, ctx)
	encodedAdopted, err := json.Marshal(adopted)
	require.NoError(t, err)
	require.Contains(t, string(encodedAdopted), "PolicyAdopted")
	first := journalFrame(t, conn, ctx)
	encoded, err := json.Marshal(first)
	require.NoError(t, err)
	require.Contains(t, string(encoded), "historical-only")
	require.NotContains(t, string(encoded), "did:web:pds.example")
	require.Contains(t, string(encoded), `"seq":1`)
}

func TestSelectedHistoryHandlerRejectsCursorBelowInclusiveFloor(t *testing.T) {
	f := newJournalHandlerFixture(t)
	request, err := http.NewRequest(http.MethodGet, f.server.URL+"/?contract="+subscribe.SelectedContract+"&streamId="+f.stream+"&cursor=0", nil)
	require.NoError(t, err)
	request.Header.Set("Sec-WebSocket-Protocol", "xrpc.v1.json")
	resp, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	require.Contains(t, string(body), "ReplayUnavailable")
}

func TestSelectedHistoryHandlerCursorlessLiveAndBroadcastWake(t *testing.T) {
	f := newJournalHandlerFixture(t)
	f.append(t, 1, "app.example.one", "before", history.Apply)
	a, b := f.dial(t, ""), f.dial(t, "")
	defer a.CloseNow()
	defer b.CloseNow()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	requireSelectedInfo(t, journalFrame(t, a, ctx))
	requireSelectedInfo(t, journalFrame(t, b, ctx))
	f.append(t, 2, "app.example.one", "after", history.Apply)
	for _, conn := range []*websocket.Conn{a, b} {
		frame := journalFrame(t, conn, ctx)
		encoded, err := json.Marshal(frame)
		require.NoError(t, err)
		if !strings.Contains(string(encoded), `"seq":2`) {
			require.Contains(t, string(encoded), "PolicyAdopted")
			frame = journalFrame(t, conn, ctx)
			encoded, err = json.Marshal(frame)
			require.NoError(t, err)
		}
		require.Contains(t, string(encoded), `"seq":2`)
	}
}

func TestSelectedHistoryHandlerFilterScansJournalAndRemovedPolicyReplay(t *testing.T) {
	f := newJournalHandlerFixture(t)
	f.append(t, 1, "app.example.one", "first", history.Apply)
	f.append(t, 2, "app.example.other", "excluded", history.Apply)
	f.append(t, 3, "app.example.one", "third", history.Apply)
	policyTime := time.Now().UnixNano()
	_, err := f.journal.AdoptPolicy(history.Policy{Revision: 2, Collections: []string{"app.example.other"}}, policyTime)
	require.NoError(t, err)
	state, ok, err := f.journal.ReadPolicyState()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(3), state.AdoptedAtJournalSequence)
	intervalsByCollection := make(map[string][]history.PolicyInterval)
	for _, interval := range state.Intervals {
		intervalsByCollection[interval.Collection] = append(intervalsByCollection[interval.Collection], interval)
	}
	require.Empty(t, intervalsByCollection["app.example.one"])
	require.Len(t, intervalsByCollection["app.example.other"], 1)
	require.Equal(t, uint64(4), intervalsByCollection["app.example.other"][0].FromJournalSequence)
	require.Nil(t, intervalsByCollection["app.example.other"][0].ThroughJournalSequence)
	conn := f.dial(t, "streamId="+f.stream+"&cursor=1&collections=app.example.one")
	defer conn.CloseNow()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	requireSelectedInfo(t, journalFrame(t, conn, ctx))
	adopted := journalFrame(t, conn, ctx)
	adoptedJSON, err := json.Marshal(adopted)
	require.NoError(t, err)
	require.Contains(t, string(adoptedJSON), "PolicyAdopted")
	frame := journalFrame(t, conn, ctx)
	encoded, err := json.Marshal(frame)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"seq":1`)
	require.Contains(t, string(encoded), "first")
	require.Contains(t, string(encoded), f.stream)
	require.NotContains(t, string(encoded), "excluded")
}

func TestSelectedHistoryHandlerRejectsMaterializedIdentity(t *testing.T) {
	f := newJournalHandlerFixture(t)
	request, err := http.NewRequest(http.MethodGet, f.server.URL+"/?contract="+subscribe.SelectedContract+"&streamId=legacy-materialized-stream&cursor=1", nil)
	require.NoError(t, err)
	request.Header.Set("Sec-WebSocket-Protocol", "xrpc.v1.json")
	resp, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	require.Contains(t, string(body), "StreamMismatch")
}

func TestSelectedHistoryHandlerInitialInfoReportsUnknownCoverageAndIndefiniteHistory(t *testing.T) {
	f := newJournalHandlerFixture(t)
	conn := f.dial(t, "")
	defer conn.CloseNow()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	frame := journalFrame(t, conn, ctx)
	encoded, err := json.Marshal(frame)
	require.NoError(t, err)
	require.Contains(t, string(encoded), "original-events")
	require.Contains(t, string(encoded), "unknown")
	require.Contains(t, string(encoded), "originalHistoryFromSeq")
}

func TestSelectedHistoryHandlerReplaysInfoAndRejectedControlsThenContinues(t *testing.T) {
	f := newJournalHandlerFixture(t)
	appendControl := func(cursor uint64, kind, reason string) {
		t.Helper()
		original, err := json.Marshal(struct {
			Version uint8  `json:"version"`
			Reason  string `json:"reason"`
			Cursor  int64  `json:"cursor"`
		}{1, reason, int64(cursor)})
		require.NoError(t, err)
		_, err = f.journal.Append(history.Append{Source: history.Source{Domain: "relay", Epoch: "epoch-1", Cursor: cursor, Path: "$" + kind, ContentHash: history.OriginalHash(original)}, Kind: kind, Time: time.Now().UnixNano(), Original: original, Policy: history.Policy{Revision: 1, Collections: []string{"app.example.one"}}, Provenance: "live", Disposition: history.Noop, Attribution: history.Attribution{StreamID: f.stream, Policy: history.EffectivePolicy{Revision: 1, Collections: []string{"app.example.one"}}, Provenance: "live"}})
		require.NoError(t, err)
	}
	appendControl(1, "source-info", "informational")
	appendControl(2, "source-gap", "rejected")
	f.append(t, 3, "app.example.one", "valid-after-controls", history.Apply)
	conn := f.dial(t, "streamId="+f.stream+"&cursor=1")
	defer conn.CloseNow()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	journalFrame(t, conn, ctx)
	journalFrame(t, conn, ctx)
	for _, expected := range []string{"SourceInfo", "SourceGap", "valid-after-controls"} {
		frame := journalFrame(t, conn, ctx)
		encoded, err := json.Marshal(frame)
		require.NoError(t, err)
		require.Contains(t, string(encoded), expected)
	}
}

func TestSelectedHistoryHandlerSourceCoverageDoesNotInventPDS(t *testing.T) {
	f := newJournalHandlerFixture(t)
	f.append(t, 1, "app.example.one", "payload", history.Apply)
	page, err := f.journal.Read(0, 1, 1)
	require.NoError(t, err)
	require.Empty(t, page.Events[0].Source.PDS)
	require.Equal(t, attribution.ProvenanceLive, attribution.Provenance(page.Events[0].Provenance))
}
