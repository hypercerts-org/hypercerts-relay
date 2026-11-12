package subscribe

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/coder/websocket"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func TestSelectedOperationalFailures(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		for _, stage := range []string{"lookup", "history", "idle-history", "legacy-sync", "broken-announcement", "cancelled"} {
			t.Run(fmt.Sprintf("compressed=%t/%s", compressed, stage), func(t *testing.T) {
				runSelectedOperationalFailures(t, compressed, stage)
			})
		}
	}
}

func decodeSelectedFailureFrame(t *testing.T, compressed bool, kind websocket.MessageType, frame []byte) []byte {
	t.Helper()
	if !compressed {
		require.Equal(t, websocket.MessageText, kind)
		return frame
	}
	require.Equal(t, websocket.MessageBinary, kind)
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderDicts(DictionaryV2()))
	require.NoError(t, err)
	defer decoder.Close()
	decoded, err := decoder.DecodeAll(frame, nil)
	require.NoError(t, err)
	return decoded
}

func runSelectedOperationalFailures(t *testing.T, compressed bool, stage string) {
	t.Helper()

	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	defer db.Close()
	manager, err := attribution.Open(db, "stream", "operational")
	require.NoError(t, err)
	policy := attribution.Policy{Revision: 1, Collections: []string{"app.example.one"}}
	require.NoError(t, manager.EnsureInitialPolicyHistory(policy, nil))
	corruptSelectedCoordinate(t, db, stage)
	var logs bytes.Buffer
	done := make(chan struct{})
	var finalCursor, announced uint64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		conn, acceptErr := websocket.Accept(w, r, nil)
		if acceptErr != nil {
			return
		}
		defer conn.CloseNow()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		selected := selectedConnection{conn: conn, deps: Subscription{SelectedAttribution: manager}, filter: &FilterV2{}, cursor: 1, compress: compressed, logger: slog.New(slog.NewTextHandler(&logs, nil))}
		runSelectedFailureStage(t, ctx, cancel, &selected, policy, stage)
		finalCursor, announced = selected.cursor, selected.announced
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	conn, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	require.NoError(t, err)
	if response != nil && response.Body != nil {
		response.Body.Close()
	}
	defer conn.CloseNow()
	kind, frame, err := conn.Read(ctx)
	if stage == "broken-announcement" || stage == "cancelled" {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
		frame = decodeSelectedFailureFrame(t, compressed, kind, frame)
		if stage == "legacy-sync" {
			require.Contains(t, string(frame), "PolicyAdopted")
			kind, frame, err = conn.Read(ctx)
			require.NoError(t, err)
			frame = decodeSelectedFailureFrame(t, compressed, kind, frame)
		}
		require.JSONEq(t, string(EncodeV2Error("InternalError", "selected delivery failed; reconnect")), string(frame))
	}
	<-done
	require.Equal(t, uint64(1), finalCursor)
	require.NotContains(t, logs.String(), "invalid-private-value")
	if stage == "broken-announcement" || stage == "cancelled" {
		require.Empty(t, logs.String())
		require.Zero(t, announced)
	}

}

func corruptSelectedCoordinate(t *testing.T, db *store.Store, stage string) {
	t.Helper()
	namespace := base64.RawURLEncoding.EncodeToString([]byte("operational"))
	coordinate := "event/0000000000000001"
	if stage == "history" || stage == "idle-history" {
		coordinate = "policy/0000000000000001"
	}
	if stage == "lookup" || stage == "history" || stage == "idle-history" {
		batch := db.NewBatch()
		require.NoError(t, batch.Set([]byte("hypercerts/attribution/v1/"+namespace+"/"+coordinate), []byte("invalid-private-value"), nil))
		require.NoError(t, db.Commit(batch, store.SyncWrites))
		require.NoError(t, batch.Close())
	}
}

func runSelectedFailureStage(t *testing.T, ctx context.Context, cancel context.CancelFunc, selected *selectedConnection, policy attribution.Policy, stage string) {
	t.Helper()
	if stage == "broken-announcement" {
		selected.conn.CloseNow()
		err := selected.announcePolicy(ctx, policy)
		require.ErrorIs(t, err, errSelectedTransport)
		selected.deliveryFailed(ctx, "announcement", err)
	} else if stage == "idle-history" {
		require.False(t, selected.announceIdlePolicies(ctx))
	} else {
		event := &segment.Event{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:test", Collection: "app.example.one", Rkey: "r", Payload: []byte{0xa0}}
		if stage == "legacy-sync" {
			event.Kind = segment.KindSync
		}
		if stage == "cancelled" {
			cancel()
			selected.deliveryFailed(ctx, stage, context.Canceled)
			return
		}
		err := selected.processSelectedRow(ctx, event)
		require.Error(t, err)
		selected.deliveryFailed(ctx, stage, err)
	}
}
