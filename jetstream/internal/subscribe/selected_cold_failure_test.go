package subscribe

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

func TestSelectedColdReadFailureTerminates(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		for _, mode := range []string{"operational", "unavailable", "cancelled", "empty", "no-progress", "uncommitted-progress"} {
			t.Run(fmt.Sprintf("compressed=%t/%s", compressed, mode), func(t *testing.T) {
				runSelectedColdReadFailureTerminates(t, compressed, mode)
			})
		}
	}
}

func runSelectedColdReadFailureTerminates(t *testing.T, compressed bool, mode string) {
	t.Helper()

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	defer db.Close()
	writer, err := ingest.Open(ingest.Config{Store: db, SegmentsDir: filepath.Join(t.TempDir(), "segments"), Logger: logger, ReadLogRetentionBytes: 1})
	require.NoError(t, err)
	defer writer.Close()
	for range 3 {
		require.NoError(t, writer.Append(t.Context(), &segment.Event{Kind: segment.KindCreate, DID: "did:plc:test", Collection: "app.example.one", Rkey: "key", Payload: []byte{0xa0}}))
		require.NoError(t, writer.Flush(t.Context()))
	}
	require.Greater(t, writer.ReadLog().FloorSeq(), uint64(1))
	manager, err := attribution.Open(db, "stream", "selected-failure")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var cursors []uint64
	var finalCursor uint64
	tail, err := New(Config{Logger: logger}, func(_ context.Context, cursor uint64, _ int) ([]*Entry, uint64, error) {
		cursors = append(cursors, cursor)
		switch mode {
		case "cancelled":
			cancel()
			return nil, 999, context.Canceled
		case "empty":
			return nil, cursor, nil
		case "uncommitted-progress":
			return []*Entry{{Event: &segment.Event{Seq: writer.ReadLog().DurableSeq()}}}, writer.ReadLog().DurableSeq() - 1, nil
		case "no-progress":
			return []*Entry{{Event: &segment.Event{Seq: writer.ReadLog().DurableSeq()}}}, cursor, nil
		case "unavailable":
			return nil, 999, errColdUnavailable
		default:
			return nil, 999, errors.New("private-path-and-credential")
		}
	}, writer.NextSeq)
	require.NoError(t, err)
	tail.SetReadLogSource(writer.ReadLog)
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		conn, acceptErr := websocket.Accept(w, r, nil)
		if acceptErr != nil {
			return
		}
		defer conn.CloseNow()
		selectedConn := selectedConnection{conn: conn, deps: Subscription{Writer: writer, Tail: tail, SelectedAttribution: manager}, filter: &FilterV2{}, config: SelectedConfig{StreamID: "stream"}, cursor: 1, compress: compressed, logger: logger}
		selectedConn.runSelectedSubscriberLoop(ctx)
		finalCursor = selectedConn.cursor
	}))
	defer server.Close()
	conn, response, err := websocket.Dial(t.Context(), "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	require.NoError(t, err)
	if response != nil && response.Body != nil {
		response.Body.Close()
	}
	defer conn.CloseNow()
	readCtx, readCancel := context.WithTimeout(t.Context(), time.Second)
	defer readCancel()
	kind, frame, readErr := conn.Read(readCtx)
	if mode == "cancelled" {
		require.Error(t, readErr)
	} else {
		require.NoError(t, readErr)
		frame = decodeSelectedFailureFrame(t, compressed, kind, frame)
		message := "stream read failed; reconnect"
		if mode == "unavailable" {
			message = "archive replay unavailable; reconnect"
		}
		if mode == "empty" || mode == "no-progress" || mode == "uncommitted-progress" {
			message = "selected delivery failed; reconnect"
		}
		require.JSONEq(t, string(EncodeV2Error("InternalError", message)), string(frame))
		_, _, readErr = conn.Read(readCtx)
		require.Error(t, readErr)
	}
	<-done
	require.Equal(t, []uint64{1}, cursors)
	require.Equal(t, uint64(1), finalCursor)
	require.NotContains(t, logs.String(), "private-path-and-credential")
	if mode == "cancelled" {
		require.NotContains(t, logs.String(), "selected cold replay failed")
	} else {
		require.Contains(t, logs.String(), "selected")
		require.Contains(t, logs.String(), "cursor=1")
	}

}
