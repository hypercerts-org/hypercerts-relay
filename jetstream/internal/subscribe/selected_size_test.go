package subscribe_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/coder/websocket"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func TestSelectedMessageSizeLimit(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		for _, replay := range []bool{false, true} {
			for _, limitMode := range []string{"metadata-over-limit", "exact", "unlimited"} {
				t.Run(fmt.Sprintf("compressed=%t/replay=%t/%s", compressed, replay, limitMode), func(t *testing.T) {
					runSelectedMessageSizeLimit(t, compressed, replay, limitMode)
				})
			}
		}
	}
}

func runSelectedMessageSizeLimit(t *testing.T, compressed, replay bool, limitMode string) {
	t.Helper()

	f := newSelectedWebsocketFixture(t)
	policy, err := f.manager.LookupPolicy(1)
	require.NoError(t, err)
	event := segment.Event{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:test", Collection: "app.example.one", Rkey: "large-key", Payload: []byte{0xa0}}
	revision := uint64(1)
	body, err := subscribe.EncodeV2Selected(&event, subscribe.SelectedEventMetadata{StreamID: f.stream, PolicyRev: &revision, Provenance: attribution.ProvenanceLive})
	require.NoError(t, err)
	limit := len(body)
	if limitMode == "metadata-over-limit" {
		limit--
	}
	if limitMode == "unlimited" {
		limit = 0
	}
	appendEvent := func() {
		require.NoError(t, f.writer.AppendAttributed(t.Context(), &event, ingest.AttributionMetadata{StreamID: f.stream, Policy: policy, Provenance: attribution.ProvenanceLive}))
		require.NoError(t, f.writer.Flush(t.Context()))
	}
	if replay {
		appendEvent()
	}
	query := fmt.Sprintf("maxMessageSizeBytes=%d", limit)
	if replay {
		query += "&streamId=" + f.stream + "&cursor=1"
	}
	if compressed {
		query += fmt.Sprintf("&zstdDictionary=%d", subscribe.DictionaryV2ID)
	}
	conn := f.dial(t, query)
	defer conn.CloseNow()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderDicts(subscribe.DictionaryV2()))
	require.NoError(t, err)
	defer decoder.Close()
	read := func() []byte {
		kind, frame, err := conn.Read(ctx)
		require.NoError(t, err)
		if compressed {
			require.Equal(t, websocket.MessageBinary, kind)
			frame, err = decoder.DecodeAll(frame, nil)
			require.NoError(t, err)
		} else {
			require.Equal(t, websocket.MessageText, kind)
		}
		return frame
	}
	require.Contains(t, string(read()), "SelectedFeed")
	if !replay {
		appendEvent()
	}
	require.Contains(t, string(read()), "PolicyAdopted")
	f.addEvent(t, 2, 1)
	if limitMode != "metadata-over-limit" {
		require.Contains(t, string(read()), `"seq":1`)
	}
	require.Contains(t, string(read()), `"seq":2`)

}
