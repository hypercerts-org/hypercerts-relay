package subscribe

import (
	"io"
	"log/slog"
	"testing"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestSelectedOversizeCountsMetricBeforeWrite(t *testing.T) {
	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	defer db.Close()
	manager, err := attribution.Open(db, "stream", "size-metric")
	require.NoError(t, err)
	metrics := NewMetrics(prometheus.NewRegistry())
	event := &segment.Event{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:test", Collection: "app.example.one", Rkey: "key", Payload: []byte{0xa0}}
	meta, err := manager.Lookup(event.Seq)
	require.ErrorIs(t, err, attribution.ErrLegacyUnknown)
	for _, compressed := range []bool{false, true} {
		require.NoError(t, (&selectedConnection{deps: Subscription{SelectedAttribution: manager, Metrics: metrics}, filter: &FilterV2{maxMessageSizeBytes: 1}, compress: compressed, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}).deliverSelected(t.Context(), event, meta))
	}
	require.Equal(t, float64(2), testutil.ToFloat64(metrics.EventsOversize))
}
