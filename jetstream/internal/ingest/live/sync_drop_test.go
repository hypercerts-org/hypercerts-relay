package live

// hypercerts: TECH-932 F8 — rows of a reconciled #sync that cannot be
// represented in a segment column (e.g. a spec-valid 256-512 byte rkey) must
// be dropped before reconciliation, like the append loop does for commits.
// Reconciliation would otherwise return a hard error and end Consumer.Run in a
// restart loop.

import (
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestDropUnarchivableSyncRows(t *testing.T) {
	dropMetrics := ingest.NewDropMetrics(prometheus.NewRegistry())
	c := &Consumer{cfg: Config{DropMetrics: dropMetrics}, logger: slog.New(slog.NewTextHandler(io.Discard, nil)).With("component", "livestream/consumer")}

	marker := segment.Event{Kind: segment.KindSync, DID: "did:plc:wideresync", Rev: "3l4qkcqzmcs2c"}
	ok := segment.Event{Kind: segment.KindCreate, DID: "did:plc:wideresync", Rev: "3l4qkcqzmcs2c", Collection: "app.bsky.feed.post", Rkey: "keepme"}
	wide := segment.Event{Kind: segment.KindCreate, DID: "did:plc:wideresync", Rev: "3l4qkcqzmcs2c", Collection: "app.bsky.feed.post", Rkey: strings.Repeat("x", 512)}

	got := c.dropUnarchivableSyncRows(t.Context(), 42, []segment.Event{marker, ok, wide})

	require.Equal(t, []segment.Event{marker, ok}, got,
		"only the unarchivable row is removed; the sync marker and archivable rows survive")
	require.InDelta(t, 1.0, testutil.ToFloat64(dropMetrics.Counter(ingest.DropSourceLive, ingest.DropReasonFieldTooLong)), 0,
		"the dropped row must be counted under field_too_long")
}
