package ingest

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func TestRecoveryAuditOutcomeLogging(t *testing.T) {
	for _, withLogger := range []bool{false, true} {
		t.Run(map[bool]string{false: "nil logger", true: "logger"}[withLogger], func(t *testing.T) {
			w := newTestWriter(t, Config{})
			event := &segment.Event{Kind: segment.KindCreate, DID: "did:plc:test", Collection: "org.example.record", Rkey: "first"}
			require.NoError(t, w.Append(t.Context(), event))
			require.NoError(t, w.Flush(t.Context()))
			manager, err := attribution.Open(w.cfg.Store, "stream", "logging")
			require.NoError(t, err)
			_, err = manager.PrepareSegment(*event, "stream", attribution.Policy{Revision: 1, Collections: []string{"org.example.record"}}, attribution.ProvenanceLive, attribution.Boundary{})
			require.NoError(t, err)
			cfg := w.cfg
			cfg.Attribution = manager
			cfg.Logger = nil
			var logs bytes.Buffer
			if withLogger {
				cfg.Logger = slog.New(slog.NewTextHandler(&logs, nil))
			}
			require.NoError(t, recoverAttributions(cfg))
			_, err = manager.Lookup(event.Seq)
			require.NoError(t, err)
			if withLogger {
				require.Contains(t, logs.String(), "attribution recovery and audit complete")
				require.Contains(t, logs.String(), "duration=")
				logs.Reset()
			}
			missing := &segment.Event{Kind: segment.KindCreate, DID: "did:plc:test", Collection: "org.example.record", Rkey: "missing"}
			require.NoError(t, w.Append(t.Context(), missing))
			require.NoError(t, w.Flush(t.Context()))
			require.ErrorContains(t, recoverAttributions(cfg), "lacks metadata")
			if withLogger {
				require.Contains(t, logs.String(), "attribution recovery and audit failed")
				require.Contains(t, logs.String(), "duration=")
				require.NotContains(t, logs.String(), "did:plc:test")
			}
		})
	}
}
