package orchestrator

import (
	"bytes"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/stretchr/testify/require"
)

func TestMergeSourceAttributionRecoveryUsesConfiguredLogger(t *testing.T) {
	fix := newMergeFixture(t, nil, nil)
	manager, err := attribution.Open(fix.store, "selected-stream", "bootstrap-live")
	require.NoError(t, err)
	var logs bytes.Buffer
	fix.cfg.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	fix.cfg.BootstrapAttribution = manager
	fix.cfg.BootstrapAttributionDomain = "bootstrap-live"
	o, err := New(fix.cfg)
	require.NoError(t, err)

	cleaned, err := o.prepareMergeSource(t.Context(), filepath.Join(fix.dataDir, "backfill", "live_segments"))
	require.NoError(t, err)
	require.False(t, cleaned)
	require.Contains(t, logs.String(), `"msg":"attribution recovery and audit complete"`)
	require.Contains(t, logs.String(), `"duration":`)
	require.NotContains(t, logs.String(), "selected-stream")
}
