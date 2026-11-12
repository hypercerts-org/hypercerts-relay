package backfill

import (
	"context"
	"log/slog"
	"net/http"
	"testing"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/policygate"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

func TestManagedHandlerRequiresCompleteConfiguration(t *testing.T) {
	writer := newTestIngest(t)
	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	defer db.Close()
	manager, err := attribution.Open(db, "stream", "domain")
	require.NoError(t, err)
	policy, err := selection.Open(db, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	for _, handler := range []*SegmentHandler{{writer: writer, attribution: manager, policyGate: policygate.New(policy, writer, "domain")}, {writer: writer, attribution: manager, streamID: "stream"}} {
		require.ErrorIs(t, handler.HandleRepo(context.Background(), "did:plc:test", nil, nil), ErrManagedPolicyConfig)
		require.ErrorIs(t, handler.HandleRepoResync(context.Background(), "did:plc:test", nil, nil), ErrManagedPolicyConfig)
	}
	_, err = newRetryRunner(RetryConfig{Store: db, Writer: writer, HTTPClient: http.DefaultClient, RelayURL: "https://relay.example", Logger: slog.Default(), Attribution: manager, PolicyGate: policygate.New(policy, writer, "domain")})
	require.ErrorIs(t, err, ErrManagedPolicyConfig)
	_, err = newRetryRunner(RetryConfig{Store: db, Writer: writer, HTTPClient: http.DefaultClient, RelayURL: "https://relay.example", Logger: slog.Default(), Attribution: manager, AttributionStreamID: "stream"})
	require.ErrorIs(t, err, ErrManagedPolicyConfig)
}
