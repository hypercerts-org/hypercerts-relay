package backfill

import (
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func TestHypercertsBootstrapAndRetrySelectBeforeStorage(t *testing.T) {
	for _, resync := range []bool{false, true} {
		t.Run(map[bool]string{false: "bootstrap", true: "retry"}[resync], func(t *testing.T) {
			dir := t.TempDir()
			db, err := store.Open(dir, nil)
			require.NoError(t, err)
			defer db.Close()
			policy, err := selection.Open(db, []string{"app.bsky.feed.post"})
			require.NoError(t, err)
			w, err := ingest.Open(ingest.Config{SegmentsDir: filepath.Join(dir, "segments"), Store: db, CollectionPolicy: policy, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
			require.NoError(t, err)
			defer w.Close()
			r, commit := buildSingleRecordRepo(t, "did:plc:test", "app.bsky.feed.post", "selected", map[string]any{"text": "selected"})
			require.NoError(t, r.Create("app.bsky.feed.like", "excluded", map[string]any{"text": "excluded-secret"}))
			h := NewSegmentHandler(w, nil, nil)
			if resync {
				err = h.HandleRepoResync(t.Context(), r.DID, r, commit)
			} else {
				err = h.HandleRepo(t.Context(), r.DID, r, commit)
			}
			require.NoError(t, err)
			require.NoError(t, w.Flush(t.Context()))
			rows := collectActiveEvents(t, filepath.Join(dir, "segments", "seg_0000000000.jss"))
			count := 0
			for _, row := range rows {
				if row.Kind.IsCommit() {
					require.Equal(t, "app.bsky.feed.post", row.Collection)
					count++
				}
				require.NotContains(t, string(row.Payload), "excluded-secret")
			}
			require.Equal(t, 1, count)
			if resync {
				require.Equal(t, segment.KindSync, rows[0].Kind)
			}
		})
	}
}
