package backfill

import (
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/history"
	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/policygate"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/jcalabro/atmos"
	"github.com/stretchr/testify/require"
)

func TestHandleRepoHistoryUsesSnapshotReconciliation(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "records", true: "empty"}[empty], func(t *testing.T) {
			dir := t.TempDir()
			db, err := store.Open(filepath.Join(dir, "db"), nil)
			require.NoError(t, err)
			defer db.Close()
			attr, err := attribution.Open(db, "backfill", "backfill")
			require.NoError(t, err)
			journal, err := history.Open(db)
			require.NoError(t, err)
			policy, err := selection.Open(db, []string{"app.bsky.feed.post"})
			require.NoError(t, err)
			require.NoError(t, attr.EnsureInitialPolicyHistory(attribution.Policy{Revision: 1, Collections: []string{"app.bsky.feed.post"}}, nil))
			w, err := ingest.Open(ingest.Config{SegmentsDir: filepath.Join(dir, "segments"), Store: db, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Attribution: attr, AttributionDomain: "backfill", History: journal, CollectionPolicy: policy})
			require.NoError(t, err)
			defer w.Close()
			h := NewSegmentHandler(w, nil, nil)
			h.attribution, h.policyGate, h.streamID = attr, policygate.New(policy, w, "backfill"), "backfill"
			did := atmos.DID("did:plc:bootstrap-history")
			r, commit := buildMultiRecordRepo(t, did, "app.bsky.feed.post", 1)
			if empty {
				r, commit = buildMultiRecordRepo(t, did, "app.bsky.feed.like", 1)
			}
			require.NoError(t, h.HandleRepo(t.Context(), did, r, commit))
			page, err := journal.Read(0, 0, 10)
			require.NoError(t, err)
			rows := page.Events
			if empty {
				require.Len(t, rows, 1)
				require.Equal(t, "snapshot-boundary", rows[0].Source.Path)
			} else {
				require.Len(t, rows, 2)
				require.Equal(t, attribution.ProvenanceSnapshot, attribution.Provenance(rows[0].Provenance))
				require.Equal(t, "app.bsky.feed.post", rows[0].Collection)
				require.Equal(t, "did:plc:bootstrap-history", rows[0].DID)
				require.Equal(t, "backfill", rows[0].Attribution.StreamID)
				require.Equal(t, uint64(1), rows[0].Policy.Revision)
				require.NotEmpty(t, rows[0].Attribution.Boundary.SnapshotID)
			}
		})
	}
}
