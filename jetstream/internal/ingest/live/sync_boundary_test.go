package live

// hypercerts: regression coverage at the real live-event boundary for #sync
// events whose replacement set cannot be reconciled. The events are produced
// by a real verifier against a signed repository, then driven through
// processBatch in both attribution modes.

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/policygate"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/streaming"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

// verifiedSyncAtOtherRevision returns a verifier-produced #sync event that
// names syncRev while the repository the verifier fetched is at repoRev.
func verifiedSyncAtOtherRevision(t *testing.T, did atmos.DID, syncRev, repoRev string) (streaming.Event, *atmossync.Verifier) {
	t.Helper()
	carBytes, _, directory := signedSyncCAR(t, did, repoRev)
	wire := &comatproto.SyncSubscribeRepos_Sync{DID: string(did), Rev: syncRev, Seq: 42, Time: "2026-05-21T00:00:00Z", Blocks: carBytes}
	body, err := wire.MarshalCBOR()
	require.NoError(t, err)
	server := newSignedSyncServer(t, carBytes, body)
	t.Cleanup(server.Close)
	syncClient := atmossync.NewClient(atmossync.Options{Client: &xrpc.Client{Host: server.URL}})
	verifier, err := atmossync.NewVerifier(atmossync.VerifierOptions{Directory: directory, StateStore: atmossync.NewMemStateStore(), Policy: gt.Some(atmossync.PolicyResync), SyncClient: gt.Some(syncClient), ResyncLimit: gt.Some(rate.Inf), ResyncBurst: gt.Some(1)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = verifier.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	client, err := streaming.NewClient(streaming.Options{URL: "ws" + server.URL[len("http"):] + "/xrpc/com.atproto.sync.subscribeRepos", Verifier: gt.Some(verifier), SyncClient: gt.Some(syncClient)})
	require.NoError(t, err)
	defer client.Close()
	event := awaitVerifiedSync(t, ctx, client)
	require.Equal(t, streaming.ResyncSyncEvent, event.Resync)
	return event, verifier
}

type boundaryConsumer struct {
	c           *Consumer
	st          *store.Store
	drops       *ingest.DropMetrics
	recovered   *[]string
	reconciled  *int
	segmentsDir string
}

func openBoundaryConsumer(t *testing.T, managed bool, verifier *atmossync.Verifier) boundaryConsumer {
	t.Helper()
	st := newTestStore(t)
	drops := ingest.NewDropMetrics(prometheus.NewRegistry())
	recovered := []string{}
	reconciled := 0
	cfg := Config{
		SegmentsDir: filepath.Join(t.TempDir(), "segments"), Store: st, SeqKey: SteadySeqKey, CursorKey: CursorKey,
		RelayURL: "https://example.invalid", Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Verifier: verifier,
		DropMetrics: drops, ReadLogRetentionBytes: 1 << 20,
		DroppedEvents: func(did string, _ uint64) { recovered = append(recovered, did) },
	}
	if managed {
		domain := "sync-boundary-" + t.Name()
		history, err := attribution.Open(st, "selected-stream", domain)
		require.NoError(t, err)
		policy, err := selection.Open(st, []string{"app.bsky.feed.post"})
		require.NoError(t, err)
		activation := uint64(1)
		require.NoError(t, history.EnsureInitialPolicyHistory(attribution.Policy{Revision: 1, Collections: []string{"app.bsky.feed.post"}}, &activation))
		cfg.Attribution, cfg.AttributionDomain, cfg.AttributionStreamID = history, domain, "selected-stream"
		cfg.CollectionPolicy, cfg.PolicyGate = policy, policygate.New(policy, nil, domain)
		cfg.ReconcileSnapshot = func(context.Context, ingest.Snapshot) error { reconciled++; return nil }
	}
	c, err := Open(cfg)
	require.NoError(t, err)
	return boundaryConsumer{c: c, st: st, drops: drops, recovered: &recovered, reconciled: &reconciled, segmentsDir: cfg.SegmentsDir}
}

func TestProcessBatchDropsSyncFetchedAtAnotherRevision(t *testing.T) {
	t.Parallel()
	for _, managed := range []bool{true, false} {
		name := "unmanaged"
		if managed {
			name = "managed"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			did := atmos.DID("did:plc:syncboundary" + name)
			event, verifier := verifiedSyncAtOtherRevision(t, did, "3mmoojp7vgo2e", "3mmoojp7vgo2f")
			b := openBoundaryConsumer(t, managed, verifier)

			// A preceding ordinary event establishes an earlier frontier.
			prior, _ := buildCommit(t, "did:plc:syncboundaryprior", "3mmoojp7vgo2d", struct{ Coll, Rkey string }{"app.bsky.feed.post", "prior"})
			require.NoError(t, b.c.processBatch(t.Context(), []streaming.Event{{Seq: 41, Commit: prior.Commit}}))
			require.NoError(t, b.c.writer.Flush(t.Context()))
			before := b.c.writer.NextSeq()

			require.NoError(t, b.c.processBatch(t.Context(), []streaming.Event{event}), "an unreconcilable #sync is dropped, not fatal")
			require.NoError(t, b.c.writer.Flush(t.Context()))

			require.Equal(t, before, b.c.writer.NextSeq(), "no marker or replacement row may be published")
			require.Zero(t, *b.reconciled, "the reconciler must not see an unbound replacement set")
			require.InDelta(t, 1.0, testutil.ToFloat64(b.drops.Counter(ingest.DropSourceLive, ingest.DropReasonInvalidRev)), 0)
			require.Equal(t, []string{string(did)}, *b.recovered, "the DID is handed to repository recovery")
			require.Equal(t, int64(42), b.c.LastUpstreamSeq())
			require.Equal(t, int64(42), b.c.cursorValue(), "the completed-event frontier covers the dropped sync")

			// The trailing dropped sync is the durable checkpoint after reopen.
			require.NoError(t, b.c.Close())
			cursor, err := LoadUpstreamCursor(b.st, CursorKey)
			require.NoError(t, err)
			require.Equal(t, int64(42), cursor)
		})
	}
}

func TestProcessBatchEmptySyncRequiresBoundRevision(t *testing.T) {
	t.Parallel()
	const did, rev = "did:plc:emptysync", "3mmoojp7vgo2g"
	empty := func(seq int64) streaming.Event {
		return streaming.Event{Seq: seq, Resync: streaming.ResyncSyncEvent, Sync: &comatproto.SyncSubscribeRepos_Sync{DID: did, Rev: rev, Time: "2026-05-21T00:00:00Z"}}
	}
	markers := func(c *Consumer) (n int) {
		entries, _, _, _ := c.writer.ReadLog().ReadFrom(1, 20)
		for _, entry := range entries {
			if entry.Event().Kind == segment.KindSync {
				n++
			}
		}
		return n
	}

	t.Run("unbound empty set is not reconciled as an empty repository", func(t *testing.T) {
		t.Parallel()
		verifier := newTestVerifier(t)
		// The verifier fetched a later revision than the #sync named.
		require.NoError(t, verifier.StateStore().SaveChain(t.Context(), did, atmossync.ChainState{Rev: "3mmoojp7vgo2h"}))
		b := openBoundaryConsumer(t, true, verifier)
		t.Cleanup(func() { _ = b.c.Close() })
		require.NoError(t, b.c.processBatch(t.Context(), []streaming.Event{empty(50)}))
		require.NoError(t, b.c.writer.Flush(t.Context()))
		require.Zero(t, *b.reconciled)
		require.Zero(t, markers(b.c))
		require.InDelta(t, 1.0, testutil.ToFloat64(b.drops.Counter(ingest.DropSourceLive, ingest.DropReasonInvalidRev)), 0)
		require.Equal(t, int64(50), b.c.cursorValue())
	})

	t.Run("no verifier state is not treated as bound", func(t *testing.T) {
		t.Parallel()
		b := openBoundaryConsumer(t, true, newTestVerifier(t))
		t.Cleanup(func() { _ = b.c.Close() })
		require.NoError(t, b.c.processBatch(t.Context(), []streaming.Event{empty(50)}))
		require.Zero(t, *b.reconciled)
	})

	t.Run("bound empty set is a valid empty repository", func(t *testing.T) {
		t.Parallel()
		verifier := newTestVerifier(t)
		require.NoError(t, verifier.StateStore().SaveChain(t.Context(), did, atmossync.ChainState{Rev: rev}))
		b := openBoundaryConsumer(t, true, verifier)
		t.Cleanup(func() { _ = b.c.Close() })
		require.NoError(t, b.c.processBatch(t.Context(), []streaming.Event{empty(50)}))
		require.NoError(t, b.c.writer.Flush(t.Context()))
		require.Equal(t, 1, *b.reconciled)
		require.Equal(t, 1, markers(b.c))
		require.Empty(t, *b.recovered)
		require.Equal(t, int64(50), b.c.cursorValue())
	})
}

func TestSyncInputRejectionKeepsLocalFailuresFatal(t *testing.T) {
	t.Parallel()
	for _, err := range []error{nil, context.Canceled, attribution.ErrInvalid, ingest.ErrAccountUnavailable, segment.ErrFieldTooLong, io.ErrUnexpectedEOF} {
		_, rejected := syncInputRejection(err)
		require.False(t, rejected, "%v", err)
	}
	_, rejected := syncInputRejection(ingest.ErrSyncRecordMismatch)
	require.True(t, rejected)
}
