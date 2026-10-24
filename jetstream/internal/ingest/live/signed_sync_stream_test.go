package live

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/policygate"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/coder/websocket"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/mst"
	atmosrepo "github.com/jcalabro/atmos/repo"
	"github.com/jcalabro/atmos/streaming"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

// TestConsumerStreamsVerifiedSyncWithReplacementOps uses the actual upstream
// websocket client and verifier. The Sync envelope and signed CAR are built
// independently; the worker-produced verified operations are then observed via
// Consumer.Run and processBatch, not injected through private verifier state.
func TestConsumerStreamsVerifiedSyncWithReplacementOps(t *testing.T) {
	t.Parallel()
	did := atmos.DID("did:plc:selectedverifiedsync")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	repo := &atmosrepo.Repo{DID: did, Clock: atmos.NewTIDClock(0), Store: mst.NewMemBlockStore()}
	repo.Tree = mst.NewTree(repo.Store)
	require.NoError(t, repo.Create("app.bsky.feed.post", "fresh", map[string]any{"text": "fresh"}))
	postCID, _, err := repo.Get("app.bsky.feed.post", "fresh")
	require.NoError(t, err)
	dataCID, err := repo.Tree.WriteBlocks(repo.Store)
	require.NoError(t, err)
	commit := &atmosrepo.Commit{DID: string(did), Version: atmossync.CommitVersion, Data: dataCID, Rev: "3mmoojp7vgo2f"}
	require.NoError(t, commit.Sign(key))
	commitBytes, err := commit.EncodeCBOR()
	require.NoError(t, err)
	commitCID := cbor.ComputeCID(cbor.CodecDagCBOR, commitBytes)
	require.NoError(t, repo.Store.PutBlock(commitCID, commitBytes))
	var carBuf bytes.Buffer
	cw, err := car.NewWriter(&carBuf, []cbor.CID{commitCID})
	require.NoError(t, err)
	for cid, data := range repo.Store.(*mst.MemBlockStore).All() {
		require.NoError(t, cw.WriteBlock(cid, data))
	}
	wireSync := &comatproto.SyncSubscribeRepos_Sync{DID: string(did), Rev: commit.Rev, Seq: 42, Time: "2026-05-21T00:00:00Z", Blocks: carBuf.Bytes()}
	body, err := wireSync.MarshalCBOR()
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/xrpc/com.atproto.sync.getRepo" {
			w.Header().Set("Content-Type", "application/vnd.ipld.car")
			_, _ = w.Write(carBuf.Bytes())
			return
		}
		if r.URL.Path != "/xrpc/com.atproto.sync.subscribeRepos" {
			http.NotFound(w, r)
			return
		}
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer conn.CloseNow()
		_ = conn.Write(r.Context(), websocket.MessageBinary, encodeFrame(t, "#sync", body))
		<-r.Context().Done()
	}))
	defer server.Close()
	resolver := &stubResolver{docs: map[atmos.DID]*identity.DIDDocument{did: buildDIDDoc(did, key.PublicKey())}}
	directory := &identity.Directory{Resolver: resolver}
	syncClient := atmossync.NewClient(atmossync.Options{Client: &xrpc.Client{Host: server.URL}})
	verifier, err := atmossync.NewVerifier(atmossync.VerifierOptions{Directory: directory, StateStore: atmossync.NewMemStateStore(), Policy: gt.Some(atmossync.PolicyResync), SyncClient: gt.Some(syncClient), ResyncLimit: gt.Some(rate.Inf), ResyncBurst: gt.Some(1)})
	require.NoError(t, err)
	defer verifier.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	wsURL := "ws" + server.URL[len("http"):] + "/xrpc/com.atproto.sync.subscribeRepos"
	client, err := streaming.NewClient(streaming.Options{URL: wsURL, Verifier: gt.Some(verifier), SyncClient: gt.Some(syncClient)})
	require.NoError(t, err)
	defer client.Close()
	var event streaming.Event
	got := false
	for batch, iterErr := range client.Events(ctx) {
		require.NoError(t, iterErr)
		for _, candidate := range batch {
			if candidate.Sync != nil {
				event, got = candidate, true
				break
			}
		}
		if got {
			break
		}
	}
	require.True(t, got)
	require.Nil(t, event.Commit)
	require.NotNil(t, event.Sync)
	require.EqualValues(t, 42, event.Seq)
	var operations []streaming.Operation
	for op, opErr := range event.Operations() {
		require.NoError(t, opErr)
		operations = append(operations, op)
	}
	require.Len(t, operations, 1)
	require.Equal(t, "app.bsky.feed.post", string(operations[0].Collection))
	require.Equal(t, "fresh", string(operations[0].RKey))
	require.Equal(t, postCID, operations[0].CID)
	st := newTestStore(t)
	streamID := "signed-sync-managed"
	domain := "signed-sync-managed"
	history, err := attribution.Open(st, streamID, domain)
	require.NoError(t, err)
	policy, err := selection.Open(st, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	initial := attribution.Policy{Revision: 1, Collections: []string{"app.bsky.feed.post"}}
	require.NoError(t, history.EnsureInitialPolicyHistory(initial, nil))
	gate := policygate.New(policy, nil, domain)
	var consumer *Consumer
	consumer, err = Open(Config{SegmentsDir: filepath.Join(t.TempDir(), "segments"), Store: st, SeqKey: SteadySeqKey, CursorKey: CursorKey, RelayURL: server.URL, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Verifier: verifier, Attribution: history, AttributionDomain: domain, AttributionStreamID: streamID, CollectionPolicy: policy, PolicyGate: gate, ReadLogRetentionBytes: 1 << 20, ReconcileSnapshot: func(ctx context.Context, snapshot ingest.Snapshot) error {
		return consumer.writer.ReconcileSnapshot(ctx, snapshot)
	}})
	require.NoError(t, err)
	defer consumer.Close()
	_, convertedErr := ConvertEvent(event, time.Now().UnixMicro())
	require.NoError(t, convertedErr)
	// Seed stale selected state with durable full-history attribution, then
	// feed the exact verified upstream Sync through the real reconciliation path.
	stale := segment.Event{Kind: segment.KindCreate, DID: string(did), Rev: "3mmoojp7vgo2e", Collection: "app.bsky.feed.post", Rkey: "stale", Payload: []byte(`{"text":"stale"}`)}
	require.NoError(t, consumer.writer.AppendAttributed(ctx, &stale, ingest.AttributionMetadata{StreamID: streamID, Policy: initial, Provenance: attribution.ProvenanceLive}))
	require.NoError(t, consumer.writer.Flush(ctx))
	localBoundarySeq := consumer.writer.NextSeq() - 1
	require.NoError(t, consumer.processBatch(ctx, []streaming.Event{event}))
	require.NoError(t, consumer.writer.Flush(ctx))
	require.Equal(t, int64(42), consumer.LastUpstreamSeq())
	entries, _, ok, _ := consumer.writer.ReadLog().ReadFrom(1, 20)
	require.True(t, ok)
	require.GreaterOrEqual(t, len(entries), 3)
	var markerIndex, deleteIndex, materializationIndex int
	for i, entry := range entries {
		ev := entry.Event()
		switch {
		case ev.Kind == segment.KindDelete && ev.Rkey == "stale":
			deleteIndex = i + 1
		case ev.Kind.IsMaterialization() && ev.Rkey == "fresh":
			materializationIndex = i + 1
			require.NotEmpty(t, ev.Payload)
		case ev.Kind == segment.KindSync:
			markerIndex = i + 1
		}
	}
	require.NotZero(t, deleteIndex)
	require.NotZero(t, materializationIndex)
	require.NotZero(t, markerIndex)
	require.Less(t, deleteIndex, markerIndex)
	require.Less(t, materializationIndex, markerIndex)
	boundarySeq := localBoundarySeq
	for _, seq := range []int{deleteIndex, materializationIndex} {
		entry := entries[seq-1]
		metadata, readErr := history.Lookup(uint64(seq))
		require.NoError(t, readErr)
		require.Equal(t, entry.Event().Seq, metadata.Seq)
		require.Equal(t, attribution.ProvenanceReconciliation, metadata.Provenance)
		require.NotNil(t, metadata.Boundary.Seq)
		require.Equal(t, uint64(boundarySeq+1), *metadata.Boundary.Seq)
	}
	// Re-deliver the same verified repo state at a distinct upstream cursor;
	// only the cursor and one sync marker may advance, never a fake replacement.
	beforeNoop := consumer.writer.NextSeq()
	event.Seq = 43
	require.NoError(t, consumer.processBatch(ctx, []streaming.Event{event}))
	require.NoError(t, consumer.writer.Flush(ctx))
	require.Equal(t, beforeNoop+1, consumer.writer.NextSeq())
	require.Equal(t, int64(43), consumer.LastUpstreamSeq())
	entries, _, ok, _ = consumer.writer.ReadLog().ReadFrom(1, 20)
	require.True(t, ok)
	markers := 0
	for _, entry := range entries {
		if entry.Event().Kind == segment.KindSync {
			markers++
		}
	}
	require.Equal(t, 2, markers)
	require.GreaterOrEqual(t, verifier.Stats().Resyncs, uint64(1))
}
