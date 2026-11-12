package live

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/policygate"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble"
	"github.com/coder/websocket"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/mst"
	atmosrepo "github.com/jcalabro/atmos/repo"
	"github.com/jcalabro/atmos/streaming"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// adoptionBarriers pauses the consumer inside its admission lease, then pauses
// the real writer's fsync when adoption drains the admitted row.
type adoptionBarriers struct {
	admitted, releaseAdmission, syncing, releaseSync chan struct{}
	admissionOnce, syncOnce                          sync.Once
	armed                                            atomic.Bool
}

func newAdoptionBarriers(t *testing.T) *adoptionBarriers {
	t.Helper()
	b := &adoptionBarriers{admitted: make(chan struct{}), releaseAdmission: make(chan struct{}), syncing: make(chan struct{}), releaseSync: make(chan struct{})}
	t.Cleanup(func() {
		b.admissionOnce.Do(func() { close(b.releaseAdmission) })
		b.syncOnce.Do(func() { close(b.releaseSync) })
	})
	return b
}

func (b *adoptionBarriers) Stamp(ctx context.Context, _ *segment.Event) error {
	if !b.armed.CompareAndSwap(true, false) {
		return nil
	}
	close(b.admitted)
	select {
	case <-b.releaseAdmission:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *adoptionBarriers) BeforeSegmentIO(_ string, op segment.IOOp) error {
	if op == segment.IOOpSync {
		select {
		case <-b.admitted:
			select {
			case <-b.syncing:
			default:
				close(b.syncing)
			}
			<-b.releaseSync
		default:
		}
	}
	return nil
}

func TestConsumerPolicyAdoptionWaitsForAdmissionAndDurability(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	events, verifier := verifiedAdoptionCommits(t, ctx)
	st := newTestStore(t)
	policy, err := selection.Open(st, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	history, err := attribution.Open(st, "consumer-adoption", "live")
	require.NoError(t, err)
	require.NoError(t, history.EnsureInitialPolicyHistory(attribution.Policy{Revision: 1, Collections: policy.Current().Collections}, nil))
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	barriers := newAdoptionBarriers(t)
	consumer, err := Open(Config{SegmentsDir: filepath.Join(t.TempDir(), "segments"), Store: st, SeqKey: SteadySeqKey, CursorKey: CursorKey, RelayURL: "https://example.invalid", Logger: logger, Verifier: verifier, Attribution: history, AttributionDomain: "live", AttributionStreamID: "consumer-adoption", CollectionPolicy: policy, ReadLogRetentionBytes: 1 << 20, TimestampStamper: barriers, SegmentIOFaultInjector: barriers, InitializePolicyGate: func(w *ingest.Writer) (*policygate.Gate, error) { return policygate.New(policy, w, "live"), nil }})
	require.NoError(t, err)
	// Register close before release cleanup so a failed assertion cannot strand Close.
	defer func() {
		barriers.admissionOnce.Do(func() { close(barriers.releaseAdmission) })
		barriers.syncOnce.Do(func() { close(barriers.releaseSync) })
		_ = consumer.Close()
	}()
	selected, ordinary := adoptionSubscribers(t, ctx, consumer, history, logger)
	require.Contains(t, readAdoptionFrame(t, ctx, selected), "SelectedFeed")
	require.Contains(t, readAdoptionFrame(t, ctx, selected), "PolicyAdopted")
	barriers.armed.Store(true)
	firstDone := make(chan error, 1)
	go func() { firstDone <- consumer.processBatch(ctx, events[:1]) }()
	awaitAdoptionSignal(t, ctx, barriers.admitted)
	adoptDone := make(chan error, 1)
	adoptStarted := make(chan struct{})
	go func() {
		close(adoptStarted)
		adoptDone <- consumer.PolicyGate().Adopt(ctx, "live", func(boundary *uint64) error {
			return history.WithPolicyCommit(attribution.Policy{Revision: 2, Collections: []string{"app.bsky.feed.like"}, EffectiveAfterSeq: boundary}, func(stage func(*pebble.Batch) error) error {
				_, updateErr := policy.Update(1, []string{"app.bsky.feed.like"}, func(_ selection.Policy, batch *pebble.Batch) error { return stage(batch) })
				return updateErr
			})
		})
	}()
	awaitAdoptionSignal(t, ctx, adoptStarted)
	assertAdoptionBlocked(t, adoptDone, "active consumer lease")
	require.EqualValues(t, 1, policy.Current().Revision)
	barriers.admissionOnce.Do(func() { close(barriers.releaseAdmission) })
	require.NoError(t, awaitAdoptionResult(t, ctx, firstDone))
	awaitAdoptionSignal(t, ctx, barriers.syncing)
	assertAdoptionBlocked(t, adoptDone, "uncommitted segment fsync")
	require.EqualValues(t, 1, policy.Current().Revision)
	barriers.syncOnce.Do(func() { close(barriers.releaseSync) })
	require.NoError(t, awaitAdoptionResult(t, ctx, adoptDone))
	adopted, err := history.LookupPolicy(2)
	require.NoError(t, err)
	require.NotNil(t, adopted.EffectiveAfterSeq)
	require.EqualValues(t, 1, *adopted.EffectiveAfterSeq)
	require.NoError(t, consumer.processBatch(ctx, events[1:]))
	require.NoError(t, consumer.writer.Flush(ctx))
	require.EqualValues(t, 3, consumer.LastUpstreamSeq())
	assertAdoptionRows(t, consumer, history)
	assertAdoptionDelivery(t, ctx, selected, ordinary)
}

func assertAdoptionBlocked(t *testing.T, result <-chan error, boundary string) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("adoption crossed %s: %v", boundary, err)
	case <-time.After(30 * time.Millisecond):
	}
}

func awaitAdoptionSignal(t *testing.T, ctx context.Context, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func awaitAdoptionResult(t *testing.T, ctx context.Context, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		return ctx.Err()
	}
}

func assertAdoptionRows(t *testing.T, consumer *Consumer, history *attribution.Manager) {
	t.Helper()
	entries, _, ok, _ := consumer.writer.ReadLog().ReadCommittedFrom(1, 10)
	require.True(t, ok)
	require.Len(t, entries, 2, "removed-policy record must never enter storage")
	for i, rkey := range []string{"admitted", "new-policy"} {
		row := entries[i].Event()
		require.Equal(t, rkey, row.Rkey)
		metadata, err := history.Lookup(row.Seq)
		require.NoError(t, err)
		require.EqualValues(t, i+1, *metadata.PolicyRev)
		require.Equal(t, attribution.ProvenanceLive, metadata.Provenance)
	}
}

func assertAdoptionDelivery(t *testing.T, ctx context.Context, selected, ordinary *websocket.Conn) {
	t.Helper()
	first := readAdoptionFrame(t, ctx, selected)
	require.Contains(t, first, `"rkey":"admitted"`)
	require.Contains(t, first, `"policyRev":1`)
	adoption := readAdoptionFrame(t, ctx, selected)
	require.Contains(t, adoption, "PolicyAdopted")
	require.Contains(t, adoption, `"policyRevision":2`)
	next := readAdoptionFrame(t, ctx, selected)
	require.Contains(t, next, `"rkey":"new-policy"`)
	require.Contains(t, next, `"policyRev":2`)
	for _, rkey := range []string{"admitted", "new-policy"} {
		frame := readAdoptionFrame(t, ctx, ordinary)
		require.Contains(t, frame, `"rkey":"`+rkey+`"`)
		require.NotContains(t, frame, "hypercerts")
		require.NotContains(t, frame, "PolicyAdopted")
	}
}

func readAdoptionFrame(t *testing.T, ctx context.Context, conn *websocket.Conn) string {
	t.Helper()
	_, frame, err := conn.Read(ctx)
	require.NoError(t, err)
	return string(frame)
}

func adoptionSubscribers(t *testing.T, ctx context.Context, consumer *Consumer, history *attribution.Manager, logger *slog.Logger) (*websocket.Conn, *websocket.Conn) {
	t.Helper()
	require.NoError(t, lifecycle.WritePhase(consumer.cfg.Store, lifecycle.PhaseSteadyState, time.Now().UTC()))
	tail, err := subscribe.New(subscribe.Config{Logger: logger}, nil, consumer.writer.NextSeq)
	require.NoError(t, err)
	tail.SetReadLogSource(consumer.writer.ReadLog)
	provider := subscribe.SelectedConfigProviderFunc(func() (subscribe.SelectedConfig, error) {
		policies, readErr := history.ReadPolicyHistory()
		if readErr != nil {
			return subscribe.SelectedConfig{}, readErr
		}
		current := policies[len(policies)-1]
		return subscribe.SelectedConfig{StreamID: "consumer-adoption", PolicyRevision: current.Revision, Collections: current.Collections, PolicyEffectiveAfterSeq: current.EffectiveAfterSeq, CurrentSeq: consumer.writer.NextSeq() - 1}, nil
	})
	server := httptest.NewServer(subscribe.NewHandler(subscribe.Subscription{Tail: tail, Store: consumer.cfg.Store, Writer: consumer.writer, Logger: logger, V2: true, SelectedConfig: provider, SelectedAttribution: history, SelectedPolicy: consumer.cfg.CollectionPolicy, SelectedStreamID: "consumer-adoption"}))
	t.Cleanup(server.Close)
	dial := func(query string) *websocket.Conn {
		conn, response, dialErr := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/"+query, nil)
		require.NoError(t, dialErr)
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		t.Cleanup(func() { _ = conn.CloseNow() })
		return conn
	}
	return dial("?contract=" + subscribe.SelectedContract), dial("")
}

// verifiedAdoptionCommits feeds independently signed, chained CARs through the
// upstream websocket verifier before processBatch consumes its public events.
func verifiedAdoptionCommits(t *testing.T, ctx context.Context) ([]streaming.Event, *atmossync.Verifier) {
	t.Helper()
	did := atmos.DID("did:plc:consumeradoption")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	blocks := mst.NewMemBlockStore()
	repo := &atmosrepo.Repo{DID: did, Clock: atmos.NewTIDClock(0), Store: blocks, Tree: mst.NewTree(blocks)}
	root, err := repo.Tree.WriteBlocks(repo.Store)
	require.NoError(t, err)
	var frames [][]byte
	for i, op := range []struct{ Coll, Rkey string }{{"app.bsky.feed.post", "admitted"}, {"app.bsky.feed.post", "removed-policy"}, {"app.bsky.feed.like", "new-policy"}} {
		commit, nextRoot := buildSyntheticChainedCommit(t, repo, key, root, op)
		root = nextRoot
		commit.Seq = int64(i + 1)
		body, marshalErr := commit.MarshalCBOR()
		require.NoError(t, marshalErr)
		frames = append(frames, encodeFrame(t, "#commit", body))
	}
	firehose := &fakeFirehose{t: t, frames: frames}
	server := httptest.NewServer(firehose.handler())
	t.Cleanup(server.Close)
	verifier, err := atmossync.NewVerifier(atmossync.VerifierOptions{Directory: &identity.Directory{Resolver: &stubResolver{docs: map[atmos.DID]*identity.DIDDocument{did: buildDIDDoc(did, key.PublicKey())}}}, StateStore: atmossync.NewMemStateStore(), Policy: gt.Some(atmossync.PolicyError)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = verifier.Close() })
	client, err := streaming.NewClient(streaming.Options{URL: "ws" + strings.TrimPrefix(server.URL, "http") + "/xrpc/com.atproto.sync.subscribeRepos", Verifier: gt.Some(verifier)})
	require.NoError(t, err)
	defer client.Close()
	var events []streaming.Event
	for batch, batchErr := range client.Events(ctx) {
		require.NoError(t, batchErr)
		events = append(events, batch...)
		if len(events) == 3 {
			break
		}
	}
	require.Len(t, events, 3)
	require.EqualValues(t, 3, verifier.Stats().EventsVerified)
	return events, verifier
}
