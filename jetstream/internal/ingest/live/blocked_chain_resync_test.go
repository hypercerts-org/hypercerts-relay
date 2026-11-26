package live

// hypercerts: a DID whose chain promotion is blocked and that has no chain
// state must take the verifier's chain-break path on its next commit, not the
// first-sighting path. Driven through a real verifier over the Pebble state
// store, with the verifier options the runtime uses.

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest/syncstate"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/mst"
	atmosrepo "github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

// blockedChainRig is one signed repository served over getRepo, verified
// against a Pebble-backed state store.
type blockedChainRig struct {
	did      atmos.DID
	state    *syncstate.PebbleStateStore
	reopen   func() *syncstate.PebbleStateStore
	verifier *atmossync.Verifier
	repo     *atmosrepo.Repo
	key      crypto.PrivateKey
	root     cbor.CID
	served   atomic.Pointer[[]byte]
	fetches  atomic.Int64
}

func newBlockedChainRig(t *testing.T) *blockedChainRig {
	t.Helper()
	r := &blockedChainRig{did: atmos.DID("did:plc:blockedchainresync")}
	st := newTestStore(t)
	r.state = syncstate.New(st)
	r.reopen = func() *syncstate.PebbleStateStore { return syncstate.New(st) }
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	r.key = key
	blocks := mst.NewMemBlockStore()
	r.repo = &atmosrepo.Repo{DID: r.did, Clock: atmos.NewTIDClock(0), Store: blocks, Tree: mst.NewTree(blocks)}
	r.root, err = r.repo.Tree.WriteBlocks(r.repo.Store)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/xrpc/com.atproto.sync.getRepo" {
			http.NotFound(w, req)
			return
		}
		r.fetches.Add(1)
		w.Header().Set("Content-Type", "application/vnd.ipld.car")
		_, _ = w.Write(*r.served.Load())
	}))
	t.Cleanup(server.Close)
	directory := &identity.Directory{Resolver: &stubResolver{docs: map[atmos.DID]*identity.DIDDocument{r.did: buildDIDDoc(r.did, key.PublicKey())}}}
	r.verifier, err = atmossync.NewVerifier(atmossync.VerifierOptions{
		Directory:   directory,
		StateStore:  r.state,
		SyncClient:  gt.Some(atmossync.NewClient(atmossync.Options{Client: &xrpc.Client{Host: server.URL}})),
		ResyncLimit: gt.Some(rate.Inf),
		ResyncBurst: gt.Some(1),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.verifier.Close() })
	return r
}

// commit appends one record, signs a commit chained to the previous root, and
// serves the resulting repository from getRepo.
func (r *blockedChainRig) commit(t *testing.T, rkey string) *comatproto.SyncSubscribeRepos_Commit {
	t.Helper()
	c, root := buildSyntheticChainedCommit(t, r.repo, r.key, r.root, struct{ Coll, Rkey string }{"app.bsky.feed.post", rkey})
	r.root = root
	served := c.Blocks
	r.served.Store(&served)
	return c
}

func (r *blockedChainRig) awaitResync(t *testing.T) atmossync.ResyncEvent {
	t.Helper()
	select {
	case ev := <-r.verifier.ResyncEvents():
		return ev
	case err := <-r.verifier.AsyncErrors():
		t.Fatalf("resync failed: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("no resync was delivered")
	}
	return atmossync.ResyncEvent{}
}

func (r *blockedChainRig) durable(t *testing.T) *atmossync.ChainState {
	t.Helper()
	state, err := r.reopen().LoadChain(t.Context(), r.did)
	require.NoError(t, err)
	return state
}

func TestBlockedChainWithoutStateResyncsOnNextCommit(t *testing.T) {
	t.Parallel()
	r := newBlockedChainRig(t)
	r.state.BlockChainPromotion(r.did)

	first := r.commit(t, "one")
	ops, err := r.verifier.VerifyCommit(t.Context(), first)
	require.NoError(t, err)
	require.Nil(t, ops, "the commit must not be yielded as a first sighting")
	ev := r.awaitResync(t)
	require.Equal(t, atmossync.ReasonChainBreak, ev.Reason)
	require.Equal(t, first.Rev, ev.NewRev)
	require.Len(t, ev.Ops, 1)
	require.Equal(t, atmos.ActionResync, ev.Ops[0].Action)
	require.EqualValues(t, 1, r.fetches.Load())

	// The fetched state is staged; nothing is durable while blocked.
	staged, err := r.state.LoadChain(t.Context(), r.did)
	require.NoError(t, err)
	require.Equal(t, first.Rev, staged.Rev)
	require.True(t, staged.Data.Equal(r.root))
	require.NoError(t, r.state.Flush())
	require.Nil(t, r.durable(t))

	// The replacement is refused again: the next commit resyncs again.
	r.state.BlockChainPromotion(r.did)
	second := r.commit(t, "two")
	ops, err = r.verifier.VerifyCommit(t.Context(), second)
	require.NoError(t, err)
	require.Nil(t, ops)
	ev = r.awaitResync(t)
	require.Equal(t, second.Rev, ev.NewRev)
	require.Len(t, ev.Ops, 2)
	require.EqualValues(t, 2, r.fetches.Load())

	// Reconciled: the block lifts, the fetched state becomes durable, and the
	// next chained commit verifies without another fetch.
	r.state.UnblockChainPromotion(r.did)
	r.state.PromoteChain(r.did, second.Rev)
	require.NoError(t, r.state.Flush())
	durable := r.durable(t)
	require.NotNil(t, durable)
	require.Equal(t, second.Rev, durable.Rev)
	require.True(t, durable.Data.Equal(r.root))

	ops, err = r.verifier.VerifyCommit(t.Context(), r.commit(t, "three"))
	require.NoError(t, err)
	require.Len(t, ops, 1)
	require.Equal(t, atmos.ActionCreate, ops[0].Action)
	require.EqualValues(t, 2, r.fetches.Load())
	require.EqualValues(t, 2, r.verifier.Stats().ChainBreaks)
}

// Control: without a block, a DID with no chain state is a first sighting.
func TestUnblockedChainWithoutStateIsFirstSighting(t *testing.T) {
	t.Parallel()
	r := newBlockedChainRig(t)
	ops, err := r.verifier.VerifyCommit(t.Context(), r.commit(t, "one"))
	require.NoError(t, err)
	require.Len(t, ops, 1)
	require.Zero(t, r.fetches.Load())
	require.Zero(t, r.verifier.Stats().ChainBreaks)
}
