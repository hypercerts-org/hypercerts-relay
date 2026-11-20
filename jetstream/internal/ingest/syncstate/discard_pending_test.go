package syncstate

import (
	"testing"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/require"
)

// hypercerts: chain state staged for a refused event must not survive to be
// promoted by a later event for the same DID.
func TestDiscardPendingChainFallsBackToArchivedState(t *testing.T) {
	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	p := New(st)
	did := atmos.DID("did:plc:discardpending")
	data := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root"))

	require.NoError(t, p.SaveChain(t.Context(), did, atmossync.ChainState{Rev: "3mmoojp7vgo2e", Data: data}))
	p.PromoteChain(did, "3mmoojp7vgo2e")
	require.NoError(t, p.Flush())

	require.NoError(t, p.SaveChain(t.Context(), did, atmossync.ChainState{Rev: "3mmoojp7vgo2f", Data: data}))
	p.DiscardPendingChain(did, "3mmoojp7vgo2e")
	later, err := p.LoadChain(t.Context(), did)
	require.NoError(t, err)
	require.Equal(t, "3mmoojp7vgo2f", later.Rev, "state staged above the rejected revision belongs to a later event")
	p.DiscardPendingChain(did, DiscardAllRevisions)
	p.PromoteChain(did, "3mmoojp7vgo2z")
	require.NoError(t, p.Flush())

	state, err := p.LoadChain(t.Context(), did)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, "3mmoojp7vgo2e", state.Rev)
}
