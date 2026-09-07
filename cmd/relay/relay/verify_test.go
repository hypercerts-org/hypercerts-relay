package relay

import (
	"bytes"
	"testing"

	"github.com/bluesky-social/indigo/atproto/repo"
	"github.com/stretchr/testify/require"
)

func TestVerifyCommitObjectRejectsMissingIdentity(t *testing.T) {
	_, event := loadIngestFixture(t)
	commit, _, err := repo.LoadCommitFromCAR(t.Context(), bytes.NewReader(event.Blocks))
	require.NoError(t, err)

	relay := &Relay{}
	require.ErrorIs(t, relay.VerifyCommitObject(t.Context(), commit, nil, "source.example"), ErrIdentityUnavailable)
}
