package relay

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/bluesky-social/indigo/atproto/identity"
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

func TestVerifyCommitObjectReportsInvalidIdentityKey(t *testing.T) {
	_, event := loadIngestFixture(t)
	commit, _, err := repo.LoadCommitFromCAR(t.Context(), bytes.NewReader(event.Blocks))
	require.NoError(t, err)

	relay := &Relay{}
	err = relay.VerifyCommitObject(t.Context(), commit, &identity.Identity{Keys: map[string]identity.VerificationMethod{
		"atproto": {Type: "Multikey", PublicKeyMultibase: "znot-a-valid-key"},
	}}, "source.example")
	require.ErrorIs(t, err, ErrIdentityUnavailable)
	require.True(t, strings.Contains(err.Error(), "parse atproto public key"), err)
}

func TestVerifyCommitDIDMismatchDoesNotRefreshIdentity(t *testing.T) {
	ident, event := loadIngestFixture(t)
	commit, _, err := repo.LoadCommitFromCAR(t.Context(), bytes.NewReader(event.Blocks))
	require.NoError(t, err)
	commit.DID = "did:plc:abcdefghijklmnopqrstuvwx"
	require.NotEqual(t, ident.DID.String(), commit.DID)

	for _, missingIdentity := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing_identity=%t", missingIdentity), func(t *testing.T) {
			directory := &rotatingDirectory{stale: ident, fresh: ident}
			r := &Relay{Dir: directory}
			var signingIdentity *identity.Identity
			if !missingIdentity {
				signingIdentity = &ident
			}
			err := r.verifyCommitObjectWithRefresh(t.Context(), commit, signingIdentity, ident.DID, "source.example")
			var rejection *permanentEventError
			require.ErrorAs(t, err, &rejection)
			require.Equal(t, rejectionReasonInvalidCommit, rejection.reasonCode)
			require.Zero(t, directory.purgeCalls)
			require.Zero(t, directory.lookupCalls)
		})
	}
}
