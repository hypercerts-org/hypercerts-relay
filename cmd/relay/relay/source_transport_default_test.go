//go:build !acceptance

package relay

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSourceTransportNormalBuildIgnoresAcceptanceOptIn(t *testing.T) {
	t.Setenv("RELAY_ACCEPTANCE_PRIVATE_HOSTS", "true")
	require.False(t, acceptancePrivateHostsEnabled())
	require.ErrorIs(t, sourceTransportAllowed("pds.example.com", true, false), ErrInvalidSourceURL)
	require.ErrorIs(t, sourceTransportAllowed("localhost:1234", true, false), ErrInvalidSourceURL)
}
