//go:build acceptance

package relay

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSourceTransportAcceptanceRequiresExplicitOptIn(t *testing.T) {
	for _, optIn := range []string{"", "false", "1", "true"} {
		t.Run(optIn, func(t *testing.T) {
			t.Setenv("RELAY_ACCEPTANCE_PRIVATE_HOSTS", optIn)
			if optIn != "true" {
				require.False(t, acceptancePrivateHostsEnabled())
				require.ErrorIs(t, sourceTransportAllowed("localhost:1234", true, false), ErrInvalidSourceURL)
				return
			}
			require.True(t, acceptancePrivateHostsEnabled())
			fixture := newSourceFixture(t)
			r, _, _ := newSourceLifecycleRelay(t)
			r.Config.TestDialContext = nil
			r.Slurper.Config.TestDialContext = nil
			r.HostChecker = NewHostClient("")
			_, _ = addPassedSource(t, r, fixture)
		})
	}
}
