//go:build acceptance

package relay

import "os"

// hypercerts: the disposable Plan 001 Docker topology uses private test-network
// PDS addresses. It requires both this build tag and an explicit runtime opt-in.
func acceptancePrivateHostsEnabled() bool {
	return os.Getenv("RELAY_ACCEPTANCE_PRIVATE_HOSTS") == "true"
}
