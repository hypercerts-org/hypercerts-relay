//go:build acceptance

package jetstreamd

import (
	"net"
	"strings"
)

// acceptancePrivateSourcesEnabled keeps Docker-network access out of ordinary
// Jetstream binaries. DevelopmentMode must also be set by the operator.
func acceptancePrivateSourcesEnabled() bool { return true }

// acceptanceSealingEnabled exposes a deterministic archive-boundary hook only
// in the disposable acceptance binary.
func acceptanceSealingEnabled() bool { return true }

func isAcceptancePrivateSource(hostname string) bool {
	host, _, err := net.SplitHostPort(hostname)
	if err != nil {
		host = hostname
	}
	switch strings.ToLower(host) {
	case "pds-a.test", "pds-b.test":
		return true
	default:
		return false
	}
}
