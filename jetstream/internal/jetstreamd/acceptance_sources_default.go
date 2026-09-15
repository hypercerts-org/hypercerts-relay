//go:build !acceptance

package jetstreamd

// Production binaries never relax source SSRF protections.
func acceptancePrivateSourcesEnabled() bool { return false }

func acceptanceSealingEnabled() bool { return false }

func isAcceptancePrivateSource(string) bool { return false }
