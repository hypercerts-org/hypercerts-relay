//go:build unix

// hypercerts: Shared container bootstrap lives in the nested module so its
// Docker build context can compile it without copying from outside that module.
package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

type secretFile struct {
	input  string
	output string
}

var relaySecret = secretFile{"HC_RELAY_CONTROL_SECRET", "RELAY_CONTROL_TOKEN_FILE"}
var jetstreamSecret = secretFile{"HC_JETSTREAM_CONTROL_SECRET", "JETSTREAM_CONTROL_TOKEN_FILE"}
var administrationSecret = secretFile{"HC_ADMIN_ENCRYPTION_SECRET", "ADMIN_ENCRYPTION_KEY_FILE"}

// jetstreamRelaySecret carries the Relay credential into Jetstream under its own
// output name. Jetstream authenticates to the Relay's private control listener
// for recovery receipts and for advancing the policy mirror before it commits a
// collection change; without this file every collection change fails. It reads
// the same input as relaySecret because both services must hold the same value.
var jetstreamRelaySecret = secretFile{"HC_RELAY_CONTROL_SECRET", "JETSTREAM_RELAY_CONTROL_TOKEN_FILE"}

// The PDS discovery service and administration share one machine credential.
// Administration mounts its machine routes only when the credential is present.
var discoverySecret = secretFile{"HC_DISCOVERY_SECRET", "DISCOVERY_ADMIN_TOKEN_FILE"}
var administrationDiscoverySecret = secretFile{"HC_DISCOVERY_SECRET", "ADMIN_DISCOVERY_TOKEN_FILE"}

// collectiondirSecret is optional: Administration enables its collection
// directory crawl action only when this credential is supplied.
var collectiondirSecret = secretFile{"HC_COLLECTIONDIR_CONTROL_SECRET", "COLLECTIONDIR_CONTROL_TOKEN_FILE"}

func clearRawSecrets() {
	for _, secret := range []secretFile{relaySecret, jetstreamSecret, administrationSecret, discoverySecret, collectiondirSecret} {
		_ = os.Unsetenv(secret.input)
	}
}

func prepare(component string) error {
	defer clearRawSecrets()
	// optional secrets are converted only when their input is set; once set,
	// they must meet the same length rule as required secrets.
	var secrets, optional []secretFile
	needsVolume := true
	switch component {
	case "relay":
		secrets = []secretFile{relaySecret}
	case "jetstream":
		secrets = []secretFile{jetstreamSecret, jetstreamRelaySecret}
	case "administration":
		secrets = []secretFile{relaySecret, jetstreamSecret, administrationSecret}
		optional = []secretFile{administrationDiscoverySecret, collectiondirSecret}
	case "pdsdiscovery":
		// Stateless: discovery keeps no local data, so it needs no volume.
		secrets = []secretFile{discoverySecret}
		needsVolume = false
	case "rainbow":
	default:
		return errors.New("unknown service name")
	}
	// Existing images retain their normal file-secret configuration and commands.
	if os.Getenv("HC_RAILWAY_STARTUP") != "1" {
		return nil
	}
	if needsVolume && os.Getenv("RAILWAY_VOLUME_MOUNT_PATH") != "/data" {
		return errors.New("a persistent Railway volume mounted at /data is required")
	}
	for _, secret := range optional {
		if os.Getenv(secret.input) != "" {
			secrets = append(secrets, secret)
		}
	}
	for _, secret := range secrets {
		if len(strings.TrimSpace(os.Getenv(secret.input))) < 32 {
			return fmt.Errorf("%s must contain at least 32 bytes", secret.input)
		}
	}
	if len(secrets) > 0 {
		dir, err := os.MkdirTemp("", "hypercerts-secrets-")
		if err != nil {
			return fmt.Errorf("create secret directory: %w", err)
		}
		complete := false
		defer func() {
			if !complete {
				_ = os.RemoveAll(dir)
			}
		}()
		for _, secret := range secrets {
			path := filepath.Join(dir, secret.output)
			if err := os.WriteFile(path, []byte(strings.TrimSpace(os.Getenv(secret.input))), 0600); err != nil {
				return fmt.Errorf("write %s: %w", secret.output, err)
			}
			if err := os.Setenv(secret.output, path); err != nil {
				return fmt.Errorf("configure %s: %w", secret.output, err)
			}
		}
		complete = true
	}
	return nil
}

func run(args []string) error {
	if len(args) < 2 {
		return errors.New("service name and executable are required")
	}
	// Preserve the original dumb-init/execvp command override behavior. The
	// container operator controls this argument; it is not request input.
	command, err := exec.LookPath(args[1])
	if err != nil {
		return fmt.Errorf("resolve service executable: %w", err)
	}
	if err := prepare(args[0]); err != nil {
		return err
	}
	// Replace the bootstrap process: the daemon receives signals and retains its
	// original argument vector, including user-provided Docker CMD overrides.
	return syscall.Exec(command, args[1:], os.Environ())
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "container startup:", err)
		os.Exit(1)
	}
}
