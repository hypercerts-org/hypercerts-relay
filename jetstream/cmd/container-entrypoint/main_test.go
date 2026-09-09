//go:build unix

package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const fixtureSecret = "deployment-fixture-credential-32-bytes"

func setup(t *testing.T) {
	t.Helper()
	t.Setenv("TMPDIR", t.TempDir())
	t.Setenv("HC_RAILWAY_STARTUP", "1")
	t.Setenv("RAILWAY_VOLUME_MOUNT_PATH", "/data")
	for _, secret := range []secretFile{relaySecret, jetstreamSecret, administrationSecret} {
		t.Setenv(secret.input, fixtureSecret)
		t.Setenv(secret.output, "")
	}
}

func TestSecretFiles(t *testing.T) {
	for component, count := range map[string]int{"relay": 1, "jetstream": 1, "administration": 3, "rainbow": 0} {
		t.Run(component, func(t *testing.T) {
			setup(t)
			if err := prepare(component); err != nil {
				t.Fatal(err)
			}
			found := 0
			for _, secret := range []secretFile{relaySecret, jetstreamSecret, administrationSecret} {
				if _, present := os.LookupEnv(secret.input); present {
					t.Fatalf("raw %s remains in environment", secret.input)
				}
				path := os.Getenv(secret.output)
				if path == "" {
					continue
				}
				found++
				data, err := os.ReadFile(path)
				if err != nil || string(data) != fixtureSecret {
					t.Fatalf("%s did not materialize correctly", secret.output)
				}
				info, err := os.Stat(path)
				if err != nil || info.Mode().Perm() != 0600 {
					t.Fatal("secret file permissions are not 0600")
				}
				info, err = os.Stat(filepath.Dir(path))
				if err != nil || info.Mode().Perm() != 0700 {
					t.Fatal("secret directory permissions are not 0700")
				}
			}
			if found != count {
				t.Fatalf("got %d files, want %d", found, count)
			}
		})
	}
}

func TestOrdinaryContainerKeepsExistingConfiguration(t *testing.T) {
	setup(t)
	t.Setenv("HC_RAILWAY_STARTUP", "")
	t.Setenv("RAILWAY_VOLUME_MOUNT_PATH", "")
	t.Setenv(relaySecret.output, "/run/secrets/existing-token")
	if err := prepare("relay"); err != nil {
		t.Fatal(err)
	}
	if os.Getenv(relaySecret.output) != "/run/secrets/existing-token" {
		t.Fatal("existing file-secret path changed")
	}
	entries, err := os.ReadDir(os.Getenv("TMPDIR"))
	if err != nil || len(entries) != 0 {
		t.Fatal("ordinary container created temporary secrets")
	}
}

func TestInvalidRailwayConfigurationFailsClosed(t *testing.T) {
	t.Run("missing volume", func(t *testing.T) {
		setup(t)
		t.Setenv("RAILWAY_VOLUME_MOUNT_PATH", "")
		if err := prepare("rainbow"); err == nil {
			t.Fatal("missing volume accepted")
		}
	})
	for _, value := range []string{"", "short-credential", strings.Repeat(" ", 32)} {
		t.Run("invalid secret", func(t *testing.T) {
			setup(t)
			t.Setenv(relaySecret.input, value)
			err := prepare("relay")
			if err == nil || !strings.Contains(err.Error(), "at least 32 bytes") {
				t.Fatal("invalid secret accepted")
			}
			if value != "" && strings.Contains(err.Error(), value) {
				t.Fatal("secret included in diagnostic")
			}
		})
	}
}

// Exercise an actual process replacement, not just the environment preparation.
func TestExecPassthrough(t *testing.T) {
	if os.Getenv("HC_TEST_EXEC_PASSTHROUGH") == "1" {
		err := run([]string{"rainbow", "sh", "-c", `printf '%s:%s' "$$" "$1"; exit 23`, "sh", "argument preserved"})
		t.Fatalf("exec unexpectedly returned: %v", err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "-test.run=^TestExecPassthrough$")
	command.Env = append(os.Environ(), "HC_TEST_EXEC_PASSTHROUGH=1", "HC_RAILWAY_STARTUP=0")
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	err = command.Run()
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ExitCode() != 23 {
		t.Fatalf("original exit status not preserved: %v; %s", err, output.String())
	}
	if output.String() != strconv.Itoa(command.Process.Pid)+":argument preserved" {
		t.Fatalf("PID or arguments changed across exec: %s", output.String())
	}
}
