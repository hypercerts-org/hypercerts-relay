package main

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestHypercertsControlSecretFile(t *testing.T) {
	for _, value := range []string{"fixture-service-credential-at-least-32-bytes\n", "short", strings.Repeat("x", 4097)} {
		t.Run(strconv.Itoa(len(value)), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "token")
			require.NoError(t, os.WriteFile(path, []byte(value), 0600))
			app := newTestApp()
			var got jetstreamd.Options
			for _, cmd := range app.Commands {
				if cmd.Name == "serve" {
					cmd.Action = func(_ context.Context, cmd *cli.Command) error {
						var err error
						got, err = serveOptionsFromCommand(cmd)
						return err
					}
				}
			}
			err := app.Run(t.Context(), []string{"jetstream", "serve", "--control-token-file", path})
			if len(value) < 32 || len(value) > 4096 {
				require.Error(t, err)
				require.NotContains(t, err.Error(), value)
			} else {
				require.NoError(t, err)
				require.Equal(t, strings.TrimSpace(value), got.ControlToken)
			}
		})
	}
}
