package main

import (
	"context"
	"testing"

	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func runServeOptions(t *testing.T, args ...string) (jetstreamd.Options, error) {
	t.Helper()
	app := newTestApp()
	var opts jetstreamd.Options
	for _, cmd := range app.Commands {
		if cmd.Name != "serve" {
			continue
		}
		cmd.Action = func(_ context.Context, cmd *cli.Command) error {
			var err error
			opts, err = serveOptionsFromCommand(cmd)
			return err
		}
		break
	}
	err := app.Run(t.Context(), append([]string{"jetstream", "serve"}, args...))
	return opts, err
}

// nolint:paralleltest
func TestServeOptionsFromCLI_PDSJobRepoWorkers(t *testing.T) {
	withClearedEnv(t)

	opts, err := runServeOptions(t)
	require.NoError(t, err)
	require.Equal(t, jetstreamd.DefaultPDSJobRepoWorkers, opts.PDSJobRepoWorkers)
	require.Equal(t, 2, opts.PDSJobRepoWorkers)

	opts, err = runServeOptions(t, "--pds-job-repo-workers=1")
	require.NoError(t, err)
	require.Equal(t, 1, opts.PDSJobRepoWorkers)

	opts, err = runServeOptions(t, "--pds-job-repo-workers=32")
	require.NoError(t, err)
	require.Equal(t, 32, opts.PDSJobRepoWorkers)

	t.Setenv("JETSTREAM_PDS_JOB_REPO_WORKERS", "8")
	opts, err = runServeOptions(t)
	require.NoError(t, err)
	require.Equal(t, 8, opts.PDSJobRepoWorkers)

	for _, value := range []string{"0", "-1", "33"} {
		_, err = runServeOptions(t, "--pds-job-repo-workers="+value)
		require.ErrorContains(t, err, "--pds-job-repo-workers must be between 1 and 32", value)
	}
}
