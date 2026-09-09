package main

import (
	"context"
	"testing"

	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestCollectionSeedCLI(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
		want []string
	}{
		{name: "default", want: selection.DefaultCollections()},
		{name: "disabled flag", args: []string{"--disable-collection-seed"}},
		{name: "disabled env", env: map[string]string{"JETSTREAM_DISABLE_COLLECTION_SEED": "true"}},
		{name: "explicit flag", args: []string{"--collections=app.bsky.feed.post"}, want: []string{"app.bsky.feed.post"}},
		{name: "explicit env", env: map[string]string{"JETSTREAM_COLLECTIONS": "app.bsky.feed.post"}, want: []string{"app.bsky.feed.post"}},
		{name: "explicit empty flag", args: []string{"--collections="}},
		{name: "explicit empty env", env: map[string]string{"JETSTREAM_COLLECTIONS": ""}},
		{name: "disabled with explicit", args: []string{"--disable-collection-seed", "--collections=app.bsky.feed.post"}, want: []string{"app.bsky.feed.post"}},
		{name: "flag overrides env", args: []string{"--disable-collection-seed=false"}, env: map[string]string{"JETSTREAM_DISABLE_COLLECTION_SEED": "true"}, want: selection.DefaultCollections()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withClearedEnv(t)
			for key, value := range tc.env {
				t.Setenv(key, value)
			}
			app := newTestApp()
			var opts jetstreamd.Options
			for _, cmd := range app.Commands {
				if cmd.Name == "serve" {
					cmd.Action = func(_ context.Context, cmd *cli.Command) error {
						var err error
						opts, err = serveOptionsFromCommand(cmd)
						return err
					}
				}
			}
			require.NoError(t, app.Run(t.Context(), append([]string{"jetstream", "serve"}, tc.args...)))
			require.ElementsMatch(t, tc.want, opts.InitialCollections)
		})
	}
}
