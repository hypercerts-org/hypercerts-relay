package main

import (
	"bytes"
	"context"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/urfave/cli/v3"
)

func TestAdminAuthenticationDoesNotLogCredentials(t *testing.T) {
	var logs bytes.Buffer
	server := collectionServer{ExepctedAuthHeader: "Bearer expected-secret", log: slog.New(slog.NewTextHandler(&logs, nil))}
	for _, tc := range []struct {
		header string
		want   bool
	}{{"", false}, {"Bearer wrong-secret", false}, {"Bearer expected-secret", true}} {
		request := httptest.NewRequest("POST", "/", nil)
		request.Header.Set("Authorization", tc.header)
		ctx := echo.New().NewContext(request, httptest.NewRecorder())
		if got := server.isAdmin(ctx); got != tc.want {
			t.Errorf("authentication for %q = %v", tc.header, got)
		}
	}
	if strings.Contains(logs.String(), "secret") {
		t.Fatal("credentials appeared in logs")
	}
}

func TestBypassFlagNotAvailable(t *testing.T) {
	for _, command := range []*cli.Command{serveCmd, offlineCrawlCmd} {
		for _, flag := range command.Flags {
			for _, name := range flag.Names() {
				if name == "ratelimit-header" {
					t.Fatalf("%s exposes bypass secret flag", command.Name)
				}
			}
		}
	}
}

func TestServeRejectsEmptyAdminToken(t *testing.T) {
	for _, token := range []string{"", "  "} {
		cmd := &cli.Command{Flags: []cli.Flag{&cli.StringFlag{Name: "admin-token"}}, Action: func(ctx context.Context, cmd *cli.Command) error { return new(collectionServer).run(ctx, cmd) }}
		err := cmd.Run(context.Background(), []string{"test", "--admin-token", token})
		if err == nil || !strings.Contains(err.Error(), "admin-token must not be empty") {
			t.Fatalf("token %q: got %v", token, err)
		}
	}
}
