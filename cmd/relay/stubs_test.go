package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bluesky-social/indigo/atproto/atclient"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/cmd/relay/relay"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

type getRepoDirectory struct {
	identity.Directory
	lookupErr error
}

func (d getRepoDirectory) LookupDID(context.Context, syntax.DID) (*identity.Identity, error) {
	return nil, d.lookupErr
}

func TestGetRepoDIDLookupErrors(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		status    int
		errorName string
	}{
		{"rate limited", identity.ErrDIDWebRateLimited, http.StatusTooManyRequests, "RateLimitExceeded"},
		{"DID not found", identity.ErrDIDNotFound, http.StatusNotFound, "RepoNotFound"},
		{"resolution failed", identity.ErrDIDResolutionFailed, http.StatusServiceUnavailable, "ServiceUnavailable"},
		{"timed out", context.DeadlineExceeded, http.StatusServiceUnavailable, "ServiceUnavailable"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := getRepoDirectory{lookupErr: fmt.Errorf("internal resolver detail: %w", tc.err)}
			svc := &Service{relay: &relay.Relay{Dir: dir}}
			router := echo.New()
			router.GET("/xrpc/com.atproto.sync.getRepo", svc.HandleComAtprotoSyncGetRepo)

			req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.sync.getRepo?did=did:web:example.com", nil)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			require.Equal(t, tc.status, rec.Code, rec.Body.String())
			var body atclient.ErrorBody
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			require.Equal(t, tc.errorName, body.Name)
			if tc.status != http.StatusNotFound {
				require.NotContains(t, body.Message, "internal resolver detail")
			}
		})
	}
}
