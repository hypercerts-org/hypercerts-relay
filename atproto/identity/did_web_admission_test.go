package identity

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
)

// hypercerts: a local did:web admission rejection is not a fact about the
// identity, so it must reach the caller without entering the negative cache.
func TestCacheDirectoryDoesNotCacheLocalAdmissionRejection(t *testing.T) {
	var admit atomic.Bool
	var limited atomic.Int32
	base := BaseDirectory{
		SkipHandleVerification: true,
		DIDWebLimitFunc: func(context.Context, string) error {
			if admit.Load() {
				return errors.New("admitted: stop before the network")
			}
			limited.Add(1)
			return ErrDIDWebRateLimited
		},
	}
	dir := NewCacheDirectory(&base, 16, time.Hour, time.Hour, time.Hour)
	did := syntax.DID("did:web:admission.example.com")

	for range 2 {
		_, err := dir.LookupDID(t.Context(), did)
		if !errors.Is(err, ErrDIDWebRateLimited) {
			t.Fatalf("expected local admission rejection, got %v", err)
		}
	}
	if got := limited.Load(); got != 2 {
		t.Fatalf("rejection must not be served from the cache: limiter consulted %d times", got)
	}

	// Any other resolution failure is still negatively cached.
	admit.Store(true)
	_, first := dir.LookupDID(t.Context(), did)
	admit.Store(false)
	_, second := dir.LookupDID(t.Context(), did)
	if first == nil || second == nil || errors.Is(second, ErrDIDWebRateLimited) {
		t.Fatalf("ordinary failure should be cached: first=%v second=%v", first, second)
	}
	if got := limited.Load(); got != 2 {
		t.Fatalf("cached failure must not consult the limiter again: %d", got)
	}
}

// hypercerts: an oversized did:web document is rejected, never truncated.
func TestResolveDIDWebRejectsOversizedDocument(t *testing.T) {
	for name, tc := range map[string]struct {
		size    int
		chunked bool
		wantErr bool
	}{
		"at limit":                 {size: int(maxDIDWebBodySize)},
		"over limit, declared":     {size: int(maxDIDWebBodySize) + 1, wantErr: true},
		"over limit, undeclared":   {size: int(maxDIDWebBodySize) + 1, chunked: true, wantErr: true},
		"well over limit, chunked": {size: 4 * int(maxDIDWebBodySize), chunked: true, wantErr: true},
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				body := strings.Repeat(" ", tc.size)
				if tc.chunked {
					w.(http.Flusher).Flush()
				}
				_, _ = w.Write([]byte(body))
			}))
			defer srv.Close()
			// did:web names a bare hostname, so route example.com (which the
			// httptest certificate covers) to the test listener.
			client := srv.Client()
			transport := client.Transport.(*http.Transport)
			transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, srv.Listener.Addr().String())
			}
			base := BaseDirectory{HTTPClient: *client}
			did := syntax.DID("did:web:example.com")
			body, err := base.resolveDIDWeb(t.Context(), did)
			if tc.wantErr {
				if !errors.Is(err, ErrDIDResolutionFailed) {
					t.Fatalf("expected rejection, got %d bytes, err=%v", len(body), err)
				}
				return
			}
			if err != nil || len(body) != tc.size {
				t.Fatalf("expected %d bytes, got %d, err=%v", tc.size, len(body), err)
			}
		})
	}
}
