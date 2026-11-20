package repoexport

import (
	"context"
	"net/http"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/gt"
)

const statusResolveTimeout = 10 * time.Second

// hypercerts: NewStatusResolver builds the DID resolver for anonymous /status
// repo verification. The DID is chosen by the viewer, so a did:web document
// fetch is as account-controlled as the PDS request that follows it and uses
// the same public-address-only, no-proxy client.
//
// plcURL is the operator-configured PLC directory. It may be a private
// address, so it is the one exception: only did:plc lookups go to it, through
// a client that uses no proxy and follows no redirect, so the exception cannot
// be carried to another origin. An empty plcURL resolves did:plc at the
// default public directory through the public client.
//
// transport is the runtime's explicit in-process test seam; when set it
// replaces both dialers.
func NewStatusResolver(plcURL string, transport http.RoundTripper) identity.Resolver {
	publicClient := newPublicPDSHTTPClient()
	publicClient.Timeout = statusResolveTimeout
	if transport != nil {
		publicClient = &http.Client{Transport: transport, Timeout: statusResolveTimeout}
	}
	public := &identity.DefaultResolver{HTTPClient: gt.Some(publicClient)}
	if plcURL == "" {
		return public
	}
	plcTransport := transport
	if plcTransport == nil {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.Proxy = nil
		plcTransport = t
	}
	plcClient := &http.Client{
		Transport: plcTransport,
		Timeout:   statusResolveTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return statusResolver{
		public: public,
		plc:    &identity.DefaultResolver{PLCURL: gt.Some(plcURL), HTTPClient: gt.Some(plcClient)},
	}
}

type statusResolver struct {
	public identity.Resolver
	plc    identity.Resolver
}

func (r statusResolver) ResolveDID(ctx context.Context, did atmos.DID) (*identity.DIDDocument, error) {
	if did.Method() == "plc" {
		return r.plc.ResolveDID(ctx, did)
	}
	return r.public.ResolveDID(ctx, did)
}

func (r statusResolver) ResolveHandle(ctx context.Context, handle atmos.Handle) (atmos.DID, error) {
	return r.public.ResolveHandle(ctx, handle)
}
