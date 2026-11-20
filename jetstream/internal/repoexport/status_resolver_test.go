package repoexport

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/stretchr/testify/require"
)

func TestStatusResolverConfinesPrivatePLCException(t *testing.T) {
	t.Parallel()
	var elsewhere atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { elsewhere.Add(1) }))
	t.Cleanup(other.Close)
	var plcHits atomic.Int32
	plc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		plcHits.Add(1)
		if r.URL.Path == "/did:plc:redirectedaaaaaaaaaaaaaaa" {
			http.Redirect(w, r, other.URL+"/", http.StatusFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"did:plc:operatoraaaaaaaaaaaaaaaa"}`))
	}))
	t.Cleanup(plc.Close)
	resolver := NewStatusResolver(plc.URL, nil)

	// The operator's PLC directory may be private: did:plc lookups reach it.
	doc, err := resolver.ResolveDID(t.Context(), atmos.DID("did:plc:operatoraaaaaaaaaaaaaaaa"))
	require.NoError(t, err)
	require.Equal(t, "did:plc:operatoraaaaaaaaaaaaaaaa", doc.ID)

	// The exception does not follow a redirect to another origin.
	_, err = resolver.ResolveDID(t.Context(), atmos.DID("did:plc:redirectedaaaaaaaaaaaaaaa"))
	require.Error(t, err)
	require.Zero(t, elsewhere.Load())

	// It does not extend to account-controlled did:web hosts either.
	before := plcHits.Load()
	for _, did := range []atmos.DID{"did:web:localhost", "did:web:127.0.0.1", "did:web:metadata.internal"} {
		_, err = resolver.ResolveDID(t.Context(), did)
		require.Error(t, err, did)
	}
	require.Equal(t, before, plcHits.Load())
	require.Zero(t, elsewhere.Load())
}

func TestStatusResolverDefaultRefusesNonPublicDIDWeb(t *testing.T) {
	t.Parallel()
	resolver := NewStatusResolver("", nil)
	_, err := resolver.ResolveDID(t.Context(), atmos.DID("did:web:localhost"))
	require.Error(t, err)
}

func TestNonPublicAddrPolicy(t *testing.T) {
	t.Parallel()
	for _, address := range []string{
		"198.17.255.255:443", "198.20.0.1:443", "198.51.99.1:443", "198.51.101.1:443",
		"192.0.1.1:443", "192.0.3.1:443", "192.88.98.1:443", "203.0.112.1:443", "203.0.114.1:443",
		"100.63.255.255:443", "100.128.0.1:443", "1.1.1.1:443", "[2a00:1450:4001::1]:443", "[2001:4860:4860::8888]:443",
	} {
		require.NoError(t, refuseNonPublicDial("tcp", address, nil), address)
	}
	for _, address := range []string{
		"0.1.2.3:80", "255.255.255.255:80", "224.0.0.1:80", "198.18.0.1:80", "198.19.255.255:80", "192.0.0.8:80",
		"[::]:80", "[::ffff:10.0.0.1]:80", "[::ffff:127.0.0.1]:80", "[64:ff9b::a00:1]:80", "[64:ff9b:1::1]:80",
		"[::a00:1]:80", "[fe80::1]:80", "[fec0::1]:80", "[fc00::1]:80", "[ff02::1]:80", "[2002:a00:1::1]:80",
		"[2001:db8::1]:80", "[2001::1]:80", "[2001:20::1]:80", "[2001:2::1]:80", "[2001:10::1]:80", "[2001:30::1]:80", "[2001:3::1]:80", "[2001:1ff::1]:80", "[3fff::1]:80", "[100::1]:80", "[4000::1]:80", "[fe80::1%25eth0]:80",
	} {
		require.Error(t, refuseNonPublicDial("tcp", address, nil), address)
	}
}
