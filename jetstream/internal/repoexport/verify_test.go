package repoexport

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/identity"
	"github.com/stretchr/testify/require"
)

const testAuthoritativeRev = "2222222222222"

func TestVerify_MatchingRoots(t *testing.T) {
	t.Parallel()

	events := []segment.Event{
		createEvent(testDID, "app.bsky.feed.post", "r1", "rev1", payload("1")),
		createEvent(testDID, "app.bsky.feed.post", "r2", "rev2", payload("2")),
	}
	authoritativeRoot, _ := expectedRoot(t, events)
	relay := newAuthoritativeCommitServer(t, testDID, testAuthoritativeRev, authoritativeRoot)

	dataDir, st := newTestDataDir(t)
	writeSegmentTree(t, st, filepath.Join(dataDir, "segments"), events)

	report, err := Verify(t.Context(), VerifyConfig{
		DataDir:          dataDir,
		DID:              testDID,
		IdentityResolver: newTestIdentityResolver(testDID, relay.URL),
		allowPrivatePDS:  true,
		Selector:         openSelector(t, dataDir),
	})
	require.NoError(t, err)
	require.Equal(t, VerifyReport{
		DID:               testDID,
		Match:             true,
		AuthoritativeRev:  testAuthoritativeRev,
		AuthoritativeRoot: authoritativeRoot.String(),
		LocalLatestRev:    "rev2",
		LocalRoot:         authoritativeRoot.String(),
		LocalRecordCount:  2,
	}, report)
}

func TestVerify_PendingEventsMatchAuthoritativeRoot(t *testing.T) {
	t.Parallel()

	// The authoritative repo already reflects both records.
	authoritativeEvents := []segment.Event{
		createEvent(testDID, "app.bsky.feed.post", "r1", "rev1", payload("1")),
		createEvent(testDID, "app.bsky.feed.like", "r2", "rev2", payload("2")),
	}
	authoritativeRoot, _ := expectedRoot(t, authoritativeEvents)
	relay := newAuthoritativeCommitServer(t, testDID, testAuthoritativeRev, authoritativeRoot)

	// Locally, only the first record made it to disk; the second (the
	// just-created like) is still in the live writer's pending block.
	// Without folding pending events in, Verify reports a root mismatch
	// even though the local state is actually current.
	dataDir, st := newTestDataDir(t)
	writeSegmentTree(t, st, filepath.Join(dataDir, "segments"), authoritativeEvents[:1])
	pending := authoritativeEvents[1:]

	report, err := Verify(t.Context(), VerifyConfig{
		DataDir:          dataDir,
		DID:              testDID,
		IdentityResolver: newTestIdentityResolver(testDID, relay.URL),
		allowPrivatePDS:  true,
		Selector:         openSelector(t, dataDir),
		PendingEvents:    pending,
	})
	require.NoError(t, err)
	require.True(t, report.Match, "expected pending like to close the gap; message=%q", report.Message)
	require.Equal(t, authoritativeRoot.String(), report.LocalRoot)
	require.Equal(t, "rev2", report.LocalLatestRev)
	require.Equal(t, 2, report.LocalRecordCount)
}

func TestVerify_MismatchingRootsReturnsReport(t *testing.T) {
	t.Parallel()

	authoritativeEvents := []segment.Event{
		createEvent(testDID, "app.bsky.feed.post", "r1", "rev1", payload("1")),
		createEvent(testDID, "app.bsky.feed.post", "r2", "rev2", payload("2")),
	}
	authoritativeRoot, _ := expectedRoot(t, authoritativeEvents)
	relay := newAuthoritativeCommitServer(t, testDID, testAuthoritativeRev, authoritativeRoot)

	localEvents := []segment.Event{
		createEvent(testDID, "app.bsky.feed.post", "r1", "rev1", payload("1")),
	}
	localRoot, localCount := expectedRoot(t, localEvents)
	dataDir, st := newTestDataDir(t)
	writeSegmentTree(t, st, filepath.Join(dataDir, "segments"), localEvents)

	report, err := Verify(t.Context(), VerifyConfig{
		DataDir:          dataDir,
		DID:              testDID,
		IdentityResolver: newTestIdentityResolver(testDID, relay.URL),
		allowPrivatePDS:  true,
		Selector:         openSelector(t, dataDir),
	})
	require.NoError(t, err)
	require.Equal(t, VerifyReport{
		DID:               testDID,
		Match:             false,
		AuthoritativeRev:  testAuthoritativeRev,
		AuthoritativeRoot: authoritativeRoot.String(),
		LocalLatestRev:    "rev1",
		LocalRoot:         localRoot.String(),
		LocalRecordCount:  localCount,
		Message:           "local reconstructed MST root does not match authoritative repo root",
	}, report)
}

func TestVerify_MissingLocalRepoReturnsMismatchReport(t *testing.T) {
	t.Parallel()

	events := []segment.Event{
		createEvent(testDID, "app.bsky.feed.post", "r1", "rev1", payload("1")),
	}
	authoritativeRoot, _ := expectedRoot(t, events)
	relay := newAuthoritativeCommitServer(t, testDID, testAuthoritativeRev, authoritativeRoot)

	emptyDir := t.TempDir()
	report, err := Verify(t.Context(), VerifyConfig{
		DataDir:          emptyDir,
		DID:              testDID,
		IdentityResolver: newTestIdentityResolver(testDID, relay.URL),
		allowPrivatePDS:  true,
		Selector:         openSelector(t, emptyDir),
	})
	require.NoError(t, err)
	require.Equal(t, testDID, report.DID)
	require.False(t, report.Match)
	require.Equal(t, testAuthoritativeRev, report.AuthoritativeRev)
	require.Equal(t, authoritativeRoot.String(), report.AuthoritativeRoot)
	require.Empty(t, report.LocalLatestRev)
	require.Empty(t, report.LocalRoot)
	require.Zero(t, report.LocalRecordCount)
	require.Contains(t, report.Message, "no local commit events")
}

func TestVerify_MalformedLatestCommitCIDReturnsError(t *testing.T) {
	t.Parallel()

	relay := newMalformedLatestCommitServer(t, testDID)

	_, err := Verify(t.Context(), VerifyConfig{
		DataDir:          t.TempDir(),
		DID:              testDID,
		IdentityResolver: newTestIdentityResolver(testDID, relay.URL),
		allowPrivatePDS:  true,
	})
	require.Error(t, err)
}

func TestVerify_MissingAuthoritativeCommitBlockReturnsError(t *testing.T) {
	t.Parallel()

	root := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root"))
	commitCID, _ := authoritativeCommitCAR(t, testDID, testAuthoritativeRev, root)
	otherCID, otherCAR := authoritativeCommitCAR(t, testDID, testAuthoritativeRev, cbor.ComputeCID(cbor.CodecDagCBOR, []byte("other-root")))
	require.False(t, otherCID.Equal(commitCID))

	relay := newCommitBlockServer(t, testDID, testAuthoritativeRev, commitCID, otherCAR)
	_, err := Verify(t.Context(), VerifyConfig{
		DataDir:          t.TempDir(),
		DID:              testDID,
		IdentityResolver: newTestIdentityResolver(testDID, relay.URL),
		allowPrivatePDS:  true,
	})
	require.ErrorContains(t, err, "not found in getBlocks response")
}

func TestVerify_AuthoritativeCommitDIDMismatchReturnsError(t *testing.T) {
	t.Parallel()

	root := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root"))
	commitCID, commitCAR := authoritativeCommitCAR(t, "did:plc:other", testAuthoritativeRev, root)
	relay := newCommitBlockServer(t, testDID, testAuthoritativeRev, commitCID, commitCAR)

	_, err := Verify(t.Context(), VerifyConfig{
		DataDir:          t.TempDir(),
		DID:              testDID,
		IdentityResolver: newTestIdentityResolver(testDID, relay.URL),
		allowPrivatePDS:  true,
	})
	require.ErrorContains(t, err, "authoritative commit DID mismatch")
}

func TestVerify_AuthoritativeCommitRevMismatchReturnsError(t *testing.T) {
	t.Parallel()

	root := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root"))
	commitCID, commitCAR := authoritativeCommitCAR(t, testDID, "2222222222223", root)
	relay := newCommitBlockServer(t, testDID, testAuthoritativeRev, commitCID, commitCAR)

	_, err := Verify(t.Context(), VerifyConfig{
		DataDir:          t.TempDir(),
		DID:              testDID,
		IdentityResolver: newTestIdentityResolver(testDID, relay.URL),
		allowPrivatePDS:  true,
	})
	require.ErrorContains(t, err, "authoritative commit rev mismatch")
}

func TestVerify_HTTPFailureReturnsError(t *testing.T) {
	t.Parallel()

	relay := newGetLatestCommitErrorServer(t, http.StatusInternalServerError)

	_, err := Verify(t.Context(), VerifyConfig{
		DataDir:          t.TempDir(),
		DID:              testDID,
		IdentityResolver: newTestIdentityResolver(testDID, relay.URL),
		allowPrivatePDS:  true,
	})
	require.Error(t, err)
}

func TestVerify_ValidatesConfig(t *testing.T) {
	t.Parallel()

	_, err := Verify(t.Context(), VerifyConfig{DID: testDID})
	require.ErrorContains(t, err, "DataDir is required")

	_, err = Verify(t.Context(), VerifyConfig{DataDir: t.TempDir()})
	require.ErrorContains(t, err, "DID is required")
}

func TestVerify_RefusesNonPublicPDSWithoutContactingIt(t *testing.T) {
	t.Parallel()

	var hits atomic.Int64
	internal := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		rw.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(internal.Close)

	for _, endpoint := range []string{
		internal.URL,
		strings.Replace(internal.URL, "127.0.0.1", "localhost", 1),
	} {
		_, err := Verify(t.Context(), VerifyConfig{
			DataDir:          t.TempDir(),
			DID:              testDID,
			IdentityResolver: newTestIdentityResolver(testDID, endpoint),
		})
		require.Error(t, err, endpoint)
	}
	require.Zero(t, hits.Load())
}

func TestPDSEndpointOrigin_AcceptsPublicOrigins(t *testing.T) {
	t.Parallel()

	for raw, want := range map[string]string{
		"https://pds.example.com":        "https://pds.example.com",
		"https://pds.example.com/":       "https://pds.example.com",
		"https://PDS.Example.com:2583":   "https://PDS.Example.com:2583",
		"http://pds.example.com":         "http://pds.example.com",
		"https://93.184.216.34":          "https://93.184.216.34",
		"http://93.184.216.34:8080":      "http://93.184.216.34:8080",
		"https://[2606:4700:4700::1111]": "https://[2606:4700:4700::1111]",
		"https://bücher.example":         "https://xn--bcher-kva.example",
		"https://bücher.example:8443":    "https://xn--bcher-kva.example:8443",
		"https://xn--bcher-kva.example":  "https://xn--bcher-kva.example",
	} {
		got, err := pdsEndpointOrigin(raw, false)
		require.NoError(t, err, raw)
		require.Equal(t, want, got, raw)
	}
}

func TestPDSEndpointOrigin_RejectsInternalOrNonOriginEndpoints(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{
		// Not a bare http(s) origin.
		"ftp://pds.example.com",
		"https://user:pass@pds.example.com",
		"https://pds.example.com/base",
		"https://pds.example.com?x=1",
		"https://pds.example.com#frag",
		"https:pds.example.com",
		"https://",
		// Internal names.
		"http://localhost:6060",
		"http://localhost.:6060",
		"http://debug.localhost",
		"http://pds",
		"http://svc.railway.internal:8080",
		"http://printer.local",
		"http://nas.lan",
		// Non-public addresses.
		"http://127.0.0.1:6060",
		"http://[::1]:6060",
		"http://[::ffff:127.0.0.1]",
		"http://10.1.2.3",
		"http://172.16.0.1",
		"http://192.168.1.1",
		"http://169.254.169.254",
		"http://100.64.0.1",
		"http://0.0.0.0",
		"http://[fd12::1]",
		"http://[fe80::1%25en0]",
	} {
		_, err := pdsEndpointOrigin(raw, false)
		require.Error(t, err, raw)
	}
}

func TestRefuseNonPublicDial(t *testing.T) {
	t.Parallel()

	for _, address := range []string{"127.0.0.1:80", "[::1]:443", "10.0.0.1:443", "169.254.169.254:80", "[fd00:ec2::254]:80", "[::ffff:192.168.0.1]:80", "100.100.1.1:443", "not-an-ip:80"} {
		require.Error(t, refuseNonPublicDial("tcp", address, nil), address)
	}
	for _, address := range []string{"192.0.2.1:443", "198.51.100.7:443", "203.0.113.5:443", "192.88.99.1:443", "240.0.0.1:443"} {
		require.Error(t, refuseNonPublicDial("tcp", address, nil), address)
	}
	for _, address := range []string{"[2606:4700:4700::1111]:443", "8.8.8.8:443"} {
		require.NoError(t, refuseNonPublicDial("tcp", address, nil), address)
	}
}

func TestPublicPDSHTTPClient_RefusesLoopbackTarget(t *testing.T) {
	t.Parallel()

	var hits atomic.Int64
	internal := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		rw.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(internal.Close)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, internal.URL, nil)
	require.NoError(t, err)
	resp, err := newPublicPDSHTTPClient().Do(req)
	if resp != nil {
		_ = resp.Body.Close()
	}
	require.Error(t, err)

	// The dial hook alone also refuses the connection, covering a name that
	// passes the request-time check but resolves to loopback at dial time.
	_, err = (&net.Dialer{Control: refuseNonPublicDial}).DialContext(t.Context(), "tcp", internal.Listener.Addr().String())
	require.Error(t, err)
	require.Zero(t, hits.Load())
}

func newAuthoritativeCommitServer(t *testing.T, did string, rev string, root cbor.CID) *httptest.Server {
	t.Helper()

	commitCID, commitCAR := authoritativeCommitCAR(t, did, rev, root)
	return newCommitBlockServer(t, did, rev, commitCID, commitCAR)
}

type testIdentityResolver struct {
	did    string
	pdsURL string
}

func newTestIdentityResolver(did, pdsURL string) *testIdentityResolver {
	return &testIdentityResolver{did: did, pdsURL: pdsURL}
}

func (r *testIdentityResolver) ResolveDID(_ context.Context, did atmos.DID) (*identity.DIDDocument, error) {
	if string(did) != r.did {
		return nil, identity.ErrDIDNotFound
	}
	return &identity.DIDDocument{
		ID: r.did,
		Service: []identity.Service{
			{
				ID:              r.did + "#atproto_pds",
				Type:            "AtprotoPersonalDataServer",
				ServiceEndpoint: r.pdsURL,
			},
		},
	}, nil
}

func (r *testIdentityResolver) ResolveHandle(_ context.Context, _ atmos.Handle) (atmos.DID, error) {
	return "", identity.ErrHandleNotFound
}

func newCommitBlockServer(t *testing.T, did string, rev string, commitCID cbor.CID, commitCAR []byte) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.getLatestCommit":
			if got := r.URL.Query().Get("did"); got != did {
				http.Error(rw, "unexpected did", http.StatusBadRequest)
				return
			}

			rw.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(rw).Encode(map[string]string{
				"rev": rev,
				"cid": commitCID.String(),
			})
		case "/xrpc/com.atproto.sync.getBlocks":
			if got := r.URL.Query().Get("did"); got != did {
				http.Error(rw, "unexpected did", http.StatusBadRequest)
				return
			}
			if got := r.URL.Query()["cids"]; len(got) != 1 || got[0] != commitCID.String() {
				http.Error(rw, "unexpected cids", http.StatusBadRequest)
				return
			}

			rw.Header().Set("Content-Type", "application/vnd.ipld.car")
			_, _ = rw.Write(commitCAR)
		default:
			http.NotFound(rw, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func authoritativeCommitCAR(t *testing.T, did string, rev string, root cbor.CID) (cbor.CID, []byte) {
	t.Helper()

	commitData := cbor.AppendMapHeader(nil, 6)
	commitData = cbor.AppendText(commitData, "did")
	commitData = cbor.AppendText(commitData, did)
	commitData = cbor.AppendText(commitData, "rev")
	commitData = cbor.AppendText(commitData, rev)
	commitData = cbor.AppendText(commitData, "sig")
	commitData = cbor.AppendBytes(commitData, []byte{1, 2, 3})
	commitData = cbor.AppendText(commitData, "data")
	commitData = cbor.AppendCIDLink(commitData, &root)
	commitData = cbor.AppendText(commitData, "prev")
	commitData = cbor.AppendNull(commitData)
	commitData = cbor.AppendText(commitData, "version")
	commitData = cbor.AppendUint(commitData, 3)

	commitCID := cbor.ComputeCID(cbor.CodecDagCBOR, commitData)
	var buf bytes.Buffer
	writeCARAllowEmptyRoots(t, &buf, nil, []car.Block{{CID: commitCID, Data: commitData}})
	return commitCID, buf.Bytes()
}

func writeCARAllowEmptyRoots(t *testing.T, buf *bytes.Buffer, roots []cbor.CID, blocks []car.Block) {
	t.Helper()

	header := cbor.AppendMapHeader(nil, 2)
	header = cbor.AppendText(header, "roots")
	header = cbor.AppendArrayHeader(header, uint64(len(roots)))
	for i := range roots {
		header = cbor.AppendCIDLink(header, &roots[i])
	}
	header = cbor.AppendText(header, "version")
	header = cbor.AppendUint(header, 1)

	_, err := buf.Write(cbor.AppendUvarint(nil, uint64(len(header))))
	require.NoError(t, err)
	_, err = buf.Write(header)
	require.NoError(t, err)

	for _, block := range blocks {
		blockLen := uint64(cbor.CIDByteLen(&block.CID) + len(block.Data))
		_, err = buf.Write(cbor.AppendUvarint(nil, blockLen))
		require.NoError(t, err)
		_, err = buf.Write(block.CID.AppendBytes(nil))
		require.NoError(t, err)
		_, err = buf.Write(block.Data)
		require.NoError(t, err)
	}
}

func newMalformedLatestCommitServer(t *testing.T, did string) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path != "/xrpc/com.atproto.sync.getLatestCommit" {
			http.NotFound(rw, r)
			return
		}
		if got := r.URL.Query().Get("did"); got != did {
			http.Error(rw, "unexpected did", http.StatusBadRequest)
			return
		}

		rw.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(rw).Encode(map[string]string{
			"rev": testAuthoritativeRev,
			"cid": "not-a-cid",
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newGetLatestCommitErrorServer(t *testing.T, status int) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		http.Error(rw, "upstream error", status)
	}))
	t.Cleanup(srv.Close)
	return srv
}
