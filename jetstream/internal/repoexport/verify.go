package repoexport

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"syscall"
	"time"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/jcalabro/jttp"
)

const (
	rootMismatchMessage          = "local reconstructed MST root does not match authoritative repo root"
	maxAuthoritativeCommitBlocks = 8
)

// VerifyConfig controls authoritative-vs-local repo root verification.
type VerifyConfig struct {
	DataDir string
	DID     string

	// IdentityResolver resolves DID documents so verification can query the
	// account's PDS for PDS-only sync endpoints. Nil uses identity.DefaultResolver.
	IdentityResolver identity.Resolver

	// Selector prunes which segments/blocks reconstruction decodes via the
	// in-memory manifest blooms. Required; see Config.Selector.
	Selector Selector

	// PendingEvents are the live writer's not-yet-flushed events, folded
	// into the local reconstruction so a record created moments ago is
	// reflected before the next compaction flush. See Config.PendingEvents.
	PendingEvents []segment.Event

	// allowPrivatePDS is a test-only escape hatch: this package's tests
	// serve the authoritative PDS from loopback httptest servers, which the
	// public-endpoint checks refuse. Unexported so no production caller can
	// set it.
	allowPrivatePDS bool
}

// VerifyReport describes the outcome of comparing a local reconstruction
// against an authoritative repo CAR.
type VerifyReport struct {
	DID               string
	Match             bool
	AuthoritativeRev  string
	AuthoritativeRoot string
	LocalLatestRev    string
	LocalRoot         string
	LocalRecordCount  int
	Message           string
}

// Verify compares cfg.DID's authoritative commit root against a locally
// reconstructed snapshot.
func Verify(ctx context.Context, cfg VerifyConfig) (VerifyReport, error) {
	if cfg.DataDir == "" {
		return VerifyReport{}, errors.New("repoexport: DataDir is required")
	}
	if cfg.DID == "" {
		return VerifyReport{}, errors.New("repoexport: DID is required")
	}
	if err := ctx.Err(); err != nil {
		return VerifyReport{}, err
	}

	authoritativeRev, authoritativeRoot, err := loadAuthoritativeRoot(ctx, cfg.DID, cfg.IdentityResolver, cfg.allowPrivatePDS)
	if err != nil {
		return VerifyReport{}, err
	}

	report := VerifyReport{
		DID:               cfg.DID,
		AuthoritativeRev:  authoritativeRev,
		AuthoritativeRoot: authoritativeRoot,
	}

	snap, err := Reconstruct(ctx, Config{
		DataDir:       cfg.DataDir,
		DID:           cfg.DID,
		Selector:      cfg.Selector,
		PendingEvents: cfg.PendingEvents,
	})
	if err != nil {
		if errors.Is(err, ErrNoLocalRepo) {
			report.Message = err.Error()
			return report, nil
		}
		return VerifyReport{}, err
	}

	report.LocalLatestRev = snap.LatestRev
	report.LocalRoot = snap.Root.String()
	report.LocalRecordCount = snap.RecordCount
	if authoritativeRoot == report.LocalRoot {
		report.Match = true
		return report, nil
	}

	report.Message = rootMismatchMessage
	return report, nil
}

func loadAuthoritativeRoot(ctx context.Context, did string, resolver identity.Resolver, allowPrivatePDS bool) (string, string, error) {
	pdsURL, err := resolvePDSEndpoint(ctx, did, resolver)
	if err != nil {
		return "", "", err
	}
	// hypercerts: The PDS endpoint comes from a DID document the account owner controls,
	// and verification runs on behalf of anonymous /status viewers, so it
	// must not be able to point the relay at internal hosts.
	pdsOrigin, err := pdsEndpointOrigin(pdsURL, allowPrivatePDS)
	if err != nil {
		return "", "", fmt.Errorf("repoexport: authoritative PDS endpoint for %s: %w", did, err)
	}
	httpClient := newPublicPDSHTTPClient()
	if allowPrivatePDS {
		httpClient = jttp.New(xrpc.BulkDownloadOpts()...)
	}
	xrpcClient := &xrpc.Client{
		Host:       pdsOrigin,
		HTTPClient: gt.Some(httpClient),
	}
	syncClient := atmossync.NewClient(atmossync.Options{Client: xrpcClient})

	rev, commitCID, err := syncClient.GetLatestCommit(ctx, atmos.DID(did))
	if err != nil {
		return "", "", fmt.Errorf("repoexport: get latest authoritative commit for %s: %w", did, err)
	}

	commit, err := loadAuthoritativeCommit(ctx, xrpcClient, did, rev, commitCID)
	if err != nil {
		return "", "", err
	}
	return rev, commit.Data.String(), nil
}

func resolvePDSEndpoint(ctx context.Context, did string, resolver identity.Resolver) (string, error) {
	parsedDID, err := atmos.ParseDID(did)
	if err != nil {
		return "", fmt.Errorf("repoexport: parse DID %q: %w", did, err)
	}
	if resolver == nil {
		resolver = NewStatusResolver("", nil)
	}
	doc, err := resolver.ResolveDID(ctx, parsedDID)
	if err != nil {
		return "", fmt.Errorf("repoexport: resolve authoritative PDS for %s: %w", did, err)
	}
	ident, err := identity.IdentityFromDocument(doc)
	if err != nil {
		return "", fmt.Errorf("repoexport: parse authoritative identity for %s: %w", did, err)
	}
	pdsURL := strings.TrimRight(ident.PDSEndpoint(), "/")
	if pdsURL == "" {
		return "", fmt.Errorf("repoexport: no authoritative PDS endpoint for %s", did)
	}
	return pdsURL, nil
}

// pdsEndpointOrigin checks that raw is a bare http(s) origin and returns it
// rebuilt from its parsed scheme and host, so nothing else in the DID
// document's value can reshape the request URL xrpc builds by appending
// "/xrpc/<nsid>". Unless allowPrivate is set, it also refuses hosts that
// name an internal target outright: non-public IP literals, localhost,
// single-label names and private naming suffixes. Any other host, including
// public IP literals and IDNs, passes; where a name actually resolves is
// enforced at request time by newPublicPDSHTTPClient.
func pdsEndpointOrigin(raw string, allowPrivate bool) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", errors.New("PDS endpoint is not a valid URL")
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return "", errors.New("PDS endpoint must be an http or https URL")
	}
	if u.Host == "" || u.Opaque != "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("PDS endpoint must be a bare origin without credentials, path, query or fragment")
	}
	origin := u.Scheme + "://" + u.Host
	if allowPrivate {
		return origin, nil
	}

	host := strings.ToLower(strings.TrimRight(u.Hostname(), "."))
	if addr, err := netip.ParseAddr(host); err == nil {
		if nonPublicAddr(addr) {
			return "", fmt.Errorf("PDS endpoint address %s is not public", host)
		}
		return origin, nil
	}
	if host == "localhost" || !strings.Contains(host, ".") {
		return "", errors.New("PDS endpoint host must not be localhost or a single-label name")
	}
	for _, suffix := range []string{".localhost", ".local", ".internal", ".lan"} {
		if strings.HasSuffix(host, suffix) {
			return "", fmt.Errorf("PDS endpoint host must not use private suffix %q", suffix)
		}
	}
	return origin, nil
}

// hypercerts: newPublicPDSHTTPClient builds the client used to query an account's PDS.
// The endpoint is account-controlled, and a public-looking name can resolve
// anywhere, so like atmos's default backfill host client it layers jttp's
// strict SSRF check on the initial request (redirect hops are checked by
// default), a dial hook that refuses any non-public address actually being
// connected to (closing the DNS-rebinding gap between that check and the
// dial), and no proxy, so every connection passes through the hook.
func newPublicPDSHTTPClient() *http.Client {
	dialer := &net.Dialer{
		// BulkDownloadOpts' dial settings, which a custom DialContext replaces.
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
		Control:   refuseNonPublicDial,
	}
	return jttp.New(append(xrpc.BulkDownloadOpts(),
		jttp.WithStrictSSRFProtection(),
		jttp.WithNoProxy(),
		jttp.WithDialContext(dialer.DialContext),
	)...)
}

// refuseNonPublicDial is a net.Dialer Control hook; address is the resolved
// IP and port about to be connected to.
func refuseNonPublicDial(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("repoexport: refusing to dial %q: %w", address, err)
	}
	addr, err := netip.ParseAddr(host)
	if err != nil || nonPublicAddr(addr) {
		return fmt.Errorf("repoexport: refusing to dial non-public address %s", host)
	}
	return nil
}

var publicIPv6 = netip.MustParsePrefix("2000::/3")

var reservedPDSNetworks = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"), netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"), netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	// IPv6 special-purpose space inside 2000::/3. The IETF protocol block
	// 2001::/23 is refused whole: it is not globally reachable by default,
	// and its few reachable assignments are anycast infrastructure, never a
	// PDS. Then documentation and 6to4.
	netip.MustParsePrefix("2001::/23"), netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"), netip.MustParsePrefix("3fff::/20"),
}

// nonPublicAddr conservatively refuses special-use and translation networks
// in addition to private addresses. Zoned addresses fail closed.
func nonPublicAddr(addr netip.Addr) bool {
	if !addr.IsValid() || addr.Zone() != "" {
		return true
	}
	addr = addr.Unmap()
	if addr.IsLoopback() || addr.IsLinkLocalUnicast() || addr.IsMulticast() || addr.IsUnspecified() || addr.IsPrivate() {
		return true
	}
	if addr.Is6() && !publicIPv6.Contains(addr) {
		return true
	}
	for _, prefix := range reservedPDSNetworks {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

func loadAuthoritativeCommit(ctx context.Context, xrpcClient *xrpc.Client, did, rev string, commitCID cbor.CID) (*repo.Commit, error) {
	body, err := xrpcClient.QueryStream(ctx, "com.atproto.sync.getBlocks", map[string]any{
		"did":  did,
		"cids": []string{commitCID.String()},
	})
	if err != nil {
		return nil, fmt.Errorf("repoexport: get authoritative commit block %s for %s: %w", commitCID.String(), did, err)
	}
	defer func() { _ = body.Close() }()

	block, err := readAuthoritativeCommitBlock(body, commitCID)
	if err != nil {
		return nil, err
	}

	var singleCommitCAR bytes.Buffer
	if err := car.WriteAll(&singleCommitCAR, []cbor.CID{commitCID}, []car.Block{block}); err != nil {
		return nil, fmt.Errorf("repoexport: encode authoritative commit block %s: %w", commitCID.String(), err)
	}

	_, commit, err := repo.LoadFromCAR(bytes.NewReader(singleCommitCAR.Bytes()))
	if err != nil {
		return nil, fmt.Errorf("repoexport: decode authoritative commit block %s: %w", commitCID.String(), err)
	}
	if commit.DID != did {
		return nil, fmt.Errorf("repoexport: authoritative commit DID mismatch: latest commit for %s returned commit for %s", did, commit.DID)
	}
	if commit.Rev != rev {
		return nil, fmt.Errorf("repoexport: authoritative commit rev mismatch: getLatestCommit returned %s but commit block contains %s", rev, commit.Rev)
	}
	return commit, nil
}

func readAuthoritativeCommitBlock(r io.Reader, commitCID cbor.CID) (car.Block, error) {
	br := bufio.NewReader(r)

	if err := readCARHeaderAllowEmptyRoots(br); err != nil {
		return car.Block{}, fmt.Errorf("repoexport: read authoritative commit CAR: %w", err)
	}
	for blocksRead := range maxAuthoritativeCommitBlocks {
		block, err := readCARBlock(br)
		if errors.Is(err, io.EOF) {
			return car.Block{}, fmt.Errorf("repoexport: authoritative commit block %s not found in getBlocks response", commitCID.String())
		}
		if err != nil {
			return car.Block{}, fmt.Errorf("repoexport: read authoritative commit block: %w", err)
		}
		if block.CID.Equal(commitCID) {
			return block, nil
		}
		if blocksRead == maxAuthoritativeCommitBlocks-1 {
			break
		}
	}

	return car.Block{}, fmt.Errorf("repoexport: authoritative getBlocks response exceeded %d blocks without commit %s", maxAuthoritativeCommitBlocks, commitCID.String())
}

func readCARHeaderAllowEmptyRoots(br *bufio.Reader) error {
	headerLen, err := binary.ReadUvarint(br)
	if err != nil {
		return fmt.Errorf("reading header length: %w", err)
	}
	if headerLen > car.MaxBlockSize {
		return fmt.Errorf("header length %d exceeds max size", headerLen)
	}

	header := make([]byte, headerLen)
	if _, err := io.ReadFull(br, header); err != nil {
		return fmt.Errorf("reading header: %w", err)
	}

	count, pos, err := cbor.ReadMapHeader(header, 0)
	if err != nil {
		return fmt.Errorf("header: %w", err)
	}

	var hasVersion bool
	for range count {
		key, newPos, err := cbor.ReadText(header, pos)
		if err != nil {
			return fmt.Errorf("header key: %w", err)
		}
		pos = newPos

		switch key {
		case "version":
			hasVersion = true
			ver, newPos, err := cbor.ReadUint(header, pos)
			if err != nil {
				return fmt.Errorf("header version: %w", err)
			}
			if ver != 1 {
				return fmt.Errorf("unsupported version %d, expected 1", ver)
			}
			pos = newPos
		default:
			pos, err = cbor.SkipValue(header, pos)
			if err != nil {
				return fmt.Errorf("skipping header key %q: %w", key, err)
			}
		}
	}
	if !hasVersion {
		return errors.New("header missing 'version'")
	}
	return nil
}

func readCARBlock(br *bufio.Reader) (car.Block, error) {
	blockLen, err := binary.ReadUvarint(br)
	if err != nil {
		return car.Block{}, err
	}
	if blockLen == 0 {
		return car.Block{}, errors.New("zero-length block")
	}
	if blockLen > car.MaxBlockSize {
		return car.Block{}, fmt.Errorf("block length %d exceeds max size", blockLen)
	}

	buf := make([]byte, blockLen)
	if _, err := io.ReadFull(br, buf); err != nil {
		return car.Block{}, fmt.Errorf("reading block: %w", err)
	}
	cid, cidLen, err := cbor.ParseCIDPrefix(buf)
	if err != nil {
		return car.Block{}, fmt.Errorf("parsing block CID: %w", err)
	}
	data := buf[cidLen:]
	expected := cbor.ComputeCID(cid.Codec(), data)
	if !expected.Equal(cid) {
		return car.Block{}, fmt.Errorf("block CID mismatch: claimed %s, computed %s", cid.String(), expected.String())
	}
	return car.Block{CID: cid, Data: data}, nil
}
