package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const discoveryPrefix = "/api/v1/sources/discovery"

// maxStateBytes bounds the discovery state response, which lists every source.
const maxStateBytes = 16 << 20

// DiscoveryState is the administration machine view of sources and policy.
type DiscoveryState struct {
	PolicyRevision uint64            `json:"policyRevision"`
	Collections    []string          `json:"collections"`
	Sources        []DiscoverySource `json:"sources"`
	// MaxAccountLimit is the administrator-set maximum account limit that
	// discovery may request.
	MaxAccountLimit int64 `json:"maxAccountLimit"`
}

type DiscoverySource struct {
	PDS          string `json:"pds"`
	State        string `json:"state"`
	AccountLimit *int64 `json:"accountLimit"`
}

// RejectedError is a non-retryable administration refusal (a 4xx other than
// an idempotency conflict).
type RejectedError struct {
	Status int
	Body   string
}

func (e *RejectedError) Error() string {
	return fmt.Sprintf("administration rejected request: %d %s", e.Status, e.Body)
}

// AdminClient calls the administration machine API with a bearer token.
type AdminClient struct {
	base  string
	token string
	http  *http.Client
}

var railwayPrivateHost = regexp.MustCompile(`^[a-z0-9-]+\.railway\.internal$`)

// NewAdminClient accepts an HTTPS origin, or plain HTTP only on loopback or
// Railway private networking, matching administration's own control rule.
func NewAdminClient(rawURL, token string) (*AdminClient, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid administration URL: %w", err)
	}
	if u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || u.Host == "" {
		return nil, errors.New("administration URL must be an origin without credentials, path or query")
	}
	switch u.Scheme {
	case "https":
	case "http":
		host := u.Hostname()
		if host != "localhost" && host != "127.0.0.1" && host != "::1" && !railwayPrivateHost.MatchString(host) {
			return nil, errors.New("administration URL must use HTTPS outside loopback or Railway private networking")
		}
	default:
		return nil, errors.New("administration URL must use HTTPS")
	}
	if len(token) < 32 {
		return nil, errors.New("administration token must contain at least 32 bytes")
	}
	return &AdminClient{
		base:  u.Scheme + "://" + u.Host,
		token: token,
		http: &http.Client{
			Timeout: 30 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func (c *AdminClient) State(ctx context.Context) (*DiscoveryState, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+discoveryPrefix+"/state", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("discovery state: %d %s", resp.StatusCode, readSnippet(resp.Body))
	}
	var state DiscoveryState
	dec := json.NewDecoder(io.LimitReader(resp.Body, maxStateBytes))
	if err := dec.Decode(&state); err != nil {
		return nil, fmt.Errorf("decoding discovery state: %w", err)
	}
	return &state, nil
}

func (c *AdminClient) EnableSource(ctx context.Context, pds string) error {
	body := map[string]any{"kind": "source", "pds": pds, "state": "enabled"}
	return c.submit(ctx, body, idempotencyKey("source:enabled:"+pds))
}

func (c *AdminClient) RaiseAccountLimit(ctx context.Context, pds string, expected, limit int64) error {
	body := map[string]any{"kind": "account_quota", "pds": pds, "expectedLimit": expected, "accountLimit": limit}
	return c.submit(ctx, body, idempotencyKey(fmt.Sprintf("account_quota:%s:%d:%d", pds, expected, limit)))
}

// submit records an operation. A repeated request with the same deterministic
// key returns the existing operation, so restarts never create duplicates.
func (c *AdminClient) submit(ctx context.Context, body any, key string) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+discoveryPrefix+"/operations", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusAccepted:
		return nil
	case resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests:
		return &RejectedError{Status: resp.StatusCode, Body: readSnippet(resp.Body)}
	default:
		return fmt.Errorf("operation request: %d %s", resp.StatusCode, readSnippet(resp.Body))
	}
}

func readSnippet(r io.Reader) string {
	b, _ := io.ReadAll(io.LimitReader(r, 512))
	return strings.TrimSpace(string(b))
}

// idempotencyNamespace is a fixed UUID namespace for discovery operations.
var idempotencyNamespace = [16]byte{0x6f, 0x1b, 0x6c, 0x8e, 0x2a, 0x55, 0x4f, 0x0d, 0x9e, 0x31, 0x7a, 0x4c, 0x52, 0x0b, 0xd3, 0x18}

// idempotencyKey returns an RFC 9562 version 5 UUID of name.
func idempotencyKey(name string) string {
	h := sha1.New()
	h.Write(idempotencyNamespace[:])
	h.Write([]byte(name))
	sum := h.Sum(nil)
	sum[6] = (sum[6] & 0x0f) | 0x50
	sum[8] = (sum[8] & 0x3f) | 0x80
	s := hex.EncodeToString(sum[:16])
	return s[0:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:32]
}
