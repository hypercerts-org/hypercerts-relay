package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
)

// RelayClient queries a reference relay, such as the Bluesky relay, with
// com.atproto.sync.getHostStatus.
type RelayClient struct {
	base string
	http *http.Client
}

func NewRelayClient(rawURL string) (*RelayClient, error) {
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return nil, errors.New("reference relay URL must be an HTTPS origin")
	}
	return &RelayClient{
		base: httpsScheme + u.Host,
		http: &http.Client{Timeout: 15 * time.Second},
	}, nil
}

func (c *RelayClient) HostStatus(ctx context.Context, hostname string) (*HostStatus, error) {
	q := url.Values{"hostname": {hostname}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/xrpc/com.atproto.sync.getHostStatus?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body := io.LimitReader(resp.Body, 64<<10)
	if resp.StatusCode != http.StatusOK {
		var xrpcErr struct {
			Error string `json:"error"`
		}
		if resp.StatusCode == http.StatusBadRequest && json.NewDecoder(body).Decode(&xrpcErr) == nil && xrpcErr.Error == "HostNotFound" {
			return nil, ErrHostNotFound
		}
		return nil, fmt.Errorf("getHostStatus: HTTP %d", resp.StatusCode)
	}
	var out comatproto.SyncGetHostStatus_Output
	if err := json.NewDecoder(body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decoding getHostStatus: %w", err)
	}
	status := &HostStatus{AccountCount: out.AccountCount}
	if out.Status != nil {
		status.Status = *out.Status
	}
	return status, nil
}
