package live

// hypercerts: This file adapts the upstream subscribeRepos dial for two
// operational faults observed on Railway staging (TECH-883):
//
//   - atmos v0.3.6 classifies every failed dial that carried an HTTP response
//     as a permanent *streaming.DialError and ends the event iterator. A relay
//     restart behind a proxy answers 502/503 for a few seconds, so one restart
//     silently stopped live ingestion for hours. Transient statuses are
//     reported to atmos without the response so its own backoff retries them.
//   - atmos caps each DID's verify queue at Parallelism*2 and sheds events
//     instead of blocking. Catch-up after a restart read the backlog faster
//     than it could be verified. Reads are paced by a token bucket so the
//     dispatch goroutine cannot outrun verification by an unbounded margin.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/bluesky-social/jetstream/internal/version"
	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/streaming"
	"golang.org/x/time/rate"
)

const (
	// DefaultVerifyParallelism is the number of atmos verification workers
	// used when Config.VerifyParallelism is zero. atmos caps each DID's verify
	// queue at twice this value; atmos's own default is 32 (queue 64), which
	// overflowed while replaying a few hours of backlog after a restart.
	DefaultVerifyParallelism = 128

	// DefaultUpstreamReadRate is the recommended upstream frame read rate, in
	// frames per second, for production. It is applied by the command-line
	// default; Config.UpstreamReadRate zero disables pacing.
	DefaultUpstreamReadRate = 500
)

// errTransientDial marks an upstream HTTP rejection that should be retried.
var errTransientDial = errors.New("transient upstream dial rejection")

// transientDialStatus reports whether an HTTP status returned instead of a
// WebSocket upgrade is expected to clear without operator action: request
// timeouts, rate limits and every server-side 5xx (including proxy 502/503/504
// responses while the relay restarts).
func transientDialStatus(code int) bool {
	switch code {
	case http.StatusRequestTimeout, http.StatusTooEarly, http.StatusTooManyRequests:
		return true
	}
	return code >= 500 && code <= 599
}

// defaultUpstreamDial mirrors atmos's built-in dial; atmos does not export it.
func defaultUpstreamDial(ctx context.Context, u string, cfg streaming.DialConfig) (streaming.Conn, *http.Response, error) {
	var subprotocols []string
	for _, s := range cfg.Subprotocols {
		subprotocols = append(subprotocols, string(s))
	}
	conn, resp, err := websocket.Dial(ctx, u, &websocket.DialOptions{
		HTTPHeader:      http.Header{"User-Agent": []string{"hypercerts-jetstream/" + version.Version}},
		Subprotocols:    subprotocols,
		CompressionMode: cfg.Compression,
	})
	if err != nil {
		return nil, resp, err
	}
	return conn, resp, nil
}

// upstreamDial wraps base with transient-rejection retry, connection logging
// and optional read pacing. limiter may be nil to disable pacing.
func upstreamDial(base streaming.DialFunc, logger *slog.Logger, metrics *Metrics, limiter *rate.Limiter) streaming.DialFunc {
	if base == nil {
		base = defaultUpstreamDial
	}
	return func(ctx context.Context, u string, cfg streaming.DialConfig) (streaming.Conn, *http.Response, error) {
		cursor := dialCursor(u)
		conn, resp, err := base(ctx, u, cfg)
		if err != nil {
			if resp != nil && transientDialStatus(resp.StatusCode) {
				status := resp.StatusCode
				if resp.Body != nil {
					_ = resp.Body.Close()
				}
				metrics.incTransientDialRejections()
				logger.WarnContext(ctx, "upstream dial rejected; retrying",
					"status", status,
					"cursor", cursor,
					"err", err,
				)
				// Returning no response makes atmos treat the failure as
				// retryable and apply its reconnect backoff.
				return nil, nil, fmt.Errorf("%w: HTTP %d: %w", errTransientDial, status, err)
			}
			return nil, resp, err
		}
		metrics.incUpstreamConnects()
		logger.InfoContext(ctx, "upstream connected", "cursor", cursor)
		if limiter != nil {
			conn = &pacedConn{Conn: conn, limiter: limiter}
		}
		return conn, resp, nil
	}
}

// dialCursor extracts the resume cursor atmos placed in the dial URL, for logs.
func dialCursor(u string) string {
	parsed, err := url.Parse(u)
	if err != nil {
		return ""
	}
	return parsed.Query().Get("cursor")
}

// pacedConn waits for a token before each upstream read. Blocking the read
// leaves frames in the socket, so TCP flow control pushes back on the relay
// instead of atmos shedding events from a full per-DID verify queue.
type pacedConn struct {
	streaming.Conn
	limiter *rate.Limiter
}

func (p *pacedConn) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	if err := p.limiter.Wait(ctx); err != nil {
		return 0, nil, err
	}
	return p.Conn.Read(ctx)
}

// newUpstreamReadLimiter returns nil when pacing is disabled.
func newUpstreamReadLimiter(perSecond float64, burst int) *rate.Limiter {
	if perSecond <= 0 {
		return nil
	}
	if burst < 1 {
		burst = 1
	}
	return rate.NewLimiter(rate.Limit(perSecond), burst)
}
