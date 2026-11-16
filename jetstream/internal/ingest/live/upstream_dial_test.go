package live

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/streaming"
	"github.com/jcalabro/gt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// trackedBody records whether the dial wrapper closed a rejected response.
type trackedBody struct {
	io.Reader
	closed atomic.Bool
}

func (b *trackedBody) Close() error { b.closed.Store(true); return nil }

func httpRejection(code int) (*http.Response, *trackedBody) {
	body := &trackedBody{Reader: strings.NewReader("")}
	return &http.Response{StatusCode: code, Body: body}, body
}

func fastBackoff() *streaming.BackoffPolicy {
	return &streaming.BackoffPolicy{
		InitialDelay: gt.Some(time.Millisecond),
		MaxDelay:     gt.Some(5 * time.Millisecond),
		Multiplier:   gt.Some(1.0),
		Jitter:       gt.Some(false),
	}
}

func TestTransientDialStatus(t *testing.T) {
	t.Parallel()
	for code, want := range map[int]bool{
		http.StatusRequestTimeout:      true,
		http.StatusTooEarly:            true,
		http.StatusTooManyRequests:     true,
		http.StatusInternalServerError: true,
		http.StatusBadGateway:          true,
		http.StatusServiceUnavailable:  true,
		http.StatusGatewayTimeout:      true,
		http.StatusOK:                  false,
		http.StatusBadRequest:          false,
		http.StatusUnauthorized:        false,
		http.StatusNotFound:            false,
	} {
		require.Equal(t, want, transientDialStatus(code), "status %d", code)
	}
}

func TestUpstreamDial_TransientRejectionIsRetryable(t *testing.T) {
	t.Parallel()
	resp, body := httpRejection(http.StatusBadGateway)
	metrics := NewMetrics(prometheus.NewRegistry())
	dial := upstreamDial(func(context.Context, string, streaming.DialConfig) (streaming.Conn, *http.Response, error) {
		return nil, resp, errors.New("expected handshake response status code 101 but got 502")
	}, slog.New(slog.DiscardHandler), metrics, nil)

	conn, gotResp, err := dial(t.Context(), "wss://relay.invalid/xrpc/com.atproto.sync.subscribeRepos?cursor=42", streaming.DialConfig{})
	require.Nil(t, conn)
	require.Nil(t, gotResp, "a transient rejection must not carry its response, or atmos treats it as permanent")
	require.ErrorIs(t, err, errTransientDial)
	require.True(t, body.closed.Load(), "the rejected response body must be closed")
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.TransientDialRejections))
	require.Equal(t, 0.0, testutil.ToFloat64(metrics.UpstreamConnects))
}

func TestUpstreamDial_PermanentRejectionKeepsResponse(t *testing.T) {
	t.Parallel()
	resp, _ := httpRejection(http.StatusNotFound)
	dial := upstreamDial(func(context.Context, string, streaming.DialConfig) (streaming.Conn, *http.Response, error) {
		return nil, resp, errors.New("got 404")
	}, slog.New(slog.DiscardHandler), nil, nil)

	_, gotResp, err := dial(t.Context(), "wss://relay.invalid/x", streaming.DialConfig{})
	require.Error(t, err)
	require.NotErrorIs(t, err, errTransientDial)
	require.Same(t, resp, gotResp)
}

func TestUpstreamDial_SuccessWrapsConnOnlyWhenPaced(t *testing.T) {
	t.Parallel()
	metrics := NewMetrics(prometheus.NewRegistry())
	base := func(context.Context, string, streaming.DialConfig) (streaming.Conn, *http.Response, error) {
		return newMemConn(), nil, nil
	}

	conn, _, err := upstreamDial(base, slog.New(slog.DiscardHandler), metrics, nil)(t.Context(), "wss://relay.invalid/x", streaming.DialConfig{})
	require.NoError(t, err)
	require.IsType(t, &memConn{}, conn)

	conn, _, err = upstreamDial(base, slog.New(slog.DiscardHandler), metrics, newUpstreamReadLimiter(10, 1))(t.Context(), "wss://relay.invalid/x", streaming.DialConfig{})
	require.NoError(t, err)
	require.IsType(t, &pacedConn{}, conn)
	require.Equal(t, 2.0, testutil.ToFloat64(metrics.UpstreamConnects))
}

func TestDialCursor(t *testing.T) {
	t.Parallel()
	require.Equal(t, "189045", dialCursor("wss://relay.invalid/xrpc/com.atproto.sync.subscribeRepos?cursor=189045"))
	require.Empty(t, dialCursor("wss://relay.invalid/xrpc/com.atproto.sync.subscribeRepos"))
	require.Empty(t, dialCursor("://bad"))
}

func TestNewUpstreamReadLimiter(t *testing.T) {
	t.Parallel()
	require.Nil(t, newUpstreamReadLimiter(0, 10))
	require.Nil(t, newUpstreamReadLimiter(-1, 10))
	l := newUpstreamReadLimiter(100, 0)
	require.NotNil(t, l)
	require.Equal(t, 1, l.Burst(), "a zero burst would make every Wait fail")
}

func TestPacedConn_LimitsReadRate(t *testing.T) {
	t.Parallel()
	frames := [][]byte{{1}, {2}, {3}, {4}, {5}}
	conn := &pacedConn{Conn: newMemConn(frames...), limiter: newUpstreamReadLimiter(50, 1)}

	start := time.Now()
	for range frames {
		_, _, err := conn.Read(t.Context())
		require.NoError(t, err)
	}
	// One token is available immediately; the remaining four arrive at 50/s.
	require.GreaterOrEqual(t, time.Since(start), 70*time.Millisecond)
}

func TestPacedConn_ReadHonoursContext(t *testing.T) {
	t.Parallel()
	conn := &pacedConn{Conn: newMemConn([]byte{1}, []byte{2}), limiter: newUpstreamReadLimiter(0.001, 1)}
	_, _, err := conn.Read(t.Context())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err = conn.Read(ctx)
	require.Error(t, err)
}

func TestConfig_LiveIngestTuning(t *testing.T) {
	t.Parallel()
	base := func() Config {
		return Config{
			SegmentsDir: "x", Store: newTestStore(t), SeqKey: "s", CursorKey: "c",
			RelayURL: "https://relay.invalid", Logger: slog.New(slog.DiscardHandler),
			Verifier: newTestVerifier(t),
		}
	}

	cfg := base()
	require.NoError(t, cfg.validate())
	cfg.applyDefaults()
	require.Equal(t, DefaultVerifyParallelism, cfg.VerifyParallelism)
	require.Equal(t, DefaultVerifyParallelism, cfg.UpstreamReadBurst)
	require.Zero(t, cfg.UpstreamReadRate, "pacing stays opt-in below the command line")

	cfg = base()
	cfg.VerifyParallelism = 8
	cfg.applyDefaults()
	require.Equal(t, 8, cfg.UpstreamReadBurst)

	for name, mutate := range map[string]func(*Config){
		"negative parallelism": func(c *Config) { c.VerifyParallelism = -1 },
		"negative rate":        func(c *Config) { c.UpstreamReadRate = -1 },
		"infinite rate":        func(c *Config) { c.UpstreamReadRate = math.Inf(1) },
		"negative burst":       func(c *Config) { c.UpstreamReadBurst = -1 },
	} {
		cfg := base()
		mutate(&cfg)
		require.ErrorIs(t, cfg.validate(), ErrInvalidConfig, name)
	}
}

// TestConsumer_Run_RetriesTransientRelayRejection reproduces the staging
// stall (TECH-883): the relay answers 503 while it restarts. Ingestion must
// back off, reconnect from the same cursor and deliver the events.
func TestConsumer_Run_RetriesTransientRelayRejection(t *testing.T) {
	t.Parallel()

	frames := [][]byte{
		encodeIdentityFrame(t, "did:plc:aaa", 42),
		encodeIdentityFrame(t, "did:plc:bbb", 43),
	}
	conn := newMemConn(frames...)
	metrics := NewMetrics(prometheus.NewRegistry())
	st := newTestStore(t)

	var dials atomic.Int64
	var dialedCursors sync.Map
	var delivered atomic.Int64
	c, err := Open(Config{
		SegmentsDir:       filepath.Join(t.TempDir(), "live_segments"),
		Store:             st,
		SeqKey:            "live_segments/seq/next",
		CursorKey:         "relay/cursor",
		RelayURL:          "https://relay.invalid",
		Logger:            slog.New(slog.DiscardHandler),
		Metrics:           metrics,
		Verifier:          newTestVerifier(t),
		MaxEventsPerBlock: 2,
		ReconnectBackoff:  fastBackoff(),
		UpstreamReadRate:  1000,
		OnEvent:           func(*segment.Event) { delivered.Add(1) },
		Dial: func(_ context.Context, u string, _ streaming.DialConfig) (streaming.Conn, *http.Response, error) {
			n := dials.Add(1)
			dialedCursors.Store(n, dialCursor(u))
			if n <= 2 {
				resp, _ := httpRejection(http.StatusServiceUnavailable)
				return nil, resp, errors.New("expected handshake response status code 101 but got 503")
			}
			return conn, nil, nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	require.NoError(t, c.saveCursorAndSyncState(41))

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(ctx) }()

	require.Eventually(t, func() bool { return delivered.Load() >= int64(len(frames)) },
		3*time.Second, 10*time.Millisecond, "ingestion did not resume after transient relay rejections")
	require.Equal(t, 2.0, testutil.ToFloat64(metrics.TransientDialRejections))
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.UpstreamConnects))
	require.GreaterOrEqual(t, testutil.ToFloat64(metrics.Reconnects), 2.0)
	for i := int64(1); i <= 3; i++ {
		cur, ok := dialedCursors.Load(i)
		require.True(t, ok, "dial %d not observed", i)
		require.Equal(t, "41", cur, "dial %d must resume from the persisted cursor", i)
	}

	select {
	case err := <-runErr:
		t.Fatalf("Run returned while the stream was healthy: %v", err)
	default:
	}
	cancel()
	select {
	case <-runErr:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// TestConsumer_Run_FailsWhenUpstreamStreamEnds pins the no-silent-stall
// contract: when atmos ends the iterator on a permanent dial rejection while
// the consumer is still meant to run, Run returns an error.
func TestConsumer_Run_FailsWhenUpstreamStreamEnds(t *testing.T) {
	t.Parallel()

	c, err := Open(Config{
		SegmentsDir:      filepath.Join(t.TempDir(), "live_segments"),
		Store:            newTestStore(t),
		SeqKey:           "live_segments/seq/next",
		CursorKey:        "relay/cursor",
		RelayURL:         "https://relay.invalid",
		Logger:           slog.New(slog.DiscardHandler),
		Verifier:         newTestVerifier(t),
		ReconnectBackoff: fastBackoff(),
		Dial: func(context.Context, string, streaming.DialConfig) (streaming.Conn, *http.Response, error) {
			resp, _ := httpRejection(http.StatusNotFound)
			return nil, resp, errors.New("expected handshake response status code 101 but got 404")
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	err = c.Run(ctx)
	require.ErrorIs(t, err, ErrUpstreamStreamEnded)
	var dialErr *streaming.DialError
	require.ErrorAs(t, err, &dialErr, "the terminal dial error must be reported")
	require.Equal(t, http.StatusNotFound, dialErr.StatusCode)
	require.NoError(t, ctx.Err(), "Run must fail promptly rather than wait for cancellation")
}

// TestConsumer_Run_DefaultsVerifyParallelism checks that Open applies the
// larger verify worker default rather than atmos's default of 32.
func TestConsumer_Run_DefaultsVerifyParallelism(t *testing.T) {
	t.Parallel()
	c, err := Open(Config{
		SegmentsDir: filepath.Join(t.TempDir(), "live_segments"),
		Store:       newTestStore(t),
		SeqKey:      "live_segments/seq/next",
		CursorKey:   "relay/cursor",
		RelayURL:    "https://relay.invalid",
		Logger:      slog.New(slog.DiscardHandler),
		Verifier:    newTestVerifier(t),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	require.Equal(t, DefaultVerifyParallelism, c.cfg.VerifyParallelism)
}

var _ streaming.Conn = (*pacedConn)(nil)
