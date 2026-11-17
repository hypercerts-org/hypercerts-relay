package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/indigo/hypercerts/version"

	"github.com/gorilla/websocket"
)

// Bluesky Jetstream accepts at most 100 wantedCollections values.
const maxWantedCollections = 100

var errReload = errors.New("collection set changed")

// Upstream reads record events from a Bluesky Jetstream instance, filtered to
// the retained collections, and passes creates and updates to the Discoverer.
type Upstream struct {
	base     string
	log      *slog.Logger
	d        *Discoverer
	lookback time.Duration
	reload   chan struct{}
	// cursor is the last observed event time in microseconds. It is kept in
	// memory only: discovery is retried by the next record on that PDS.
	cursor atomic.Int64
}

func NewUpstream(rawURL string, lookback time.Duration, log *slog.Logger, d *Discoverer) (*Upstream, error) {
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "wss" && u.Scheme != "ws") || u.Host == "" {
		return nil, errors.New("upstream must be a ws:// or wss:// Jetstream subscribe URL")
	}
	return &Upstream{base: rawURL, log: log, d: d, lookback: lookback, reload: make(chan struct{}, 1)}, nil
}

// Reload asks the reader to reconnect with the current collection set.
func (u *Upstream) Reload() {
	select {
	case u.reload <- struct{}{}:
	default:
	}
}

func (u *Upstream) Run(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		wanted, err := wantedCollections(u.d.Collections())
		if err != nil || len(wanted) == 0 {
			// Never connect without a filter: that would be the whole network.
			if err != nil {
				u.log.Error("cannot build upstream filter", "err", err)
			} else {
				u.log.Warn("no retained collections; upstream paused")
			}
			select {
			case <-ctx.Done():
				return
			case <-u.reload:
			}
			continue
		}
		started := time.Now()
		err = u.consume(ctx, wanted)
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, errReload) {
			u.log.Info("reconnecting upstream with updated collections", "collections", len(wanted))
			backoff = time.Second
			continue
		}
		upstreamReconnects.Inc()
		if time.Since(started) > time.Minute {
			backoff = time.Second
		}
		u.log.Warn("upstream disconnected", "err", err, "retryIn", backoff)
		select {
		case <-ctx.Done():
			return
		case <-u.reload:
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, time.Minute)
	}
}

type jetstreamEvent struct {
	DID    string `json:"did"`
	TimeUS int64  `json:"time_us"`
	Kind   string `json:"kind"`
	Commit *struct {
		Operation  string `json:"operation"`
		Collection string `json:"collection"`
	} `json:"commit"`
}

func (u *Upstream) consume(ctx context.Context, wanted []string) error {
	target, err := url.Parse(u.base)
	if err != nil {
		return err
	}
	q := target.Query()
	q.Del("wantedCollections")
	for _, c := range wanted {
		q.Add("wantedCollections", c)
	}
	cursor := u.cursor.Load()
	if cursor == 0 {
		cursor = time.Now().Add(-u.lookback).UnixMicro()
	}
	q.Set("cursor", strconv.FormatInt(cursor, 10))
	target.RawQuery = q.Encode()

	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second, Proxy: http.ProxyFromEnvironment}
	conn, resp, err := dialer.DialContext(ctx, target.String(), http.Header{"User-Agent": {"hypercerts-pdsdiscovery/" + version.Version}})
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	if err != nil {
		return fmt.Errorf("dialing upstream: %w", err)
	}
	u.log.Info("connected to upstream", "collections", len(wanted), "cursor", cursor)

	const pongWait = 90 * time.Second
	conn.SetReadLimit(4 << 20)
	_ = conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(pongWait)) })

	var reloaded atomic.Bool
	done := make(chan struct{})
	defer close(done)
	go func() {
		ping := time.NewTicker(30 * time.Second)
		defer ping.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				conn.Close()
				return
			case <-u.reload:
				reloaded.Store(true)
				conn.Close()
				return
			case <-ping.C:
				if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(10*time.Second)); err != nil {
					conn.Close()
					return
				}
			}
		}
	}()
	defer conn.Close()

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if reloaded.Load() {
				return errReload
			}
			return err
		}
		_ = conn.SetReadDeadline(time.Now().Add(pongWait))
		var evt jetstreamEvent
		if err := json.Unmarshal(msg, &evt); err != nil {
			continue
		}
		if evt.TimeUS > u.cursor.Load() {
			u.cursor.Store(evt.TimeUS)
		}
		if evt.Kind != "commit" || evt.Commit == nil {
			continue
		}
		if evt.Commit.Operation != "create" && evt.Commit.Operation != "update" {
			continue
		}
		obsCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		u.d.Observe(obsCtx, evt.DID, evt.Commit.Collection)
		cancel()
	}
}

// wantedCollections returns the Jetstream filter. Above the 100-value limit it
// collapses NSIDs into the longest common prefixes ("a.b.*") that fit; the
// Discoverer still filters exact NSIDs.
func wantedCollections(collections []string) ([]string, error) {
	if len(collections) <= maxWantedCollections {
		return collections, nil
	}
	maxDepth := 0
	for _, c := range collections {
		maxDepth = max(maxDepth, strings.Count(c, ".")) // segments - 1
	}
	for depth := maxDepth; depth >= 2; depth-- {
		set := map[string]struct{}{}
		for _, c := range collections {
			parts := strings.Split(c, ".")
			n := min(depth, len(parts)-1)
			set[strings.Join(parts[:n], ".")+".*"] = struct{}{}
		}
		if len(set) <= maxWantedCollections {
			out := make([]string, 0, len(set))
			for p := range set {
				out = append(out, p)
			}
			slices.Sort(out)
			return out, nil
		}
	}
	return nil, fmt.Errorf("%d collections cannot be expressed in %d upstream filters", len(collections), maxWantedCollections)
}
