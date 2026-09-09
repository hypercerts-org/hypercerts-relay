package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/cmd/relay/relay/models"
	"github.com/bluesky-social/indigo/cmd/relay/stream"
	"github.com/bluesky-social/indigo/cmd/relay/stream/schedulers/parallel"
	"github.com/bluesky-social/indigo/util/ssrf"

	"github.com/RussellLuo/slidingwindow"
	"github.com/gorilla/websocket"
)

var ErrFutureCursor = errors.New("host rejected future cursor")

type ProcessMessageFunc func(ctx context.Context, evt *stream.XRPCStreamEvent, hostname string, hostID uint64) error
type PersistCursorFunc func(ctx context.Context, cursors *[]HostCursor) error
type PersistHostStatusFunc func(ctx context.Context, hostID uint64, state models.HostStatus) error

// `Slurper` is the sub-system of the relay which manages active websocket firehose connections to upstream hosts (eg, PDS instances).
//
// It configures rate-limits, tracks cursors, and retries connections. It passes received messages on to the main relay via a callback function. `Slurper` does not talk to the database directly, but does have some callback to persist host state (cursors and hosting status for some error conditions).
type Slurper struct {
	processCallback ProcessMessageFunc
	Config          *SlurperConfig

	subsLk       sync.Mutex
	subs         map[string]*Subscription
	closed       bool
	shutdownOnce sync.Once
	shutdownErr  error

	shutdownChan   chan bool
	shutdownResult chan error

	logger *slog.Logger
}

type SlurperConfig struct {
	// hypercerts: Apply explicit policy before queueing a raw source event.
	WaitRateCapacity    func(context.Context, string) error
	UserAgent           string
	ConcurrencyPerHost  int
	QueueDepthPerHost   int
	PersistCursorPeriod time.Duration

	BaselinePerSecondLimit int64
	BaselinePerHourLimit   int64
	BaselinePerDayLimit    int64
	TrustedPerSecondLimit  int64
	TrustedPerHourLimit    int64
	TrustedPerDayLimit     int64

	// callback functions. technically optional but effectively required
	PersistCursorCallback     PersistCursorFunc
	PersistHostStatusCallback PersistHostStatusFunc
}

func DefaultSlurperConfig() *SlurperConfig {
	// NOTE: many of these defaults are overruled by DefaultRelayConfig, or even process CLI arg defaults
	return &SlurperConfig{
		UserAgent:          "indigo-relay (atproto-relay)",
		ConcurrencyPerHost: 40,
		// NOTE: queue depth doesn't do anything with current parallel scheduler implementation
		QueueDepthPerHost:   1000,
		PersistCursorPeriod: time.Second * 4,

		// these are the minimum event rates for regular public hosts
		BaselinePerSecondLimit: 50,
		BaselinePerHourLimit:   2500,
		BaselinePerDayLimit:    20_000,

		// these are the fixed event rates for trusted hosts (eg, same service provider as relay)
		TrustedPerSecondLimit: 5_000,
		TrustedPerHourLimit:   50_000_000,
		TrustedPerDayLimit:    500_000_000,
	}
}

// represents an active client connection to a remote host
type Subscription struct {
	Hostname string
	HostID   uint64
	LastSeq  atomic.Int64
	Limiters *StreamLimiters

	scheduler *parallel.Scheduler
	lk        sync.RWMutex
	ctx       context.Context
	cancel    func()
	// hypercerts: Track connection completion separately from subscription registration.
	done      chan struct{}
	state     string
	finishErr error
}

// pulls lastSeq from underlying scheduler in to this Subscription
func (sub *Subscription) UpdateSeq() {
	// hypercerts: Protect the scheduler while connections restart and cursors are saved.
	sub.lk.RLock()
	defer sub.lk.RUnlock()
	// possible for this to get called before a connection has fully been set up
	if sub.scheduler == nil {
		return
	}
	seq := sub.scheduler.LastSeq()
	for current := sub.LastSeq.Load(); seq > 0 && seq > current; current = sub.LastSeq.Load() {
		if sub.LastSeq.CompareAndSwap(current, seq) {
			break
		}
	}
}

func (sub *Subscription) HostCursor() HostCursor {
	return HostCursor{
		HostID:  sub.HostID,
		LastSeq: sub.LastSeq.Load(),
	}
}

type StreamLimiterCounts struct {
	PerSecond int64
	PerHour   int64
	PerDay    int64
}

type StreamLimiters struct {
	PerSecond *slidingwindow.Limiter
	PerHour   *slidingwindow.Limiter
	PerDay    *slidingwindow.Limiter
}

func (sl *StreamLimiters) Counts() StreamLimiterCounts {
	return StreamLimiterCounts{
		PerSecond: sl.PerSecond.Limit(),
		PerHour:   sl.PerHour.Limit(),
		PerDay:    sl.PerDay.Limit(),
	}
}

func NewSlurper(processCallback ProcessMessageFunc, config *SlurperConfig) (*Slurper, error) {
	if processCallback == nil {
		return nil, fmt.Errorf("processCallback is required")
	}
	if config == nil {
		config = DefaultSlurperConfig()
	}

	s := &Slurper{
		processCallback: processCallback,
		Config:          config,
		subs:            make(map[string]*Subscription),
		shutdownChan:    make(chan bool),
		shutdownResult:  make(chan error),
		logger:          slog.Default().With("system", "slurper"),
	}

	// Start a goroutine to persist cursors (both periodically and and on shutdown)
	go func() {
		for {
			select {
			case <-s.shutdownChan:
				s.logger.Info("starting shutdown host cursor flush")
				s.shutdownResult <- s.persistCursors(context.Background())
				return
			case <-time.After(config.PersistCursorPeriod):
				if err := s.persistCursors(context.Background()); err != nil {
					s.logger.Error("failed to flush cursors", "err", err)
				}
			}
		}
	}()

	return s, nil
}

func windowFunc() (slidingwindow.Window, slidingwindow.StopFunc) {
	return slidingwindow.NewLocalWindow()
}

func (s *Slurper) ComputeLimiterCounts(accountLimit int64, trusted bool) StreamLimiterCounts {
	if trusted {
		return StreamLimiterCounts{
			PerSecond: s.Config.TrustedPerSecondLimit,
			PerHour:   s.Config.TrustedPerHourLimit,
			PerDay:    s.Config.TrustedPerDayLimit,
		}
	}
	return StreamLimiterCounts{
		PerSecond: s.Config.BaselinePerSecondLimit + (accountLimit / 1000),
		PerHour:   s.Config.BaselinePerHourLimit + accountLimit,
		PerDay:    s.Config.BaselinePerDayLimit + accountLimit*10,
	}
}

func (s *Slurper) UpdateLimiters(hostname string, accountLimit int64, trusted bool) error {

	newLims := s.ComputeLimiterCounts(accountLimit, trusted)

	s.subsLk.Lock()
	defer s.subsLk.Unlock()

	sub, ok := s.subs[hostname]
	if !ok {
		return fmt.Errorf("updating limits for %s: %w", hostname, ErrHostInactive)
	}

	sub.Limiters.PerSecond.SetLimit(newLims.PerSecond)
	sub.Limiters.PerHour.SetLimit(newLims.PerHour)
	sub.Limiters.PerDay.SetLimit(newLims.PerDay)

	return nil
}

func (s *Slurper) GetLimits(hostname string) (*StreamLimiterCounts, error) {
	s.subsLk.Lock()
	defer s.subsLk.Unlock()

	sub, ok := s.subs[hostname]
	if !ok {
		return nil, fmt.Errorf("reading limits for %s: %w", hostname, ErrHostInactive)
	}

	slc := sub.Limiters.Counts()
	return &slc, nil
}

// Shutdown shuts down the entire Slurper (all subscriptions)
func (s *Slurper) Shutdown() error {
	// hypercerts: Finish source processing before shutting down output persistence.
	s.shutdownOnce.Do(func() {
		s.subsLk.Lock()
		s.closed = true
		subs := make([]*Subscription, 0, len(s.subs))
		for _, sub := range s.subs {
			subs = append(subs, sub)
			sub.cancel()
		}
		s.subsLk.Unlock()
		for _, sub := range subs {
			<-sub.done
			s.shutdownErr = errors.Join(s.shutdownErr, sub.finishErr)
		}
		s.shutdownChan <- true
		s.shutdownErr = errors.Join(s.shutdownErr, <-s.shutdownResult)
	})
	return s.shutdownErr
}

func (s *Slurper) CheckIfSubscribed(hostname string) bool {
	s.subsLk.Lock()
	defer s.subsLk.Unlock()

	_, ok := s.subs[hostname]
	return ok
}

// high-level entry point for opening a subscription (websocket connection). This might be called when adding a new host, or when re-connecting to a previously subscribed host.
//
// NOTE: the `host` parameter (a database row) contains metadata about the host at a point in time. Subsequent changes to the database aren't reflected in that struct, and changes to the struct don't get persisted to database.
func (s *Slurper) Subscribe(host *models.Host) error {
	s.subsLk.Lock()
	defer s.subsLk.Unlock()
	if s.closed {
		return fmt.Errorf("slurper is shut down")
	}

	_, ok := s.subs[host.Hostname]
	if ok {
		return fmt.Errorf("already subscribed: %s", host.Hostname)
	}

	counts := s.ComputeLimiterCounts(host.AccountLimit, host.Trusted)
	perSec, _ := slidingwindow.NewLimiter(time.Second, counts.PerSecond, windowFunc)
	perHour, _ := slidingwindow.NewLimiter(time.Hour, counts.PerHour, windowFunc)
	perDay, _ := slidingwindow.NewLimiter(time.Hour*24, counts.PerDay, windowFunc)
	limiters := &StreamLimiters{
		PerSecond: perSec,
		PerHour:   perHour,
		PerDay:    perDay,
	}

	ctx, cancel := context.WithCancel(context.Background())
	sub := Subscription{
		Hostname: host.Hostname,
		HostID:   host.ID,
		Limiters: limiters,
		ctx:      ctx,
		cancel:   cancel,
		done:     make(chan struct{}),
		state:    "configured",
	}
	sub.LastSeq.Store(host.LastSeq)
	s.subs[host.Hostname] = &sub

	go s.subscribeWithRedialer(ctx, host, &sub)

	return nil
}

// Main event-loop for a subscription (websocket connection to upstream host), expected to be called as a goroutine.
//
// On connection failure (drop or failed initial connection), will attempt re-connects, with backoff.
func (s *Slurper) subscribeWithRedialer(ctx context.Context, host *models.Host, sub *Subscription) {

	logger := s.logger.With("host", host.Hostname)
	defer func() {
		// hypercerts: Save completed work even when source cancellation ends the connection.
		sub.UpdateSeq()
		if s.Config.PersistCursorCallback != nil {
			flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			batch := []HostCursor{sub.HostCursor()}
			sub.finishErr = s.Config.PersistCursorCallback(flushCtx, &batch)
			cancel()
		}
		s.subsLk.Lock()
		if s.subs[host.Hostname] == sub {
			delete(s.subs, host.Hostname)
		}
		s.subsLk.Unlock()
		close(sub.done)
	}()

	d := websocket.Dialer{
		HandshakeTimeout: time.Second * 5,
	}

	// if this isn't a localhost / private connection, then we should enable SSRF protections
	if !host.NoSSL {
		netDialer := ssrf.PublicOnlyDialer()
		d.NetDialContext = netDialer.DialContext
	}

	cursor := host.LastSeq

	var backoff int
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		u := host.SubscribeReposURL()
		if cursor > 0 {
			u = fmt.Sprintf("%s?cursor=%d", u, cursor)
		}
		hdr := make(http.Header)
		hdr.Add("User-Agent", s.Config.UserAgent)
		sub.setState("connecting")
		conn, resp, err := d.DialContext(ctx, u, hdr)
		if err != nil {
			sub.setState("failing")
			// hypercerts: Preserve the connection failure reason for operator diagnosis.
			logger.Warn("dialing failed", "backoff", backoff, "err", err)
			timer := time.NewTimer(sleepForBackoff(backoff))
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			backoff++

			if backoff > 15 {
				logger.Warn("host does not appear to be online, disabling for now")
				if s.Config.PersistHostStatusCallback != nil {
					if err := s.Config.PersistHostStatusCallback(ctx, sub.HostID, models.HostStatusOffline); err != nil {
						logger.Error("failed to update host status", "err", err)
					}
				}
				return
			}

			continue
		}

		// check if we connected to a relay (eg, this indigo relay, or rainbow) and drop if so
		serverHdr := resp.Header.Get("Server")
		if strings.Contains(serverHdr, "atproto-relay") {
			_ = conn.Close()
			sub.setState("failing")
			logger.Warn("subscribed host is atproto relay of some kind, banning", "header", "Server", "value", serverHdr, "url", u)
			if err := s.Config.PersistHostStatusCallback(ctx, sub.HostID, models.HostStatusBanned); err != nil {
				logger.Error("failed to update host status", "err", err)
			}
			return
		}

		logger.Debug("event subscription response", "code", resp.StatusCode, "url", u)
		sub.setState("connected")
		connectedInbound.Inc()
		if err := s.handleConnection(ctx, conn, sub); err != nil {

			// TODO: measure the last N connection error times and if they're coming too fast reconnect slower or don't reconnect and wait for requestCrawl
			// hypercerts: Preserve stream-processing errors as well as retry context.
			logger.Warn("host connection failed", "backoff", backoff, "err", err)

			// for all other errors, keep retrying / reconnecting
		}
		connectedInbound.Dec()
		_ = conn.Close()
		sub.setState("failing")

		updatedCursor := sub.LastSeq.Load()
		if updatedCursor > cursor {
			// did we make any progress?
			cursor = updatedCursor
			backoff = 0

			// hypercerts: The deferred bounded flush owns cursor persistence after cancellation.
			if ctx.Err() != nil {
				return
			}

			// persist updated cursor
			if s.Config.PersistCursorCallback != nil {
				batch := []HostCursor{sub.HostCursor()}
				if err := s.Config.PersistCursorCallback(ctx, &batch); err != nil {
					logger.Warn("failed to persist cursor", "err", err)
				}
			}
		}
	}
}

func sleepForBackoff(b int) time.Duration {
	if b == 0 {
		return 0
	}

	if b < 10 {
		return (time.Duration(b) * 2 * time.Second) + (time.Millisecond * time.Duration(rand.Intn(1000)))
	}

	return time.Second * 30
}

// Configures event processing for a websocket connection, using the parallel schedule helper library, with all events processed using the configured callback function.
func (s *Slurper) handleConnection(ctx context.Context, conn *websocket.Conn, sub *Subscription) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	rsc := &stream.RepoStreamCallbacks{
		RepoCommit: func(evt *comatproto.SyncSubscribeRepos_Commit) error {
			logger := s.logger.With("host", sub.Hostname, "did", evt.Repo, "seq", evt.Seq, "eventType", "commit")
			logger.Debug("got remote repo event")
			// hypercerts: Return processing failures before the scheduler acknowledges the source event.
			return s.processCallback(ctx, &stream.XRPCStreamEvent{RepoCommit: evt}, sub.Hostname, sub.HostID)
		},
		RepoSync: func(evt *comatproto.SyncSubscribeRepos_Sync) error {
			logger := s.logger.With("host", sub.Hostname, "did", evt.Did, "seq", evt.Seq, "eventType", "sync")
			logger.Debug("commit event")
			return s.processCallback(ctx, &stream.XRPCStreamEvent{RepoSync: evt}, sub.Hostname, sub.HostID)
		},
		RepoIdentity: func(evt *comatproto.SyncSubscribeRepos_Identity) error {
			logger := s.logger.With("host", sub.Hostname, "did", evt.Did, "seq", evt.Seq, "eventType", "identity")
			logger.Debug("identity event")
			return s.processCallback(ctx, &stream.XRPCStreamEvent{RepoIdentity: evt}, sub.Hostname, sub.HostID)
		},
		RepoAccount: func(evt *comatproto.SyncSubscribeRepos_Account) error {
			logger := s.logger.With("host", sub.Hostname, "did", evt.Did, "seq", evt.Seq, "eventType", "account")
			logger.Debug("account event")
			return s.processCallback(ctx, &stream.XRPCStreamEvent{RepoAccount: evt}, sub.Hostname, sub.HostID)
		},
		Error: func(evt *stream.ErrorFrame) error {
			logger := s.logger.With("host", sub.Hostname)
			logger.Warn("error event from upstream", "name", evt.Error, "message", evt.Message)
			switch evt.Error {
			case "FutureCursor":
				if err := s.Config.PersistHostStatusCallback(ctx, sub.HostID, models.HostStatusIdle); err != nil {
					logger.Error("failed updating host status due to future cursor", "err", err)
				}
				logger.Warn("dropping connection to host due to future cursor")
				sub.cancel()
				return ErrFutureCursor
			default:
				return fmt.Errorf("error frame: %s: %s", evt.Error, evt.Message)
			}
		},
		RepoInfo: func(info *comatproto.SyncSubscribeRepos_Info) error {
			s.logger.Debug("info event", "name", info.Name, "message", info.Message, "host", sub.Hostname)
			return nil
		},
	}

	limiters := []*slidingwindow.Limiter{
		sub.Limiters.PerSecond,
		sub.Limiters.PerHour,
		sub.Limiters.PerDay,
	}

	// NOTE: `InstrumentedRepoStreamCallbacks` is where event limiters get called/enforced
	instrumentedRSC := stream.NewInstrumentedRepoStreamCallbacks(limiters, rsc.EventHandler)

	scheduler := parallel.NewScheduler(
		s.Config.ConcurrencyPerHost,
		s.Config.QueueDepthPerHost,
		conn.RemoteAddr().String(),
		func(_ context.Context, evt *stream.XRPCStreamEvent) error {
			return instrumentedRSC.EventHandler(ctx, evt)
		},
	)
	sub.lk.Lock()
	sub.scheduler = scheduler
	sub.lk.Unlock()
	defer func() {
		sub.UpdateSeq()
		sub.lk.Lock()
		sub.scheduler = nil
		sub.lk.Unlock()
	}()
	// hypercerts: Stop socket reads when asynchronous processing fails.
	go func() {
		select {
		case <-scheduler.Done():
			cancel()
		case <-ctx.Done():
		}
		_ = conn.Close()
	}()
	connLogger := s.logger.With("host", sub.Hostname)
	err := stream.HandleRepoStream(ctx, conn, rateScheduler{Scheduler: scheduler, wait: s.Config.WaitRateCapacity, host: sub.Hostname}, connLogger)
	if processErr := scheduler.Err(); processErr != nil {
		return processErr
	}
	return err
}

type HostCursor struct {
	HostID  uint64
	LastSeq int64
}

// persistCursors sends all cursors to callback to be persisted in database (if registered)
func (s *Slurper) persistCursors(ctx context.Context) error {
	if s.Config.PersistCursorCallback == nil {
		s.logger.Warn("skipping cursor persist because no PersistCursorCallback registered")
		return nil
	}
	start := time.Now()

	// gather cursors: lock overall set, then lock each individual subscription while gathering
	s.subsLk.Lock()
	cursors := make([]HostCursor, len(s.subs))
	i := 0
	for _, sub := range s.subs {
		sub.UpdateSeq()
		cursors[i] = sub.HostCursor()
		i++
	}
	s.subsLk.Unlock()

	err := s.Config.PersistCursorCallback(ctx, &cursors)
	s.logger.Info("finished persisting cursors", "count", len(cursors), "duration", time.Since(start).String(), "err", err)
	return err
}

// gets a snapshot of current subscription hostnames
func (s *Slurper) GetActiveSubHostnames() []string {
	s.subsLk.Lock()
	defer s.subsLk.Unlock()

	var keys []string
	for k := range s.subs {
		keys = append(keys, k)
	}
	return keys
}

func (s *Slurper) KillUpstreamConnection(ctx context.Context, hostname string, ban bool) error {
	s.subsLk.Lock()
	sub, ok := s.subs[hostname]
	s.subsLk.Unlock()
	if !ok {
		return fmt.Errorf("killing connection %q: %w", hostname, ErrHostInactive)
	}
	if ban && s.Config.PersistHostStatusCallback != nil {
		if err := s.Config.PersistHostStatusCallback(ctx, sub.HostID, models.HostStatusBanned); err != nil {
			return fmt.Errorf("failed to set host as banned: %w", err)
		}
	}
	// hypercerts: Persist the ban before cancellation and wait without holding the subscription lock.
	return s.StopSource(ctx, hostname)
}
