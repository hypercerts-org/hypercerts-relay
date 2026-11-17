package main

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/util/ssrf"
	"github.com/bluesky-social/indigo/util/svcutil"
	"github.com/bluesky-social/indigo/xrpc"

	"github.com/hashicorp/golang-lru/v2"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/urfave/cli/v3"
)

var serveCmd = &cli.Command{
	Name: "serve",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "api-listen",
			Value:   ":2510",
			Sources: cli.EnvVars("COLLECTIONS_API_LISTEN"),
		},
		&cli.StringFlag{
			Name:    "metrics-listen",
			Value:   ":2511",
			Sources: cli.EnvVars("COLLECTIONS_METRICS_LISTEN"),
		},
		&cli.StringFlag{
			Name:     "pebble",
			Usage:    "path to store pebble db",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "dau-directory",
			Usage:    "directory to store DAU pebble db",
			Required: true,
		},
		&cli.StringFlag{
			Name:    "upstream",
			Usage:   "URL, e.g. wss://bsky.network",
			Sources: cli.EnvVars("COLLECTIONS_UPSTREAM"),
		},
		&cli.StringFlag{
			Name:    "admin-token",
			Usage:   "admin authentication",
			Sources: cli.EnvVars("COLLECTIONS_ADMIN_TOKEN"),
		},
		&cli.Float64Flag{
			Name:  "crawl-qps",
			Usage: "per-PDS crawl queries-per-second limit",
			Value: 100,
		},
		&cli.Uint64Flag{
			Name:    "clist-min-dids",
			Usage:   "filter collection list to >= N dids",
			Value:   5,
			Sources: cli.EnvVars("COLLECTIONS_CLIST_MIN_DIDS"),
		},
		&cli.IntFlag{
			Name:    "max-did-collections",
			Usage:   "stop recording new collections per did after it has >= this many collections",
			Value:   1000,
			Sources: cli.EnvVars("COLLECTIONS_MAX_DID_COLLECTIONS"),
		},
		&cli.StringFlag{
			Name:    "sets-json-path",
			Usage:   "file path of JSON file containing static word sets",
			Sources: cli.EnvVars("HEPA_SETS_JSON_PATH", "COLLECTIONS_SETS_JSON_PATH"),
		},
		&cli.BoolFlag{
			Name: "verbose",
		},
	},
	Action: func(ctx context.Context, cmd *cli.Command) error {
		var server collectionServer
		return server.run(ctx, cmd)
	},
}

type BadwordChecker interface {
	HasBadword(string) bool
}

type collectionServer struct {
	ctx context.Context

	// the primary directory, all repos ever and their collections
	pcd *PebbleCollectionDirectory

	// daily-active-user directory, new directory every 00:00:00 UTC
	dauDirectory     *PebbleCollectionDirectory
	dauDirectoryPath string    // currently open dauDirectory, {dauDirectoryDir}/{YYYY}{mm}{dd}.pebble
	dauDay           time.Time // YYYY-MM-DD 00:00:00 UTC
	dauTomorrow      time.Time
	dauDirectoryDir  string

	statsCache        *CollectionStats
	statsCacheWhen    time.Time
	statsCacheLock    sync.Mutex
	statsCacheFresh   sync.Cond
	statsCachePending bool

	// (did,collection) pairs from firehose
	ingestFirehose chan DidCollection
	// (did,collection) pairs from PDS crawl (don't apply to dauDirectory)
	ingestCrawl chan DidCollection

	log *slog.Logger

	AdminToken         string
	ExepctedAuthHeader string
	PerPDSCrawlQPS     float64

	activeCrawls     map[string]activeCrawl
	activeCrawlsLock sync.Mutex

	// hypercerts: latest ended crawl per host, guarded by activeCrawlsLock and
	// bounded by maxCrawlResults, so administrators can see an outcome after a
	// crawl leaves activeCrawls.
	crawlResults map[string]crawlResult
	// hypercerts: reported by crawlStatus so clients can detect a restart that
	// discarded in-memory crawl state.
	processStart time.Time
	// hypercerts: test seam; nil uses the public-only crawl transport.
	crawlHTTPClient *http.Client

	shutdown chan struct{}

	wg           sync.WaitGroup
	producerWG   sync.WaitGroup
	backgroundWG sync.WaitGroup
	cancel       context.CancelFunc

	apiServer     *http.Server
	metricsServer *http.Server

	MinDidsForCollectionList uint64
	MaxDidCollections        int

	didCollectionCounts *lru.Cache[string, int]

	badwords BadwordChecker
}

type activeCrawl struct {
	start time.Time
	stats *CrawlStats
}

// hypercerts: bounded in-memory history of ended crawls; lost on restart.
const maxCrawlResults = 256

type crawlResult struct {
	start          time.Time
	end            time.Time
	reposDescribed uint32
	describeErrors uint32
	errorCategory  string
	httpStatus     int
}

func (cs *collectionServer) run(ctx context.Context, cmd *cli.Command) error {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
	cs.shutdown = make(chan struct{})
	cs.processStart = time.Now() // hypercerts: see crawlStatus process_start.
	level := slog.LevelInfo
	if cmd.Bool("verbose") {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)

	// hypercerts: require an explicit admin secret before opening any services.
	if strings.TrimSpace(cmd.String("admin-token")) == "" {
		return fmt.Errorf("admin-token must not be empty")
	}
	if cmd.IsSet("sets-json-path") {
		badwords, err := loadBadwords(cmd.String("sets-json-path"))
		if err != nil {
			return err
		}
		cs.badwords = badwords
	}
	// hypercerts: apply the configured crawl rate so bootstrap can make progress.
	cs.PerPDSCrawlQPS = cmd.Float64("crawl-qps")
	if cs.PerPDSCrawlQPS <= 0 {
		return fmt.Errorf("crawl-qps must be positive")
	}
	cs.MinDidsForCollectionList = cmd.Uint64("clist-min-dids")
	cs.MaxDidCollections = cmd.Int("max-did-collections")
	cs.ingestFirehose = make(chan DidCollection, 1000)
	cs.ingestCrawl = make(chan DidCollection, 1000)
	var err error
	cs.didCollectionCounts, err = lru.New[string, int](1_000_000) // TODO: configurable LRU size
	if err != nil {
		return fmt.Errorf("lru init, %w", err)
	}
	cs.log = log
	cs.ctx, cs.cancel = context.WithCancel(ctx)
	defer cs.cancel()
	cs.AdminToken = cmd.String("admin-token")
	cs.ExepctedAuthHeader = "Bearer " + cs.AdminToken
	pebblePath := cmd.String("pebble")
	cs.pcd = &PebbleCollectionDirectory{
		log: cs.log,
	}
	err = cs.pcd.Open(pebblePath)
	if err != nil {
		return fmt.Errorf("%s: failed to open pebble db: %w", pebblePath, err)
	}
	cs.dauDirectoryDir = cmd.String("dau-directory")
	if cs.dauDirectoryDir != "" {
		err := cs.openDau()
		if err != nil {
			cs.pcd.Close()
			return err
		}
	}
	cs.statsCacheFresh.L = &cs.statsCacheLock

	apiServerEcho, err := cs.createApiServer(ctx, cmd.String("api-listen"))
	if err != nil {
		if cs.dauDirectory != nil {
			cs.dauDirectory.Close()
		}
		cs.pcd.Close()
		return err
	}
	cs.wg.Add(1)
	go cs.ingestReceiver()
	cs.wg.Add(1)
	go func() { cs.StartApiServer(ctx, apiServerEcho) }()

	cs.createMetricsServer(cmd.String("metrics-listen"))
	cs.wg.Add(1)
	go func() { cs.StartMetricsServer(ctx) }()

	upstream := cmd.String("upstream")
	if upstream != "" {
		fh := Firehose{
			Log:  log,
			Host: upstream,
			Seq:  -1,
		}
		seq, seqok, err := cs.pcd.GetSequence()
		if err != nil {
			cs.log.Warn("db get seq", "err", err)
		} else if seqok {
			fh.Seq = seq
		}
		fhevents := make(chan *events.XRPCStreamEvent, 1000)
		cs.producerWG.Add(1)
		go cs.firehoseThread(&fh, fhevents)
		cs.producerWG.Add(1)
		go cs.handleFirehose(fhevents)
	} else {
		close(cs.ingestFirehose)
	}

	select {
	case <-signals:
	case <-ctx.Done():
	}
	log.Info("received shutdown signal")
	return cs.Shutdown()
}

func (cs *collectionServer) openDau() error {
	now := time.Now().UTC()
	ymd := now.Format("2006-01-02")
	fname := fmt.Sprintf("d%s.pebble", ymd)
	fpath := filepath.Join(cs.dauDirectoryDir, fname)
	daud := &PebbleCollectionDirectory{
		log: cs.log,
	}
	err := daud.Open(fpath)
	if err != nil {
		return fmt.Errorf("%s: failed to open dau pebble db: %w", fpath, err)
	}
	cs.dauDirectory = daud
	cs.dauDirectoryPath = fpath
	cs.dauDay = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	cs.dauTomorrow = cs.dauDay.AddDate(0, 0, 1)
	cs.log.Info("DAU db opened", "path", fpath)
	return nil
}

// hypercerts: retain stores until handlers, producers and all database users exit.
func (cs *collectionServer) Shutdown() error {
	close(cs.shutdown)
	cs.cancel()
	// Shutdown closes listeners immediately and waits for active handlers. Do not
	// close stores on a timeout while a handler can still use them.
	var shutdownErr error
	if cs.apiServer != nil {
		shutdownErr = errors.Join(shutdownErr, cs.apiServer.Shutdown(context.Background()))
	}
	if cs.metricsServer != nil {
		shutdownErr = errors.Join(shutdownErr, cs.metricsServer.Shutdown(context.Background()))
	}
	cs.producerWG.Wait()
	close(cs.ingestCrawl)
	cs.wg.Wait()
	cs.backgroundWG.Wait()
	if cs.dauDirectory != nil {
		shutdownErr = errors.Join(shutdownErr, cs.dauDirectory.Close())
	}
	return errors.Join(shutdownErr, cs.pcd.Close())
}

// firehoseThreads is responsible for connecting to upstream firehose source
func (cs *collectionServer) firehoseThread(fh *Firehose, fhevents chan<- *events.XRPCStreamEvent) {
	defer cs.producerWG.Done()
	if err := fh.subscribeWithRedialer(cs.ctx, fhevents); err != nil {
		cs.log.Error("failed to subscribe to redialer", "err", err)
	}
}

// handleFirehose consumes XRPCStreamEvent from firehoseThread(), further parses data and applies
// hypercerts: drain accepted stream events before closing the ingestion queue.
func (cs *collectionServer) handleFirehose(fhevents <-chan *events.XRPCStreamEvent) {
	defer cs.producerWG.Done()
	defer close(cs.ingestFirehose)
	for evt := range fhevents {
		firehoseReceivedCounter.Inc()
		if evt.RepoCommit != nil {
			firehoseCommits.Inc()
			cs.handleCommit(evt.RepoCommit)
		}
		// The cursor is deliberately not advanced: queueing is not durable
		// processing and write failures cannot currently be associated with events.
	}
}

func (cs *collectionServer) handleCommit(commit *comatproto.SyncSubscribeRepos_Commit) {
	for _, op := range commit.Ops {
		// op.Path is collection/rkey
		nsid, _, err := syntax.ParseRepoPath(op.Path)
		if err != nil {
			cs.log.Warn("bad op path", "repo", commit.Repo, "err", err)
			return
		}
		firehoseCommitOps.WithLabelValues(op.Action).Inc()
		if op.Action == "create" || op.Action == "update" {
			firehoseDidcSet.Inc()
			cs.ingestFirehose <- DidCollection{
				Did:        commit.Repo,
				Collection: nsid.String(),
			}
		}
	}
}

func (cs *collectionServer) createMetricsServer(addr string) {
	e := echo.New()
	e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))
	e.Any("/debug/pprof/*", echo.WrapHandler(http.DefaultServeMux))

	cs.metricsServer = &http.Server{
		Addr:    addr,
		Handler: e,
	}
}

func (cs *collectionServer) StartMetricsServer(ctx context.Context) {
	defer cs.wg.Done()
	defer cs.log.Info("metrics server exit")

	err := cs.metricsServer.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("error in metrics server", "err", err)
		os.Exit(1)
	}
}

func (cs *collectionServer) createApiServer(ctx context.Context, addr string) (*echo.Echo, error) {
	var lc net.ListenConfig
	li, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	e := echo.New()
	e.HideBanner = true

	e.Use(svcutil.MetricsMiddleware)
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowHeaders: []string{echo.HeaderOrigin, echo.HeaderContentType, echo.HeaderAccept, echo.HeaderAuthorization},
	}))

	e.GET("/_health", cs.healthz)

	e.GET("/xrpc/com.atproto.sync.listReposByCollection", cs.getDidsForCollection)
	e.GET("/v1/getDidsForCollection", cs.getDidsForCollection)
	e.GET("/v1/listCollections", cs.listCollections)

	// TODO: allow public 'requestCrawl' API?
	//e.GET("/xrpc/com.atproto.sync.requestCrawl", cs.crawlPds)
	//e.POST("/xrpc/com.atproto.sync.requestCrawl", cs.crawlPds)

	// admin auth heador required
	e.POST("/admin/pds/requestCrawl", cs.crawlPds) // same as relay
	e.GET("/admin/crawlStatus", cs.crawlStatus)

	e.Listener = li
	srv := &http.Server{
		Handler: e,
	}
	cs.apiServer = srv
	return e, nil
}

func (cs *collectionServer) StartApiServer(ctx context.Context, e *echo.Echo) {
	defer cs.wg.Done()
	defer cs.log.Info("api server exit")
	err := cs.apiServer.Serve(e.Listener)
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("error in api server", "err", err)
		os.Exit(1)
	}
}

const statsCacheDuration = time.Second * 300

func getLimit(c echo.Context, min, defaultLim, max int) int {
	limstr := c.QueryParam("limit")
	if limstr == "" {
		return defaultLim
	}
	lvx, err := strconv.ParseInt(limstr, 10, 64)
	if err != nil {
		return defaultLim
	}
	lv := int(lvx)
	if lv < min {
		return min
	}
	if lv > max {
		return max
	}
	return lv
}

// /xrpc/com.atproto.sync.listReposByCollection?collection={}&cursor={}&limit={50<=N<=1000}
// /v1/getDidsForCollection?collection={}&cursor={}&limit={50<=N<=1000}
//
// returns
// {"dids":["did:A", "..."], "cursor":"opaque text"}
func (cs *collectionServer) getDidsForCollection(c echo.Context) error {
	ctx := c.Request().Context()
	collection := c.QueryParam("collection")
	_, err := syntax.ParseNSID(collection)
	if err != nil {
		return c.JSON(http.StatusBadRequest, xrpc.XRPCError{ErrStr: "BadRequest", Message: fmt.Sprintf("bad collection nsid, %s", err.Error())})
	}
	cursor := c.QueryParam("cursor")
	limit := getLimit(c, 1, 500, 10_000)
	they, nextCursor, err := cs.pcd.ReadCollection(ctx, collection, cursor, limit)
	if err != nil {
		slog.Error("ReadCollection", "collection", collection, "cursor", cursor, "limit", limit, "err", err)
		return c.JSON(http.StatusInternalServerError, xrpc.XRPCError{ErrStr: "DatabaseError", Message: "failed to read DIDs for collection"})
	}
	cs.log.Info("getDidsForCollection", "collection", collection, "cursor", cursor, "limit", limit, "count", len(they), "nextCursor", nextCursor)
	var out comatproto.SyncListReposByCollection_Output
	out.Repos = make([]*comatproto.SyncListReposByCollection_Repo, len(they))
	for i, rec := range they {
		out.Repos[i] = &comatproto.SyncListReposByCollection_Repo{Did: rec.Did}
	}
	if nextCursor != "" {
		out.Cursor = &nextCursor
	}
	return c.JSON(http.StatusOK, out)
}

// return cached collection stats if they're fresh
// return new collection stats if they can be calculated quickly
// return stale cached collection stats if new stats take too long
// just wait for fresh stats if there are no cached stats
// stalenessAllowed is how old stats can be before we try to recalculate them, 0=default of 5 minutes
func (cs *collectionServer) getStatsCache(stalenessAllowed time.Duration) (*CollectionStats, error) {
	if stalenessAllowed <= 0 {
		stalenessAllowed = statsCacheDuration
	}
	var statsCache *CollectionStats
	var staleCache *CollectionStats
	var waiter *freshStatsWaiter
	cs.statsCacheLock.Lock()
	if cs.statsCache != nil {
		if time.Since(cs.statsCacheWhen) < stalenessAllowed {
			// has fresh!
			statsCache = cs.statsCache
		} else if !cs.statsCachePending {
			cs.statsCachePending = true
			cs.backgroundWG.Add(1)
			go cs.statsBuilder()
			staleCache = cs.statsCache
		} else {
			staleCache = cs.statsCache
		}
		if staleCache != nil {
			waiter = &freshStatsWaiter{
				cs: cs,
				// hypercerts: a timed-out requester must not block the stats lock.
				freshCache: make(chan *CollectionStats, 1),
			}
			go waiter.waiter()
		}
	} else if !cs.statsCachePending {
		cs.statsCachePending = true
		// hypercerts: register before the HTTP handler returns; shutdown joins handlers before background workers.
		cs.backgroundWG.Add(1)
		go cs.statsBuilder()
	}
	cs.statsCacheLock.Unlock()

	if statsCache != nil {
		// return fresh-enough data
		return statsCache, nil
	}

	if staleCache == nil {
		// block forever waiting for fresh data
		cs.statsCacheLock.Lock()
		for cs.statsCache == nil && cs.statsCachePending {
			cs.statsCacheFresh.Wait()
		}
		statsCache = cs.statsCache
		cs.statsCacheLock.Unlock()
		if statsCache == nil {
			return nil, errors.New("stats unavailable during shutdown")
		}
		return statsCache, nil
	}

	// wait for up to a second for fresh data, on timeout return stale data
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()
	select {
	case <-timeout.C:
		cs.statsCacheLock.Lock()
		waiter.l.Lock()
		waiter.obsolete = true
		waiter.l.Unlock()
		cs.statsCacheLock.Unlock()
		return staleCache, nil
	case statsCache = <-waiter.freshCache:
		return statsCache, nil
	}
}

type freshStatsWaiter struct {
	cs         *collectionServer
	l          sync.Mutex
	obsolete   bool
	freshCache chan *CollectionStats
}

func (fsw *freshStatsWaiter) waiter() {
	fsw.cs.statsCacheLock.Lock()
	defer fsw.cs.statsCacheLock.Unlock()
	fsw.cs.statsCacheFresh.Wait()
	fsw.l.Lock()
	defer fsw.l.Unlock()
	if fsw.obsolete {
		close(fsw.freshCache)
	} else {
		fsw.freshCache <- fsw.cs.statsCache
	}
}

func (cs *collectionServer) statsBuilder() {
	defer cs.backgroundWG.Done()
	for {
		start := time.Now()
		stats, err := cs.pcd.GetCollectionStats()
		dt := time.Since(start)
		if err == nil {
			statsCalculations.Observe(dt.Seconds())
			countsum := uint64(0)
			for _, v := range stats.CollectionCounts {
				countsum += v
			}
			cs.log.Info("stats built", "dt", dt, "total", countsum)
			cs.statsCacheLock.Lock()
			cs.statsCache = &stats
			cs.statsCacheWhen = time.Now()
			cs.statsCacheFresh.Broadcast()
			cs.statsCachePending = false
			cs.statsCacheLock.Unlock()
			return
		} else {
			cs.log.Error("GetCollectionStats", "dt", dt, "err", err)
			select {
			case <-time.After(2 * time.Second):
			case <-cs.shutdown:
				cs.statsCacheLock.Lock()
				cs.statsCachePending = false
				cs.statsCacheFresh.Broadcast()
				cs.statsCacheLock.Unlock()
				return
			}
		}
	}
}

func (cs *collectionServer) hasBadword(collection string) bool {
	if cs.badwords != nil {
		return cs.badwords.HasBadword(collection)
	}
	return false
}

// /v1/listCollections?c={}&cursor={}&limit={50<=limit<=1000}
//
// admin may set ?stalesec={} for a maximum number of seconds stale data is accepted
//
// returns
// {"collections":{"app.bsky.feed.post": 123456789, "some collection": 42}, "cursor":"opaque text"}
func (cs *collectionServer) listCollections(c echo.Context) error {
	stalenessAllowed := statsCacheDuration
	stalesecStr := c.QueryParam("stalesec")
	if stalesecStr != "" && cs.isAdmin(c) {
		stalesec, err := strconv.ParseInt(stalesecStr, 10, 64)
		if err != nil {
			return c.JSON(http.StatusBadRequest, xrpc.XRPCError{ErrStr: "BadRequest", Message: "invalid 'stalesec' query parameter"})
		}
		if stalesec == 0 {
			stalenessAllowed = 1
		} else {
			stalenessAllowed = time.Duration(stalesec) * time.Second
		}
		cs.log.Info("stalesec", "q", stalesecStr, "d", stalenessAllowed)
	}
	stats, err := cs.getStatsCache(stalenessAllowed)
	if err != nil {
		slog.Error("getStatsCache", "err", err)
		return c.JSON(http.StatusInternalServerError, xrpc.XRPCError{ErrStr: "DatabaseError", Message: "failed to read stats"})
	}
	cursor := c.QueryParam("cursor")
	collections, hasQueryCollections := c.QueryParams()["c"]
	limit := getLimit(c, 50, 500, 1000)
	var out ListCollectionsResponse
	if hasQueryCollections {
		out.Collections = make(map[string]uint64, len(collections))
		for _, collection := range collections {
			count, ok := stats.CollectionCounts[collection]
			if ok {
				out.Collections[collection] = count
			}
		}
	} else {
		allCollections := make([]string, 0, len(stats.CollectionCounts))
		for collection := range stats.CollectionCounts {
			allCollections = append(allCollections, collection)
		}
		sort.Strings(allCollections)
		out.Collections = make(map[string]uint64, limit)
		count := 0
		for _, collection := range allCollections {
			if (cursor == "") || (collection > cursor) {
				if cs.hasBadword(collection) {
					// don't show badwords in public list of collections
					continue
				}
				if stats.CollectionCounts[collection] < cs.MinDidsForCollectionList {
					// don't show experimental/spam collections only implemented by a few DIDs
					continue
				}
				// TODO: probably regex based filter for collection-spam
				out.Collections[collection] = stats.CollectionCounts[collection]
				count++
				if count >= limit {
					out.Cursor = collection
				}
			}
		}
	}
	return c.JSON(http.StatusOK, out)
}

type ListCollectionsResponse struct {
	Collections map[string]uint64 `json:"collections"`
	Cursor      string            `json:"cursor"`
}

// hypercerts: drain both queues after their producers finish, even on errors.
func (cs *collectionServer) ingestReceiver() {
	defer cs.wg.Done()
	firehose, crawl := cs.ingestFirehose, cs.ingestCrawl
	for firehose != nil || crawl != nil {
		select {
		case didc, ok := <-firehose:
			if !ok {
				firehose = nil
				continue
			}
			if err := cs.ingestDidc(didc, true); err != nil {
				cs.log.Error("firehose ingest", "err", err)
			}
		case didc, ok := <-crawl:
			if !ok {
				crawl = nil
				continue
			}
			if err := cs.ingestDidc(didc, false); err != nil {
				cs.log.Error("crawl ingest", "err", err)
			}
		}
	}
}

func (cs *collectionServer) ingestDidc(didc DidCollection, dau bool) error {
	count, ok := cs.didCollectionCounts.Get(didc.Did)
	var err error
	if !ok {
		count, err = cs.pcd.CountDidCollections(didc.Did)
		if err != nil {
			return fmt.Errorf("count did collections, %s %w", didc.Did, err)
		}
		cs.didCollectionCounts.Add(didc.Did, count)
	}
	if count >= cs.MaxDidCollections {
		cs.log.Warn("did too many collections", "did", didc.Did)
		return nil
	}
	err = cs.pcd.MaybeSetCollection(didc.Did, didc.Collection)
	if err != nil {
		cs.log.Warn("pcd write", "err", err)
		return err
	}
	if dau && cs.dauDirectory != nil {
		err = cs.maybeDauWrite(didc)
		if err != nil {
			cs.log.Warn("dau write", "err", err)
			return err
		}
	}
	return nil
}

func (cs *collectionServer) maybeDauWrite(didc DidCollection) error {
	now := time.Now()
	if now.After(cs.dauTomorrow) {
		cs.backgroundWG.Add(1)
		go func(oldDau *PebbleCollectionDirectory, day time.Time) {
			defer cs.backgroundWG.Done()
			dauStats(oldDau, day, cs.dauDirectoryDir, cs.log)
		}(cs.dauDirectory, cs.dauDay)
		cs.dauDirectory = nil
		err := cs.openDau()
		if err != nil {
			return fmt.Errorf("dau reopen, %w", err)
		}
	}
	return cs.dauDirectory.MaybeSetCollection(didc.Did, didc.Collection)
}

// write {dauDirectoryDir}/d{YYYY-MM-DD}.pebble stats summary to {dauDirectoryDir}/d{YYYY-MM-DD}.csv.gz
func dauStats(oldDau *PebbleCollectionDirectory, dauDay time.Time, dauDir string, log *slog.Logger) {
	fname := fmt.Sprintf("d%s.csv.gz", dauDay.Format("2006-01-02"))
	outstatsPath := filepath.Join(dauDir, fname)
	log = log.With("path", outstatsPath)
	log.Info("DAU stats summarize")
	stats, err := oldDau.GetCollectionStats()
	e2 := oldDau.Close()
	if e2 != nil {
		log.Error("old DAU close", "err", e2)
	}
	if err != nil {
		log.Error("old DAU stats", "err", err)
	} else {
		log.Info("DAU stats summarized", "rows", len(stats.CollectionCounts))
		pcdStatsToCsvGz(stats, outstatsPath, log)
	}
}

func pcdStatsToCsvGz(stats CollectionStats, outpath string, log *slog.Logger) {
	fout, err := os.Create(outpath)
	if err != nil {
		log.Error("DAU stats open", "err", err)
		return
	}
	defer fout.Close()
	gzout := gzip.NewWriter(fout)
	defer gzout.Close()
	csvout := csv.NewWriter(gzout)
	defer csvout.Flush()
	err = csvout.Write([]string{"collection", "count"})
	if err != nil {
		log.Error("DAU stats header", "err", err)
		return
	}
	var row [2]string
	rowcount := 0
	for collection, count := range stats.CollectionCounts {
		row[0] = collection
		row[1] = strconv.FormatUint(count, 10)
		err = csvout.Write(row[:])
		if err != nil {
			log.Error("DAU stats row", "err", err)
			return
		}
		rowcount++
	}
	log.Info("DAU stats ok", "rows", rowcount)
}

type CrawlRequest struct {
	Host  string   `json:"hostname,omitempty"`
	Hosts []string `json:"hosts,omitempty"`
}

type CrawlRequestResponse struct {
	Message string `json:"message,omitempty"`
	Error   string `json:"error,omitempty"`
	// hypercerts: per-host outcome keyed by normalized host URL, so callers can
	// tell a new crawl from one that was already active.
	Hosts map[string]CrawlRequestHost `json:"hosts"`
}

// hypercerts: State is "started" or "already_active"; Start identifies the
// crawl in crawlStatus and matches its RFC 3339 formatting.
type CrawlRequestHost struct {
	State string `json:"state"`
	Start string `json:"start"`
}

func hostOrUrlToUrl(host string) string {
	xu, err := url.Parse(host)
	if err != nil {
		xu = new(url.URL)
		xu.Host = host
		xu.Scheme = "https"
		return xu.String()
	} else if xu.Scheme == "" {
		xu.Scheme = "https"
		return xu.String()
	}
	return host
}

func (cs *collectionServer) isAdmin(c echo.Context) bool {
	authHeader := c.Request().Header.Get("Authorization")
	if authHeader == "" {
		return false
	}
	if authHeader == cs.ExepctedAuthHeader {
		return true
	}
	// hypercerts: never log either supplied or expected credentials.
	return false
}

// /admin/pds/requestCrawl
// same API signature as relay admin requestCrawl
// starts a crawl and returns. See /v1/crawlStatus
// requires header `Authorization: Bearer {admin token}`
//
// POST {"hostname":"one hostname or URL", "hosts":["up to 1000 hosts", "..."]}
// OR
// POST /admin/pds/requestCrawl?host={one host}
//
// hypercerts: the reply's "hosts" map reports each normalized host as
// "started" or "already_active" with the crawl start time.
func (cs *collectionServer) crawlPds(c echo.Context) error {
	isAdmin := cs.isAdmin(c)
	if !isAdmin {
		return c.JSON(http.StatusForbidden, xrpc.XRPCError{ErrStr: "AdminRequired", Message: "this endpoint requires admin auth"})
	}
	reply := CrawlRequestResponse{Message: "ok", Hosts: make(map[string]CrawlRequestHost)}
	hostQ := c.QueryParam("host")
	if hostQ != "" {
		cs.startCrawlReply(hostQ, reply.Hosts)
		return c.JSON(http.StatusOK, reply)
	}

	var req CrawlRequest
	err := c.Bind(&req)
	if err != nil {
		cs.log.Info("bad crawl bind", "err", err)
		return c.JSON(http.StatusBadRequest, xrpc.XRPCError{ErrStr: "BadRequest", Message: fmt.Sprintf("failed to parse body: %s", err)})
	}
	if req.Host != "" {
		cs.startCrawlReply(req.Host, reply.Hosts)
	}
	for _, host := range req.Hosts {
		cs.startCrawlReply(host, reply.Hosts)
	}
	return c.JSON(http.StatusOK, reply)
}

// hypercerts: record one host's synchronous registration outcome in a reply.
func (cs *collectionServer) startCrawlReply(hostIn string, hosts map[string]CrawlRequestHost) {
	host, started, start := cs.startCrawl(hostIn)
	state := "already_active"
	if started {
		state = "started"
	}
	hosts[host] = CrawlRequestHost{State: state, Start: formatCrawlTime(start)}
}

// hypercerts: register crawls before the HTTP handler returns so shutdown can
// join them and the caller learns whether a crawl started. A duplicate returns
// the active crawl's start and spawns nothing.
func (cs *collectionServer) startCrawl(hostIn string) (host string, started bool, start time.Time) {
	host = hostOrUrlToUrl(hostIn)
	if host != hostIn {
		cs.log.Info("going to crawl", "in", hostIn, "as", host)
	}
	ok, start, crawlStats := cs.recordCrawlStart(host, time.Now())
	if !ok {
		cs.log.Info("not crawling dup", "host", host)
		return host, false, start
	}
	cs.producerWG.Add(1)
	go func() {
		defer cs.producerWG.Done()
		cs.crawlThread(host, start, crawlStats)
	}()
	return host, true, start
}

// hypercerts: the crawl is registered by startCrawl before this runs.
func (cs *collectionServer) crawlThread(host string, start time.Time, crawlStats *CrawlStats) {
	httpClient := cs.crawlHTTPClient
	if httpClient == nil {
		httpClient = &http.Client{
			Transport: ssrf.PublicOnlyTransport(),
		}
	}
	rpcClient := xrpc.Client{
		Host:   host,
		Client: httpClient,
	}
	// hypercerts: never forward shared bypass credentials to crawl targets.
	crawler := Crawler{
		Ctx:       cs.ctx,
		RpcClient: &rpcClient,
		QPS:       cs.PerPDSCrawlQPS,
		Results:   cs.ingestCrawl,
		Log:       cs.log,
	}
	crawler.Stats = crawlStats
	cs.log.Info("crawling", "host", host)
	err := crawler.CrawlPDSRepoCollections()
	cs.finishCrawl(host, start, crawlStats, err)
	pdsCrawledCounter.Inc()
	if err != nil {
		cs.log.Warn("crawl err", "host", host, "err", err)
	} else {
		dt := time.Since(start)
		cs.log.Info("crawl done", "host", host, "dt", dt)
	}
}

// recordCrawlStart returns true if ok, false if duplicate
// hypercerts: a duplicate also returns the active crawl's start.
func (cs *collectionServer) recordCrawlStart(host string, start time.Time) (ok bool, activeStart time.Time, stats *CrawlStats) {
	cs.activeCrawlsLock.Lock()
	defer cs.activeCrawlsLock.Unlock()
	if cs.activeCrawls == nil {
		cs.activeCrawls = make(map[string]activeCrawl)
	} else {
		existing, dup := cs.activeCrawls[host]
		if dup {
			return false, existing.start, nil
		}
	}
	stats = new(CrawlStats)
	cs.activeCrawls[host] = activeCrawl{
		start: start,
		stats: stats,
	}
	return true, start, stats
}

// hypercerts: end an active crawl and retain its outcome in one critical
// section, so a status reader never sees it in neither or both maps. The
// oldest result is evicted beyond maxCrawlResults.
func (cs *collectionServer) finishCrawl(host string, start time.Time, stats *CrawlStats, err error) {
	result := crawlResult{
		start:          start,
		end:            time.Now(),
		reposDescribed: stats.ReposDescribed.Load(),
		describeErrors: stats.DescribeErrors.Load(),
	}
	if err != nil {
		result.errorCategory, result.httpStatus = classifyCrawlError(err)
	}
	cs.activeCrawlsLock.Lock()
	defer cs.activeCrawlsLock.Unlock()
	if active, ok := cs.activeCrawls[host]; ok && active.start.Equal(start) {
		delete(cs.activeCrawls, host)
	}
	if cs.crawlResults == nil {
		cs.crawlResults = make(map[string]crawlResult)
	}
	cs.crawlResults[host] = result
	for len(cs.crawlResults) > maxCrawlResults {
		oldestHost := ""
		var oldestEnd time.Time
		for candidate, previous := range cs.crawlResults {
			if oldestHost == "" || previous.end.Before(oldestEnd) {
				oldestHost, oldestEnd = candidate, previous.end
			}
		}
		delete(cs.crawlResults, oldestHost)
	}
}

// hypercerts: reduce a crawl failure to a bounded category and optional HTTP
// status. Raw error text can contain upstream response content and is only
// logged, never returned to API clients.
func classifyCrawlError(err error) (category string, httpStatus int) {
	var xrpcErr *xrpc.Error
	var netErr net.Error
	var opErr *net.OpError
	var urlErr *url.Error
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled", 0
	case errors.Is(err, ssrf.ErrUnsafeNetworkAddress):
		return "blocked_address", 0
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &netErr) && netErr.Timeout():
		return "timeout", 0
	case errors.As(err, &xrpcErr):
		return "http", xrpcErr.StatusCode
	case errors.As(err, &opErr), errors.As(err, &urlErr):
		return "transport", 0
	default:
		return "unknown", 0
	}
}

// hypercerts: one format for crawl times, so a request reply's start matches
// crawlStatus and identifies the crawl.
func formatCrawlTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

type CrawlStatusResponse struct {
	HostCrawls map[string]HostCrawl `json:"host_starts"`
	ServerTime string               `json:"server_time"`
	// hypercerts: additive fields; see HostCrawlResult and processStart.
	ProcessStart string                     `json:"process_start,omitempty"`
	HostResults  map[string]HostCrawlResult `json:"host_results"`
}
type HostCrawl struct {
	Start          string `json:"start"`
	ReposDescribed uint32 `json:"seen"`
	DescribeErrors uint32 `json:"describe_errors"` // hypercerts: describeRepo failures skipped so far.
}

// hypercerts: the latest ended crawl for a host. State is "finished" when the
// repository listing ended without error, otherwise "failed".
type HostCrawlResult struct {
	Start          string `json:"start"`
	End            string `json:"end"`
	ReposDescribed uint32 `json:"seen"`
	DescribeErrors uint32 `json:"describe_errors"`
	State          string `json:"state"`
	ErrorCategory  string `json:"error_category,omitempty"`
	HTTPStatus     int    `json:"http_status,omitempty"`
}

// GET /v1/crawlStatus
func (cs *collectionServer) crawlStatus(c echo.Context) error {
	authHeader := c.Request().Header.Get("Authorization")
	if authHeader != cs.ExepctedAuthHeader {
		return c.JSON(http.StatusBadRequest, xrpc.XRPCError{ErrStr: "AdminAuthRequired", Message: "this endpoint requires admin-level auth"})
	}
	var out CrawlStatusResponse
	out.HostCrawls = make(map[string]HostCrawl)
	cs.activeCrawlsLock.Lock()
	defer cs.activeCrawlsLock.Unlock()
	for host, rec := range cs.activeCrawls {
		start := rec.start
		out.HostCrawls[host] = HostCrawl{
			Start:          start.UTC().Format(time.RFC3339Nano),
			ReposDescribed: rec.stats.ReposDescribed.Load(),
			DescribeErrors: rec.stats.DescribeErrors.Load(),
		}
	}
	out.ServerTime = time.Now().UTC().Format(time.RFC3339Nano)
	// hypercerts: report retained crawl outcomes and the process start.
	if !cs.processStart.IsZero() {
		out.ProcessStart = formatCrawlTime(cs.processStart)
	}
	out.HostResults = make(map[string]HostCrawlResult, len(cs.crawlResults))
	for host, result := range cs.crawlResults {
		state := "finished"
		if result.errorCategory != "" {
			state = "failed"
		}
		out.HostResults[host] = HostCrawlResult{
			Start:          formatCrawlTime(result.start),
			End:            formatCrawlTime(result.end),
			ReposDescribed: result.reposDescribed,
			DescribeErrors: result.describeErrors,
			State:          state,
			ErrorCategory:  result.errorCategory,
			HTTPStatus:     result.httpStatus,
		}
	}
	return c.JSON(http.StatusOK, out)
}

func (cs *collectionServer) healthz(c echo.Context) error {
	// TODO: check database or upstream health?
	return c.JSON(http.StatusOK, map[string]any{"status": "ok"})
}

func loadBadwords(path string) (*BadwordsRE, error) {
	fin, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("%s: could not open badwords, %w", path, err)
	}
	dec := json.NewDecoder(fin)
	var rules map[string][]string
	err = dec.Decode(&rules)
	if err != nil {
		return nil, fmt.Errorf("%s: badwords json, %w", path, err)
	}

	// compile a regex to search a string for any instance of a bad word, because we're expecting things runpooptogether
	badwords := rules["worst-words"]
	rwords := make([]string, len(badwords))
	for i, word := range badwords {
		rwords[i] = regexp.QuoteMeta(word)
	}
	reStr := strings.Join(rwords, "|")
	re, err := regexp.Compile(reStr)
	if err != nil {
		return nil, fmt.Errorf("%s: badwords regex, %w", path, err)
	}
	return &BadwordsRE{re: re}, nil
}

type BadwordsRE struct {
	re *regexp.Regexp
}

func (bw *BadwordsRE) HasBadword(s string) bool {
	// TODO: if this is too slow, try more specialized algorithm e.g. https://en.wikipedia.org/wiki/Aho%E2%80%93Corasick_algorithm
	return bw.re.FindString(s) != ""
}
