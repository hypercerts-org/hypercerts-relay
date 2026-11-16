package main

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync/atomic"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/util"
	"github.com/bluesky-social/indigo/xrpc"

	"github.com/urfave/cli/v3"
	"golang.org/x/time/rate"
)

type DidCollection struct {
	Did        string `json:"d"`
	Collection string `json:"c"`
}

// hypercerts: drain crawl results and report buffered output failures to the caller.
func DidCollectionsToCsv(out io.Writer, sources <-chan DidCollection) error {
	writer := csv.NewWriter(out)
	var writeErr error
	var row [2]string
	for dc := range sources {
		row[0] = dc.Did
		row[1] = dc.Collection
		if err := writer.Write(row[:]); err != nil && writeErr == nil {
			writeErr = err
		}
	}
	writer.Flush()
	return errors.Join(writeErr, writer.Error())
}

var offlineCrawlCmd = &cli.Command{
	Name:  "offline_crawl",
	Usage: "crawl a PDS to csv out",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "host",
			Usage: "hostname or URL of PDS",
		},
		&cli.StringFlag{
			Name:  "csv-out",
			Usage: "path for output or - for stdout",
		},
		&cli.Float64Flag{
			Name:  "qps",
			Usage: "queries per second to do vs target PDS",
			Value: 50, // large PDS: 500_000 repos, 10_000 seconds, ~3 hours
		},
	},
	Action: func(ctx context.Context, cmd *cli.Command) error {
		log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hostname := cmd.String("host")
		hosturl, err := url.Parse(hostname)
		if err != nil {
			hosturl = new(url.URL)
			hosturl.Scheme = "https"
			hosturl.Host = hostname
		}
		rpcClient := xrpc.Client{
			Host:   hosturl.String(),
			Client: util.RobustHTTPClient(),
		}
		// hypercerts: offline crawls also never forward shared bypass credentials.
		log.Info("will crawl", "url", rpcClient.Host)
		csvOutPath := cmd.String("csv-out")
		var fout io.Writer = os.Stdout
		var outputFile *os.File
		if csvOutPath != "" && csvOutPath != "-" {
			outputFile, err = os.Create(csvOutPath)
			if err != nil {
				return fmt.Errorf("%s: could not open for writing: %w", csvOutPath, err)
			}
			fout = outputFile
		}
		qps := cmd.Float64("qps")
		results := make(chan DidCollection, 100)
		// hypercerts: wait for the CSV consumer and file close before returning.
		csvDone := make(chan error, 1)
		go func() { csvDone <- DidCollectionsToCsv(fout, results) }()
		crawler := Crawler{
			Ctx:       ctx,
			RpcClient: &rpcClient,
			QPS:       qps,
			Results:   results,
			Log:       log,
		}
		err = crawler.CrawlPDSRepoCollections()
		close(results)
		csvErr := <-csvDone
		var closeErr error
		if outputFile != nil {
			closeErr = outputFile.Close()
		}
		log.Info("done")
		return errors.Join(err, csvErr, closeErr)
	},
}

type Crawler struct {
	Ctx       context.Context
	RpcClient *xrpc.Client
	QPS       float64
	Results   chan<- DidCollection
	Log       *slog.Logger
	Stats     *CrawlStats
}

type CrawlStats struct {
	ReposDescribed atomic.Uint32
}

// CrawlPDSRepoCollections
// write results to chan
// does _not_ close chan
// (allow multiple threads of PDS queries running to one output chan, e.g. feeding into SetFromResults() )
func (cr *Crawler) CrawlPDSRepoCollections() error {
	var cursor string
	limiter := rate.NewLimiter(rate.Limit(cr.QPS), 1)
	for {
		// hypercerts: stop promptly on shutdown rather than starting more requests.
		if err := limiter.Wait(cr.Ctx); err != nil {
			return err
		}
		repos, err := atproto.SyncListRepos(cr.Ctx, cr.RpcClient, cursor, 1000)
		if err != nil {
			return fmt.Errorf("%s: sync repos: %w", cr.RpcClient.Host, err)
		}
		pdsRepoPages.Inc()
		slog.Debug("got repo list", "count", len(repos.Repos))
		for _, xr := range repos.Repos {
			if err := limiter.Wait(cr.Ctx); err != nil {
				return err
			}
			desc, err := atproto.RepoDescribeRepo(cr.Ctx, cr.RpcClient, xr.Did)
			if err != nil {
				erst := err.Error()
				if strings.Contains(erst, "RepoDeactivated") || strings.Contains(erst, "RepoTakendown") {
					slog.Info("repo unavail", "host", cr.RpcClient.Host, "did", xr.Did, "err", err)
				} else {
					slog.Warn("repo desc", "host", cr.RpcClient.Host, "did", xr.Did, "err", err)
				}
				continue
			}
			pdsReposDescribed.Inc()
			for _, collection := range desc.Collections {
				select {
				case cr.Results <- DidCollection{Did: xr.Did, Collection: collection}:
				case <-cr.Ctx.Done():
					return cr.Ctx.Err()
				}
			}
			if cr.Stats != nil {
				cr.Stats.ReposDescribed.Add(1)
			}
		}
		if repos.Cursor != nil {
			cursor = *repos.Cursor
		} else {
			break
		}
	}
	return nil
}
