package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

// hypercerts: stats builders must be registered once before their request handler returns.
func TestShutdownAfterStatsRequest(t *testing.T) {
	for _, stale := range []bool{false, true} {
		name := "first request"
		if stale {
			name = "stale refresh"
		}
		t.Run(name, func(t *testing.T) {
			var db PebbleCollectionDirectory
			if err := db.Open(filepath.Join(t.TempDir(), "primary")); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cs := &collectionServer{ctx: ctx, cancel: cancel, shutdown: make(chan struct{}), pcd: &db, log: slog.New(slog.NewTextHandler(io.Discard, nil)), ingestCrawl: make(chan DidCollection)}
			cs.statsCacheFresh.L = &cs.statsCacheLock
			if stale {
				cs.statsCache = &CollectionStats{}
				cs.statsCacheWhen = time.Now().Add(-2 * statsCacheDuration)
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			cs.apiServer = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if _, err := cs.getStatsCache(0); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
				}
			})}
			cs.wg.Add(1)
			go func() { defer cs.wg.Done(); _ = cs.apiServer.Serve(listener) }()
			resp, err := http.Get("http://" + listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("stats request: %s", resp.Status)
			}
			done := make(chan error, 1)
			go func() { done <- cs.Shutdown() }()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("shutdown did not finish after stats request")
			}
		})
	}
}

// hypercerts: exercise shutdown while an HTTP handler and a producer still own work.
func TestShutdownWaitsForHandlersAndDrainsProducers(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "primary")
	var db PebbleCollectionDirectory
	if err := db.Open(dbPath); err != nil {
		t.Fatal(err)
	}
	var daily PebbleCollectionDirectory
	dailyPath := filepath.Join(dir, "daily")
	if err := daily.Open(dailyPath); err != nil {
		t.Fatal(err)
	}
	cache, err := lru.New[string, int](10)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cs := &collectionServer{ctx: ctx, cancel: cancel, shutdown: make(chan struct{}), pcd: &db, dauDirectory: &daily, dauTomorrow: time.Now().Add(time.Hour), didCollectionCounts: cache, MaxDidCollections: 100, log: slog.New(slog.NewTextHandler(io.Discard, nil)), ingestFirehose: make(chan DidCollection, 2), ingestCrawl: make(chan DidCollection, 2)}
	cs.ingestFirehose <- DidCollection{Did: "alice", Collection: "example.firehose"}
	close(cs.ingestFirehose)
	cs.wg.Add(1)
	go cs.ingestReceiver()

	entered, release := make(chan struct{}), make(chan struct{})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cs.apiServer = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		if _, err := db.GetCollectionStats(); err != nil {
			t.Error(err)
		}
	})}
	cs.wg.Add(1)
	go func() { defer cs.wg.Done(); _ = cs.apiServer.Serve(listener) }()
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		resp, err := http.Get("http://" + listener.Addr().String())
		if err == nil {
			resp.Body.Close()
		}
	}()
	<-entered
	producerRelease := make(chan struct{})
	cs.producerWG.Add(1)
	go func() {
		defer cs.producerWG.Done()
		<-producerRelease
		cs.ingestCrawl <- DidCollection{Did: "bob", Collection: "example.crawl"}
	}()
	done := make(chan error, 1)
	go func() { done <- cs.Shutdown() }()
	<-ctx.Done()
	close(release)
	<-requestDone
	close(producerRelease)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	var reopened PebbleCollectionDirectory
	if err := reopened.Open(dbPath); err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for _, collection := range []string{"example.firehose", "example.crawl"} {
		rows, _, err := reopened.ReadCollection(context.Background(), collection, "", 10)
		if err != nil || len(rows) != 1 {
			t.Fatalf("%s: rows %v, error %v", collection, rows, err)
		}
	}
	var reopenedDaily PebbleCollectionDirectory
	if err := reopenedDaily.Open(dailyPath); err != nil {
		t.Fatal(err)
	}
	defer reopenedDaily.Close()
	rows, _, err := reopenedDaily.ReadCollection(context.Background(), "example.firehose", "", 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("daily rows %v, error %v", rows, err)
	}
}
