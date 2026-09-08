// Command jetstream-source is a loopback-only integration fixture. It feeds
// signed fixture frames through the real Indigo durable event manager and
// Relay subscribeRepos handler. PDS admission/verification is tested separately.
package main

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/cmd/relay/relay"
	"github.com/bluesky-social/indigo/cmd/relay/stream"
	"github.com/bluesky-social/indigo/cmd/relay/stream/eventmgr"
	"github.com/bluesky-social/indigo/cmd/relay/stream/persist/diskpersist"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type fixtureUIDs struct{}

func (fixtureUIDs) DidToUid(_ context.Context, did string) (uint64, error) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(did))
	return h.Sum64(), nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	dir := flag.String("data-dir", "", "disposable fixture state")
	address := flag.String("address-file", "", "write bound loopback URL here")
	flag.Parse()
	if *dir == "" || *address == "" {
		return fmt.Errorf("data-dir and address-file required")
	}
	db, err := gorm.Open(sqlite.Open(filepath.Join(*dir, "relay.sqlite")), &gorm.Config{})
	if err != nil {
		return err
	}
	p, err := diskpersist.NewDiskPersistence(filepath.Join(*dir, "events"), "", db, nil)
	if err != nil {
		return err
	}
	p.SetUidSource(fixtureUIDs{})
	events := eventmgr.NewEventManager(p)
	r, err := relay.NewRelay(db, events, identity.NewMockDirectory(), nil)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /xrpc/com.atproto.sync.subscribeRepos", func(w http.ResponseWriter, req *http.Request) {
		var cursor *int64
		if raw := req.URL.Query().Get("cursor"); raw != "" {
			n, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				http.Error(w, "cursor", 400)
				return
			}
			cursor = &n
		}
		_ = r.HandleSubscribeRepos(w, req, cursor, "127.0.0.1")
	})
	mux.HandleFunc("GET /xrpc/com.atproto.sync.listHosts", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"hosts":[]}`)
	})
	mux.HandleFunc("GET /xrpc/com.atproto.sync.listRepos", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"repos":[]}`)
	})
	mux.HandleFunc("POST /fixture/emit", func(w http.ResponseWriter, req *http.Request) {
		var ev stream.XRPCStreamEvent
		if err := ev.Deserialize(http.MaxBytesReader(w, req.Body, 2<<20)); err != nil {
			http.Error(w, "invalid fixture frame", 400)
			return
		}
		if err := events.AddEvent(req.Context(), &ev); err != nil {
			http.Error(w, "persist fixture", 500)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	if err := os.WriteFile(*address, []byte("http://"+ln.Addr().String()), 0600); err != nil {
		return err
	}
	return http.Serve(ln, mux)
}
