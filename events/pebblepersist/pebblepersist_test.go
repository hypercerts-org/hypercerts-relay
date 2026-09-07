package pebblepersist

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	atproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

func testCID() cid.Cid {
	value, err := cid.NewPrefixV1(cid.Raw, mh.SHA2_256).Sum(make([]byte, 32))
	if err != nil {
		panic(err)
	}
	return value
}

// hypercerts: keep this Relay persistence test independent from non-Relay
// storage packages so the scoped fork can test every retained package.
func TestPebblePersist(t *testing.T) {
	options := DefaultPebblePersistOptions
	options.DbPath = filepath.Join(t.TempDir(), "pebble.db")
	persistence, err := NewPebblePersistance(&options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if persistence.db != nil {
			if err := persistence.Shutdown(context.Background()); err != nil {
				t.Error(err)
			}
		}
	})

	eventManager := events.NewEventManager(persistence)
	ctx := context.Background()
	const eventCount = 100
	for i := range eventCount {
		commit := lexutil.LexLink(testCID())
		record := lexutil.LexLink(testCID())
		event := &events.XRPCStreamEvent{
			RepoCommit: &atproto.SyncSubscribeRepos_Commit{
				Repo:   "did:example:123",
				Commit: commit,
				Ops: []*atproto.SyncSubscribeRepos_RepoOp{{
					Action: "create",
					Cid:    &record,
					Path:   "app.bsky.feed.post/test",
				}},
				Seq:  int64(i),
				Time: time.Now().UTC().Format(time.RFC3339Nano),
			},
		}
		if err := eventManager.AddEvent(ctx, event); err != nil {
			t.Fatal(err)
		}
	}

	if err := persistence.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	playbackCount := 0
	if err := persistence.Playback(ctx, 0, func(*events.XRPCStreamEvent) error {
		playbackCount++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if playbackCount != eventCount {
		t.Fatalf("expected %d events, got %d", eventCount, playbackCount)
	}
}
