package relay

import (
	"context"
	"hash/fnv"
	"sync"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/cmd/relay/stream"
)

func (r *Relay) processSourceEvent(ctx context.Context, evt *stream.XRPCStreamEvent, hostname string, hostID uint64) error {
	var did string
	switch {
	case evt.RepoCommit != nil:
		did = evt.RepoCommit.Repo
	case evt.RepoSync != nil:
		did = evt.RepoSync.Did
	case evt.RepoIdentity != nil:
		did = evt.RepoIdentity.Did
	case evt.RepoAccount != nil:
		did = evt.RepoAccount.Did
	}
	lock := r.accountEventLock(did)
	lock.Lock()
	defer lock.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return r.processRepoEvent(ctx, evt, hostname, hostID)
}

func (r *Relay) accountEventLock(did string) *sync.Mutex {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(NormalizeDID(syntax.DID(did))))
	return &r.eventLocks[hash.Sum32()%uint32(len(r.eventLocks))]
}
