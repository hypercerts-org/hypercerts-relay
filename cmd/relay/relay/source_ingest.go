package relay

import (
	"context"
	"hash/fnv"

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
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(NormalizeDID(syntax.DID(did))))
	lock := &r.eventLocks[hash.Sum32()%uint32(len(r.eventLocks))]
	lock.Lock()
	defer lock.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return r.processRepoEvent(ctx, evt, hostname, hostID)
}
