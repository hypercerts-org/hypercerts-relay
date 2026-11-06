package live

import (
	"context"

	"github.com/bluesky-social/jetstream/internal/history"
	"github.com/bluesky-social/jetstream/internal/ingest/syncstate"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
)

type LifecycleOrdering interface {
	AppliedLifecycleSequences(context.Context, string) (identity, account int64, err error)
	AppliedLifecycleJournalSequence(context.Context, string, segment.Kind) (uint64, error)
}

type syncStateLifecycleOrdering struct{ store *syncstate.PebbleStateStore }

type journalLifecycleOrdering struct {
	LifecycleOrdering
	journal *history.Store
}

func (s syncStateLifecycleOrdering) AppliedLifecycleJournalSequence(context.Context, string, segment.Kind) (uint64, error) {
	return 0, nil
}

func (s syncStateLifecycleOrdering) AppliedLifecycleSequences(ctx context.Context, did string) (int64, int64, error) {
	identity, err := s.store.LoadAppliedIdentitySeq(ctx, atmos.DID(did))
	if err != nil {
		return 0, 0, err
	}
	account, err := s.store.LoadAppliedAccountSeq(ctx, atmos.DID(did))
	if err != nil {
		return 0, 0, err
	}
	hosting, err := s.store.LoadAppliedHosting(ctx, atmos.DID(did))
	if err != nil {
		return 0, 0, err
	}
	if hosting != nil && hosting.Seq > account {
		account = hosting.Seq
	}
	return identity, account, nil
}

func (s journalLifecycleOrdering) AppliedLifecycleSequences(_ context.Context, did string) (identity, account int64, err error) {
	identity, _, err = s.journal.LatestAppliedLifecycle(did, segment.KindIdentity, history.TargetMain)
	if err != nil {
		return 0, 0, err
	}
	account, _, err = s.journal.LatestAppliedLifecycle(did, segment.KindAccount, history.TargetMain)
	return identity, account, err
}

func (s journalLifecycleOrdering) AppliedLifecycleJournalSequence(_ context.Context, did string, kind segment.Kind) (uint64, error) {
	_, sequence, err := s.journal.LatestAppliedLifecycle(did, kind, history.TargetMain)
	return sequence, err
}
