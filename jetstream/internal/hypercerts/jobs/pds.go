package jobs

import (
	"context"
	"errors"
	"io"
	"net/http"
	"slices"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
)

type PDSProcessor struct {
	Manager    *Manager
	HTTPClient *http.Client
	Directory  *identity.Directory
	Reconcile  func(context.Context, ingest.Snapshot) error
}

func (p PDSProcessor) Run(ctx context.Context, job Job) error {
	// Direct snapshots stay on the explicitly admitted origin; a redirect must
	// not silently enroll a migration target or change coverage attribution.
	httpClient := *p.HTTPClient
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	client := atmossync.NewClient(atmossync.Options{Client: &xrpc.Client{Host: job.PDS, HTTPClient: gt.Some(&httpClient), Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}, Directory: gt.Some(p.Directory)})
	for page, err := range client.ListRepos(ctx, 100, job.Cursor) {
		if err != nil {
			return inputFailure(ctx, "source_unavailable")
		}
		for _, entry := range page.Entries {
			if !entry.Active {
				continue
			}
			if rev, ok := job.CompletedRepos[string(entry.DID)]; ok && rev == entry.Rev {
				continue
			}
			if err := p.repository(ctx, client, job, entry); err != nil {
				return err
			}
		}
		if err := p.Manager.Checkpoint(job.ID, "", "", page.NextCursor); err != nil {
			return err
		}
		job.Cursor = page.NextCursor
	}
	return nil
}

func inputFailure(ctx context.Context, code string) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return ctx.Err()
	}
	return &InputError{Code: code, Unavailable: true}
}

func (p PDSProcessor) repository(ctx context.Context, client *atmossync.Client, job Job, entry atmossync.ListReposEntry) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if err := p.verifySource(ctx, entry.DID, job.PDS); err != nil {
		return err
	}
	r, commit, err := fetchRepository(ctx, client, entry.DID)
	if err != nil {
		return err
	}
	if err := p.verifySnapshot(ctx, client, job, entry, commit); err != nil {
		return err
	}
	snapshot, err := projectSnapshot(ctx, job, entry.DID, r, commit.Rev)
	if err != nil {
		return err
	}
	if err := p.Manager.Apply(job.ID, func() error { return p.Reconcile(ctx, snapshot) }); err != nil {
		if errors.Is(err, ingest.ErrAccountUnavailable) {
			return &InputError{Code: "account_unavailable", Unavailable: true}
		}
		return err
	}
	return p.Manager.Checkpoint(job.ID, string(entry.DID), commit.Rev, job.Cursor)
}

func (p PDSProcessor) verifySource(ctx context.Context, did atmos.DID, pds string) error {
	ident, err := p.Directory.LookupDID(ctx, did)
	if err != nil {
		return inputFailure(ctx, "identity_unavailable")
	}
	actual, err := normalizeSource(ident.PDSEndpoint())
	if err != nil || actual != pds {
		return &InputError{Code: "source_changed", Unavailable: true}
	}
	return nil
}

func fetchRepository(ctx context.Context, client *atmossync.Client, did atmos.DID) (*repo.Repo, *repo.Commit, error) {
	body, err := client.GetRepoStream(ctx, did, "")
	if err != nil {
		return nil, nil, inputFailure(ctx, "repository_unavailable")
	}
	defer body.Close()
	// Bound transient full-CAR input. Exceeding the bound is explicit incomplete
	// coverage; unrelated CAR blocks are never written to Jetstream segments.
	limited := &io.LimitedReader{R: body, N: 64 << 20}
	r, commit, err := repo.LoadFromCAR(limited)
	if limited.N == 0 {
		return nil, nil, &InputError{Code: "repository_size_limit", Unavailable: true}
	}
	if err != nil {
		return nil, nil, &InputError{Code: "invalid_repository"}
	}
	return r, commit, nil
}

func (p PDSProcessor) verifySnapshot(ctx context.Context, client *atmossync.Client, job Job, entry atmossync.ListReposEntry, commit *repo.Commit) error {
	if commit.DID != string(entry.DID) {
		return &InputError{Code: "repository_did_mismatch"}
	}
	if tid, err := atmos.ParseTID(commit.Rev); err != nil || tid.Time().After(time.Now().Add(5*time.Minute)) {
		return &InputError{Code: "invalid_revision"}
	}
	if err := client.VerifyCommit(ctx, commit); err != nil {
		p.Directory.Purge(ctx, entry.DID)
		if err := client.VerifyCommit(ctx, commit); err != nil {
			return &InputError{Code: "verification_failed"}
		}
		if err := p.verifySource(ctx, entry.DID, job.PDS); err != nil {
			return err
		}
	}
	if commit.Rev < entry.Rev {
		return &InputError{Code: "snapshot_behind_listing", Unavailable: true}
	}
	return nil
}

func projectSnapshot(ctx context.Context, job Job, did atmos.DID, r *repo.Repo, rev string) (ingest.Snapshot, error) {
	snapshot := ingest.Snapshot{DID: string(did), Rev: rev, Collections: job.Policy.Collections}
	err := r.Tree.Walk(func(key string, cid cbor.CID) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		collection, rkey := repo.SplitMSTKey(key)
		if !slices.Contains(job.Policy.Collections, collection) {
			return nil
		}
		payload, err := r.Store.GetBlock(cid)
		if err != nil {
			return err
		}
		event := segment.Event{Kind: segment.KindCreateResync, DID: string(did), Rev: rev, Collection: collection, Rkey: rkey, Payload: payload, WitnessedAt: time.Now().UnixMicro()}
		if err := segment.ValidateEvent(event); err != nil {
			return err
		}
		snapshot.Records = append(snapshot.Records, event)
		return nil
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return ingest.Snapshot{}, err
		}
		return ingest.Snapshot{}, &InputError{Code: "unrepresentable_snapshot", Unavailable: true}
	}
	return snapshot, nil
}
