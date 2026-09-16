package jobs

import (
	"bufio"
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
	// A directory is required even for an empty inventory: otherwise a later
	// page could be treated as covered without any identity verification.
	if p.Directory == nil {
		return inputFailure(ctx, "identity_unavailable")
	}
	client := p.client(job)
	for page, err := range client.ListRepos(ctx, 100, job.Cursor) {
		if err != nil {
			return inputFailure(ctx, "source_unavailable")
		}
		if err := p.processPage(ctx, client, &job, page); err != nil {
			return err
		}
	}
	return p.completeEnumeration(job)
}

func (p PDSProcessor) client(job Job) *atmossync.Client {
	// Direct snapshots stay on the explicitly admitted origin; a redirect must
	// not silently enroll a migration target or change coverage attribution.
	httpClient := *p.HTTPClient
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return atmossync.NewClient(atmossync.Options{Client: &xrpc.Client{Host: job.PDS, HTTPClient: gt.Some(&httpClient), Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}, Directory: gt.Some(p.Directory)})
}

func (p PDSProcessor) processPage(ctx context.Context, client *atmossync.Client, job *Job, page atmossync.ListReposPage) error {
	active, err := p.processEntries(ctx, client, job, page.Entries)
	if err != nil {
		// A permanent rejection is durable, but it is not inventory progress.
		// Leave this page's cursor and subtotal unchanged so a retry remains
		// incomplete at the rejected listing position.
		return err
	}
	return p.checkpointPage(job, page.NextCursor, active)
}

func (p PDSProcessor) processEntries(ctx context.Context, client *atmossync.Client, job *Job, entries []atmossync.ListReposEntry) (int, error) {
	active := 0
	for _, entry := range entries {
		if !entry.Active {
			continue
		}
		if !job.TotalReposKnown {
			active++
		}
		discardProgress, err := p.processActiveEntry(ctx, client, job, entry)
		if err != nil {
			if discardProgress {
				return 0, err
			}
			return active, err
		}
	}
	return active, nil
}

// processActiveEntry returns whether a transient failure must discard this
// page's uncheckpointed enumeration subtotal.
func (p PDSProcessor) processActiveEntry(ctx context.Context, client *atmossync.Client, job *Job, entry atmossync.ListReposEntry) (bool, error) {
	did := string(entry.DID)
	if err := p.validateListedSnapshot(job, entry); err != nil {
		return false, err
	}
	if rev, ok := job.CompletedRepos[did]; ok && rev == entry.Rev {
		return false, nil
	}
	if err := p.repository(ctx, client, *job, entry); err != nil {
		return p.handleRepositoryFailure(job, entry, err)
	}
	// Keep this page-local snapshot current: duplicate entries must not trigger
	// a second download before the page checkpoint commits.
	job.CompletedRepos[did] = entry.Rev
	return false, nil
}

func (p PDSProcessor) validateListedSnapshot(job *Job, entry atmossync.ListReposEntry) error {
	did := string(entry.DID)
	if _, err := atmos.ParseDID(did); err != nil {
		if rejection, ok := p.Manager.lookupSnapshotRejection(job.PDS, job.Policy.Revision, did, entry.Rev, directPDSSnapshotRejectionKind); ok {
			return &InputError{Code: rejection.Code}
		}
		return p.rejectSnapshot(job, did, entry.Rev, "invalid_listing_did")
	}
	if _, err := atmos.ParseTID(entry.Rev); err != nil {
		return p.rejectSnapshot(job, did, entry.Rev, "invalid_listing_revision")
	}
	if rejection, ok := p.Manager.lookupSnapshotRejection(job.PDS, job.Policy.Revision, did, entry.Rev, directPDSSnapshotRejectionKind); ok {
		// hypercerts: A matching durable verdict prevents another untrusted CAR
		// download, but must leave this page unacknowledged.
		return &InputError{Code: rejection.Code}
	}
	return nil
}

func (p PDSProcessor) handleRepositoryFailure(job *Job, entry atmossync.ListReposEntry, err error) (bool, error) {
	var input *InputError
	if !errors.As(err, &input) || input.Unavailable {
		return true, err
	}
	return false, p.rejectSnapshot(job, string(entry.DID), entry.Rev, input.Code)
}

// rejectSnapshot persists a permanent input verdict without acknowledging the
// listed position. Retries must stop at that position until the listing changes.
func (p PDSProcessor) rejectSnapshot(job *Job, did, listedRevision, code string) error {
	rejection, err := p.Manager.recordSnapshotRejection(job.PDS, job.Policy.Revision, did, listedRevision, directPDSSnapshotRejectionKind, code)
	if err != nil {
		return err
	}
	return &InputError{Code: rejection.Code}
}

func (p PDSProcessor) checkpointPage(job *Job, cursor string, active int) error {
	if job.TotalReposKnown {
		if err := p.Manager.Checkpoint(job.ID, "", "", cursor); err != nil {
			return err
		}
	} else {
		if err := p.Manager.CheckpointEnumeration(job.ID, cursor, active, cursor == ""); err != nil {
			return err
		}
		job.EnumeratedRepos += active
		if cursor == "" {
			job.TotalRepos = job.EnumeratedRepos
			job.TotalReposKnown = true
		}
	}
	job.Cursor = cursor
	return nil
}

func (p PDSProcessor) completeEnumeration(job Job) error {
	// Atmos does not yield an empty terminal page. It is still a complete,
	// durable inventory and therefore has a total of the saved subtotal.
	if !job.TotalReposKnown {
		return p.Manager.CheckpointEnumeration(job.ID, job.Cursor, 0, true)
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
	if err := p.verifySnapshot(ctx, job, entry, commit); err != nil {
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

// repositoryReadErrors records a non-EOF getRepo body failure so it cannot
// be misclassified as a permanent CAR syntax error by the decoder above it.
type repositoryReadErrors struct {
	io.Reader
	err error
}

func (r *repositoryReadErrors) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err != nil && !errors.Is(err, io.EOF) && r.err == nil {
		r.err = err
	}
	return n, err
}

func (p PDSProcessor) verifySource(ctx context.Context, did atmos.DID, pds string) error {
	if p.Directory == nil {
		return inputFailure(ctx, "identity_unavailable")
	}
	// A cache-only directory can still reject an already-moved source before a
	// download. It cannot accept a snapshot: verifySnapshot requires a resolver
	// and forces a refresh after the download.
	if p.Directory.Resolver == nil {
		if p.Directory.Cache == nil {
			return inputFailure(ctx, "identity_unavailable")
		}
		ident, ok := p.Directory.Cache.Get(ctx, "did:"+string(did))
		if !ok || ident == nil {
			return inputFailure(ctx, "identity_unavailable")
		}
		return verifySourceIdentity(ident, pds)
	}
	ident, err := p.Directory.LookupDID(ctx, did)
	if err != nil || ident == nil {
		return inputFailure(ctx, "identity_unavailable")
	}
	return verifySourceIdentity(ident, pds)
}

func verifySourceIdentity(ident *identity.Identity, pds string) error {
	if ident == nil {
		return &InputError{Code: "identity_unavailable", Unavailable: true}
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
		return nil, nil, &InputError{Code: "repository_unavailable", Unavailable: true}
	}
	defer body.Close()
	// Bound transient full-CAR input. Exceeding the bound is explicit incomplete
	// coverage; unrelated CAR blocks are never written to Jetstream segments.
	limited := &io.LimitedReader{R: body, N: 64 << 20}
	readErrors := &repositoryReadErrors{Reader: limited}
	// hypercerts: A direct getRepo response is a full snapshot. Reject a CAR
	// that parses at a block boundary but omits reachable blocks as unavailable
	// rather than materializing a partial repository.
	r, commit, err := repo.LoadCompleteFromCAR(bufio.NewReader(readErrors))
	if limited.N == 0 {
		return nil, nil, &InputError{Code: "repository_size_limit", Unavailable: true}
	}
	if ctx.Err() != nil || readErrors.err != nil {
		return nil, nil, &InputError{Code: "repository_unavailable", Unavailable: true}
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, nil, &InputError{Code: "repository_incomplete", Unavailable: true}
	}
	if err != nil {
		return nil, nil, &InputError{Code: "invalid_repository"}
	}
	return r, commit, nil
}

func (p PDSProcessor) verifySnapshot(ctx context.Context, job Job, entry atmossync.ListReposEntry, commit *repo.Commit) error {
	if p.Directory == nil || p.Directory.Resolver == nil {
		return inputFailure(ctx, "identity_unavailable")
	}
	if commit.DID != string(entry.DID) {
		return &InputError{Code: "repository_did_mismatch"}
	}
	listedTID, err := atmos.ParseTID(entry.Rev)
	if err != nil {
		return &InputError{Code: "invalid_listing_revision"}
	}
	commitTID, err := atmos.ParseTID(commit.Rev)
	if err != nil || commitTID.Time().After(time.Now().Add(5*time.Minute)) {
		return &InputError{Code: "invalid_revision"}
	}
	// Resolve this snapshot independently instead of purging the shared
	// directory cache used by live consumers. Direct PDS jobs still require a
	// fresh binding, but their verification must not evict another request's
	// identity entry.
	refreshed, err := p.freshSnapshotIdentity(ctx, entry.DID)
	if err != nil || refreshed == nil {
		return inputFailure(ctx, "identity_unavailable")
	}
	if err := verifySourceIdentity(refreshed, job.PDS); err != nil {
		return err
	}
	key, err := refreshed.PublicKey()
	if err != nil {
		return inputFailure(ctx, "identity_unavailable")
	}
	if err := commit.VerifySignature(key); err != nil {
		return &InputError{Code: "verification_failed"}
	}
	if commitTID.Integer() < listedTID.Integer() {
		return &InputError{Code: "snapshot_behind_listing", Unavailable: true}
	}
	return nil
}

func (p PDSProcessor) freshSnapshotIdentity(ctx context.Context, did atmos.DID) (*identity.Identity, error) {
	document, err := p.Directory.Resolver.ResolveDID(ctx, did)
	if err != nil {
		return nil, err
	}
	return identity.IdentityFromDocument(document)
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
