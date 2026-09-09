package relay

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/repo"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/cmd/relay/relay/models"
)

var (
	ErrFutureRev   = errors.New("commit revision in the future")
	ErrRevSequence = errors.New("commit revision out of order")

	// Identity failures are dynamic: source progress must wait for a fresh DID document.
	ErrIdentityUnavailable = errors.New("commit identity unavailable")
	ErrIdentityRefresh     = errors.New("commit identity refresh failed")
	ErrCommitSignature     = errors.New("commit signature verification failed")
)

const futureRevTolerance = time.Minute * 5
const MaxMessageBlocksBytes = 2_000_000
const MaxCommitOps = 200

// High-level entrypoint for verifying #commit messages.
//
// Always verifies: loading commit and repo; field syntax; commit signature; future rev
//
// Strict verification: use of deprecated fields; MST inversion; all ops present in blocks
//
// Does not check: account/host matching; host-level sequence; account-level rev ordering; DID syntax
//
// `ident` arg must be a resolved identity.
// `prevRepo` arg represents previous state, and is optional/nullable.
// `hostname` arg is piped through just for logging, not for validating account/host match
// returns an AccountRepo with empty UID, containing metadata about *this* commit
func (r *Relay) VerifyRepoCommit(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Commit, ident *identity.Identity, prevRepo *models.AccountRepo, hostname string) (*models.AccountRepo, error) {
	logger := r.Logger.With("host", hostname, "did", evt.Repo, "rev", evt.Rev)

	if len(evt.Blocks) > MaxMessageBlocksBytes {
		return nil, newPermanentEventError(rejectionReasonMalformedCommit, fmt.Errorf("blocks size (%d bytes) exceeds protocol limit", len(evt.Blocks)))
	}

	if len(evt.Ops) > MaxCommitOps {
		return nil, newPermanentEventError(rejectionReasonMalformedCommit, fmt.Errorf("too many ops in commit: %d", len(evt.Ops)))
	}

	// even in lenient/legacy mode (eg, tooBig), we need to verify commit
	commit, commitCID, err := repo.LoadCommitFromCAR(ctx, bytes.NewReader(evt.Blocks))
	if err != nil {
		return nil, newPermanentEventError(rejectionReasonMalformedCommit, err)
	}

	if err := r.verifyCommitObjectWithRefresh(ctx, commit, ident, syntax.DID(evt.Repo), hostname); err != nil {
		return nil, err
	}

	// consistency between event fields and commit fields
	// hypercerts: Bind the advertised commit CID to the verified CAR root.
	if evt.Commit.String() != commitCID.String() {
		return nil, newPermanentEventError(rejectionReasonInvalidCommit, errors.New("commit CID does not match CAR root"))
	}
	if evt.Repo != commit.DID {
		return nil, newPermanentEventError(rejectionReasonInvalidCommit, fmt.Errorf("mismatched inner commit DID field: %s", commit.DID))
	}
	if evt.Rev != commit.Rev {
		return nil, newPermanentEventError(rejectionReasonInvalidCommit, fmt.Errorf("mismatched inner commit rev field: %s", commit.Rev))
	}

	err = r.VerifyCommitMessageStrict(ctx, evt, commit, prevRepo, hostname)
	if err != nil {
		if r.Config.LenientSyncValidation {
			// hypercerts: lenient validation is an explicit operator policy, not a silent bypass.
			logger.Warn("lenient policy allowed strict commit validation failure", "policyRevision", RejectionPolicyRevision)
		} else {
			return nil, err
		}
	}

	resp := models.AccountRepo{
		Rev:           commit.Rev,
		CommitCID:     commitCID.String(),
		CommitDataCID: commit.Data.String(),
	}
	return &resp, nil
}

// the parts of basic verification which are common between #commit and #sync messages
func (r *Relay) VerifyCommitObject(ctx context.Context, commit *repo.Commit, ident *identity.Identity, hostname string) error {
	if commit == nil {
		return newPermanentEventError(rejectionReasonMalformedCommit, errors.New("missing commit"))
	}
	// `VerifyStructure` checks that commit object field syntax is correct
	if err := commit.VerifyStructure(); err != nil {
		return newPermanentEventError(rejectionReasonMalformedCommit, err)
	}

	// this re-parse is technically duplicate work
	rev, err := syntax.ParseTID(commit.Rev)
	if err != nil {
		return newPermanentEventError(rejectionReasonMalformedCommit, fmt.Errorf("commit rev syntax: %w", err))
	}
	if rev.Time().Compare(time.Now().Add(futureRevTolerance)) > 0 {
		return fmt.Errorf("%w: %s: %s", ErrFutureRev, rev, rev.Time().String())
	}

	if ident == nil {
		return ErrIdentityUnavailable
	}
	// NOTE: may eventually want to cache cryptographic key parsing
	pubkey, err := ident.PublicKey()
	if err != nil {
		return ErrIdentityUnavailable
	}

	if err := commit.VerifySignature(pubkey); err != nil {
		return ErrCommitSignature
	}
	return nil
}

// verifyCommitObjectWithRefresh gives a rotated signing key one fresh DID lookup
// before classifying its signature as permanently invalid.
func (r *Relay) verifyCommitObjectWithRefresh(ctx context.Context, commit *repo.Commit, ident *identity.Identity, expectedDID syntax.DID, hostname string) error {
	// hypercerts: A different repository cannot justify refreshing the claimed DID's key.
	if commit != nil && commit.DID != expectedDID.String() {
		return newPermanentEventError(rejectionReasonInvalidCommit, errors.New("mismatched inner commit DID field"))
	}
	err := r.VerifyCommitObject(ctx, commit, ident, hostname)
	if err == nil || (!errors.Is(err, ErrCommitSignature) && !errors.Is(err, ErrIdentityUnavailable)) {
		return err
	}

	if err := r.Dir.Purge(ctx, expectedDID.AtIdentifier()); err != nil {
		return ErrIdentityRefresh
	}
	refreshed, err := r.Dir.LookupDID(ctx, expectedDID)
	if err != nil || refreshed == nil {
		return ErrIdentityRefresh
	}
	if err := r.VerifyCommitObject(ctx, commit, refreshed, hostname); err != nil {
		if errors.Is(err, ErrCommitSignature) {
			return newPermanentEventError(rejectionReasonInvalidSignature, err)
		}
		return err
	}
	return nil
}

func (r *Relay) VerifyCommitMessageStrict(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Commit, commit *repo.Commit, prevRepo *models.AccountRepo, hostname string) error {

	logger := r.Logger.With("host", hostname, "did", commit.DID, "rev", commit.Rev)

	// first check things which would skip MST inversion entirely
	if len(evt.Blocks) == 0 {
		return newPermanentEventError(rejectionReasonInvalidCommit, errors.New("commit messaging missing blocks"))
	}
	if evt.TooBig {
		return newPermanentEventError(rejectionReasonInvalidCommit, errors.New("deprecated tooBig commit flag set"))
	}
	// hypercerts: Verify initial records and timestamps even when previous state is unknown.
	if evt.PrevData == nil && prevRepo != nil {
		return newPermanentEventError(rejectionReasonInvalidCommit, errors.New("missing prevData field"))
	}
	if prevRepo != nil {
		if evt.PrevData != nil && evt.PrevData.String() != prevRepo.CommitDataCID {
			logger.Warn("commit with miss-matching prevData", "prevData", evt.PrevData, "prevRepo.CommitDataCID", prevRepo.CommitDataCID)
		}
		if evt.Since != nil && *evt.Since != prevRepo.Rev {
			logger.Warn("commit with miss-matching since", "since", evt.Since, "prevRepo.Rev", prevRepo.Rev)
		}
		if evt.Rev <= prevRepo.Rev {
			return newPermanentEventError(rejectionReasonInvalidCommit, fmt.Errorf("%w: %s before or equal to %s", ErrRevSequence, evt.Rev, prevRepo.Rev))
		}
	}

	// TODO: break out this function in to smaller chunks. For example, missing PrevData
	if _, err := repo.VerifyCommitMessage(ctx, evt); err != nil {
		return newPermanentEventError(rejectionReasonInvalidMST, err)
	}

	// finally less-important checks
	if evt.Rebase {
		return newPermanentEventError(rejectionReasonInvalidCommit, errors.New("deprecated rebase commit flag set"))
	}
	_, err := syntax.ParseDatetime(evt.Time)
	if err != nil {
		return newPermanentEventError(rejectionReasonInvalidCommit, fmt.Errorf("commit timestamp syntax: %w", err))
	}
	return nil
}

// High-level entrypoint for verifying #sync messages.
//
// Always verifies: loading commit and repo; field syntax; commit signature; future rev
//
// Does not check: account/host matching; host-level sequence; account-level rev ordering; DID syntax
//
// `ident` arg must be a resolved identity.
// `hostname` arg is piped through just for logging, not for validating account/host match
// returns an AccountRepo with empty UID, containing metadata about *this* commit
func (r *Relay) VerifyRepoSync(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Sync, ident *identity.Identity, hostname string) (*models.AccountRepo, error) {
	//logger := r.Logger.With("host", hostname, "did", evt.Did, "rev", evt.Rev)

	if len(evt.Blocks) > MaxMessageBlocksBytes {
		return nil, newPermanentEventError(rejectionReasonMalformedCommit, fmt.Errorf("blocks size (%d bytes) exceeds protocol limit", len(evt.Blocks)))
	}

	// even in lenient/legacy mode (eg, tooBig), we need to verify commit
	commit, commitCID, err := repo.LoadCommitFromCAR(ctx, bytes.NewReader(evt.Blocks))
	if err != nil {
		return nil, newPermanentEventError(rejectionReasonMalformedCommit, err)
	}

	if err := r.verifyCommitObjectWithRefresh(ctx, commit, ident, syntax.DID(evt.Did), hostname); err != nil {
		return nil, err
	}

	// consistency between event fields and commit fields
	if evt.Did != commit.DID {
		return nil, newPermanentEventError(rejectionReasonInvalidCommit, fmt.Errorf("mismatched inner commit DID field: %s", commit.DID))
	}
	if evt.Rev != commit.Rev {
		return nil, newPermanentEventError(rejectionReasonInvalidCommit, fmt.Errorf("mismatched inner commit rev field: %s", commit.Rev))
	}

	resp := models.AccountRepo{
		Rev:           commit.Rev,
		CommitCID:     commitCID.String(),
		CommitDataCID: commit.Data.String(),
	}
	return &resp, nil
}
