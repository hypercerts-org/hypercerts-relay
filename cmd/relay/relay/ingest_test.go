package relay

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/cmd/relay/relay/models"
	"github.com/bluesky-social/indigo/cmd/relay/stream"
	"github.com/bluesky-social/indigo/cmd/relay/stream/eventmgr"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/stretchr/testify/require"
)

const ingestTestHostID = uint64(71)

type ingestFixture struct {
	Accounts []struct {
		Identity identity.Identity `json:"identity"`
	} `json:"accounts"`
	Messages []struct {
		Frame struct {
			Body json.RawMessage `json:"body"`
		} `json:"frame"`
	} `json:"messages"`
}

func loadIngestFixture(t *testing.T) (identity.Identity, comatproto.SyncSubscribeRepos_Commit) {
	t.Helper()

	contents, err := os.ReadFile(filepath.Join("..", "testing", "testdata", "legacy.json"))
	require.NoError(t, err)

	var fixture ingestFixture
	require.NoError(t, json.Unmarshal(contents, &fixture))
	require.Len(t, fixture.Accounts, 1)
	require.Len(t, fixture.Messages, 3)

	var commit comatproto.SyncSubscribeRepos_Commit
	require.NoError(t, json.Unmarshal(fixture.Messages[2].Frame.Body, &commit))
	return fixture.Accounts[0].Identity, commit
}

type testEventPersistence struct {
	persistErr  error
	events      []*stream.XRPCStreamEvent
	broadcaster func(*stream.XRPCStreamEvent)
}

func (p *testEventPersistence) Persist(ctx context.Context, evt *stream.XRPCStreamEvent) error {
	if p.persistErr != nil {
		return p.persistErr
	}
	p.events = append(p.events, evt)
	if p.broadcaster != nil {
		p.broadcaster(evt)
	}
	return nil
}

func (p *testEventPersistence) Playback(context.Context, int64, func(*stream.XRPCStreamEvent) error) error {
	return nil
}

func (p *testEventPersistence) TakeDownRepo(context.Context, uint64) error { return nil }
func (p *testEventPersistence) Flush(context.Context) error                { return nil }
func (p *testEventPersistence) Shutdown(context.Context) error             { return nil }

func (p *testEventPersistence) SetEventBroadcaster(broadcaster func(*stream.XRPCStreamEvent)) {
	p.broadcaster = broadcaster
}

func newIngestTestRelay(t *testing.T, directory identity.Directory, did syntax.DID, migrateRejections bool, persistErr error) (*Relay, *gorm.DB, *testEventPersistence) {
	t.Helper()

	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)

	persistence := &testEventPersistence{persistErr: persistErr}
	config := DefaultRelayConfig()
	relay, err := NewRelay(db, eventmgr.NewEventManager(persistence), directory, config)
	require.NoError(t, err)
	if migrateRejections {
		require.NoError(t, db.AutoMigrate(&models.RejectedEvent{}))
	}
	require.NoError(t, db.Create(&models.Account{
		UID:            1,
		DID:            did.String(),
		HostID:         ingestTestHostID,
		Status:         models.AccountStatusActive,
		UpstreamStatus: models.AccountStatusActive,
	}).Error)

	return relay, db, persistence
}

func newFixtureDirectory(ident identity.Identity) *identity.MockDirectory {
	directory := identity.NewMockDirectory()
	directory.Insert(ident)
	return directory
}

func TestPermanentCommitRejectionIsDurableAndIdempotent(t *testing.T) {
	ident, commit := loadIngestFixture(t)
	relay, _, _ := newIngestTestRelay(t, newFixtureDirectory(ident), ident.DID, true, nil)
	commit.Blocks = []byte{0xff}

	event := &stream.XRPCStreamEvent{RepoCommit: &commit}
	require.NoError(t, relay.processRepoEvent(t.Context(), event, "source.example", ingestTestHostID))
	require.NoError(t, relay.processRepoEvent(t.Context(), event, "source.example", ingestTestHostID))

	rejections, err := relay.ListRejectedEvents(t.Context(), 0, 1)
	require.NoError(t, err)
	require.Len(t, rejections, 1)
	rejection := rejections[0]
	require.Equal(t, ingestTestHostID, rejection.SourceHostID)
	require.Equal(t, commit.Seq, rejection.SourcePosition)
	require.Equal(t, ident.DID.String(), rejection.DID)
	require.Equal(t, "commit", rejection.EventKind)
	require.Equal(t, relay.rejectionPolicyRevision(), rejection.PolicyRevision)
	require.Equal(t, rejectionReasonMalformedCommit, rejection.ReasonCode)
	require.NotEmpty(t, rejection.IdempotencyKey)

	_, err = relay.GetAccountRepo(t.Context(), 1)
	require.ErrorIs(t, err, ErrAccountRepoNotFound)
}

func TestRejectedEventWriteFailureLeavesCommitReplayable(t *testing.T) {
	ident, commit := loadIngestFixture(t)
	relay, db, _ := newIngestTestRelay(t, newFixtureDirectory(ident), ident.DID, true, nil)
	require.NoError(t, db.Callback().Create().Before("gorm:create").Register("reject-rejected-event-write", func(tx *gorm.DB) {
		if tx.Statement.Table == (models.RejectedEvent{}).TableName() {
			tx.AddError(errors.New("rejection storage unavailable"))
		}
	}))
	commit.Blocks = []byte{0xff}

	err := relay.processRepoEvent(t.Context(), &stream.XRPCStreamEvent{RepoCommit: &commit}, "source.example", ingestTestHostID)
	require.Error(t, err)

	_, err = relay.GetAccountRepo(t.Context(), 1)
	require.ErrorIs(t, err, ErrAccountRepoNotFound)
}

func TestMissingIdentityLeavesCommitReplayable(t *testing.T) {
	ident, commit := loadIngestFixture(t)
	relay, _, _ := newIngestTestRelay(t, identity.NewMockDirectory(), ident.DID, true, nil)

	err := relay.processRepoEvent(t.Context(), &stream.XRPCStreamEvent{RepoCommit: &commit}, "source.example", ingestTestHostID)
	require.ErrorIs(t, err, ErrIdentityUnavailable)

	rejections, listErr := relay.ListRejectedEvents(t.Context(), 0, 10)
	require.NoError(t, listErr)
	require.Empty(t, rejections)
	_, repoErr := relay.GetAccountRepo(t.Context(), 1)
	require.ErrorIs(t, repoErr, ErrAccountRepoNotFound)
}

func TestStrictInvalidCommitIsRejected(t *testing.T) {
	ident, commit := loadIngestFixture(t)
	relay, _, _ := newIngestTestRelay(t, newFixtureDirectory(ident), ident.DID, true, nil)
	commit.TooBig = true

	require.NoError(t, relay.processRepoEvent(t.Context(), &stream.XRPCStreamEvent{RepoCommit: &commit}, "source.example", ingestTestHostID))
	rejections, err := relay.ListRejectedEvents(t.Context(), 0, 10)
	require.NoError(t, err)
	require.Len(t, rejections, 1)
	require.Equal(t, rejectionReasonInvalidCommit, rejections[0].ReasonCode)
}

func TestCommitOutputFailureLeavesRevisionReplayable(t *testing.T) {
	ident, commit := loadIngestFixture(t)
	persistErr := errors.New("output unavailable")
	relay, _, persistence := newIngestTestRelay(t, newFixtureDirectory(ident), ident.DID, true, persistErr)

	err := relay.processRepoEvent(t.Context(), &stream.XRPCStreamEvent{RepoCommit: &commit}, "source.example", ingestTestHostID)
	require.Error(t, err)
	_, repoErr := relay.GetAccountRepo(t.Context(), 1)
	require.ErrorIs(t, repoErr, ErrAccountRepoNotFound)

	persistence.persistErr = nil
	require.NoError(t, relay.processRepoEvent(t.Context(), &stream.XRPCStreamEvent{RepoCommit: &commit}, "source.example", ingestTestHostID))
	repo, repoErr := relay.GetAccountRepo(t.Context(), 1)
	require.NoError(t, repoErr)
	require.Equal(t, commit.Rev, repo.Rev)
}

func TestSyncOutputFailureLeavesRevisionReplayable(t *testing.T) {
	ident, commit := loadIngestFixture(t)
	persistErr := errors.New("output unavailable")
	relay, _, persistence := newIngestTestRelay(t, newFixtureDirectory(ident), ident.DID, true, persistErr)
	sync := comatproto.SyncSubscribeRepos_Sync{
		Did:    commit.Repo,
		Rev:    commit.Rev,
		Blocks: commit.Blocks,
		Seq:    commit.Seq,
	}

	err := relay.processRepoEvent(t.Context(), &stream.XRPCStreamEvent{RepoSync: &sync}, "source.example", ingestTestHostID)
	require.Error(t, err)
	_, repoErr := relay.GetAccountRepo(t.Context(), 1)
	require.ErrorIs(t, repoErr, ErrAccountRepoNotFound)

	persistence.persistErr = nil
	require.NoError(t, relay.processRepoEvent(t.Context(), &stream.XRPCStreamEvent{RepoSync: &sync}, "source.example", ingestTestHostID))
	repo, repoErr := relay.GetAccountRepo(t.Context(), 1)
	require.NoError(t, repoErr)
	require.Equal(t, sync.Rev, repo.Rev)
}

type rotatingDirectory struct {
	stale      identity.Identity
	fresh      identity.Identity
	purgeErr   error
	purged     bool
	purgeCalls int
}

func (d *rotatingDirectory) LookupHandle(context.Context, syntax.Handle) (*identity.Identity, error) {
	return nil, identity.ErrHandleNotFound
}

func (d *rotatingDirectory) LookupDID(context.Context, syntax.DID) (*identity.Identity, error) {
	if d.purged {
		ident := d.fresh
		return &ident, nil
	}
	ident := d.stale
	return &ident, nil
}

func (d *rotatingDirectory) Lookup(ctx context.Context, atid syntax.AtIdentifier) (*identity.Identity, error) {
	did, err := atid.AsDID()
	if err != nil {
		return nil, err
	}
	return d.LookupDID(ctx, did)
}

func (d *rotatingDirectory) Purge(context.Context, syntax.AtIdentifier) error {
	d.purgeCalls++
	if d.purgeErr != nil {
		return d.purgeErr
	}
	d.purged = true
	return nil
}

func wrongSigningIdentity(ident identity.Identity) identity.Identity {
	stale := ident
	stale.Keys = make(map[string]identity.VerificationMethod, len(ident.Keys))
	for name, key := range ident.Keys {
		stale.Keys[name] = key
	}
	key := stale.Keys["atproto"]
	key.PublicKeyMultibase = "zQ3shbzd9YoCFQrzfdw2AGpxUHTjUhh69MXRh7hHBavx9wQon"
	stale.Keys["atproto"] = key
	return stale
}

func TestKeyRotationRefreshesIdentityBeforeAcceptingCommit(t *testing.T) {
	ident, commit := loadIngestFixture(t)
	directory := &rotatingDirectory{stale: wrongSigningIdentity(ident), fresh: ident}
	relay, _, _ := newIngestTestRelay(t, directory, ident.DID, true, nil)

	require.NoError(t, relay.processRepoEvent(t.Context(), &stream.XRPCStreamEvent{RepoCommit: &commit}, "source.example", ingestTestHostID))
	require.Equal(t, 1, directory.purgeCalls)
	_, err := relay.GetAccountRepo(t.Context(), 1)
	require.NoError(t, err)
}

func TestKeyRefreshFailureLeavesCommitReplayable(t *testing.T) {
	ident, commit := loadIngestFixture(t)
	directory := &rotatingDirectory{
		stale:    wrongSigningIdentity(ident),
		fresh:    ident,
		purgeErr: errors.New("directory unavailable"),
	}
	relay, _, _ := newIngestTestRelay(t, directory, ident.DID, true, nil)

	err := relay.processRepoEvent(t.Context(), &stream.XRPCStreamEvent{RepoCommit: &commit}, "source.example", ingestTestHostID)
	require.ErrorIs(t, err, ErrIdentityRefresh)
	require.Equal(t, 1, directory.purgeCalls)

	rejections, listErr := relay.ListRejectedEvents(t.Context(), 0, 10)
	require.NoError(t, listErr)
	require.Empty(t, rejections)
}

func TestInvalidSignatureIsRejectedOnlyAfterIdentityRefresh(t *testing.T) {
	ident, commit := loadIngestFixture(t)
	stale := wrongSigningIdentity(ident)
	directory := &rotatingDirectory{stale: stale, fresh: stale}
	relay, _, _ := newIngestTestRelay(t, directory, ident.DID, true, nil)

	require.NoError(t, relay.processRepoEvent(t.Context(), &stream.XRPCStreamEvent{RepoCommit: &commit}, "source.example", ingestTestHostID))
	require.Equal(t, 1, directory.purgeCalls)
	rejections, err := relay.ListRejectedEvents(t.Context(), 0, 10)
	require.NoError(t, err)
	require.Len(t, rejections, 1)
	require.Equal(t, rejectionReasonInvalidSignature, rejections[0].ReasonCode)
	_, repoErr := relay.GetAccountRepo(t.Context(), 1)
	require.ErrorIs(t, repoErr, ErrAccountRepoNotFound)
}
