package relay

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/cmd/relay/relay/models"
	"github.com/bluesky-social/indigo/cmd/relay/stream"
	"github.com/bluesky-social/indigo/cmd/relay/stream/eventmgr"
	"github.com/bluesky-social/indigo/cmd/relay/stream/persist/diskpersist"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func newSourceLifecycleRelay(t *testing.T) (*Relay, sourceTestDB, chan int64) {
	t.Helper()

	r, db := newSourceRelay(t)
	processed := make(chan int64, 8)
	config := DefaultSlurperConfig()
	config.ConcurrencyPerHost = 1
	config.PersistCursorPeriod = time.Hour
	config.PersistCursorCallback = r.PersistHostCursors
	config.PersistHostStatusCallback = r.UpdateHostStatus
	slurper, err := NewSlurper(func(_ context.Context, evt *stream.XRPCStreamEvent, _ string, _ uint64) error {
		processed <- evt.Sequence()
		return nil
	}, config)
	require.NoError(t, err)
	r.Slurper = slurper
	r.HostChecker = &HostClient{Client: &http.Client{Timeout: time.Second}}
	t.Cleanup(func() { require.NoError(t, slurper.Shutdown()) })

	return r, db, processed
}

func requireNoSourceConnection(t *testing.T, fixture *sourceFixture) {
	t.Helper()

	select {
	case connection := <-fixture.connections:
		_ = connection.conn.Close()
		t.Fatal("unexpected source connection")
	case <-time.After(200 * time.Millisecond):
	}
}

func addPassedSource(t *testing.T, r *Relay, fixture *sourceFixture) (*SourceView, *sourceConnection) {
	t.Helper()

	added, err := r.AddSource(context.Background(), "http://"+fixture.host)
	require.NoError(t, err)
	require.Equal(t, models.SourceStateEnabled, added.DesiredState)
	require.Equal(t, models.SourceValidationPending, added.Validation.Status)
	require.False(t, r.Slurper.CheckIfSubscribed(fixture.host))
	requireNoSourceConnection(t, fixture)

	validated, err := r.ValidateSource(context.Background(), added.HostID, added.Revision)
	require.NoError(t, err)
	require.Equal(t, models.SourceValidationPassed, validated.Validation.Status)
	return validated, fixture.next(t)
}

func TestSourceAddDefersConnectionAndListsQuietSource(t *testing.T) {
	ctx := context.Background()
	fixture := newSourceFixture(t)
	r, _, _ := newSourceLifecycleRelay(t)

	added, err := r.AddSource(ctx, "http://"+fixture.host)
	require.NoError(t, err)
	require.Equal(t, models.SourceStateEnabled, added.DesiredState)
	require.Equal(t, models.SourceValidationPending, added.Validation.Status)
	require.Equal(t, "configured", added.RuntimeState)
	require.False(t, r.Slurper.CheckIfSubscribed(fixture.host))
	requireNoSourceConnection(t, fixture)

	page, err := r.ListSources(ctx, 0, 1)
	require.NoError(t, err)
	require.Len(t, page.Sources, 1)
	require.Equal(t, added.HostID, page.Sources[0].HostID)
	require.Equal(t, int64(-1), page.Sources[0].LastDurableCursor)

	validated, err := r.ValidateSource(ctx, added.HostID, added.Revision)
	require.NoError(t, err)
	require.Equal(t, models.SourceValidationPassed, validated.Validation.Status)
	_ = fixture.next(t)
}

func TestSourceEnableAndRetryPreserveActiveSubscription(t *testing.T) {
	ctx := context.Background()
	fixture := newSourceFixture(t)
	r, _, processed := newSourceLifecycleRelay(t)
	validated, connection := addPassedSource(t, r, fixture)

	// The private control adapter enables the source after validating it, then
	// repeats AddSource and SetSourceState when an administrator retries.
	enabled, err := r.SetSourceState(ctx, validated.HostID, validated.Revision, models.SourceStateEnabled)
	require.NoError(t, err)
	require.Equal(t, validated.Revision, enabled.Revision)
	duplicate, err := r.AddSource(ctx, "http://"+fixture.host)
	require.NoError(t, err)
	retried, err := r.SetSourceState(ctx, duplicate.HostID, duplicate.Revision, models.SourceStateEnabled)
	require.NoError(t, err)
	require.Equal(t, enabled.Revision, retried.Revision)
	requireNoSourceConnection(t, fixture)

	connection.emit(t, &stream.XRPCStreamEvent{RepoIdentity: &comatproto.SyncSubscribeRepos_Identity{
		Seq: 7, Did: "did:plc:abcdefghijklmnopqrstuvwx", Time: "2026-09-07T12:00:00Z",
	}})
	select {
	case sequence := <-processed:
		require.Equal(t, int64(7), sequence)
	case <-time.After(5 * time.Second):
		t.Fatal("original source connection stopped processing events")
	}
}

func TestSubscribeRejectsDrainingSubscription(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	host := &models.Host{Hostname: "draining.example"}
	sub := &Subscription{Hostname: host.Hostname, ctx: ctx}
	slurper := &Slurper{subs: map[string]*Subscription{host.Hostname: sub}}

	require.ErrorContains(t, slurper.Subscribe(host), "subscription is stopping")
	require.Same(t, sub, slurper.subs[host.Hostname])
}

func TestSourceDisableDuplicateAddAndReenableResumeDurableCursor(t *testing.T) {
	ctx := context.Background()
	fixture := newSourceFixture(t)
	r, _, processed := newSourceLifecycleRelay(t)
	validated, connection := addPassedSource(t, r, fixture)

	connection.emit(t, &stream.XRPCStreamEvent{RepoIdentity: &comatproto.SyncSubscribeRepos_Identity{
		Seq: 7, Did: "did:plc:abcdefghijklmnopqrstuvwx", Time: "2026-09-07T12:00:00Z",
	}})
	select {
	case sequence := <-processed:
		require.Equal(t, int64(7), sequence)
	case <-time.After(5 * time.Second):
		t.Fatal("source event was not processed")
	}
	require.Eventually(t, func() bool {
		r.Slurper.subsLk.Lock()
		sub := r.Slurper.subs[fixture.host]
		r.Slurper.subsLk.Unlock()
		if sub == nil {
			return false
		}
		sub.UpdateSeq()
		return sub.LastSeq.Load() == 7
	}, 5*time.Second, time.Millisecond)
	host, err := r.GetHostByID(ctx, validated.HostID)
	require.NoError(t, err)
	require.Equal(t, int64(-1), host.LastSeq)
	disabled, err := r.SetSourceState(ctx, validated.HostID, validated.Revision, models.SourceStateDisabled)
	require.NoError(t, err)
	require.Equal(t, models.SourceStateDisabled, disabled.DesiredState)
	require.Eventually(t, func() bool {
		select {
		case <-connection.closed:
			return true
		default:
			return false
		}
	}, 5*time.Second, time.Millisecond)
	require.False(t, r.Slurper.CheckIfSubscribed(fixture.host))
	require.Eventually(t, func() bool {
		host, err := r.GetHostByID(ctx, validated.HostID)
		return err == nil && host.LastSeq == 7
	}, 5*time.Second, time.Millisecond)

	duplicate, err := r.AddSource(ctx, "http://"+fixture.host)
	require.NoError(t, err)
	require.Equal(t, models.SourceStateDisabled, duplicate.DesiredState)
	require.Equal(t, disabled.Revision, duplicate.Revision)
	requireNoSourceConnection(t, fixture)

	enabled, err := r.SetSourceState(ctx, disabled.HostID, disabled.Revision, models.SourceStateEnabled)
	require.NoError(t, err)
	require.Equal(t, models.SourceValidationPassed, enabled.Validation.Status)
	resumed := fixture.next(t)
	require.Equal(t, "7", resumed.cursor)
}

func TestSourceRestartOnlyReconnectsEnabledPassedPolicies(t *testing.T) {
	ctx := context.Background()
	enabledIdle := newSourceFixture(t)
	enabledOffline := newSourceFixture(t)
	pending := newSourceFixture(t)
	disabled := newSourceFixture(t)
	r, db, _ := newSourceLifecycleRelay(t)

	for _, source := range []struct {
		fixture    *sourceFixture
		status     models.HostStatus
		state      models.SourceState
		validation models.SourceValidationStatus
	}{
		{enabledIdle, models.HostStatusIdle, models.SourceStateEnabled, models.SourceValidationPassed},
		{enabledOffline, models.HostStatusOffline, models.SourceStateEnabled, models.SourceValidationPassed},
		{pending, models.HostStatusActive, models.SourceStateEnabled, models.SourceValidationPending},
		{disabled, models.HostStatusActive, models.SourceStateDisabled, models.SourceValidationPassed},
	} {
		host := &models.Host{Hostname: source.fixture.host, NoSSL: true, Status: source.status, AccountLimit: 100}
		require.NoError(t, db.Create(host))
		require.NoError(t, db.Create(&models.Source{
			HostID:           host.ID,
			State:            source.state,
			Revision:         1,
			ValidationStatus: source.validation,
			RecoveryRequired: true,
		}))
	}

	restarted, err := NewSlurper(r.Slurper.processCallback, r.Slurper.Config)
	require.NoError(t, err)
	previous := r.Slurper
	r.Slurper = restarted
	require.NoError(t, previous.Shutdown())
	t.Cleanup(func() { require.NoError(t, restarted.Shutdown()) })

	require.NoError(t, r.ResubscribeAllHosts(ctx))
	_ = enabledIdle.next(t)
	_ = enabledOffline.next(t)
	requireNoSourceConnection(t, pending)
	requireNoSourceConnection(t, disabled)
}

func TestNewRelayMigratesSourcePolicyWithoutOverwritingExistingState(t *testing.T) {
	ctx := context.Background()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "relay.db")), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.Host{}, &models.Source{}))

	legacy := &models.Host{Hostname: "legacy.example.com", Status: models.HostStatusIdle}
	banned := &models.Host{Hostname: "banned.example.com", Status: models.HostStatusBanned}
	existing := &models.Host{Hostname: "policy.example.com", Status: models.HostStatusActive}
	for _, host := range []*models.Host{legacy, banned, existing} {
		require.NoError(t, db.Create(host).Error)
	}
	existingSource := &models.Source{
		HostID:           existing.ID,
		State:            models.SourceStateRemoved,
		Revision:         9,
		LastOperation:    "state:removed",
		ValidationStatus: models.SourceValidationFailed,
		RecoveryRequired: false,
	}
	require.NoError(t, db.Create(existingSource).Error)
	require.False(t, existingSource.RecoveryRequired, "inserting a completed recovery must preserve false")

	events := eventmgr.NewEventManager(&testEventPersistence{})
	r, err := NewRelay(db, events, identity.NewMockDirectory(), nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, r.Slurper.Shutdown())
		require.NoError(t, events.Shutdown(ctx))
	})

	require.True(t, db.Migrator().HasTable(&models.AccountSourceObservation{}))
	for _, expected := range []struct {
		hostID     uint64
		state      models.SourceState
		validation models.SourceValidationStatus
		revision   uint64
	}{
		{legacy.ID, models.SourceStateEnabled, models.SourceValidationPassed, 1},
		{banned.ID, models.SourceStateDisabled, models.SourceValidationPassed, 1},
		{existing.ID, models.SourceStateRemoved, models.SourceValidationFailed, 9},
	} {
		var got models.Source
		require.NoError(t, db.Where("host_id = ?", expected.hostID).First(&got).Error)
		require.Equal(t, expected.state, got.State)
		require.Equal(t, expected.validation, got.ValidationStatus)
		require.Equal(t, expected.revision, got.Revision)
	}
	var preserved models.Source
	require.NoError(t, db.Where("host_id = ?", existing.ID).First(&preserved).Error)
	require.Equal(t, *existingSource, preserved)
	require.False(t, preserved.RecoveryRequired)
}

func TestSourceRemovalRetainsRawReplay(t *testing.T) {
	ctx := context.Background()
	fixture := newSourceFixture(t)
	did := syntax.DID("did:plc:abcdefghijklmnopqrstuvwx")
	directory := identity.NewMockDirectory()
	directory.Insert(identity.Identity{
		DID: did, Handle: syntax.HandleInvalid,
		Services: map[string]identity.ServiceEndpoint{
			"atproto_pds": {Type: "AtprotoPersonalDataServer", URL: "http://" + fixture.host},
		},
	})

	root := t.TempDir()
	db, err := gorm.Open(sqlite.Open(filepath.Join(root, "relay.db")), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	persistence, err := diskpersist.NewDiskPersistence(filepath.Join(root, "events"), "", db, nil)
	require.NoError(t, err)
	events := eventmgr.NewEventManager(persistence)
	r, err := NewRelay(db, events, directory, nil)
	require.NoError(t, err)
	persistence.SetUidSource(r)
	r.HostChecker = &HostClient{Client: &http.Client{Timeout: time.Second}}
	t.Cleanup(func() {
		require.NoError(t, r.Slurper.Shutdown())
		require.NoError(t, events.Shutdown(ctx))
	})

	added, err := r.AddSource(ctx, "http://"+fixture.host)
	require.NoError(t, err)
	validated, err := r.ValidateSource(ctx, added.HostID, added.Revision)
	require.NoError(t, err)
	connection := fixture.next(t)
	connection.emit(t, &stream.XRPCStreamEvent{RepoIdentity: &comatproto.SyncSubscribeRepos_Identity{
		Seq: 1, Did: did.String(), Time: "2026-09-07T12:00:00Z",
	}})
	require.Eventually(t, func() bool {
		if err := r.Slurper.persistCursors(ctx); err != nil {
			return false
		}
		host, err := r.GetHostByID(ctx, validated.HostID)
		return err == nil && host.LastSeq == 1
	}, 5*time.Second, time.Millisecond)

	removed, err := r.SetSourceState(ctx, validated.HostID, validated.Revision, models.SourceStateRemoved)
	require.NoError(t, err)
	require.Equal(t, models.SourceStateRemoved, removed.DesiredState)

	since := int64(0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_ = r.HandleSubscribeRepos(w, req, &since, "127.0.0.1")
	}))
	t.Cleanup(server.Close)
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/xrpc/com.atproto.sync.subscribeRepos", nil)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	kind, reader, err := conn.NextReader()
	require.NoError(t, err)
	require.Equal(t, websocket.BinaryMessage, kind)
	var header stream.EventHeader
	require.NoError(t, header.UnmarshalCBOR(reader))
	require.Equal(t, "#identity", header.MsgType)
	var replayed comatproto.SyncSubscribeRepos_Identity
	require.NoError(t, replayed.UnmarshalCBOR(reader))
	require.Equal(t, did.String(), replayed.Did)
}

func TestPublicSubscribeCannotCreateOrReviveManagedSources(t *testing.T) {
	ctx := context.Background()
	r, db, _ := newSourceLifecycleRelay(t)
	unknown := newSourceFixture(t)

	require.Error(t, r.SubscribeToHost(ctx, unknown.host, true, false))
	var unknownCount int64
	require.NoError(t, db.Model(&models.Host{}).Where("hostname = ?", unknown.host).Count(&unknownCount).Error)
	require.Zero(t, unknownCount)
	requireNoSourceConnection(t, unknown)

	disabledFixture := newSourceFixture(t)
	validated, connection := addPassedSource(t, r, disabledFixture)
	disabled, err := r.SetSourceState(ctx, validated.HostID, validated.Revision, models.SourceStateDisabled)
	require.NoError(t, err)
	select {
	case <-connection.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("disabled source connection did not close")
	}
	require.Error(t, r.SubscribeToHost(ctx, disabledFixture.host, true, false))
	require.Equal(t, models.SourceStateDisabled, disabled.DesiredState)
	requireNoSourceConnection(t, disabledFixture)
}
