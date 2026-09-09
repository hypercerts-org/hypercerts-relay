package relay

import (
	"testing"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/cmd/relay/relay/models"
	"github.com/bluesky-social/indigo/cmd/relay/stream"
	"github.com/stretchr/testify/require"
)

func TestMigrationPreservesFormerSourceWithoutAdmittingTarget(t *testing.T) {
	ident, _ := loadIngestFixture(t)
	directory := newFixtureDirectory(ident)
	r, db, persistence := newIngestTestRelay(t, directory, ident.DID, true, nil)
	event := func(seq int64) *stream.XRPCStreamEvent {
		return &stream.XRPCStreamEvent{RepoIdentity: &comatproto.SyncSubscribeRepos_Identity{
			Seq: seq, Did: ident.DID.String(), Time: "2026-09-07T12:00:00Z",
		}}
	}
	require.NoError(t, r.processSourceEvent(t.Context(), event(1), "source.example", ingestTestHostID))
	require.Len(t, persistence.events, 1)
	ident.Services["atproto_pds"] = identity.ServiceEndpoint{Type: "AtprotoPersonalDataServer", URL: "https://target.example"}
	directory.Insert(ident)
	require.NoError(t, r.processSourceEvent(t.Context(), event(2), "source.example", ingestTestHostID))
	require.Len(t, persistence.events, 1)
	rejections, err := r.ListRejectedEvents(t.Context(), 0, 10)
	require.NoError(t, err)
	require.Len(t, rejections, 1)
	require.Equal(t, "source_host_mismatch", rejections[0].ReasonCode)
	page, err := r.ListSourceAccounts(t.Context(), ingestTestHostID, "", 10)
	require.NoError(t, err)
	require.Len(t, page.Accounts, 1)
	require.Equal(t, "target.example", page.Accounts[0].ResolvedHostname)
	require.Equal(t, "unknown", page.Accounts[0].TargetCoverage)
	require.Equal(t, ingestTestHostID, page.Accounts[0].CurrentHostID)
	var targets int64
	require.NoError(t, db.Model(&models.Host{}).Where("hostname = ?", "target.example").Count(&targets).Error)
	require.Zero(t, targets)
	require.False(t, r.Slurper.CheckIfSubscribed("target.example"))

	target, err := r.AddSource(t.Context(), "https://target.example")
	require.NoError(t, err)
	// Model an admitted target without opening an external connection.
	require.NoError(t, db.Model(&models.Source{}).Where("host_id = ?", target.HostID).Update("validation_status", models.SourceValidationPassed).Error)
	require.NoError(t, db.Model(&models.Host{}).Where("id = ?", target.HostID).Update("account_limit", 0).Error)
	require.NoError(t, r.processSourceEvent(t.Context(), event(1), "target.example", target.HostID))
	require.Len(t, persistence.events, 2)
	page, err = r.ListSourceAccounts(t.Context(), ingestTestHostID, "", 10)
	require.NoError(t, err)
	require.Equal(t, ingestTestHostID, page.Accounts[0].ObservedSourceHostID)
	require.Equal(t, target.HostID, page.Accounts[0].CurrentHostID)
	require.Equal(t, "incomplete", page.Accounts[0].TargetCoverage)
	require.Equal(t, admissionReasonHostAccountLimit, page.Accounts[0].AdmissionReason)
	current, err := r.ListSourceAccounts(t.Context(), target.HostID, "", 10)
	require.NoError(t, err)
	require.Len(t, current.Accounts, 1)
	require.True(t, current.Accounts[0].RecoveryRequired)
	require.Equal(t, models.AccountStatusHostThrottled, current.Accounts[0].CurrentStatus)
	former, err := r.GetHostByID(t.Context(), ingestTestHostID)
	require.NoError(t, err)
	require.Zero(t, former.AccountCount)
}

func TestSourcePolicyMigrationDoesNotReviveDisabledHost(t *testing.T) {
	r, db := newSourceRelay(t)
	for _, status := range []models.HostStatus{models.HostStatusActive, models.HostStatusOffline, models.HostStatusBanned} {
		host := &models.Host{Hostname: string(status) + ".example", Status: status, LastSeq: 42}
		require.NoError(t, db.Create(host))
	}
	require.NoError(t, r.MigrateDatabase())
	page, err := r.ListSources(t.Context(), 0, 10)
	require.NoError(t, err)
	require.Len(t, page.Sources, 3)
	for _, source := range page.Sources {
		require.Equal(t, int64(42), source.LastDurableCursor)
		if source.HostStatus == models.HostStatusBanned {
			require.Equal(t, models.SourceStateDisabled, source.DesiredState)
		}
	}
	_, err = r.SetSourceState(t.Context(), page.Sources[0].HostID, 1, models.SourceStateRemoved)
	require.NoError(t, err)
	require.NoError(t, r.MigrateDatabase())
	page, err = r.ListSources(t.Context(), 0, 10)
	require.NoError(t, err)
	require.Equal(t, models.SourceStateRemoved, page.Sources[0].DesiredState)
	_, err = r.ValidateSource(t.Context(), page.Sources[0].HostID, 1)
	require.ErrorIs(t, err, ErrSourceRevisionConflict)
}
