package relay

import (
	"context"
	"testing"

	"github.com/bluesky-social/indigo/cmd/relay/relay/models"
	"github.com/stretchr/testify/require"
)

func TestHostCursorPreservesPolicyAndProgress(t *testing.T) {
	r, db := testRelayWithHostDB(t)
	host := models.Host{Hostname: "disabled.example.com", Status: models.HostStatusBanned, LastSeq: 10}
	require.NoError(t, db.Create(&host).Error)
	for _, seq := range []int64{20, 15} {
		require.NoError(t, r.PersistHostCursors(context.Background(), &[]HostCursor{{HostID: host.ID, LastSeq: seq}}))
	}
	got, err := r.GetHostByID(context.Background(), host.ID)
	require.NoError(t, err)
	require.Equal(t, models.HostStatusBanned, got.Status)
	require.Equal(t, int64(20), got.LastSeq)
}

func TestHostCursorFailureRollsBackBatch(t *testing.T) {
	r, db := testRelayWithHostDB(t)
	hosts := []models.Host{
		{Hostname: "first.example.com", LastSeq: 10},
		{Hostname: "second.example.com", LastSeq: 10},
	}
	require.NoError(t, db.Create(&hosts).Error)
	require.NoError(t, db.Exec(`CREATE TRIGGER fail_cursor BEFORE UPDATE OF last_seq ON host
		WHEN NEW.hostname = 'second.example.com'
		BEGIN SELECT RAISE(ABORT, 'injected cursor failure'); END`).Error)
	cursors := []HostCursor{{HostID: hosts[0].ID, LastSeq: 20}, {HostID: hosts[1].ID, LastSeq: 20}}
	require.ErrorContains(t, r.PersistHostCursors(context.Background(), &cursors), "injected cursor failure")
	for _, host := range hosts {
		got, err := r.GetHostByID(context.Background(), host.ID)
		require.NoError(t, err)
		require.Equal(t, int64(10), got.LastSeq)
	}
}
