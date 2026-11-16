package relay

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gorm.io/gorm"

	"github.com/bluesky-social/indigo/cmd/relay/relay/models"
	"github.com/stretchr/testify/require"
)

func TestSourceHandoffAcknowledgmentContracts(t *testing.T) {
	valid := `{"id":"job-1","pds":"https://handoff.example.com","sourceRevision":3,"state":"pending","reason":"source_added","policyRevision":1,"policy":{"revision":1}}`
	cases := []struct {
		name     string
		status   int
		body     string
		redirect bool
		accepted bool
	}{
		{name: "valid 202", status: 202, body: valid, accepted: true},
		{name: "failed durable job 202", status: 202, body: `{"id":"job-failed","pds":"https://handoff.example.com","sourceRevision":3,"state":"failed","reason":"source_added","policyRevision":1,"policy":{"revision":1}}`, accepted: true},
		{name: "malformed 200", status: 200, body: "not json"},
		{name: "unrelated 200", status: 200, body: valid},
		{name: "mismatched coordinate", status: 202, body: `{"id":"job-1","pds":"https://other.example","sourceRevision":3,"state":"pending","reason":"source_added","policyRevision":1}`},
		{name: "redirect", status: 302, redirect: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, db := newSourceRelay(t)
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				calls++
				if tc.redirect {
					http.Redirect(w, req, "/other", http.StatusFound)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			r.Config.JetstreamControlURL, r.Config.JetstreamControlToken = server.URL, "secret-token"
			host := &models.Host{Hostname: "handoff.example.com"}
			require.NoError(t, db.Create(host))
			row := &models.SourceHandoff{HostID: host.ID, PDS: "https://handoff.example.com", SourceRevision: 3, State: string(models.SourceStateEnabled), NextRetryAt: time.Now().Add(-time.Hour)}
			require.NoError(t, db.DB.Create(row).Error)
			require.NoError(t, r.ProcessSourceHandoffs(context.Background()))
			var remaining models.SourceHandoff
			err := db.First(&remaining, "host_id = ?", host.ID).Error
			if tc.accepted {
				require.ErrorIs(t, err, gorm.ErrRecordNotFound)
			} else {
				require.NoError(t, err)
				require.Equal(t, uint(1), remaining.Attempts)
			}
			if tc.redirect {
				require.Equal(t, 1, calls)
			}
		})
	}
}

func TestSourceHandoffAcceptsExistingDurableJob(t *testing.T) {
	r, db := newSourceRelay(t)
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"id":"job-existing","pds":"https://handoff.example.com","sourceRevision":3,"state":"complete","reason":"source_added","policyRevision":1,"policy":{"revision":1}}`))
	}))
	defer server.Close()
	r.Config.JetstreamControlURL, r.Config.JetstreamControlToken = server.URL, "secret-token"
	host := &models.Host{Hostname: "handoff.example.com", Status: models.HostStatusActive}
	require.NoError(t, db.Create(host))
	require.NoError(t, db.Create(&models.Source{HostID: host.ID, State: models.SourceStateEnabled, Revision: 3, ValidationStatus: models.SourceValidationPassed}))
	require.NoError(t, db.DB.Create(&models.SourceHandoff{HostID: host.ID, PDS: "https://handoff.example.com", SourceRevision: 3, State: string(models.SourceStateEnabled), NextRetryAt: time.Now().Add(-time.Hour)}).Error)
	require.NoError(t, r.ProcessSourceHandoffs(context.Background()))
	var remaining int64
	require.NoError(t, db.Model(&models.SourceHandoff{}).Count(&remaining).Error)
	require.Zero(t, remaining)
	require.Equal(t, 1, calls, "the durable-job acknowledgment must prevent a duplicate request")
}

func TestManagedSourceRemovalHandoffSendsDelete(t *testing.T) {
	r, db := newSourceRelay(t)
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		calls++
		require.Equal(t, http.MethodDelete, req.Method)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	r.Config.JetstreamControlURL, r.Config.JetstreamControlToken = server.URL, "secret-token"
	host := &models.Host{Hostname: "handoff.example.com", Status: models.HostStatusActive}
	require.NoError(t, db.Create(host))
	require.NoError(t, db.Create(&models.Source{HostID: host.ID, State: models.SourceStateEnabled, Revision: 3, ValidationStatus: models.SourceValidationPassed}))
	_, err := r.SetSourceState(context.Background(), host.ID, 3, models.SourceStateRemoved)
	require.NoError(t, err)
	require.NoError(t, r.ProcessSourceHandoffs(context.Background()))
	var remaining int64
	require.NoError(t, db.Model(&models.SourceHandoff{}).Count(&remaining).Error)
	require.Zero(t, remaining)
	require.Equal(t, 1, calls)
}

func TestSourceHandoffDeleteRequires204(t *testing.T) {
	for _, status := range []int{200, 202, 204} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			r, db := newSourceRelay(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
			defer server.Close()
			sender := newSourceHandoffSender(server.URL, "secret-token")
			row := models.SourceHandoff{PDS: "https://handoff.example.com", SourceRevision: 4, State: string(models.SourceStateDisabled)}
			err := sender.send(context.Background(), row)
			if status == http.StatusNoContent {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			_ = r
			_ = db
		})
	}
}

var _ = json.Valid
