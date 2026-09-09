package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/cmd/relay/relay"
	"github.com/bluesky-social/indigo/cmd/relay/relay/models"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type countingHostChecker struct {
	checkCalls int
	checkErr   error
}

func (c *countingHostChecker) CheckHost(context.Context, string) error {
	c.checkCalls++
	return c.checkErr
}

func (c *countingHostChecker) FetchAccountStatus(context.Context, *identity.Identity) (models.AccountStatus, error) {
	return models.AccountStatusInactive, nil
}

func newSourceHandlerService(t *testing.T) (*Service, *relay.Relay, *gorm.DB) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	r, err := relay.NewRelay(db, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		sqlDB, err := db.DB()
		require.NoError(t, err)
		require.NoError(t, sqlDB.Close())
	})
	slurper := r.Slurper
	r.Slurper = nil
	t.Cleanup(func() { require.NoError(t, slurper.Shutdown()) })
	svc, err := NewService(r, DefaultServiceConfig())
	require.NoError(t, err)
	return svc, r, db
}

func invokeRequestCrawl(t *testing.T, svc *Service, hostname string, admin bool) *httptest.ResponseRecorder {
	t.Helper()
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.sync.requestCrawl", nil)
	rec := httptest.NewRecorder()
	err := svc.handleComAtprotoSyncRequestCrawl(e.NewContext(req, rec), &comatproto.SyncRequestCrawl_Input{Hostname: hostname}, admin)
	require.NoError(t, err)
	return rec
}

func TestPublicRequestCrawlUnknownSourceDoesNotValidateOrCreateHost(t *testing.T) {
	svc, r, db := newSourceHandlerService(t)
	checker := &countingHostChecker{}
	r.HostChecker = checker

	rec := invokeRequestCrawl(t, svc, "https://unknown.example", false)

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Zero(t, checker.checkCalls)
	var hosts int64
	require.NoError(t, db.Model(&models.Host{}).Where("hostname = ?", "unknown.example").Count(&hosts).Error)
	require.Zero(t, hosts)
}

func TestPublicRequestCrawlDeniesBlockedSource(t *testing.T) {
	svc, r, _ := newSourceHandlerService(t)
	checker := &countingHostChecker{}
	r.HostChecker = checker
	ctx := context.Background()
	_, err := r.AddSource(ctx, "https://blocked.example")
	require.NoError(t, err)
	require.NoError(t, r.SetSourceBlocked(ctx, "blocked.example", true))

	rec := invokeRequestCrawl(t, svc, "https://blocked.example", false)

	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Zero(t, checker.checkCalls)
}

func TestAdminRequestCrawlValidationFailureIsSafe(t *testing.T) {
	svc, r, _ := newSourceHandlerService(t)
	checker := &countingHostChecker{checkErr: errors.New("upstream credentials rejected")}
	r.HostChecker = checker

	rec := invokeRequestCrawl(t, svc, "https://admin-validate.example", true)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, 1, checker.checkCalls)
	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, "managed source validation failed", body["message"])
	require.NotContains(t, rec.Body.String(), "admin-validate.example")
	require.NotContains(t, rec.Body.String(), "credentials")
}
