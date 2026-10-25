package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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

type forwardingRoundTripper struct {
	forwarded chan<- struct{}
}

func (r *forwardingRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	r.forwarded <- struct{}{}
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
}

func TestAdminRequestCrawlDoesNotForwardOnValidationError(t *testing.T) {
	svc, r, _ := newSourceHandlerService(t)
	r.HostChecker = &countingHostChecker{checkErr: errors.New("host check failed")}
	forwarded := make(chan struct{}, 1)
	svc.config.SiblingRelayHosts = []string{"sibling.example"}
	svc.siblingClient = http.Client{Transport: &forwardingRoundTripper{forwarded: forwarded}}

	rec := invokeRequestCrawl(t, svc, "https://admin-no-forward.example", true)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	select {
	case <-forwarded:
		t.Fatal("validation failure was forwarded to a sibling relay")
	case <-time.After(25 * time.Millisecond):
	}
}

type recordingHostChecker struct {
	urls []string
}

func (c *recordingHostChecker) CheckHost(_ context.Context, host string) error {
	c.urls = append(c.urls, host)
	return nil
}

func (*recordingHostChecker) FetchAccountStatus(context.Context, *identity.Identity) (models.AccountStatus, error) {
	return models.AccountStatusInactive, nil
}

func TestPublicRequestCrawlStoredHTTPSUsesHTTPSForValidation(t *testing.T) {
	svc, r, _ := newSourceHandlerService(t)
	_, err := r.AddSource(context.Background(), "https://stored-https.example")
	require.NoError(t, err)
	checker := &recordingHostChecker{}
	r.HostChecker = checker

	rec := invokeRequestCrawl(t, svc, "stored-https.example", false)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, []string{"https://stored-https.example"}, checker.urls)
}

func TestPublicRequestCrawlAdmitsAndValidatesNewSource(t *testing.T) {
	svc, r, db := newSourceHandlerService(t)
	checker := &countingHostChecker{}
	r.HostChecker = checker

	rec := invokeRequestCrawl(t, svc, "https://public-new.example", false)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, 1, checker.checkCalls)
	var source models.Source
	require.NoError(t, db.Where("host_id = (SELECT id FROM host WHERE hostname = ?)", "public-new.example").First(&source).Error)
	require.Equal(t, models.SourceValidationPassed, source.ValidationStatus)
}

func TestPublicRequestCrawlValidationFailureIsNotForwarded(t *testing.T) {
	svc, r, db := newSourceHandlerService(t)
	checker := &countingHostChecker{checkErr: errors.New("host check failed")}
	r.HostChecker = checker
	forwarded := make(chan struct{}, 1)
	svc.config.SiblingRelayHosts = []string{"sibling.example"}
	svc.siblingClient = http.Client{Transport: &forwardingRoundTripper{forwarded: forwarded}}

	rec := invokeRequestCrawl(t, svc, "https://public-fail.example", false)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, 1, checker.checkCalls)
	var source models.Source
	require.NoError(t, db.Where("host_id = (SELECT id FROM host WHERE hostname = ?)", "public-fail.example").First(&source).Error)
	require.Equal(t, models.SourceValidationFailed, source.ValidationStatus)
	select {
	case <-forwarded:
		t.Fatal("validation failure was forwarded to a sibling relay")
	case <-time.After(25 * time.Millisecond):
	}
}

func TestPublicRequestCrawlDuplicateValidationIsIdempotent(t *testing.T) {
	svc, r, db := newSourceHandlerService(t)
	checker := &countingHostChecker{}
	r.HostChecker = checker

	first := invokeRequestCrawl(t, svc, "https://public-duplicate.example", false)
	second := invokeRequestCrawl(t, svc, "https://public-duplicate.example", false)

	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, http.StatusOK, second.Code)
	require.Equal(t, 1, checker.checkCalls)
	var count int64
	require.NoError(t, db.Model(&models.Source{}).Count(&count).Error)
	require.EqualValues(t, 1, count)
}

func TestPublicRequestCrawlAdmitsPreviouslyRegisteredSource(t *testing.T) {
	svc, r, db := newSourceHandlerService(t)
	checker := &countingHostChecker{}
	r.HostChecker = checker
	_, err := r.AddSource(context.Background(), "https://unknown.example")
	require.NoError(t, err)

	rec := invokeRequestCrawl(t, svc, "https://unknown.example", false)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, 1, checker.checkCalls)
	var hosts int64
	require.NoError(t, db.Model(&models.Host{}).Where("hostname = ?", "unknown.example").Count(&hosts).Error)
	require.EqualValues(t, 1, hosts)
}

func TestPublicRequestCrawlLimiterReturnsTooManyRequestsWithoutRows(t *testing.T) {
	svc, r, db := newSourceHandlerService(t)
	r.HostPerDayLimiter.SetLimit(0)
	checker := &countingHostChecker{}
	r.HostChecker = checker

	rec := invokeRequestCrawl(t, svc, "https://limited-public.example", false)

	require.Equal(t, http.StatusTooManyRequests, rec.Code)
	require.Equal(t, 0, checker.checkCalls)
	var hosts, sources int64
	require.NoError(t, db.Model(&models.Host{}).Where("hostname = ?", "limited-public.example").Count(&hosts).Error)
	require.NoError(t, db.Model(&models.Source{}).Count(&sources).Error)
	require.Zero(t, hosts)
	require.Zero(t, sources)
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
	svc, r, db := newSourceHandlerService(t)
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

	var source models.Source
	require.NoError(t, db.Where("host_id = (SELECT id FROM host WHERE hostname = ?)", "admin-validate.example").First(&source).Error)
	require.Equal(t, models.SourceValidationFailed, source.ValidationStatus)
	require.Equal(t, "validate", source.LastOperation)
}

func TestAdminRequestCrawlAdmitsAndValidatesNewPublicSource(t *testing.T) {
	svc, r, db := newSourceHandlerService(t)
	checker := &countingHostChecker{}
	r.HostChecker = checker

	rec := invokeRequestCrawl(t, svc, "https://admin-public.example", true)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, 1, checker.checkCalls)
	var source models.Source
	require.NoError(t, db.Where("host_id = (SELECT id FROM host WHERE hostname = ?)", "admin-public.example").First(&source).Error)
	require.Equal(t, models.SourceValidationPassed, source.ValidationStatus)
}

func TestAdminRequestCrawlDuplicateValidationIsIdempotent(t *testing.T) {
	svc, r, db := newSourceHandlerService(t)
	checker := &countingHostChecker{}
	r.HostChecker = checker

	first := invokeRequestCrawl(t, svc, "https://admin-duplicate.example", true)
	second := invokeRequestCrawl(t, svc, "https://admin-duplicate.example", true)

	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, http.StatusOK, second.Code)
	require.Equal(t, 1, checker.checkCalls)
	var count int64
	require.NoError(t, db.Model(&models.Source{}).Count(&count).Error)
	require.EqualValues(t, 1, count)
}
