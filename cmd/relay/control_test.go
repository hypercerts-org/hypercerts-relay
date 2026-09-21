package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPrivateControlAuthenticationAndSourceLifecycle(t *testing.T) {
	svc, r, _ := newSourceHandlerService(t)
	r.HostChecker = &countingHostChecker{}
	token := strings.Repeat("x", 32)
	handler, err := svc.controlHandler(token)
	require.NoError(t, err)
	call := func(method, path, body, auth string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Authorization", auth)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}
	require.Equal(t, 401, call("PUT", "/hypercerts/v1/source", `{"pds":"https://managed.example","state":"enabled"}`, "").Code)
	for _, state := range []string{"enabled", "disabled", "enabled", "removed"} {
		rec := call("PUT", "/hypercerts/v1/source", `{"pds":"https://managed.example","state":"`+state+`"}`, "Bearer "+token)
		require.Equal(t, 200, rec.Code, rec.Body.String())
		require.Contains(t, rec.Body.String(), `"DesiredState":"`+state+`"`)
	}
	require.Equal(t, 400, call("PUT", "/hypercerts/v1/limits", `{"scope":"global","eventsPerSecond":0}`, "Bearer "+token).Code)
	require.Equal(t, 200, call("PUT", "/hypercerts/v1/limits", `{"scope":"global","eventsPerSecond":10}`, "Bearer "+token).Code)
	require.Contains(t, call("GET", "/hypercerts/v1/limits", "", "Bearer "+token).Body.String(), `"eventsPerSecond":10`)
}

// TestRecoveryReceiptAcceptance proves the Relay receiver only. The submitted
// coordinate is synthetic: Plan 006 owns proving a completed Jetstream job
// actually submits it after its durable boundary.
func TestRecoveryReceiptAcceptance(t *testing.T) {
	svc, r, _ := newSourceHandlerService(t)
	r.HostChecker = &countingHostChecker{}
	token := strings.Repeat("x", 32)
	handler, err := svc.controlHandler(token)
	require.NoError(t, err)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	call := func(method, path string, value any, authenticated bool) (int, []byte) {
		t.Helper()
		body, err := json.Marshal(value)
		require.NoError(t, err)
		req, err := http.NewRequest(method, server.URL+path, bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		if authenticated {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		response, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer response.Body.Close()
		result, err := io.ReadAll(response.Body)
		require.NoError(t, err)
		return response.StatusCode, result
	}

	type sourceView struct {
		Revision         uint64 `json:"Revision"`
		RecoveryRequired bool   `json:"RecoveryRequired"`
	}
	decodeSource := func(body []byte) sourceView {
		t.Helper()
		var source sourceView
		require.NoError(t, json.Unmarshal(body, &source))
		return source
	}

	status, body := call("PUT", "/hypercerts/v1/source", map[string]string{"pds": "https://receipt-control.example", "state": "enabled"}, true)
	require.Equal(t, http.StatusOK, status, string(body))
	first := decodeSource(body)
	require.True(t, first.RecoveryRequired)
	receipt := map[string]any{
		"pds":             "https://receipt-control.example",
		"sourceRevision":  first.Revision,
		"policyRevision":  2,
		"jobId":           "0123456789abcdef0123456789abcdef",
		"durableBoundary": "completed:2026-09-16T12:00:00Z",
	}
	status, _ = call("POST", "/hypercerts/v1/source/recovery-receipt", receipt, false)
	require.Equal(t, http.StatusUnauthorized, status)
	status, body = call("POST", "/hypercerts/v1/source/recovery-receipt", receipt, true)
	require.Equal(t, http.StatusOK, status, string(body))
	require.False(t, decodeSource(body).RecoveryRequired)

	advance := map[string]any{
		"pds":            "https://receipt-control.example",
		"sourceRevision": first.Revision,
		"policyRevision": 3,
	}
	status, _ = call("POST", "/hypercerts/v1/source/recovery-policy", advance, false)
	require.Equal(t, http.StatusUnauthorized, status)
	status, body = call("POST", "/hypercerts/v1/source/recovery-policy", advance, true)
	require.Equal(t, http.StatusOK, status, string(body))
	require.True(t, decodeSource(body).RecoveryRequired)
	status, _ = call("POST", "/hypercerts/v1/source/recovery-receipt", receipt, true)
	require.Equal(t, http.StatusConflict, status)
	receipt["policyRevision"] = 3
	receipt["jobId"] = "abcdef0123456789abcdef0123456789"
	status, body = call("POST", "/hypercerts/v1/source/recovery-receipt", receipt, true)
	require.Equal(t, http.StatusOK, status, string(body))
	require.False(t, decodeSource(body).RecoveryRequired)

	status, body = call("PUT", "/hypercerts/v1/source", map[string]string{"pds": "https://receipt-control.example", "state": "disabled"}, true)
	require.Equal(t, http.StatusOK, status, string(body))
	require.True(t, decodeSource(body).RecoveryRequired)
	status, body = call("PUT", "/hypercerts/v1/source", map[string]string{"pds": "https://receipt-control.example", "state": "enabled"}, true)
	require.Equal(t, http.StatusOK, status, string(body))
	current := decodeSource(body)
	require.True(t, current.RecoveryRequired)
	status, _ = call("POST", "/hypercerts/v1/source/recovery-receipt", receipt, true)
	require.Equal(t, http.StatusConflict, status)
	receipt["sourceRevision"] = current.Revision
	receipt["durableBoundary"] = "completed:2026-09-16T12:01:00Z"
	status, body = call("POST", "/hypercerts/v1/source/recovery-receipt", receipt, true)
	require.Equal(t, http.StatusOK, status, string(body))
	require.False(t, decodeSource(body).RecoveryRequired)
}

// Launched only by the cross-language management acceptance harness.
func TestControlPlaneAcceptanceFixture(t *testing.T) {
	ready := os.Getenv("CONTROL_ACCEPTANCE_READY")
	if ready == "" {
		t.Skip("cross-language fixture")
	}
	svc, r, _ := newSourceHandlerService(t)
	r.Logger = slog.Default()
	r.HostChecker = &countingHostChecker{}
	h, err := svc.controlHandler("fixture-service-credential-32-bytes-minimum")
	require.NoError(t, err)
	server := httptest.NewServer(h)
	defer server.Close()
	require.NoError(t, os.WriteFile(ready, []byte(server.URL), 0600))
	deadline := time.NewTimer(2 * time.Minute)
	defer deadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline.C:
			t.Fatal("acceptance harness did not shut down")
		case <-ticker.C:
			if _, err := os.Stat(ready + ".stop"); err == nil {
				return
			}
		}
	}
}
