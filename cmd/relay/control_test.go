package main

import (
	"github.com/stretchr/testify/require"
	"log/slog"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
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
