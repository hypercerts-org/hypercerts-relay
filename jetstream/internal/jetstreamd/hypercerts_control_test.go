package jetstreamd

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/jobs"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

func TestHypercertsPrivateInterfaceReportsUnavailablePDS(t *testing.T) {
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.subscribeRepos":
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.CloseNow()
			_, _, _ = conn.Read(r.Context())
		case "/xrpc/com.atproto.sync.listHosts":
			_, _ = io.WriteString(w, `{"hosts":[]}`)
		default:
			_, _ = io.WriteString(w, `{"repos":[]}`)
		}
	}))
	defer relay.Close()
	unavailable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "fixture unavailable", 503) }))
	defer unavailable.Close()
	private, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	opts := testOptions(t)
	opts.RelayURL = relay.URL
	opts.CollectionSelection = true
	opts.InitialCollections = []string{"app.bsky.feed.post"}
	opts.InitialPDSSources = []string{unavailable.URL}
	opts.ControlToken = "private-fixture-credential-at-least-32-bytes"
	opts.DebugListener = private
	opts.DebugAddr = ""
	opts.LogOutput = io.Discard
	rt, err := Build(t.Context(), opts)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()
	defer func() { cancel(); <-done; require.NoError(t, rt.Close(context.Background())) }()
	ready, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	require.NoError(t, rt.WaitSteadyState(ready))
	require.Eventually(t, func() bool { all := rt.BackfillJobs.List(); return len(all) == 1 && all[0].State == jobs.Incomplete }, 5*time.Second, 10*time.Millisecond)
	fetch := func(base, token string) (int, []byte) {
		req, err := http.NewRequestWithContext(t.Context(), "GET", base+"/hypercerts/v1/jobs", nil)
		require.NoError(t, err)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return resp.StatusCode, body
	}
	status, _ := fetch("http://"+private.Addr().String(), "")
	require.Equal(t, 401, status)
	status, body := fetch("http://"+private.Addr().String(), opts.ControlToken)
	require.Equal(t, 200, status)
	var response struct {
		Jobs []struct {
			State  jobs.State `json:"state"`
			PDS    string     `json:"pds"`
			Policy struct {
				Revision uint64 `json:"revision"`
			} `json:"policy"`
			HistoryComplete bool `json:"historyComplete"`
		} `json:"jobs"`
	}
	require.NoError(t, json.Unmarshal(body, &response))
	require.Len(t, response.Jobs, 1)
	require.Equal(t, jobs.Incomplete, response.Jobs[0].State)
	require.Equal(t, unavailable.URL, response.Jobs[0].PDS)
	require.Equal(t, uint64(1), response.Jobs[0].Policy.Revision)
	require.False(t, response.Jobs[0].HistoryComplete)
	// Enabling control must leave profiling disabled on the same listener.
	resp, err := http.Get("http://" + private.Addr().String() + "/debug/pprof/")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	status, body = fetch("http://"+rt.PublicAddr(), opts.ControlToken)
	require.Equal(t, 404, status)
	require.NotContains(t, string(body), unavailable.URL)
}
func TestHypercertsControlRequiresManagedPolicyAndPrivateListener(t *testing.T) {
	for _, managed := range []bool{false, true} {
		opts := testOptions(t)
		opts.ControlToken = "private-fixture-credential-at-least-32-bytes"
		opts.CollectionSelection = managed
		if managed {
			opts.DebugAddr = ""
		}
		_, err := Build(t.Context(), opts)
		require.Error(t, err)
	}
}

func TestHypercertsProfilingRequiresPrivateListener(t *testing.T) {
	opts := testOptions(t)
	opts.EnablePprof = true
	opts.DebugAddr = ""
	_, err := Build(t.Context(), opts)
	require.ErrorContains(t, err, "profiling requires a private debug listener")
}
