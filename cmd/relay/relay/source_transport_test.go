package relay

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/bluesky-social/indigo/cmd/relay/relay/models"
	"github.com/bluesky-social/indigo/cmd/relay/stream"
	"github.com/stretchr/testify/require"
)

func TestSourceTransportRejectsPlaintextBeforeRegistration(t *testing.T) {
	t.Setenv("RELAY_ACCEPTANCE_PRIVATE_HOSTS", "")
	for _, raw := range []string{"http://pds.example.com", "ws://pds.example.com", "http://localhost:1234", "ws://localhost:1234"} {
		t.Run(raw, func(t *testing.T) {
			r, db := newSourceRelay(t)
			_, err := r.AddSource(t.Context(), raw)
			require.ErrorIs(t, err, ErrInvalidSourceURL)
			_, err = r.AddPublicSource(t.Context(), raw)
			require.ErrorIs(t, err, ErrInvalidSourceURL)
			require.ErrorIs(t, r.AdmitPublicSource(t.Context(), raw), ErrInvalidSourceURL)
			for _, admin := range []bool{false, true} {
				require.ErrorIs(t, r.SubscribeToHost(t.Context(), "pds.example.com", true, admin), ErrInvalidSourceURL)
			}
			var hosts, sources int64
			require.NoError(t, db.Model(&models.Host{}).Count(&hosts).Error)
			require.NoError(t, db.Model(&models.Source{}).Count(&sources).Error)
			require.Zero(t, hosts)
			require.Zero(t, sources)
		})
	}
}

func TestSourceTransportPersistedPlaintextFailsClosed(t *testing.T) {
	t.Setenv("RELAY_ACCEPTANCE_PRIVATE_HOSTS", "")
	fixture := newSourceFixture(t)
	_, db := newSourceRelay(t)
	host := &models.Host{Hostname: fixture.host, NoSSL: true, LastSeq: 42, Status: models.HostStatusActive}
	require.NoError(t, db.Create(host))
	r, err := NewRelay(db.DB, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Slurper.Shutdown()) })
	r.HostChecker = NewMockHostChecker()
	r.HostChecker.(*MockHostChecker).Hosts[host.BaseURL()] = true
	_, err = r.AddSource(t.Context(), "https://"+host.Hostname)
	require.ErrorIs(t, err, ErrInvalidSourceURL)
	require.ErrorIs(t, r.ResubscribeAllHosts(t.Context()), ErrInvalidSourceURL)
	require.False(t, r.Slurper.CheckIfSubscribed(host.Hostname))
	require.ErrorIs(t, r.Slurper.Subscribe(host), ErrInvalidSourceURL)
	require.ErrorIs(t, r.AdmitPublicSource(t.Context(), "https://"+host.Hostname), ErrInvalidSourceURL)
	view, err := r.ValidateSource(t.Context(), host.ID, 1)
	require.ErrorIs(t, err, ErrSourceValidationFailed)
	require.Equal(t, models.SourceValidationFailed, view.Validation.Status)
	require.Equal(t, models.SourceValidationReasonHostCheckFailed, view.Validation.Reason)
	require.EqualValues(t, 2, view.Revision)
	stored, err := r.GetHostByID(t.Context(), host.ID)
	require.NoError(t, err)
	require.True(t, stored.NoSSL)
	require.EqualValues(t, 42, stored.LastSeq)
	requireNoSourceConnection(t, fixture)
}

func TestSourceTransportInjectionIsLocalOnly(t *testing.T) {
	t.Setenv("RELAY_ACCEPTANCE_PRIVATE_HOSTS", "")
	r, _ := newSourceRelay(t)
	r.Config.TestDialContext = (&net.Dialer{}).DialContext
	_, err := r.AddSource(t.Context(), "http://public.example.com")
	require.ErrorIs(t, err, ErrInvalidSourceURL)
	_, err = r.AddSource(t.Context(), "http://localhost:1234")
	require.NoError(t, err)
	config := DefaultSlurperConfig()
	config.TestDialContext = r.Config.TestDialContext
	slurper, err := NewSlurper(func(context.Context, *stream.XRPCStreamEvent, string, uint64) error { return nil }, config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, slurper.Shutdown()) })
	require.ErrorIs(t, slurper.Subscribe(&models.Host{Hostname: "public.example.com", NoSSL: true}), ErrInvalidSourceURL)
}

func TestHostClientRejectsPlaintextAndDowngradeRedirect(t *testing.T) {
	t.Setenv("RELAY_ACCEPTANCE_PRIVATE_HOSTS", "")
	var plaintext atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		plaintext.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	secure := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.Redirect(w, req, target.URL, http.StatusFound)
	}))
	defer secure.Close()
	hc := NewHostClient("")
	hc.Client.Transport = sourceHTTPTransport{transport: secure.Client().Transport}
	require.Error(t, hc.CheckHost(t.Context(), target.URL))
	require.Error(t, hc.CheckHost(t.Context(), secure.URL))
	require.Zero(t, plaintext.Load())
}

func TestHostClientRetainsTLSVerification(t *testing.T) {
	t.Setenv("RELAY_ACCEPTANCE_PRIVATE_HOSTS", "")
	secure := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_, _ = w.Write([]byte(`{"did":"did:web:fixture.example","availableUserDomains":[]}`))
	}))
	defer secure.Close()
	hc := NewHostClient("")
	hc.Client.Transport = sourceHTTPTransport{transport: http.DefaultTransport.(*http.Transport).Clone()}
	require.ErrorContains(t, hc.CheckHost(t.Context(), secure.URL), "certificate")
}
