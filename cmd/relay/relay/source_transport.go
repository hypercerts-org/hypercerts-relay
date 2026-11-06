package relay

import (
	"fmt"
	"strings"

	"github.com/bluesky-social/indigo/cmd/relay/relay/models"
)

func sourceTransportAllowed(hostname string, noSSL, testDial bool) error {
	if !noSSL || acceptancePrivateHostsEnabled() || (testDial && strings.HasPrefix(hostname, "localhost:")) {
		return nil
	}
	return fmt.Errorf("%w: PDS sources require HTTPS", ErrInvalidSourceURL)
}

func (r *Relay) sourceURL(hostname string, noSSL bool) (string, error) {
	if err := sourceTransportAllowed(hostname, noSSL, r.Config.TestDialContext != nil); err != nil {
		return "", err
	}
	return sourceHostURL(&models.Host{Hostname: hostname, NoSSL: noSSL}), nil
}
