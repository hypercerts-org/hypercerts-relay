package orchestrator

import (
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"strings"

	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/ingest/live"
)

func bootstrapHistoryCapture(o *Orchestrator) *live.OriginalCapture {
	if o.cfg.History == nil || o.cfg.BootstrapAttribution == nil || o.cfg.CollectionPolicy == nil {
		return nil
	}
	return &live.OriginalCapture{Journal: o.cfg.History, Domain: sourceDomain(o.cfg.RelayURL), Epoch: o.cfg.History.CaptureGeneration(), Policy: func() selection.Policy { return o.cfg.CollectionPolicy.Current() }}
}

func steadyHistoryCapture(o *Orchestrator) *live.OriginalCapture {
	if o.cfg.History == nil || o.cfg.Attribution == nil || o.cfg.CollectionPolicy == nil {
		return nil
	}
	return &live.OriginalCapture{Journal: o.cfg.History, Domain: sourceDomain(o.cfg.RelayURL), Epoch: o.cfg.History.CaptureGeneration(), Policy: func() selection.Policy { return o.cfg.CollectionPolicy.Current() }}
}

func sourceDomain(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" {
		return "relay-unknown"
	}
	u.User, u.RawQuery, u.Fragment = nil, "", ""
	u.Path = strings.TrimRight(u.EscapedPath(), "/")
	canonical := strings.ToLower(u.Scheme) + "://" + strings.ToLower(u.Host) + u.Path
	sum := sha256.Sum256([]byte(canonical))
	return "relay-" + hex.EncodeToString(sum[:16])
}
