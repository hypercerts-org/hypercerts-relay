package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"

	lru "github.com/hashicorp/golang-lru/v2"
)

// Submitter is the administration machine API used to request operations.
type Submitter interface {
	EnableSource(ctx context.Context, pds string) error
	RaiseAccountLimit(ctx context.Context, pds string, expected, limit int64) error
}

// HostStatusLookup reports a reference relay's view of a PDS host.
type HostStatusLookup interface {
	HostStatus(ctx context.Context, hostname string) (*HostStatus, error)
}

// HostStatus is the subset of com.atproto.sync.getHostStatus that is used.
type HostStatus struct {
	AccountCount *int64
	Status       string
}

var ErrHostNotFound = errors.New("host not found on reference relay")

// httpsScheme prefixes every PDS origin this service accepts.
const httpsScheme = "https://"

type Config struct {
	// DenyHosts holds exact hostnames, or "*.suffix" patterns that match any
	// subdomain of suffix.
	DenyHosts []string
	DryRun    bool
	// AccountThreshold is the reference-relay account count above which an
	// account limit is raised for a newly added PDS.
	AccountThreshold int64
	// PendingQuotaTTL bounds how long an added PDS waits to appear as an
	// enabled source before its account limit check is abandoned.
	PendingQuotaTTL time.Duration
	SeenDIDCapacity int
}

type sourceInfo struct {
	State        string
	AccountLimit *int64
}

type snapshot struct {
	collections    map[string]struct{}
	collectionList []string
	sources        map[string]sourceInfo
	// maxAccountLimit is the administrator-set maximum for requested limits.
	maxAccountLimit int64
}

// pendingLimit is an account limit to request once an added PDS appears.
type pendingLimit struct {
	since  time.Time
	target int64
}

// Discoverer decides whether an observed record identifies a new PDS, asks
// administration to enable it, and later raises its account limit.
type Discoverer struct {
	cfg   Config
	log   *slog.Logger
	dir   identity.Directory
	admin Submitter
	hosts HostStatusLookup
	now   func() time.Time

	state atomic.Pointer[snapshot]
	seen  *lru.Cache[string, struct{}]

	mu        sync.Mutex
	attempted map[string]struct{}
	// pending holds PDS origins this process enabled whose account limit is
	// still to be raised, with the rounded reference-relay target.
	pending map[string]pendingLimit
}

func NewDiscoverer(cfg Config, log *slog.Logger, dir identity.Directory, admin Submitter, hosts HostStatusLookup) (*Discoverer, error) {
	if cfg.SeenDIDCapacity < 1 {
		cfg.SeenDIDCapacity = 100_000
	}
	if cfg.PendingQuotaTTL <= 0 {
		cfg.PendingQuotaTTL = time.Hour
	}
	seen, err := lru.New[string, struct{}](cfg.SeenDIDCapacity)
	if err != nil {
		return nil, err
	}
	return &Discoverer{
		cfg:       cfg,
		log:       log,
		dir:       dir,
		admin:     admin,
		hosts:     hosts,
		now:       time.Now,
		seen:      seen,
		attempted: map[string]struct{}{},
		pending:   map[string]pendingLimit{},
	}, nil
}

// SetState replaces the known sources and collections. It reports whether the
// collection set changed, which requires an upstream reconnect.
func (d *Discoverer) SetState(state *DiscoveryState) bool {
	next := &snapshot{
		collections:     make(map[string]struct{}, len(state.Collections)),
		sources:         make(map[string]sourceInfo, len(state.Sources)),
		maxAccountLimit: state.MaxAccountLimit,
	}
	for _, c := range state.Collections {
		if _, err := syntax.ParseNSID(c); err != nil {
			d.log.Warn("ignoring invalid collection from administration", "collection", c)
			continue
		}
		if _, dup := next.collections[c]; !dup {
			next.collections[c] = struct{}{}
			next.collectionList = append(next.collectionList, c)
		}
	}
	slices.Sort(next.collectionList)
	for _, s := range state.Sources {
		next.sources[originKey(s.PDS)] = sourceInfo{State: s.State, AccountLimit: s.AccountLimit}
	}
	prev := d.state.Swap(next)
	return prev == nil || !slices.Equal(prev.collectionList, next.collectionList)
}

// Collections returns the current sorted collection list.
func (d *Discoverer) Collections() []string {
	if snap := d.state.Load(); snap != nil {
		return snap.collectionList
	}
	return nil
}

// Observe handles one record create or update seen upstream.
func (d *Discoverer) Observe(ctx context.Context, rawDID, collection string) {
	snap := d.state.Load()
	if snap == nil {
		return
	}
	// The upstream filter may be a prefix; only exact retained NSIDs count.
	if _, ok := snap.collections[collection]; !ok {
		return
	}
	if d.seen.Contains(rawDID) {
		return
	}
	did, err := syntax.ParseDID(rawDID)
	if err != nil {
		return
	}
	ident, err := d.dir.LookupDID(ctx, did)
	if err != nil {
		// Not cached as seen, so a later record retries the lookup.
		candidates.WithLabelValues("resolve_failed").Inc()
		d.log.Debug("DID resolution failed", "did", rawDID, "err", err)
		return
	}
	d.seen.Add(rawDID, struct{}{})
	origin, err := normalizeOrigin(ident.PDSEndpoint())
	if err != nil {
		candidates.WithLabelValues("invalid_pds").Inc()
		d.log.Debug("ignoring unusable PDS endpoint", "did", rawDID, "endpoint", ident.PDSEndpoint(), "err", err)
		return
	}
	outcome := d.consider(ctx, snap, origin)
	if outcome == "lookup_failed" || outcome == "submit_failed" {
		// Let a later record from this DID try again.
		d.seen.Remove(rawDID)
	}
	candidates.WithLabelValues(outcome).Inc()
	switch outcome {
	case "known", "attempted":
	case "denied":
		d.log.Debug("PDS candidate", "outcome", outcome, "pds", origin, "did", rawDID, "collection", collection)
	default:
		d.log.Info("PDS candidate", "outcome", outcome, "pds", origin, "did", rawDID, "collection", collection)
	}
}

func (d *Discoverer) consider(ctx context.Context, snap *snapshot, origin string) string {
	if _, ok := snap.sources[origin]; ok {
		return "known"
	}
	if d.denied(hostOf(origin)) {
		return "denied"
	}
	d.mu.Lock()
	if _, ok := d.attempted[origin]; ok {
		d.mu.Unlock()
		return "attempted"
	}
	d.attempted[origin] = struct{}{}
	d.mu.Unlock()

	// The reference relay's view decides whether to add the PDS at all, and
	// the account limit to request once it is a source.
	status, err := d.hosts.HostStatus(ctx, hostOf(origin))
	switch {
	case errors.Is(err, ErrHostNotFound):
		status = nil
	case err != nil:
		d.mu.Lock()
		delete(d.attempted, origin)
		d.mu.Unlock()
		d.log.Warn("reference relay host status failed", "pds", origin, "err", err)
		return "lookup_failed"
	case status.Status == "banned":
		return "upstream_banned"
	}
	target, reason := int64(0), "upstream_host_unknown"
	if status != nil {
		target, reason = accountLimitTarget(status, d.cfg.AccountThreshold)
	}

	if d.cfg.DryRun {
		d.log.Info("dry run: would enable PDS", "pds", origin, "accountLimit", min(target, snap.maxAccountLimit), "accountLimitReason", reason)
		return "dry_run"
	}
	if err := d.admin.EnableSource(ctx, origin); err != nil {
		var rejected *RejectedError
		if errors.As(err, &rejected) {
			// Administration refused the request; do not repeat it.
			d.log.Warn("administration rejected PDS enable", "pds", origin, "err", err)
			return "rejected"
		}
		d.mu.Lock()
		delete(d.attempted, origin)
		d.mu.Unlock()
		d.log.Warn("PDS enable request failed", "pds", origin, "err", err)
		return "submit_failed"
	}
	if target > 0 {
		d.mu.Lock()
		d.pending[origin] = pendingLimit{since: d.now(), target: target}
		d.mu.Unlock()
	} else {
		quotas.WithLabelValues(reason).Inc()
	}
	return "submitted"
}

// Reconcile raises the account limit of PDSes this process enabled once they
// appear as enabled sources. It runs after each successful state poll.
func (d *Discoverer) Reconcile(ctx context.Context) {
	snap := d.state.Load()
	if snap == nil {
		return
	}
	d.mu.Lock()
	pending := maps.Clone(d.pending)
	d.mu.Unlock()
	for origin, p := range pending {
		if ctx.Err() != nil {
			return
		}
		expired := d.now().Sub(p.since) > d.cfg.PendingQuotaTTL
		done, outcome := d.reconcileOne(ctx, snap, origin, p.target)
		if !done && expired {
			done, outcome = true, "expired_"+outcome
		}
		if done {
			d.mu.Lock()
			delete(d.pending, origin)
			d.mu.Unlock()
			quotas.WithLabelValues(outcome).Inc()
			d.log.Info("PDS account limit check finished", "pds", origin, "outcome", outcome)
		}
	}
}

func (d *Discoverer) reconcileOne(ctx context.Context, snap *snapshot, origin string, target int64) (bool, string) {
	src, ok := snap.sources[origin]
	if !ok {
		return false, "source_missing"
	}
	if src.State != "enabled" {
		return true, "source_not_enabled"
	}
	if src.AccountLimit == nil {
		return true, "limit_unknown"
	}
	if snap.maxAccountLimit < 1 {
		return true, "maximum_unset"
	}
	// The administrator-set maximum applies when the request is made.
	target = min(target, snap.maxAccountLimit)
	if *src.AccountLimit >= target {
		return true, "limit_sufficient"
	}
	if err := d.admin.RaiseAccountLimit(ctx, origin, *src.AccountLimit, target); err != nil {
		var rejected *RejectedError
		if errors.As(err, &rejected) {
			d.log.Warn("administration rejected account limit", "pds", origin, "err", err)
			return true, "rejected"
		}
		d.log.Warn("account limit request failed", "pds", origin, "err", err)
		return false, "submit_failed"
	}
	d.log.Info("requested PDS account limit", "pds", origin, "from", *src.AccountLimit, "to", target)
	return true, "submitted"
}

// accountLimitTarget rounds an unthrottled upstream account count above the
// threshold up to the next thousand, for example 1400 accounts to 2000. The
// administrator-set maximum is applied when the limit is requested.
func accountLimitTarget(status *HostStatus, threshold int64) (int64, string) {
	if status.Status == "throttled" {
		return 0, "upstream_throttled"
	}
	if status.AccountCount == nil || *status.AccountCount <= threshold {
		return 0, "below_threshold"
	}
	return (*status.AccountCount + 999) / 1000 * 1000, "rounded"
}

func (d *Discoverer) denied(host string) bool {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	for _, pattern := range d.cfg.DenyHosts {
		if suffix, ok := strings.CutPrefix(pattern, "*."); ok {
			if strings.HasSuffix(host, "."+suffix) {
				return true
			}
		} else if host == pattern {
			return true
		}
	}
	return false
}

// normalizeOrigin accepts only a public HTTPS PDS origin and returns it as
// https://host[:port] in lower case, without a default port.
func normalizeOrigin(endpoint string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil {
		return "", err
	}
	if u.Scheme != "https" {
		return "", errors.New("PDS endpoint is not HTTPS")
	}
	if u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("PDS endpoint is not a bare origin")
	}
	host := strings.ToLower(u.Hostname())
	if net.ParseIP(host) != nil {
		return "", errors.New("PDS endpoint is an IP address")
	}
	handle, err := syntax.ParseHandle(host)
	if err != nil || !handle.AllowedTLD() {
		return "", fmt.Errorf("PDS endpoint hostname %q is not a public domain", host)
	}
	if port := u.Port(); port != "" && port != "443" {
		return httpsScheme + host + ":" + port, nil
	}
	return httpsScheme + host, nil
}

// originKey normalises an administration source origin for comparison.
func originKey(origin string) string {
	key := strings.ToLower(strings.TrimSuffix(origin, "/"))
	if strings.HasPrefix(key, httpsScheme) {
		key = strings.TrimSuffix(key, ":443")
	}
	return key
}

// hostOf returns host[:port] of a normalised origin, the form Relay hostnames
// and getHostStatus use.
func hostOf(origin string) string {
	_, host, _ := strings.Cut(origin, "://")
	return host
}
