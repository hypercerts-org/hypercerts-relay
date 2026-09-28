---
'hypercerts-relay': major
---

Deliver the Hypercerts Relay with Jetstream and Admin web interface.

### Relay sources and durable events

Add managed PDS sources with independent validation, enablement, disablement,
removal, account quotas, lifecycle revisions, and status reporting. Public crawl
requests cannot add or re-enable a source. Relay persists raw output before
advancing source progress, so processing, cursor, disk-write, and sync failures
remain replayable; consumers must tolerate repeated retained output. Persist
versioned Jetstream recovery receipts before clearing recovery-required status,
and preserve those flags across source policy changes. Recovery acknowledgements
must match the source and collection-policy revisions, job identifier, and
durable completion boundary. Permanent verification rejections contain no record
payload. A completed matching backfill job retries its private Relay recovery
receipt after restart.

Add single-process rate admission with a renewable database lease, bounded
admission measurements, and persistent global and per-PDS limits. A second Relay
using the same database exits instead of splitting the policy. Graceful shutdown
releases the lease and an expired lease permits recovery after a failed process.
Private rate-policy responses expose bounded admitted-frame and waited-frame
counters with process-local measurement scope; token balances and counters reset
with the documented one-second burst.

### Jetstream retention and recovery

Add the locally maintained Jetstream v2 archive, replay, live-subscription, and
backfill runtime. Persist a versioned collection policy and apply it before
record materialization on bootstrap, live, sync-replacement, restart, and retry
paths. The default empty policy stores no record payloads; identity, account,
and sync markers remain available and cursors continue advancing for excluded
records. Seed new data directories with the bundled Hypercerts and Certified
collections unless an explicit policy or opt-out is supplied. Persisted policies
are reused and never overwritten on restart, and existing archived data is not
purged.

Add durable, idempotent jobs for configured PDS sources, collection policy
changes, backfill, and quota recovery. Jobs freeze a validated repository
inventory, identify their source and policy revisions, resume persisted progress,
and report current-state coverage only. Unavailable input remains incomplete;
historical completeness is never inferred from a snapshot. Direct-PDS snapshot
rejection metadata retains the most recent 1,000 permanent rejections; older
records are evicted, allowing a later retry to refetch an evicted listed
revision. An operator must retry a failed job after correcting its input.

Expose authenticated private Jetstream and Relay control APIs for policy,
sources, jobs, progress, cancellation, retry, quotas, and recovery receipts.
Enable the Relay control API with `RELAY_CONTROL_ADDR` and
`RELAY_CONTROL_TOKEN_FILE`; enable the Jetstream API with
`JETSTREAM_CONTROL_TOKEN_FILE` and `JETSTREAM_DEBUG_ADDR`. Operations use
durable idempotency keys and separate retry namespaces. Public listeners do not
expose control routes. Profiling is opt-in through `JETSTREAM_ENABLE_PPROF=true`
or `--enable-pprof` and must be disabled after diagnostics.

### Administration

Add the separate OAuth administration application for PDS and collection policy,
backfill jobs, rate limits, coverage, and actor-attributed audit history.
Requests persist before service application and can be inspected or retried.
Seed the initial administrator once with `ADMIN_SEED_DID`; administrators can
grant and revoke access, with immediate session revocation and durable audit
records. Store refreshed display names and verified handles while retaining DID
authorization and cached access when profile lookup fails. Existing
administrators must sign in again to populate missing profile metadata, and
revoking access also clears the cached profile. Accounts without profile
metadata retain their DID fallback. Existing access history prevents reseeding,
and a restart cannot restore revoked seed access.

Add collection suggestions and removal controls, quota and admission metadata,
current-state coverage, operator-facing labels, source-account inventory, and
accessible desktop/mobile administration workflows. The local acceptance OAuth
callback is a documented test-only double and does not qualify a production
OAuth provider. Quota edits are validated and durably audited; stale edits are
rejected, retries of the same value are safe, and reducing a quota does not
remove existing accounts. Removing a source stops acquisition without purging
retained archive rows. If an already-active source is enabled again, the
connection is preserved; retry an incomplete enable request before retrying its
backfill. The access CLI remains available when no administrator can sign in.

The administration UI uses Hypercerts branding, displays the saved profile name
in bold with the verified handle below it, and fetches the verified Switzer font
at build time from Fontshare. Bare PDS hostnames are normalized to HTTPS;
automatic polling is removed; coverage is grouped by PDS; audit history and
requested changes are combined; and administrator rows show the last login.
Backfill progress reports processed repositories out of the durable active
total.

The source table shows Jetstream’s latest active-repository inventory and
processed-repository count beside Relay-observed accounts, and selected
collections are collapsed by default. The UI distinguishes the PDS admission
quota limit from Relay-observed account totals. Jetstream status-page host
filters and account lookup controls have accessible labels.

Claims, retries, and cancellation are atomic. Operation pages are ordered by
creation time and clients must pass the opaque `next` cursor unchanged. Existing
Jetstream retry receipts migrate into separate namespaces; do not downgrade
Jetstream while journal commands remain retryable. SQLite sidecar files are
protected, raw container secret inputs are cleared before service startup, and
control origins are validated: use loopback or a single-service
`*.railway.internal` host only with `HC_RAILWAY_STARTUP=1`; all other hosts must
use HTTPS. Edge client IPs are honored for login limits, while other proxies
must set `ADMIN_TRUST_PROXY` with trusted peer CIDRs. Event admission remains
responsive during policy persistence, rate limits apply to every source frame,
and management opens only after subscription initialization.

### Deployment and upgrade operations

Provide canonical Railway-compatible container packaging with optional runtime
secret bootstrap while keeping infrastructure configuration private. Set
`HC_RAILWAY_STARTUP=1` to convert `HC_RELAY_CONTROL_SECRET`,
`HC_JETSTREAM_CONTROL_SECRET`, and `HC_ADMIN_ENCRYPTION_SECRET` into restricted
files; existing file-based credential interfaces remain supported. Preserve
Relay database and event-store state together with Jetstream archive data and
cursor positions before upgrades. Support forward upgrades and restore-only
qualification from copied state; binary rollback after state migration is not
supported.

For new Jetstream data directories, set `JETSTREAM_COLLECTIONS` to an explicit
comma-separated list of exact NSIDs, or use the bundled Hypercerts and Certified
seed collections by default. `JETSTREAM_DISABLE_COLLECTION_SEED=true` (or
`--disable-collection-seed`) disables that seed, while an explicit collections
list takes precedence. Set `JETSTREAM_PDS_SOURCES` to initialize configured
direct-PDS sources. The Jetstream service uses `JETSTREAM_RELAY_URL` for its
owned Relay URL. Disposable private-host acceptance uses
`JETSTREAM_ACCEPTANCE_PRIVATE_HOSTS` (or `--acceptance-private-hosts`) only with
an acceptance-tag build; production images retain their private-host checks.
Existing deployments can apply a desired collection list through the
administration Collections screen. The administration image includes its
production TypeScript loader.

Direct-PDS bootstrap refreshes one stale DID signing-key cache entry after a
signature mismatch. Source account pagination is optimized, commits for a
different DID are rejected before identity refresh, and connection-failure logs
include the failure reason. Railway Jetstream images use ordinary Docker layer
caching rather than deployment-specific cache mounts. Profiling remains
unauthenticated on the private listener, so restrict network access while it is
enabled.
