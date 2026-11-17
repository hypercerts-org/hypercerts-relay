# Management contract

## Authentication and authorization

The administration application owns browser-facing AT Protocol OAuth, local
sessions, the durable administrator DID allowlist, operation journaling and audit
attribution. OAuth authentication alone grants no privilege. Every protected
request authorizes the immutable DID against local membership; handles and display
names are presentation data only.

There is one full-administrator role. Enterprise OIDC, domain or handle rules,
and additional roles are not selected. Removing a DID immediately revokes its
local sessions and stored OAuth state. Session revocation may force new OAuth
login without removing membership. The local `administration/server/access.ts`
CLI is the audited break-glass recovery path. Administrator DIDs do not belong in
this repository.

Browser mutations require the authenticated session, same-origin request and
CSRF token. The browser API is `/api/v1`; the application uses private bearer
tokens to call Relay and Jetstream service-owner APIs. Those private interfaces
are not public OAuth APIs.

## Operations and audit

Administration records a durable requested operation before contacting a service,
then reports `applying`, `applied`, `failed`, `canceled` or `incomplete` from its
observed result. `applied` acknowledges an owner operation or job submission; it
never establishes archive or historical completeness. Operations are actor
attributed, body-size bounded and idempotency keyed where required.

The application exposes bounded pages of administrators, operations, sources,
policy, jobs, coverage, limits, status and audit records. It does not extend the
inherited Basic-auth Relay admin UI.

## Rate policy

Relay owns persisted raw-frame rate policy. The unit is every
`subscribeRepos` event frame per second, including non-record frames. A single
active Relay process applies a global policy and optional policy for each
admitted PDS origin; both must admit a frame. Configured values use the existing
1 through 1,000,000 events/second range, while an unset scope has no policy.

The token bucket permits a one-second burst. On exhaustion Relay pauses upstream
socket reads before scheduler admission; it does not intentionally drop admitted
events or grow an unbounded queue. A reconnect resumes from the last durable
source cursor, and an upstream replay gap requires the current-state recovery
path. There is no HTTP 429 contract for a PDS firehose connection. Fairness is
best-effort, and multi-active/distributed Relay enforcement is out of scope.

Before Relay opens source sockets it acquires a renewable singleton database
lease. A second Relay using the same database must fail admission rather than
divide the process-local global bucket; graceful shutdown releases the lease and
an expired lease can be recovered after a failed process. Lease acquire and
renewal use database time; each successful operation establishes a conservative
process-local monotonic deadline. Source-socket startup and every redial check
that fence, while raw-frame reservation holds it through token decrement without
a per-frame database write. Lease loss, deadline expiry, or graceful release
cancels existing source sockets before a replacement takes over.
`/hypercerts/v1/limits`
reports bounded per-policy admitted-frame, waited-frame and waiting-connection
counters with `single_active_relay_process` / `process_local_since_start`
scope labels. Those counters and token balances reset on a new active process;
the configured policy does not.

Jetstream coverage remains current-state evidence only. A configured or pending
source is not complete, unavailable acquisition remains incomplete, and the
administration API labels historical PDS attribution `unknown` rather than
assigning old archive data to a currently observed host.

## Automatic PDS discovery

The optional `cmd/pdsdiscovery` service automates PDS admission through
administration, so its changes go through the same journal and audit as an
administrator's. This adds automatic PDS admission as a responsibility. It
belongs to administration and is requested by the discovery service; it does
not move into Relay or Jetstream.

- Administration mounts the discovery `state` and `operations` routes under
  `/api/v1/sources/discovery` only when `ADMIN_DISCOVERY_TOKEN_FILE` is
  configured. Those two routes take a bearer machine token, not an OAuth
  session; the token grants no access to any other `/api/v1` route, and a
  browser session cannot use them. The `settings` route under the same prefix
  is always available to administrators, with a session and CSRF.
- `GET /api/v1/sources/discovery/state` returns the applied collection policy
  and every Relay source with its desired state and account limit.
- `POST /api/v1/sources/discovery/operations` accepts only two commands: a `source` with
  state `enabled`, and an `account_quota` whose `accountLimit` is greater than
  its `expectedLimit`. Every other command gets HTTP 403. Accepted operations
  are recorded under the actor `service:pds-discovery`.
- An operator's disable or remove always wins. The service skips any PDS that
  is already a source in any state. Administration also refuses a machine
  enable for a source that Relay reports as `disabled` or `removed`, both when
  the request arrives and again before the worker applies it. It reports
  `source_disabled_by_operator` (HTTP 409, or a failed operation).
- There is no cap on the number of additions and no built-in denylist; an
  optional denylist can exclude hosts. A host that the reference relay reports
  as `banned` is not added.
- An account limit is raised only for a PDS that this service added. The new
  limit is the reference relay's account count rounded up to the next thousand,
  and only when that count is above the threshold and the host is not
  throttled there. The limit never exceeds the administrator-set maximum
  (100,000 by default), which is changed in the Sources screen, audited as
  `discovery_settings_changed`, and enforced by administration on every
  discovery request.
- Discovery is best effort. It is not evidence of complete source or
  collection coverage.

## Collection directory crawls

Administration may ask the optional collection directory to crawl enabled,
validation-passed HTTPS sources through a `collection_crawl` operation. It checks
each source against Relay when the operation is applied, sends only
`https://<Relay hostname>` to the directory's bearer-authenticated
`/admin/pds/requestCrawl`, and records one actor-attributed crawl row and audit
entry per accepted source. `applied` means the directory accepted the request.
Administration then follows `/admin/crawlStatus` and records whether the crawl
finished, failed with a bounded category, or has an unknown outcome after a
directory restart. A crawl adds candidate DID/collection pairs only: it is not
Relay admission, not Jetstream backfill and not proof of current records.

## Acceptance mapping

| Management operation | Owner and required evidence |
| --- | --- |
| Source enable, disable and remove | Relay/Jetstream; [TECH-637](https://linear.app/hypercerts/issue/TECH-637/persist-cross-service-recovery-receipts-for-admitted-pdss) and [TECH-596](https://linear.app/hypercerts/issue/TECH-596/backfill-record-collections-for-a-pds) |
| Global exact collection-policy revision | Jetstream; [TECH-595](https://linear.app/hypercerts/issue/TECH-595/store-only-enabled-record-collections-in-jetstream-v2) and [TECH-596](https://linear.app/hypercerts/issue/TECH-596/backfill-record-collections-for-a-pds) |
| Job submission, retry and cancellation | Jetstream; [TECH-596](https://linear.app/hypercerts/issue/TECH-596/backfill-record-collections-for-a-pds) |
| Global or PDS raw-frame rate policy | Relay; [TECH-602](https://linear.app/hypercerts/issue/TECH-602/manage-global-and-pds-specific-relay-rate-limits) |
| Administrator membership, session revocation and CSRF-protected mutation | Administration; [TECH-599](https://linear.app/hypercerts/issue/TECH-599/secure-the-relay-administration-api-with-at-protocol-oauth) |
| Manual collection directory crawl of one or all enabled sources | Administration and collection directory; [TECH-900](https://linear.app/hypercerts/issue/TECH-900/add-a-manual-collectiondir-crawl-action-to-the-administration-interface) |

Neither the rate-limit nor administration work has production OAuth qualification
or a deployment claim.
