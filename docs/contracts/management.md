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

## Acceptance mapping

| Management operation | Owner and required evidence |
| --- | --- |
| Source enable, disable and remove | Relay/Jetstream; [TECH-637](https://linear.app/hypercerts/issue/TECH-637/persist-cross-service-recovery-receipts-for-admitted-pdss) and [TECH-596](https://linear.app/hypercerts/issue/TECH-596/backfill-record-collections-for-a-pds) |
| Global exact collection-policy revision | Jetstream; [TECH-595](https://linear.app/hypercerts/issue/TECH-595/store-only-enabled-record-collections-in-jetstream-v2) and [TECH-596](https://linear.app/hypercerts/issue/TECH-596/backfill-record-collections-for-a-pds) |
| Job submission, retry and cancellation | Jetstream; [TECH-596](https://linear.app/hypercerts/issue/TECH-596/backfill-record-collections-for-a-pds) |
| Global or PDS raw-frame rate policy | Relay; [TECH-602](https://linear.app/hypercerts/issue/TECH-602/manage-global-and-pds-specific-relay-rate-limits) |
| Administrator membership, session revocation and CSRF-protected mutation | Administration; [TECH-599](https://linear.app/hypercerts/issue/TECH-599/secure-the-relay-administration-api-with-at-protocol-oauth) |

Neither the rate-limit nor administration work has production OAuth qualification
or a deployment claim.
