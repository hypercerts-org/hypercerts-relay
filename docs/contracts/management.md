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

## Acceptance mapping

| Management operation | Owner and required evidence |
| --- | --- |
| Source enable, disable and remove | Relay/Jetstream; Plans 004 and 006: T03 and T09 |
| Global exact collection-policy revision | Jetstream; Plans 005 and 006: T04, T05 and T07 |
| Job submission, retry and cancellation | Jetstream; Plan 006: T02, T05, T06, T08 and T09 |
| Global or PDS raw-frame rate policy | Relay; Plan 007: T10, T11 and T12 |
| Administrator membership, session revocation and CSRF-protected mutation | Administration; Plan 008: T13 and T14 |

Neither Plan 007 nor Plan 008 has production OAuth qualification or a deployment
claim.
