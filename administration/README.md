# Relay administration

TECH-588 introduces a separate Svelte/Tailwind application and Node management
service. It talks only to versioned private Relay and Jetstream APIs. It never
opens their databases, inherits their Basic-auth UI, or implements ingestion.

For Railway containers, volumes, networking and administrator bootstrap, see
[the Railway runbook](../docs/railway.md).

## Run locally

Use Node 24.18 or later, Go versions required by each module, and npm. From this directory:

```sh
npm ci
npm run build
# Set the configuration below, including ADMIN_SEED_DID for first access, then:
npm run server
```

| Variable                       | Meaning                                                                                                 |
| ------------------------------ | ------------------------------------------------------------------------------------------------------- |
| `ADMIN_PUBLIC_ORIGIN`          | Exact externally visible HTTPS origin; local development uses `http://127.0.0.1:3000`.                  |
| `ADMIN_SEED_DID`               | Optional full DID to grant ordinary administrator access once, on an uninitialized database.            |
| `ADMIN_DATABASE`               | Dedicated SQLite file on durable local storage.                                                         |
| `ADMIN_ENCRYPTION_KEY_FILE`    | File containing a random secret of at least 32 bytes, used to encrypt OAuth tokens and handshake state. |
| `ADMIN_BIND` / `PORT`          | Listener, default `127.0.0.1:3000`. Put an HTTPS proxy in front for remote use.                         |
| `RELAY_CONTROL_URL`            | Private Relay origin, without `/hypercerts/v1`.                                                         |
| `RELAY_CONTROL_TOKEN_FILE`     | Mounted Relay service credential, at least 32 bytes.                                                    |
| `JETSTREAM_CONTROL_URL`        | Private Jetstream operations origin.                                                                    |
| `JETSTREAM_CONTROL_TOKEN_FILE` | Mounted Jetstream service credential, at least 32 bytes.                                                |

Run one administration process per database. Use a process supervisor; this is a
stateful Node service, not a stateless function. Keep the database directory and
secret files accessible only to its service account. SQLite uses WAL and FULL
synchronization. OAuth values are AES-256-GCM encrypted; retain the encryption key
with the database for restart. Replacing that key requires signing in again and
clearing the old OAuth state through the access-revocation command.

In Relay, set `RELAY_CONTROL_ADDR` and `RELAY_CONTROL_TOKEN_FILE` together. This
starts a separate bearer-authenticated listener. It is never mounted on the public
API. In Jetstream, configure `JETSTREAM_DEBUG_ADDR` and
`JETSTREAM_CONTROL_TOKEN_FILE` as documented in `../jetstream/README.md`. Use
private transport or TLS between services. No component is deployed by this change.

`/oauth-client-metadata.json` must be reachable by authorization servers at the
public origin. OAuth uses the maintained AT Protocol Node client for discovery,
PAR, PKCE, DPoP and callback validation; only the `atproto` scope is requested.
The browser receives an opaque HTTP-only session cookie and a separate CSRF token,
never OAuth or service credentials. Login binds the callback to its initiating
browser. Sessions expire after eight hours. Unsafe API requests require the exact
Origin and session CSRF token; protected reads also require current administrator
membership. All responses prohibit caching.

## Administrator access

Set `ADMIN_SEED_DID` to the initial operator's full DID at runtime. It is not a
handle, password, or special superadministrator. An invalid or empty configured
value fails startup. Omit the variable when not needed.

On an uninitialized database, startup atomically grants this DID, records an
`administrator_seed` audit event and saves a durable initialization marker.
Restarts and changes to the variable never grant it again. Existing administrator
membership or recorded access history also prevents seeding. Removing every
administrator does not reset initialization; retain the database across deploys.

After signing in, use **Administrators** to grant access to another DID or remove
another administrator, including the seed DID. Removal immediately invalidates
all their local sessions and stored OAuth tokens, and records the acting admin in
the audit history. Admins cannot remove their own access through this API or UI;
another administrator must do that. Normal OAuth sign-in is still required.

The CLI is the recovery path when administrator access is lost. An operator with
filesystem access to the control-plane host can grant/remove DIDs:

```sh
ADMIN_DATABASE=/data/control.db npx tsx server/access.ts grant did:plc:...
ADMIN_DATABASE=/data/control.db npx tsx server/access.ts remove did:plc:...
ADMIN_DATABASE=/data/control.db npx tsx server/access.ts revoke did:plc:...
```

`remove` revokes access and all local sessions immediately; `revoke` retains
membership but invalidates sessions. These commands write local-operator audit
events. Membership is checked for every request. Sign out revokes the current
session; “Revoke my sessions” invalidates every local session for that DID and
removes stored OAuth tokens. This does not claim to revoke third-party sessions
at the user's PDS. No administrators or credentials are shipped in the application.

## Management contract

The browser-facing API is `/api/v1`; all endpoints below require authorization.

| Endpoint                                 | Contract                                                                                          |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `GET /administrators`                 | Paginated current administrator DIDs.                                                             |
| `POST /administrators`                | `{did, action}` with `grant` or `remove`; actor audited, CSRF protected; cannot remove oneself.            |
| `GET /session`                           | Current DID, CSRF token, expiry.                                                                  |
| `POST /signout`, `/revoke-sessions`      | Revoke the current or all local sessions.                                                         |
| `POST /operations`                       | Validated command plus UUID `Idempotency-Key`; returns durable operation and HTTP 202.            |
| `GET /operations`, `/operations/{id}`    | Requested command, actor, state, observed result and bounded failure code.                        |
| `POST /operations/{id}/retry`, `/cancel` | Retry failed/incomplete work or cancel work not yet applying; actor audited.                      |
| `GET /sources`, `/source?pds=…`          | Relay's desired, validation, connection, account-quota and durable-cursor state.                  |
| `GET /policy`                            | Jetstream's applied collection-policy revision.                                                   |
| `GET /jobs`, `/coverage`                 | Jetstream job state and source/policy-scoped current-state coverage; optional exact `pds` filter. |
| `GET /limits`, `/status`, `/audit`       | Applied policies, bounded service health, durable actor-attributed history.                       |

Commands are discriminated by `kind`: `source` (`pds`, `state`), `collections`
(`expectedRevision`, exact `collections`), `job` (`pds`, `reason`), `job_action`
(`id`, `action`), `account_quota` (`pds`, `expectedLimit`, `accountLimit`), and
`limit` (`scope`, `eventsPerSecond`). Bodies are limited to
64 KiB. Lists use bounded pages and opaque `after` cursors; restart pagination to
include newly added items that sort before the current cursor. Collection lists
are limited to 1,000 exact NSIDs per request.

The transactional journal records `requested` before contacting either service,
then `applying`, and finally `applied`, `failed`, `canceled` or `incomplete`.
`applied` acknowledges configuration/job submission, never archive completeness.
Partial source updates retain their observed service acknowledgment. On restart,
applying operations return to requested; source updates reconcile desired state,
collection revisions detect stale writes, and Jetstream persists request receipts
for job submission and retry/cancel so lost responses cannot duplicate work.
Unavailable services remain incomplete until an explicit retry. Source removal
stops acquisition without deleting archives. Job progress remains owned by Jetstream.

Coverage is a page of PDS/policy results with the exact selected collections, job
ID, progress and incomplete reason. A completed current snapshot has
`historyComplete: false`; historical PDS attribution is explicitly unknown.
Sources without jobs have unknown coverage. Source detail links directly to its
filtered jobs and coverage; connection state and durable cursors come from Relay.

## Rate policy

Global and per-PDS values are integer **events/second**, inclusive range 1–1,000,000,
with a one-second token-bucket burst. Both constraints must admit an event before
it enters the scheduler. They cover commit, sync, identity and account frames;
control/error frames can still pass. Waiting pauses further socket reads and is
cancelable. Policy changes wake waiting connections; persistence precedes the
applied acknowledgment. Buckets refill on restart; configured values persist.
Existing host limits and account-admission quotas remain independently enforced.

The UI reports waiting connections, not an invented total upstream backlog.
Reconnect resumes the last durable source cursor. If upstream replay has expired,
a Jetstream quota-recovery job can acquire current records but cannot reconstruct
unknown historical events. Inspect the source's recovery flag and actual job state.

## Verification

```sh
npm run check
npm test
npm run test:components
npm run build
npx playwright install chromium
npm run test:browser
```

`npm test` includes the real Go private management endpoints over loopback using
disposable databases. These fixtures simulate source validation and unavailable
PDS acquisition; Relay and Jetstream's own suites cover their actual ingestion and
backfill engines. Browser tests use the real control-plane HTTP API with explicit
test-only OAuth/service doubles. They exercise operator flows and desktop/mobile
accessibility; they are not a live external-PDS OAuth qualification. Production
`server/main.ts` cannot select those doubles. The root verification scripts remain
required for Go service changes.
