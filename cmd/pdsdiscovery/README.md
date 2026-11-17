# PDS discovery

`pdsdiscovery` enables PDS sources that publish retained collections. When a
record in a retained collection appears on a PDS that is not yet a source, it
asks the administration application to enable that PDS. Once the PDS appears as
an enabled source, it raises the source's account limit to match the account
count that a reference relay reports.

This service is optional. It changes nothing on its own: every addition and
limit change is a journaled, audited administration operation, attributed to
the actor `service:pds-discovery`. It never calls the Relay or Jetstream
directly.

## How it works

1. On start, and then every `DISCOVERY_POLL_INTERVAL`, it reads
   `GET /api/v1/sources/discovery/state` from administration. That state lists
   the retained collections and every PDS source in any state.
2. It subscribes to a public Bluesky Jetstream, filtered to the retained
   collections with `wantedCollections`.
3. For each record create or update, it resolves the author's DID to a PDS.
   The PDS is skipped when any of these hold:
   - the PDS is already a source in any state;
   - its endpoint is not a public HTTPS origin;
   - it matches the optional denylist;
   - this process has already asked for it.
4. It asks the reference relay about the host with
   `com.atproto.sync.getHostStatus`. If that relay reports the host as
   `banned`, the PDS is not added. If the lookup fails, the PDS is retried on a
   later record. A host the reference relay does not know is still added.
5. Otherwise it submits
   `{kind: "source", pds, state: "enabled"}`. The administration worker applies
   it through the Relay, and the Relay hands the new source to Jetstream for
   backfill. Administration also checks the Relay, both when it receives the
   request and again before applying it. If the source exists as `disabled` or
   `removed`, it refuses the request with `source_disabled_by_operator`, so an
   operator's disable or remove always wins, even against a stale poll.
6. After a later poll shows the new PDS as an enabled source, it applies the
   account-limit rule below, using the account count from step 4.

There is no cap on how many PDSes are added; the Relay and Jetstream are sized
for the additions.

### Account limit rule

The account limit is raised only when both of these hold:

- the PDS has more than `DISCOVERY_ACCOUNT_THRESHOLD` accounts (default 100);
- the reference relay does not report the host as `throttled`.

The new limit is the account count rounded up to the next thousand. For
example, 1,400 accounts gives a limit of 2,000. The limit never exceeds the
maximum account limit that administrators set in the administration Sources
screen (100,000 by default). Administration also refuses any discovery request
above that maximum.

The request is `{kind: "account_quota", pds, expectedLimit, accountLimit}`.
`expectedLimit` is the current limit, so an operator change made in the
meantime causes a conflict instead of being overwritten. The service only ever
raises a limit, and only for PDSes that this process added. If the host is
unknown to the reference relay, is throttled there, or is at or below the
threshold, the Relay default limit stays.

## Configuration

Every flag also reads the environment variable shown.

| Variable | Default | Meaning |
| --- | --- | --- |
| `DISCOVERY_ADMIN_URL` | required | Administration origin. It must use HTTPS, except for loopback or `*.railway.internal`. |
| `DISCOVERY_ADMIN_TOKEN_FILE` | required | File holding the machine token, at least 32 bytes. Administration reads the same value from `ADMIN_DISCOVERY_TOKEN_FILE`. |
| `DISCOVERY_UPSTREAM` | `wss://jetstream2.us-east.bsky.network/subscribe` | Bluesky Jetstream subscribe URL. |
| `DISCOVERY_REFERENCE_RELAY` | `https://relay1.us-east.bsky.network` | Relay queried with `getHostStatus`. |
| `DISCOVERY_POLL_INTERVAL` | `1m` | Interval between administration state polls. The minimum is `5s`. |
| `DISCOVERY_DENY_HOSTS` | unset | Optional comma-separated exact hostnames or `*.suffix` patterns that are never enabled. Nothing is excluded by default. |
| `DISCOVERY_DRY_RUN` | `false` | Log candidates and their would-be account limit without requesting anything. |
| `DISCOVERY_ACCOUNT_THRESHOLD` | `100` | Account count above which the account limit is raised. |
| `DISCOVERY_REPLAY_LOOKBACK` | `10m` | Upstream replay window on a cold start. |
| `DISCOVERY_METRICS_ADDR` | unset | Listen address for `/metrics` and `/healthz`. |
| `DISCOVERY_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`. |

The maximum account limit is not a service flag. Administrators set it in the
"Automatic PDS discovery" panel on the Sources screen, and discovery reads it
with each state poll.

With `HC_RAILWAY_STARTUP=1`, the container entrypoint writes
`HC_DISCOVERY_SECRET` to the token file. Set the same secret on administration.
The service is stateless and needs no volume.

## Operation

Start with `DISCOVERY_DRY_RUN=true` and read the `PDS candidate` log lines
before you enable additions. Useful signals:

- `pdsdiscovery_candidates_total{outcome}` counts records by outcome.
  Outcomes include `submitted`, `known`, `denied`, `upstream_banned`,
  `lookup_failed`, `dry_run`, `rejected` and `submit_failed`.
- `pdsdiscovery_account_limit_checks_total{outcome}` counts account-limit
  checks by outcome.
- `pdsdiscovery_admin_last_poll_success_timestamp_seconds` and
  `pdsdiscovery_admin_poll_failures_total` track administration polling.
- `pdsdiscovery_upstream_reconnects_total` counts upstream reconnects.
- `/healthz` returns 503 when the last successful administration poll is older
  than five poll intervals.

## Limitations

- **Discovery is best effort.** A PDS is found only when one of its accounts
  writes a retained record while the service is connected. A missed PDS is
  found on its next write. Discovery makes no claim of completeness.
- **No cursor is persisted.** A cold start replays `DISCOVERY_REPLAY_LOOKBACK`.
- **One attempt per PDS.** Each enable request uses a deterministic idempotency
  key derived from the PDS origin, so a restart cannot create a duplicate. If
  administration fails to apply the request, an operator retries it in the
  Operations screen.
- **Account-limit checks are held in memory.** A pending check is lost on
  restart and abandoned after an hour (or ten poll intervals, if that is
  longer). An operator can set the limit in the Sources screen.
- **The upstream filter is limited to 100 values.** Above 100 retained
  collections, the filter collapses them into shared NSID prefixes, and only
  exact NSIDs count.
