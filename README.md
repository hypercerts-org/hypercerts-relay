<!-- hypercerts: project-owned documentation; review this file during every Indigo upstream sync. -->

# Hypercerts Relay

Hypercerts Relay is the controlled AT Protocol event source for Hypercerts services. It contains the Relay, Rainbow, and collection directory Indigo closure plus a separately maintained, copied Jetstream v2 runtime module.

The Relay connects to approved PDS instances and publishes a raw `com.atproto.sync.subscribeRepos` stream. The relay does not select Hypercerts records or provide a consumer archive. The `jetstream/` module receives this stream, retains only the enabled record collections, and performs durable collection backfills. Rainbow is a deferred raw-stream fan-out option, not part of the initial launch topology.

The optional collection directory consumes the raw Relay stream and crawls configured PDSs to serve `com.atproto.sync.listReposByCollection`. It helps consumers discover candidate repositories for a collection; it does not replace Jetstream's archive or backfills. Results may be stale or incomplete, so consumers must verify records. See the [collection directory operator guide](cmd/collectiondir/README.md) for bootstrap, security, and recovery limitations.

The administration control plane manages PDS sources, record collections, rate limits, backfills, and relay telemetry. Its Svelte web interface is not the inherited Relay admin UI: the existing Basic-auth interface under `cmd/relay/relay-admin-ui` remains upstream code and is not the control plane.

## Status

This repository contains the maintained Relay and Rainbow base, an optional collection directory, durable Relay processing and rejections, a managed PDS registry, a copied Jetstream v2 runtime, collection-selection policy, scoped backfill jobs, and the administration control plane. Their remaining acceptance, recovery, upgrade and deployment gates are tracked in the implementation plans; their presence is not deployment qualification.

## Event durability

Relay saves output before advancing account revisions or source cursors. Processing and storage failures stop acknowledgment and reconnect from successful progress. Source cursor transactions preserve administrative status and never reduce saved progress. Shutdown waits for source processing before closing output persistence.

An unavailable identity remains retryable. Permanent verification failures create a durable rejection containing source metadata and a fixed reason, without record contents. Signature failures require identity refresh before rejection. Startup adds the rejection table; back up Relay state before upgrading. Stored output can repeat after a failed revision update, so downstream consumers must tolerate duplicates.

An ambiguous disk write or sync failure stops further persistence until restart. Startup discards only an incomplete trailing write before resuming; complete retained events remain replayable. Per-event file synchronization replaces buffered acknowledgments and can reduce throughput.

## Relay-to-Jetstream source handoff

The Relay's durable source lifecycle queue sends handoffs only when both
`JETSTREAM_CONTROL_URL` and `JETSTREAM_CONTROL_TOKEN` are configured on the Relay
process. If both are absent, delivery stays disabled and queued rows are retained.
If exactly one is absent, Relay fails startup.
This outbound token is separate from Jetstream's inbound `JETSTREAM_CONTROL_TOKEN_FILE`
for its private control API; configure that on Jetstream as documented in
[`jetstream/README.md`](jetstream/README.md).

## Managed PDS sources

The Go adapter on `relay.Relay` separates durable desired state from connection state. It does not expose a new HTTP API or OAuth UI.

| Operation | Behavior |
| --- | --- |
| `AddSource(ctx, origin)` | Register an origin with pending validation. Do not connect until validation passes. Repeated registration preserves disabled state and the stored TLS scheme. |
| `ValidateSource(ctx, hostID, revision)` | Check the stored origin, persist a bounded result, then reconcile its connection. No insecure fallback. |
| `SetSourceState(ctx, hostID, revision, state)` | Enable, disable, or remove acquisition. Stop waits for processing and cursor persistence. Removal retains host, account, rejection, and replay data. |
| `ListSources(ctx, afterHostID, limit)` | Page through all sources, including quiet sources. Report desired/runtime state, validation, revision, durable cursor, and account quota. |
| `ListSourceAccounts(ctx, hostID, afterDID, limit)` | Page through source observations, current resolved hosts, account placement, quota exclusions, and incomplete/unknown target coverage. |
| `ListRejectedEvents(ctx, afterID, limit)` | Page through non-payload rejection outcomes and their verification policy revisions. |

Pages default to 100 entries and cap at 1,000. State and validation changes require the current revision; an immediate identical retry is idempotent. A stale conflicting request must refresh its revision.

Startup creates `source` and `account_source_observation` tables. Existing host rows become admitted sources; existing bans become disabled sources. Subsequent migrations preserve managed policy. Back up the database and event store together before upgrading. Restart reconnects only enabled, validated, non-banned sources from their saved cursors, including quiet or previously unavailable sources.

Public `requestCrawl` can register a new source and synchronously validates it before returning success; validation failures return a generic client error while retaining the durable failed validation result. A failed public check is not retried for one minute; concurrent checks for the same source return HTTP 409, and a state/revision change during validation rejects the stale result without persisting it. Repeating the request for a source that already passed validation is idempotent and does not repeat the host check. Public requests cannot re-enable a disabled source, and pending/new admission observes durable desired state. Authenticated `requestCrawl` uses the same synchronous validation behavior. Both paths require HTTPS PDS origins in normal builds, even for administrators or when `AllowInsecureHosts` is set, and retain domain-ban protections; sibling relays are notified only after local admission succeeds. The existing authenticated block/unblock handlers delegate to the same durable policy. They remain legacy administration, not the planned control plane.

### Operator transport policy and migration

For Relay operators, PDS sources must be reachable at public HTTPS origins with valid TLS certificates. Bare hostnames select HTTPS; HTTP and WS origins are rejected before new host/source rows are saved. Source checks cannot follow HTTPS redirects to HTTP. The public-network-only dialer remains in force for source sockets and host checks.

Operators already using public HTTPS PDS sources need no action. If you previously used plaintext or private-network sources, make those PDS instances available through public HTTPS before upgrading. Persisted `NoSSL` hosts fail closed during validation and restart; Relay does not rewrite their stored scheme or cursor. Inspect and explicitly migrate legacy source state through your approved state-migration procedure before restarting acquisition. Back up the database and event store first. Remove `--allow-insecure-hosts` or `RELAY_ALLOW_INSECURE_HOSTS=true`: startup now rejects that obsolete option rather than silently ignoring it.

DID migration never admits or connects the resolved target automatically. Each observed source remains separately recorded while current resolution is refreshed. An explicitly admitted target remains incomplete until a separately owned Jetstream v2 recovery job reaches its durable boundary. This Relay adapter cannot claim complete coverage or reconstruct deleted historical state. Account quota exclusions use `host-account-limit`; they are distinct from stream throughput limits.

## Included components

| Component | Purpose |
| --- | --- |
| `cmd/relay` | Indigo Relay base for accepting PDS subscriptions and emitting the raw repository event stream. |
| `cmd/rainbow` | Indigo raw-stream fan-out proxy for reducing direct Relay connections. |
| `cmd/collectiondir` | Optional directory for discovering candidate repository DIDs by collection; see the [operator guide](cmd/collectiondir/README.md) for limitations and exposure. |
| `jetstream/` | Copied Jetstream v2 runtime module for archive, replay, live subscriptions, and Hypercerts-owned collection/backfill work. See [`jetstream/README.md`](jetstream/README.md) for provenance and operations. |
| `atproto`, `api`, `events`, `models`, `splitter`, `util`, `xrpc`, and `lex/util` | The dependency closure required to build and test the Indigo services. |

## Development

Local plaintext PDS fixtures are limited to localhost through the existing injected test dialer; the production CLI exposes no such injection. Disposable acceptance builds retain their separate two-part gate: `-tags acceptance` **and** `RELAY_ACCEPTANCE_PRIVATE_HOSTS=true`. Neither the build tag alone nor the environment variable in a normal build permits insecure PDS traffic. Never use acceptance builds or opt-ins for production.

For branch rulesets on `main`, `dev` (staging), and `production`, require these
GitHub Actions status checks:

- `Verify Relay and Rainbow`
- `Lint Go`
- `Validate Changesets`
- `Verify Jetstream`
- `Verify administration`
- `Verify container packaging`

These checks run on every pull request to those branches and on merge queues.
`Verify container packaging` requires the repository boundary check and all four
container checks to succeed. Keep `Track local baseline test` non-blocking, and
do not require image publishing, which runs only after a merge to staging.

After changing check names, run the updated workflows successfully on a pull
request before selecting the names in the ruleset. GitHub requires a successful
check within the past seven days; see
[troubleshooting required status checks](https://docs.github.com/en/pull-requests/how-tos/merge-and-close-pull-requests/troubleshooting-required-status-checks).

Install Go 1.26.10 or the toolchain selected by `go.mod`. Relay links SQLite through CGO, so a working C compiler is also required.

```bash
git clone git@github.com:hypercerts-org/hypercerts-relay.git
cd hypercerts-relay
./scripts/verify.sh
```

The verification script runs focused tests, static checks, and builds for Relay, Rainbow, and collection directory. One Relay test is inconsistent across environments at the tracked Indigo baseline: `TestClaimDueAccountLimitAlertsRepeatsAfterInterval` fails in the local verification environment but passed the published GitHub Actions run. The script skips only that local failure. CI runs the test separately as a non-blocking signal while the difference is investigated.

Build a service locally with:

```bash
go build ./cmd/relay
go build ./cmd/rainbow
go build ./cmd/collectiondir
(cd jetstream && go build ./cmd/jetstream)
```

Do not infer production defaults from those commands. Deployment configuration, persistence, credentials, observability, and PDS allowlisting are part of the component work and must be reviewed before a service is exposed.

## Releases

Releases use Changesets, like the other maintained Hypercerts services. Add a named release note for an operator-visible change, then run the `Release` workflow from `main`. It opens a reviewed version pull request; merging that pull request creates and validates the version tag against Relay, Rainbow, Jetstream, and Administration. Use the `hypercerts-github-release` skill to create the GitHub Release with generated notes from that tag. Neither step publishes a container image or claims a deployment.

See [RELEASING.md](RELEASING.md) for the exact process and rollback guidance.

## Project documents

- [FORK.md](FORK.md) — Indigo provenance, upstream remote setup, and synchronization rules.
- [AGENTS.md](AGENTS.md) — implementation and operational guidance for contributors and agents.
- [RELEASING.md](RELEASING.md) — release preparation, workflow behavior, and corrections.
- [CHANGELOG.md](CHANGELOG.md) — Hypercerts Relay release history.
- [`.agents/skills/hypercerts-relay/SKILL.md`](.agents/skills/hypercerts-relay/SKILL.md) — focused repository guidance for work on the Relay and Rainbow fork.
- [`.agents/skills/hypercerts-railway-release/SKILL.md`](.agents/skills/hypercerts-railway-release/SKILL.md) — immutable Railway staging and production promotion guidance.
- [`.agents/skills/hypercerts-railway-development/SKILL.md`](.agents/skills/hypercerts-railway-development/SKILL.md) — isolated all-services Railway development-environment guidance.
- [`.agents/skills/hypercerts-github-release/SKILL.md`](.agents/skills/hypercerts-github-release/SKILL.md) — source-tag and GitHub Release guidance.
- [`jetstream/README.md`](jetstream/README.md) — Jetstream v2 provenance, maintenance, and local operation.
- [`docs/contracts/`](docs/contracts/) — approved data, job, management, ownership, and [initial-launch topology](docs/contracts/launch-topology.md) contracts.

## License

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option. This covers the code and documentation in the repository, which come from three sources:

- **From Indigo**, the Indigo files in the paths listed in [`.hypercerts/upstream-paths`](.hypercerts/upstream-paths): copyright Bluesky Social PBC, `@whyrusleeping` and contributors, under Indigo's dual license ([LICENSE-DUAL](LICENSE-DUAL)).
- **From Jetstream**, the files in [`jetstream/`](jetstream) listed in [`.hypercerts/jetstream-upstream-paths`](.hypercerts/jetstream-upstream-paths): copyright Bluesky Social PBC and contributors, under Jetstream's dual license ([jetstream/LICENSE-DUAL](jetstream/LICENSE-DUAL)).
- **Everything else**: copyright 2026 Hypercerts Foundation, under the same two licenses. This includes the administration control plane in [`administration/`](administration), the files we added under the Indigo paths and in `jetstream/` (such as the code for managed PDS sources in `cmd/relay/relay/` and the packages in [`jetstream/internal/hypercerts/`](jetstream/internal/hypercerts)), and our changes to Indigo and Jetstream files.

A file that states its own license keeps it. For example, [`util/ssrf/ssrf.go`](util/ssrf/ssrf.go) is dedicated to the public domain under CC0.

The licenses don't cover the Hypercerts logo in `administration/src/brand/assets/logo/` and `administration/public/favicon.svg`, which is a mark of the Hypercerts Foundation. They also don't cover the fonts in `administration/src/brand/assets/fonts/`, which keep their own licenses; see the [brand notes](administration/src/brand/README.md).
