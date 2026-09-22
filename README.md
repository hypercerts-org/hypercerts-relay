<!-- hypercerts: project-owned documentation; review this file during every Indigo upstream sync. -->

# Hypercerts Relay

Hypercerts Relay is the controlled AT Protocol event source for Hypercerts services. It contains the Relay and Rainbow Indigo closure plus a separately maintained, copied Jetstream v2 runtime module.

The Relay connects to approved PDS instances and publishes a raw `com.atproto.sync.subscribeRepos` stream. The relay does not select Hypercerts records or provide a consumer archive. The `jetstream/` module receives this stream, retains only the enabled record collections, and performs durable collection backfills. Rainbow is a deferred raw-stream fan-out option, not part of the initial launch topology.

The administration control plane manages PDS sources, record collections, rate limits, backfills, and relay telemetry. Its Svelte web interface is not the inherited Relay admin UI: the existing Basic-auth interface under `cmd/relay/relay-admin-ui` remains upstream code and is not the control plane.

## Status

This repository contains the maintained Relay and Rainbow base, durable Relay processing and rejections, a managed PDS registry, a copied Jetstream v2 runtime, collection-selection policy, scoped backfill jobs, and the administration control plane. Their remaining acceptance, recovery, upgrade and deployment gates are tracked in the implementation plans; their presence is not deployment qualification.

## Event durability

Relay saves output before advancing account revisions or source cursors. Processing and storage failures stop acknowledgment and reconnect from successful progress. Source cursor transactions preserve administrative status and never reduce saved progress. Shutdown waits for source processing before closing output persistence.

An unavailable identity remains retryable. Permanent verification failures create a durable rejection containing source metadata and a fixed reason, without record contents. Signature failures require identity refresh before rejection. Startup adds the rejection table; back up Relay state before upgrading. Stored output can repeat after a failed revision update, so downstream consumers must tolerate duplicates.

An ambiguous disk write or sync failure stops further persistence until restart. Startup discards only an incomplete trailing write before resuming; complete retained events remain replayable. Per-event file synchronization replaces buffered acknowledgments and can reduce throughput.

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

Public `requestCrawl` can reconnect only an already admitted, enabled source; it cannot add, validate, or re-enable a source. The existing authenticated block/unblock handlers delegate to the same durable policy. They remain legacy administration, not the planned control plane.

DID migration never admits or connects the resolved target automatically. Each observed source remains separately recorded while current resolution is refreshed. An explicitly admitted target remains incomplete until a separately owned Jetstream v2 recovery job reaches its durable boundary. This Relay adapter cannot claim complete coverage or reconstruct deleted historical state. Account quota exclusions use `host-account-limit`; they are distinct from stream throughput limits.

## Included components

| Component | Purpose |
| --- | --- |
| `cmd/relay` | Indigo Relay base for accepting PDS subscriptions and emitting the raw repository event stream. |
| `cmd/rainbow` | Indigo raw-stream fan-out proxy for reducing direct Relay connections. |
| `jetstream/` | Copied Jetstream v2 runtime module for archive, replay, live subscriptions, and Hypercerts-owned collection/backfill work. See [`jetstream/README.md`](jetstream/README.md) for provenance and operations. |
| `atproto`, `api`, `events`, `models`, `splitter`, `util`, `xrpc`, and `lex/util` | The dependency closure required to build and test Relay and Rainbow. |

## Development

Install Go 1.26.1 or the toolchain selected by `go.mod`. Relay links SQLite through CGO, so a working C compiler is also required.

```bash
git clone git@github.com:hypercerts-org/hypercerts-relay.git
cd hypercerts-relay
./scripts/verify.sh
```

The verification script runs focused tests for Relay and Rainbow, static checks, and builds both binaries. One Relay test is inconsistent across environments at the tracked Indigo baseline: `TestClaimDueAccountLimitAlertsRepeatsAfterInterval` fails in the local verification environment but passed the published GitHub Actions run. The script skips only that local failure. CI runs the test separately as a non-blocking signal while the difference is investigated.

Build a service locally with:

```bash
go build ./cmd/relay
go build ./cmd/rainbow
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

This repository retains Indigo's dual licensing under [MIT](LICENSE-MIT) and [Apache-2.0](LICENSE-APACHE). See the license files for their terms.
