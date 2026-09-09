<!-- hypercerts: project-owned documentation; review this file during every Indigo upstream sync. -->

# Hypercerts Relay

Hypercerts Relay is the controlled AT Protocol event source for Hypercerts services. It contains the Relay, Rainbow, and the Go packages those services need.

The Relay connects to approved PDS instances and publishes a raw `com.atproto.sync.subscribeRepos` stream. The relay does not select Hypercerts records or provide a consumer archive. A separately operated Jetstream v2 service receives this stream, retains only the enabled record collections, and performs durable collection backfills. Rainbow sits in front of the raw Relay stream when connection pooling and fan-out are needed.

The administration control plane will manage PDS sources, record collections, rate limits, backfills, and relay telemetry. Its Svelte web interface is not the inherited Relay admin UI: the existing Basic-auth interface under `cmd/relay/relay-admin-ui` remains upstream code until the control-plane work replaces it.

## Status

This repository establishes the maintained Relay and Rainbow base and its delivery process. It does not yet contain the Hypercerts PDS policy, Jetstream integration, collection retention, backfill behavior, or the new administration control plane. Those changes are tracked as component work before deployment.

## Included components

| Component | Purpose |
| --- | --- |
| `cmd/relay` | Indigo Relay base for accepting PDS subscriptions and emitting the raw repository event stream. |
| `cmd/rainbow` | Indigo raw-stream fan-out proxy for reducing direct Relay connections. |
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
```

Do not infer production defaults from those commands. Deployment configuration, persistence, credentials, observability, and PDS allowlisting are part of the component work and must be reviewed before a service is exposed.

## Releases

Releases use Changesets, like the other maintained Hypercerts services. Add a named release note for an operator-visible change, then run the `Release` workflow from `main`. It opens a reviewed version pull request; merging that pull request creates the version tag and GitHub Release. It does not publish a container image or claim a deployment.

See [RELEASING.md](RELEASING.md) for the exact process and rollback guidance.

## Project documents

- [FORK.md](FORK.md) — Indigo provenance, upstream remote setup, and synchronization rules.
- [AGENTS.md](AGENTS.md) — implementation and operational guidance for contributors and agents.
- [RELEASING.md](RELEASING.md) — release preparation, workflow behavior, and corrections.
- [CHANGELOG.md](CHANGELOG.md) — Hypercerts Relay release history.
- [`.agents/skills/hypercerts-relay/SKILL.md`](.agents/skills/hypercerts-relay/SKILL.md) — focused repository guidance for work on the Relay and Rainbow fork.

## License

This repository retains Indigo's dual licensing under [MIT](LICENSE-MIT) and [Apache-2.0](LICENSE-APACHE). See the license files for their terms.
