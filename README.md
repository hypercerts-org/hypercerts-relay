<!-- hypercerts: project-owned documentation; review this file during every Indigo upstream sync. -->

# Hypercerts Relay

Hypercerts Relay is the controlled AT Protocol event source for Hypercerts services. It is a deliberately small fork of [Bluesky Indigo](https://github.com/bluesky-social/indigo) containing the Relay, Rainbow, and the Go packages those services need.

The Relay connects to approved PDS instances and publishes a raw `com.atproto.sync.subscribeRepos` stream. The relay does not select Hypercerts records or provide a consumer archive. A separately operated Jetstream v2 service receives this stream, retains only the enabled record collections, and performs durable collection backfills. Rainbow sits in front of the raw Relay stream when connection pooling and fan-out are needed.

The administration control plane will manage PDS sources, record collections, rate limits, backfills, and relay telemetry. Its Svelte web interface is not the inherited Relay admin UI: the existing Basic-auth interface under `cmd/relay/relay-admin-ui` remains upstream code until the control-plane work replaces it.

## Status

This repository establishes the maintained fork and its delivery process. It does not yet contain the Hypercerts PDS policy, Jetstream integration, collection retention, backfill behavior, or the new administration control plane. Those changes are tracked as component work before deployment.

## Included components

| Component | Purpose |
| --- | --- |
| `cmd/relay` | Indigo Relay base for accepting PDS subscriptions and emitting the raw repository event stream. |
| `cmd/rainbow` | Indigo raw-stream fan-out proxy for reducing direct Relay connections. |
| `atproto`, `api`, `events`, `models`, `splitter`, `util`, `xrpc`, and `lex/util` | The dependency closure required to build and test Relay and Rainbow. |

The Go module path intentionally remains `github.com/bluesky-social/indigo`. Relay and Rainbow are built from this repository; the project does not publish these packages as a standalone Go library. Keeping the upstream import path prevents a broad, low-value rewrite and keeps upstream changes straightforward to review.

## Development

Install Go 1.26.1 or the toolchain selected by `go.mod`. Relay links SQLite through CGO, so a working C compiler is also required.

```bash
git clone git@github.com:hypercerts-org/hypercerts-relay.git
cd hypercerts-relay
git remote add upstream https://github.com/bluesky-social/indigo.git
./scripts/verify.sh
```

The verification script runs focused tests for Relay and Rainbow, static checks, and builds both binaries. One upstream Relay test currently fails at the tracked Indigo baseline: `TestClaimDueAccountLimitAlertsRepeatsAfterInterval`. The script skips only that test and prints the reason. CI also runs it in a visible, non-blocking job so that a future upstream correction is apparent.

Build a service locally with:

```bash
go build ./cmd/relay
go build ./cmd/rainbow
```

Do not infer production defaults from those commands. Deployment configuration, persistence, credentials, observability, and PDS allowlisting are part of the component work and must be reviewed before a service is exposed.

## Working in the fork

Read [AGENTS.md](AGENTS.md) before changing code. The short form:

- Keep Hypercerts behavior at a narrow boundary and mark unavoidable edits to upstream-owned Go files with `// hypercerts:`.
- Prefer new Hypercerts-owned packages and configuration over broad changes to Indigo internals.
- Keep the raw Relay stream, Jetstream retention and backfill, Rainbow fan-out, and the administration control plane as separate responsibilities.
- Do not change the Go module path without an explicit decision to publish a supported Hypercerts Go module.

## Updating from Indigo

`origin` is `hypercerts-org/hypercerts-relay`; `upstream` is `bluesky-social/indigo`. Every changed upstream line can become a future merge conflict, so upstream synchronization is reviewed work:

1. The `Check Indigo upstream` workflow checks weekly whether `upstream/main` is ahead and opens a review pull request when it can merge cleanly.
2. The workflow never resolves conflicts, commits conflict markers, or merges the pull request. If Git reports a conflict, it fails and a maintainer resolves it on a dedicated upstream-sync branch.
3. Review the upstream diff, every `// hypercerts:` edit, Relay and Rainbow verification, and operational behavior before merging.
4. Merge the sync pull request into `main`; do not rebase `main` onto upstream.

For a manual sync, create a branch from current `main`, fetch `upstream/main`, merge it with a merge commit, run `./scripts/verify.sh`, and open a pull request. Keep these pull requests small and frequent. `git log upstream/main..main` shows fork-only commits and helps identify local behavior that needs review.

## Releases

Releases are manually initiated from `main` with the `Release` GitHub workflow. Before starting it, update [CHANGELOG.md](CHANGELOG.md) with a dated version section and ensure the intended changes have passed review. The workflow runs verification, creates a signed-off annotated `vX.Y.Z` tag, and creates a GitHub Release from that changelog section. It does not publish a container image or claim a deployment.

See [RELEASING.md](RELEASING.md) for the exact process and rollback guidance.

## Project documents

- [AGENTS.md](AGENTS.md) — implementation and upstream-sync rules for contributors and agents.
- [RELEASING.md](RELEASING.md) — release preparation, workflow behavior, and corrections.
- [CHANGELOG.md](CHANGELOG.md) — Hypercerts Relay release history.
- [`.agents/skills/hypercerts-relay/SKILL.md`](.agents/skills/hypercerts-relay/SKILL.md) — focused repository guidance for work on the Relay and Rainbow fork.

## License

This fork retains Indigo's dual licensing under [MIT](LICENSE-MIT) and [Apache-2.0](LICENSE-APACHE). See the license files for their terms.
