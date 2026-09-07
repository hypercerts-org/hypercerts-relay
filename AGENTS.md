<!-- hypercerts: project-owned contributor instructions; review this file during every Indigo upstream sync. -->

# Hypercerts Relay agent guide

## Purpose and current scope

Hypercerts Relay is a small, maintained fork of Bluesky Indigo. It contains only the Indigo Relay, Rainbow, and their Go dependency closure. The target service shape is:

```text
Approved PDS instances
  -> Hypercerts Indigo Relay
  -> Rainbow for raw-stream connection pooling and fan-out
  -> Hypercerts Jetstream v2 for collection retention and PDS backfill
  -> consumers

Administration control plane and Svelte web UI
  -> PDS allowlist, retained collections, rate limits, jobs, telemetry
```

The Relay publishes raw `com.atproto.sync.subscribeRepos` events. Jetstream v2, in its own repository, owns retention of selected record collections and PDS/collection backfill. Rainbow is a raw event-stream proxy. Do not move those responsibilities between components without an explicit architecture decision.

The inherited Basic-auth Relay admin UI is upstream code. It is not the Hypercerts administration control plane and must not be extended as a substitute for the planned AT Protocol OAuth UI.

## Repository layout

- `cmd/relay` contains the Relay command and its services.
- `cmd/rainbow` contains the raw Relay fan-out proxy.
- `atproto`, `api`, `events`, `models`, `splitter`, `util`, `xrpc`, and `lex/util` are the selected Indigo dependency closure.
- `.agents/skills/hypercerts-relay` is the project skill for work in this fork.
- `scripts/verify.sh` is the focused local and CI verification entry point.

The Go module path intentionally stays `github.com/bluesky-social/indigo`. Do not rewrite imports or change it merely because this repository has a Hypercerts remote. It is an internal service fork, not a separately supported Go library; retaining the module path makes upstream synchronization reviewable.

## Before changing code

1. Inspect the branch, working tree, and remotes. Preserve unrelated work.
2. Read the nearest command, service, and test code together. Relay changes commonly span command configuration, persistent state, stream scheduling, and operator-facing behavior.
3. Identify whether the change belongs in the Relay, Rainbow, Jetstream v2, or the administration control plane. Only implement it in this repository when it belongs to Relay or Rainbow.
4. Keep the raw event stream complete. Filtering records for storage belongs in Jetstream, not in Relay or Rainbow.
5. Treat PDS admission, rate limits, event persistence, account status, and administrative actions as operational behavior. Add focused tests and document new configuration or operator actions.

## Fork and upstream policy

Read `FORK.md` before changing upstream-derived code or synchronizing Indigo. It owns the exact remote URL, the required remote verification, and the merge process.

- Put new Hypercerts behavior in clearly owned packages or configuration whenever practical.
- For an unavoidable upstream-file edit, add a short `// hypercerts:` comment explaining why the fork diverges. Keep the diff narrow.
- Never automatically resolve an upstream merge conflict, commit conflict markers, or auto-merge an upstream-sync pull request.
- Merge `upstream/main` into a branch from current `main`; do not rebase `main` onto upstream.
- Review upstream changes that overlap `// hypercerts:` markers, then run the focused verification before the sync pull request is merged.

## Validation

Run this before requesting review for Relay or Rainbow changes:

```bash
./scripts/verify.sh
git diff --check
```

`./scripts/verify.sh` runs Relay and Rainbow tests, `go vet`, and builds both commands. It excludes only the locally failing baseline test `TestClaimDueAccountLimitAlertsRepeatsAfterInterval`; the CI workflow runs that test separately as a non-blocking signal. Do not add further skipped tests without recording the observed behavior and removal condition in the script and README.

Use `gofmt` on changed Go files. Validate workflow changes with the repository's workflow checks when available, and inspect the rendered YAML for permissions, branch conditions, and write steps.

## Releases

The repository uses Changesets, consistent with other Hypercerts services. Add a named `.changeset/` file for an operator-visible change; the `Release` workflow opens a version pull request from `main`, and its merge creates the tag and GitHub Release. Do not hand-edit `CHANGELOG.md` or tag a release. The workflow does not deploy a service, publish a container, or alter a running relay.

See `RELEASING.md` and the `writing-changesets` project skill for release notes, version selection, and correction procedures.

## Safety

Do not add credentials to source, test fixtures, workflow files, or logs. Use repository secrets only for an approved publishing or deployment integration.

Do not delete or rewrite relay state, rebuild a production event store, add a PDS to a running relay, change rate limits, or publish a release without explicit authorization. Read-only diagnosis and local development are safe to continue within the requested scope.
