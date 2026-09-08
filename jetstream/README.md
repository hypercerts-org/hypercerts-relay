# Hypercerts Jetstream v2

This directory is the Hypercerts-owned runtime copy of Jetstream v2. It reads
the raw `com.atproto.sync.subscribeRepos` stream published by the Relay and
serves Jetstream's archive, replay, and live-subscription interfaces. It is a
separate Go module so its dependency graph and release cadence remain isolated
from the Indigo module at this repository root.

## Provenance and maintenance

The initial runtime closure was copied from
[`bluesky-social/jetstream`](https://github.com/bluesky-social/jetstream) at
commit `9a30defd224e9058814a7d6ce8d9e4fc48d5493c` (2026-08-14). It includes the
command, archive format, XRPC API, runtime internals, generated lexicons, and
the tests required to build and exercise them. Repository metadata, examples,
design notes, and unrelated development tooling were deliberately not copied.

This is not an Indigo upstream subtree and `scripts/apply-upstream-update.sh`
does not update it. Before taking an upstream Jetstream change, compare the
specific files against the recorded commit, preserve Hypercerts-owned changes,
update this provenance section, and run the Jetstream test suite. The copied
code retains its upstream module path and dual MIT/Apache-2.0 license; see
`LICENSE-MIT`, `LICENSE-APACHE`, and `LICENSE-DUAL`.

The current applied revision is `.hypercerts/jetstream-upstream-base`; the initial
revision above remains the provenance of the import. The exact copied upstream
paths are in `.hypercerts/jetstream-upstream-paths`. New upstream dependencies or
paths need an explicit maintainer review and allowlist update.

For an update, create a dated review branch from current `main`, then run:

```bash
git fetch https://github.com/bluesky-social/jetstream.git main
python3 scripts/jetstream-upstream.py FETCH_HEAD
python3 scripts/jetstream-upstream.py FETCH_HEAD --apply
./scripts/verify-jetstream.sh
git diff --cached --check
```

The script reports affected Hypercerts markers and applies only the recorded
closure under `jetstream/`. It stops at conflicts without advancing the baseline;
a maintainer must resolve each conflict and explicitly update the baseline after
resolution. Never auto-resolve conflicts or merge an upstream branch into `main`.
The weekly `Check Jetstream upstream` workflow opens a review PR after verification;
only a human may merge it. Verification includes the full module suite, vet, build,
stress lifecycle oracle, and restart oracle checks. The Indigo remote and update
script remain independent. See [deployment source and build contexts](../docs/deployment.md).

## Operation

Build and test from this directory:

```bash
go test ./...
go build ./cmd/jetstream
```

Run it behind the owned Relay using a persistent, Jetstream-owned data path:

```bash
JETSTREAM_RELAY_URL=http://relay:2470 \
JETSTREAM_DATA_DIR=/data/jetstream \
go run ./cmd/jetstream serve
```

Do not point `JETSTREAM_RELAY_URL` at Rainbow when direct Relay access is
available. Rainbow is optional raw-stream connection pooling; Jetstream owns
archive materialization, retention, replay, direct-PDS backfill, and its own
durable state. Back up the Jetstream data directory as one unit. It is not
safe to combine it with Relay's database or event-store backup.

`Dockerfile` builds this module with `docker build -f jetstream/Dockerfile
jetstream`. It does not publish a service or make a deployment decision.

## Hypercerts policy work

TECH-587 owns versioned selected-collection policy and durable scoped backfill
jobs. The imported upstream runtime is deliberately the baseline for that
work, not proof that its whole-network defaults satisfy Hypercerts policy. A
Hypercerts policy must be enforced before record payloads are materialized on
bootstrap, live ingestion, retries, replacement syncs, and restart; required
identity, account, sync, delete, cursor, and progress behavior remains
durable. Job outcomes must identify the source and policy revision and report
unavailable history as incomplete.
