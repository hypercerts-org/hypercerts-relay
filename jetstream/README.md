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
JETSTREAM_COLLECTIONS=org.hypercerts.claim.activity \
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

`serve` opens a durable global exact-NSID policy before ingestion. On a new data
directory, `JETSTREAM_COLLECTIONS` (or `--collections`) initializes revision 1.
The example NSID above is illustrative: configure your actual enabled collections.
An empty list stores no record payloads; prefixes and malformed NSIDs are rejected.
Restart restores the persisted revision, including an intentionally empty list.
Startup environment changes do not overwrite the persisted policy.

`go test ./internal/jetstreamd -run '^TestHypercertsIndigoArchiveRestartAndLive$'`
checks the owned Indigo Relay's disk event manager and real `subscribeRepos`
handler, the Jetstream archive across restart, and a public client that replays
the archive, continues live, and reconnects from its saved cursor. It builds the
loopback-only `tests/jetstream-source` fixture from the parent Go module and uses
disposable state. The fixture injects signed PDS frames at Relay's admitted-event
boundary; PDS admission and Relay validation have their own Relay tests.

The acquisition writers enforce the same policy on bootstrap segments, temporary
bootstrap live segments, steady-state live commits, failed-repository retries,
and sync replacement rows, before segment or readable-log writes. Selected deletes
and required identity/account/sync markers are retained. Filtered-only commits
still advance the durable source/verifier boundary. Archive merging and compaction
do not apply the current acquisition policy to previously retained rows.

This boundary concerns Jetstream record materialization. Raw Relay/Rainbow data
and transient full-repository CAR downloads can contain excluded records; this
does not promise their absence from upstream storage or transient input buffers.
The internal embedding API retains upstream behavior unless `CollectionSelection`
is enabled; the production `serve` command always enables it.

## Scoped PDS backfill jobs

On a new data directory, `JETSTREAM_PDS_SOURCES` (or `--pds-sources`) initializes
explicit direct-PDS origins, separated by commas. HTTPS is required except for
loopback fixtures. These are Jetstream acquisition targets; admitting a PDS into
the raw Relay remains a separate Relay operation. Persisted sources override the
startup list, so a removed source is not silently re-enabled after restart.

Adding a source schedules its current collection policy immediately, including
for a quiet PDS. Changing the enabled collection list atomically records the next
policy revision, cancels older pending/running work, and schedules each enabled
source. Backfill and quota-recovery requests, cancellation, and retry use durable
job IDs scoped to the exact origin and policy revision. Removal cancels that
source's work without purging archived rows or enrolling a migration destination.

Jobs enumerate repositories directly from the named PDS, verify signed snapshots
against the DID identity, and reconcile only selected collections. Missing selected
records produce per-record deletes; another collection is not replaced by a
DID-wide tombstone. Managed steady-state whole-repository live/retry syncs use scoped record
replacements and deletes instead of a DID-wide sync tombstone. This preserves newer collection snapshots across delayed sync delivery.
Inactive account events block stale snapshot publication. Archive writes and
per-collection revision boundaries are
synced before repository progress is acknowledged. Delayed older live/retry rows
cannot overwrite a completed snapshot; newer live rows win. Restart retries a
running job from its persisted page/repository progress. A crash before progress
acknowledgment may replay work safely.

Coverage is explicitly `current_state`, with `historyComplete: false`: a PDS
snapshot cannot prove historical event completeness. Unavailable sources, changed
DID hosting, stale snapshots, and the 64 MiB per-repository input limit produce
`incomplete`; malformed or unverifiable input produces `failed`. Both require an
explicit retry. Local persistence failures stop the runtime without acknowledging
completion. Each repository request has a two-minute deadline. Reconciliation
currently scans the archive under its rewrite lock, so large archives can pause
live appends during an individual repository reconciliation.

## Private service interface

Mount a secret file containing a random service credential of at least 32 bytes,
then set `JETSTREAM_CONTROL_TOKEN_FILE=/run/secrets/jetstream-control` and
`JETSTREAM_DEBUG_ADDR=127.0.0.1:6060` (or use the matching CLI flags). The token is
read at startup; rotation requires restart. No credential means no control routes.
A credential without an operations listener is a configuration error. Keep this
listener private: its existing metrics and pprof endpoints have their existing
access behavior. The control routes require `Authorization: Bearer <credential>`
and return `Cache-Control: no-store`. Use TLS/private transport between services.
The public listener does not expose the API. This is a service contract for the
separate administration control plane, not the OAuth administration UI.

All paths below start with `/hypercerts/v1`:

| Method and path | Request / result |
| --- | --- |
| `GET /policy` | Current revision and exact collection list. |
| `PUT /policy` | `{"expectedRevision":1,"collections":["app.bsky.feed.post"]}`; atomically updates policy and jobs. Explicit `[]` disables record acquisition. |
| `GET /sources` | Explicit PDS origins and enabled flags. |
| `POST /sources` | `{"pds":"https://pds.example"}`; adds source and returns its job. |
| `DELETE /sources` | Same body; cancels acquisition without deleting the archive. |
| `POST /jobs` | `{"pds":"https://pds.example","reason":"backfill"}` or reason `quota_recovery`; duplicate active work is reused, a later terminal recovery creates a fresh job. |
| `GET /jobs?limit=100&after=<cursor>` | Sorted page, maximum 200 jobs, optional `nextCursor`. Concurrent additions may require a fresh listing. |
| `GET /jobs/{id}` | Durable state, progress, attempt count, and coverage. |
| `POST /jobs/{id}/cancel` | Cancels pending/running work. |
| `POST /jobs/{id}/retry` | Resumes current-policy work from durable progress; a removed source or obsolete revision returns conflict. |

Each job reports `id`, `pds`, `policy.revision`, `policy.collections`, `reason`,
`state`, `attempts`, `completedRepos` (count), page `cursor`, bounded `errorCode`,
creation/start/finish times, `coverage`, and `historyComplete`. Job states are
`pending`, `running`, `failed`, `canceled`, `incomplete`, and `complete`. A completed
job identifies the exact PDS origin and policy revision whose **current state** it
covers. `historyComplete: false` explicitly records that older history is not
proven recoverable; administration clients must not present this as full historical
coverage. An unreachable PDS returns `incomplete`, never `complete`.

Errors use a bounded JSON `error` code: 400 invalid input, 401 authentication,
404 unknown job/source, 409 policy/state conflict, and 500 persistence failure.
Mutation bodies are limited to 64 KiB and reject unknown fields/trailing JSON.
Local persistence errors do not acknowledge a successful change or expose raw
storage errors. Job progress and outcomes survive restart.
