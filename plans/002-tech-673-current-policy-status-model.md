# Plan 002: Make one current-policy coverage status model authoritative

> **Executor instructions**: Follow this plan step by step. Run every verification
> command and confirm the expected result before moving to the next step. If a
> STOP condition occurs, stop and report it. Do not improvise a second status
> model. When done, update this plan's row in `plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat 24a557bb..HEAD -- jetstream/internal/hypercerts/jobs jetstream/internal/hypercerts/control administration/server administration/tests jetstream/README.md administration/README.md .changeset`
> If any in-scope file changed since this plan was written, compare the current
> code against the excerpts below. Stop if the existing control contract was
> materially redesigned.

## Status

- **Priority**: P1
- **Effort**: L
- **Risk**: HIGH, this changes persisted job semantics and the private control contract.
- **Depends on**: none, but consume durable acquisition, history, and diagnostic evidence supplied by TECH-663 if that work lands first.
- **Category**: bug
- **Planned at**: commit `24a557bb`, 2026-10-08
- **Linear issue**: TECH-673

## Why this matters

The current private coverage endpoint chooses the newest job for each PDS by
creation time, without proving that the job still matches an enabled source and
the current collection-policy revision. It exposes only completed-repository and
initial-inventory counters. That allows stale or job-wide evidence to be read as
current-policy coverage, and leaves the public status page and administration
UI with no single projection to consume.

After this work, a versioned, explicitly scoped status snapshot will be the only
source for current PDS and aggregate acquisition coverage. It must distinguish
current-state acquisition from event-history coverage and live-ingest freshness;
unknown must remain unknown rather than becoming healthy by omission.

## Current state

- `jetstream/internal/hypercerts/jobs/jobs.go` owns durable source and backfill
  state. `Job` currently stores `CompletedRepos`, `TotalRepos`, a policy snapshot,
  terminal state, and a bounded `ErrorCode` (`jobs.go:50-76`). New durable
  counters must be updated atomically with repository checkpoints, and must
  remain valid when reopening old state documents.
- `jetstream/internal/hypercerts/control/handler.go` serves the private API.
  `latestCoverageJobs` selects only the most recently created job for each PDS
  (`handler.go:258-276`) and `coverageView` contains only a job-derived fraction
  (`handler.go:521-554`). Do not retain this selection rule as the definition of
  current coverage.
- `jetstream/internal/hypercerts/control/handler_test.go:260-305` establishes
  the existing truthfulness rule: an unavailable source is `incomplete` and
  current-state coverage never claims historical completeness.
- `administration/server/services.ts:91-118` proxies Jetstream's coverage and
  uses its compact summary to enrich Relay Sources. It must consume the shared
  snapshot rather than re-derive semantics in Node.
- `administration/server/app.ts:223-236` adds only the explicit
  `historicalPDSAttribution: "unknown"` presentation field. Keep the control
  plane as a proxy, not an alternate status authority.
- `jetstream/internal/ingest/live/metrics.go:237-259` owns the last observed
  steady-state upstream event. It is distinct from snapshot acquisition.
- Repository language: Go in `jetstream/`, Node/TypeScript in
  `administration/`. Persisted Go job data is JSON in Pebble and must be
  backward-compatible; tests use `testify/require`. TypeScript contracts are in
  `administration/server/contracts.ts` and must be checked by `svelte-check`
  plus `tsc`.
- Product requirement: `PRODUCT.md:13-17` says the operator experience must
  show what was applied, what failed, and what remains incomplete, while keeping
  unknown historical PDS attribution unknown. `DESIGN.md:6-7,33-34` requires
  preserved status meanings and accessible, explicit status text.

## Commands you will need

| Purpose | Command | Expected on success |
| --- | --- | --- |
| Jetstream tests | `cd jetstream && go test ./...` | exit 0 |
| Administration typecheck | `npm --prefix administration run check` | exit 0, no diagnostics |
| Administration service tests | `npm --prefix administration test` | exit 0, all tests pass |
| Administration component tests | `npm --prefix administration run test:components` | exit 0, all tests pass |
| Repository verification | `./scripts/verify.sh` | exit 0 |
| Diff validation | `git diff --check` | exit 0, no whitespace errors |

## Scope

**In scope**:

- `jetstream/internal/hypercerts/jobs/jobs.go` and narrowly related job-runner
  files/tests — persist and update evidence required to distinguish inventory,
  scanned, matching, no-match, unresolved, and attributable record totals.
- `jetstream/internal/hypercerts/control/handler.go` and
  `handler_test.go` — publish one explicit, versioned current-policy status
  projection, including PDS and aggregate views.
- New focused Go package/files beneath `jetstream/internal/hypercerts/` only if
  needed to make the projection reusable by both control and public status.
- `administration/server/{contracts,services,app}.ts` and their tests — proxy
  the new contract without rewriting its semantics.
- `jetstream/README.md`, `administration/README.md`, and a named `.changeset/`
  file — document the private API semantics and operator-visible contract.

**Out of scope**:

- `jetstream/internal/status/`, `jetstream/internal/web/`, and its status
  template. TECH-674 is the only branch that renders public status.
- `administration/src/`. TECH-675 owns the screens and repository diagnostics.
- Relay/Rainbow admission, raw event persistence, historical recovery, and the
  job retry execution algorithm. Those remain TECH-662/TECH-663 work.
- Changing or deleting archived records, retrying live production jobs, or
  modifying deployed configuration.

## Git workflow

- Branch from current `main`: `tech/673-current-policy-status-model`.
- This is one branch and one pull request for TECH-673. Do not add TECH-674 or
  TECH-675 rendering changes to it.
- Commit logical units using the existing imperative style, for example
  `fix(coverage): page latest backfill state by PDS`. Do not push or open a PR
  unless explicitly instructed.
- Add one named Changeset because this changes the operator-visible control API.

## Steps

### Step 1: Define the stable projection before wiring endpoints

Create a Go-owned status projection type under
`jetstream/internal/hypercerts/` with a documented schema version and explicit
scope. It must have:

1. **Per-PDS current-policy evidence** for every enabled source, including the
   source origin/revision, current policy revision/collections, and whether a
   matching job exists.
2. **Acquisition state**, using a vocabulary that differentiates `unknown`,
   `running`, `retry_waiting`, `stopped_incomplete`, `failed`, `canceled`, and
   `complete`. Map existing persisted `jobs.State` deliberately; do not expose a
   UI label derived from a string replacement.
3. **Count semantics**: `initialInventory`, `scanned`, `matching`, `noMatch`,
   `unresolved`, and `attributableRecords`. Counts that do not have durable
   evidence must be absent/unknown, not zero. A successfully verified no-match
   repository increments `scanned` and `noMatch`; it is successful work, not an
   unresolved repository.
4. **Independent evidence sections** for current snapshot acquisition,
   event-history coverage/boundaries, and live connection/freshness. Use an
   explicit unknown state for history evidence not owned by the job manager.
   Do not claim that a cursor, connection, or completed current snapshot proves
   the other two sections.
5. **Diagnostics** derived only from bounded durable evidence: failure category,
   last progress time, job attempts, affected repository when safe and present,
   plus retry timing/action when supplied by the job layer. Do not place raw CAR
   content, service credentials, unbounded errors, or storage keys in the API.

Document the JSON field meanings in Go comments and the Jetstream README. Keep
`Job` as the durable execution record; the new type is a projection from the
current enabled-source set plus the current policy, not a second mutable store.

**Verify**: `cd jetstream && go test ./internal/hypercerts/...` → exit 0 after
adding compile-time/unit coverage for the projection constructor.

### Step 2: Persist the evidence required for truthful counters

Extend the job persistence and its repository-checkpoint path so counters have
real evidence instead of inferring matching progress from a job-wide fraction.
Inspect all checkpoint, no-record reconciliation, failure, cancellation, retry,
and restart paths before changing the schema.

- Preserve migration compatibility for documents written before these counters
  existed. An old job must project unknown attribution, not fabricated zeros.
- Record each inventory outcome exactly once per PDS/policy/job coordinate:
  scanned-and-matching, scanned-and-no-match, unresolved, or terminal invalid
  input. Prevent duplicate retry processing from increasing counts.
- Update `lastProgressAt` only after the durable outcome/checkpoint is committed.
- When an explicit retry starts a new inventory, reset only counters belonging to
  that obsolete inventory, in the same atomic write as the existing inventory
  reset (`jobs.go:630-649`).
- Preserve the current safety contract: an incomplete unavailable repository
  stays unresolved; a permanent invalid snapshot is `failed` or durable rejection
  according to the existing job policy; neither becomes no-match.

Add focused restart/idempotency tests alongside the current job tests. Cover a
sparse selected collection, all no-match inventory, interrupted download,
transient retry waiting, terminal unavailable repository, and an old persisted
job that lacks the new fields.

**Verify**: `cd jetstream && go test ./internal/hypercerts/jobs/...` → exit 0,
including the new restart and counter-semantics cases.

### Step 3: Replace job-recency coverage selection with current-policy projection

Change `/hypercerts/v1/coverage` in `handler.go` so it returns the shared
projection for every enabled source, filtered/paginated by exact PDS. A source
without a current-policy job must appear as `unknown`; do not disappear because
there is no job. A completed job is eligible only when its source revision and
policy revision match the enabled source and currently applied policy.

Keep `/jobs` as execution history, but make its per-job view reuse the shared
counter/diagnostic types where applicable. Add a separate aggregate object that
combines only enabled sources and the current policy. Its completeness must be:

- complete only if every enabled source has a matching complete acquisition;
- incomplete if any matching source is running, waiting, stopped, failed,
  canceled, or unresolved;
- unknown if evidence is absent or unavailable.

Do not aggregate collection rows by copying a single job fraction into every
collection. If true per-collection evidence is unavailable, state that the
progress is job-wide and leave matching/record attribution unknown at the
collection level.

Retain bounded request parsing, authentication, source filtering, and pagination
limits. Update control tests to prove old-policy jobs, disabled sources, and one
complete source cannot make the aggregate/current policy complete; verify
no-match completion and unavailable/missing metrics cases.

**Verify**: `cd jetstream && go test ./internal/hypercerts/control/...` → exit
0, with regressions for stale policy/source revisions and the aggregate truth
table.

### Step 4: Make administration proxy the projection without interpretation

Update `administration/server/contracts.ts`, `services.ts`, and `app.ts` to
transport the new typed projection and aggregate unchanged, apart from the
existing browser-only `historicalPDSAttribution: "unknown"` presentation field.
Do not recompute "latest", coverage, or health from timestamps in Node.

Update `administration/tests/control.test.ts` to verify that the API preserves:
current policy/source scope, all count-semantic fields, state, diagnostic
category/timing, aggregate status, missing evidence, and independent history/
live fields. Preserve the current 3-second source-table enrichment bound in
`Services.coverageFor`.

**Verify**: `npm --prefix administration run check && npm --prefix administration test` → both exit 0.

### Step 5: Document and validate the private contract

Revise `jetstream/README.md` control-route table and the coverage section in
`administration/README.md`. State exactly what each count means, that no-match
means a successful scan, that unknown is not zero, and that snapshot, historical
coverage, and freshness are separate. Include no credentials or production PDS
examples.

Add a named `.changeset/` file describing the new current-policy coverage and
status contract. It must not claim deployment or data recovery.

**Verify**: `cd jetstream && go test ./...`; then from repository root run
`npm --prefix administration run check`, `npm --prefix administration test`,
`./scripts/verify.sh`, and `git diff --check` → all exit 0.

## Test plan

- Model the control HTTP tests after
  `jetstream/internal/hypercerts/control/handler_test.go:260-364`.
- Model durable counter/restart tests after existing job-manager tests in
  `jetstream/internal/hypercerts/jobs/`.
- Add contract tests for: multiple enabled sources with mixed states; stale
  source and policy jobs; no-job unknown source; sparse matches; no-match
  completion; incomplete/failed/waiting work; missing telemetry; independent
  history and live freshness; and aggregate truthfulness.
- Run the commands in Steps 1-5 and the full Jetstream suite.

## Done criteria

- [x] `/coverage` returns every enabled source and a current-policy aggregate;
  old-policy, disabled, or stale-source jobs cannot prove completion.
- [x] The response distinguishes inventory, scanned, matching, no-match,
  unresolved, and attributable record counts, with unknown represented
  explicitly when evidence is absent.
- [x] Snapshot acquisition, history coverage, and live freshness are independent
  fields with no implicit healthy inference.
- [x] Go control/job tests, administration check/tests, `./scripts/verify.sh`,
  and `git diff --check` all exit 0.
- [x] A named Changeset and both operator/API documents describe the contract.
- [x] Only in-scope files changed and `plans/README.md` is updated.

## STOP conditions

Stop and report instead of improvising if:

- TECH-663 has introduced an incompatible persisted acquisition/history model;
  reconcile the contract before adding another store.
- Accurate matching/no-match/record counts require reading archived payloads or
  cannot be obtained at the durable repository checkpoint boundary.
- A safe aggregate requires modifying Relay/Rainbow raw-stream behavior.
- Existing persisted job documents cannot be migrated without a destructive
  rewrite or changing the meaning of historical fields.

## Maintenance notes

TECH-674 and TECH-675 must consume this model rather than adding their own
coverage calculations. Reviewers should verify that all completeness decisions
are scoped to enabled sources and the current policy, and that no-match success
never becomes missing data. Future collection-level evidence may extend the
projection, but must not present job-wide counters as collection-specific facts.


## Execution status

Implemented on `fix/tech-673` from `24a557bb`. Independent Standards and Spec
reviews passed after one consolidated repair: durable acquisition generations
keep older-job explicit retries authoritative over newer-created completion.
Full Jetstream tests, administration check/service/component tests, repository
verification, canonical rootless Jetstream image build, and diff checks all
passed on the reviewed candidate. No deployment or remote publication occurred.
