# Plan 001: Resolve open PR feedback without weakening ownership boundaries

> **Executor instructions**: Follow this plan in order. Run every verification
> command and confirm the stated result before continuing. Preserve unrelated
> local work; do not reset, stash, force-push, merge, or modify `main`. Update
> the status row in `plans/README.md` only after a human reviewer accepts the
> resulting work.
>
> **Drift check (run first)**:
> `git diff --stat c952652a..HEAD -- go.mod go.sum cmd/relay/relay administration/tests docs/upgrade-restore.md tests/upgrade jetstream && git diff --stat -- go.mod go.sum cmd/relay/relay administration/tests docs/upgrade-restore.md tests/upgrade jetstream && git status --short`
>
> If a listed file differs from the excerpts below, reconcile the new state
> before editing. At plan creation the worktree already contains uncommitted
> edits in `cmd/relay/relay/sources.go`, `cmd/relay/relay/sources_test.go`,
> `administration/tests/control-fixtures.ts`, and
> `administration/tests/t13-admin-acceptance.test.ts`. Treat them as another
> contributor's work: inspect and preserve them, but do not assume they fully
> solve the defects.

## Status

- **Priority**: P1
- **Effort**: L
- **Risk**: HIGH
- **Depends on**: none
- **Category**: bug, security, tests, dependencies, docs
- **Planned at**: commit `c952652a`, 2026-09-21

## Why this matters

The open stack has four real release blockers: one dependency update fails to
compile, a cross-service completion receipt can clear recovery for obsolete
work, test cleanup can close state under in-flight work, and T19 can claim an
upgrade/restore pass after several seeded fields change. The recovery-receipt
case crosses Relay and Jetstream ownership, so a local ordering heuristic is
not enough: it must retain a durable, authoritative definition of the current
Jetstream policy revision. The target outcome is a stack whose open review
threads either have an evidence-backed fix or a documented, approved reason to
reject them.

## Current state

### Repository conventions and boundaries

- The root Go module is `github.com/bluesky-social/indigo`, with Go 1.26.1
  selected in `go.mod`; do not rewrite module imports.
- `AGENTS.md` requires `./scripts/verify.sh` and `git diff --check` for Relay
  changes. Jetstream work additionally uses `cd jetstream && go test ./...`.
- `AGENTS.md` assigns collection retention, policy revisions, and backfill to
  `jetstream/`; Relay owns raw stream admission and source lifecycle. Do not
  implement Jetstream policy selection in Relay or move archive ownership.
- `docs/contracts/jobs.md:5-18` says Jetstream owns policy-scoped job state;
  the Relay recovery receipt is the cross-service boundary. The receipt must
  not become a browser or administration API.
- The initial topology is intentionally single-active Relay
  (`docs/contracts/launch-topology.md:3-6,32-38`). This plan must not attempt
  active/active coordination or alter rate-admission behaviour.

### PR #21: incompatible `go-car` dependency update

- `go.mod:31` currently pins `github.com/ipld/go-car`
  `v0.6.1-0.20230509095817-92d28eb23ba4`; PR #21 proposes `v0.6.3`.
- The PR #21 merge workflow, under Go 1.26.1, fails compiling
  `github.com/ipfs/go-log@v1.0.5` because the selected
  `github.com/ipfs/go-log/v2` no longer exports `LevelFromString`.
- `go.mod:87-88` records the conflicting indirect modules. This is an actual
  compilation failure, not the repository's separately tracked flaky baseline
  test.

### PR #26: stale recovery acknowledgement

- `cmd/relay/relay/sources.go:346-405` validates an enabled source and matching
  source revision, persists the submitted `PolicyRevision`, then clears
  `recovery_required`. The committed PR head has no current-policy comparison.
- A local worktree change uses `MAX(policy_revision)` over prior Relay receipts.
  That rejects only a lower value than an already acknowledged receipt; it
  cannot reject policy revision 1 when Jetstream has silently advanced to 2
  before Relay receives any receipt. Do not retain that approach as the sole
  guard.
- `cmd/relay/relay/models/source.go:56-77` stores a receipt but no authoritative
  current Jetstream policy version. Existing tests in
  `cmd/relay/relay/sources_test.go:42-159` cover source revisions and delivery
  idempotency; follow their GORM/`require` style.

### PR #30: administration test process shutdown

- `administration/tests/browser-server.ts:38-55` schedules detached
  `worker.tick()` calls, calls `server.close()` without awaiting its callback,
  and immediately closes the in-memory store and fixtures.
- `administration/server/worker.ts:26-67` can use the store after asynchronous
  `services.apply()`, including in its error transition. Closing the store first
  can create an unhandled rejection or test flake.
- `administration/tests/control-fixtures.ts:24-37` currently adds the `exit`
  listener after sending the stop signal. A child can exit in that interval.
- `administration/tests/t13-admin-acceptance.test.ts:94-100` checks session
  status and CSRF but does not assert the OAuth administrator DID returned by
  `/api/v1/session`.
- Administration checks use TypeScript, `node:test`, component tests, a Vite
  build, and Playwright; commands are defined in `administration/package.json`.

### PR #32: upgrade/restore contract and assertions

- `docs/upgrade-restore.md:5-8` permits Relay state to be rebuilt/lost, while
  `docs/upgrade-restore.md:51-57` requires Relay state in the complete restored
  snapshot. The operator contract is contradictory.
- `tests/upgrade/t19-relay-state.go.tmpl:71` seeds a recovery receipt with
  `DurableBoundary`; the verification at lines 107-110 finds the receipt but
  never compares that field. It seeds a raw account event with `Active`, `Seq`,
  and `Time` at line 89, while verification at lines 127-137 only compares
  `Did` and event count.
- `tests/upgrade/t19-jetstream-state.go.tmpl:77` seeds one complete archive
  event; lines 120-123 check only count, DID, collection, and non-empty payload.
  They omit exact sequence, witness time, kind, rkey, rev, and payload bytes.
- `tests/upgrade/t19` copies stopped predecessor state into direct and restored
  candidate paths. Keep that forward-only, disposable procedure and its
  sanitized evidence boundary unchanged.

### Stale thread to resolve, not reimplement

- `jetstream/internal/jetstreamd/runtime.go:678-704` validates control origins:
  HTTPS is allowed; HTTP is restricted to loopback or opt-in
  `*.railway.internal`. `runtime_test.go:350-382` covers those cases. The open
  PR #28 CodeRabbit transport thread is therefore stale; do not duplicate this
  already-landed fix.

## Commands you will need

| Purpose | Command | Expected on success |
|---|---|---|
| Relay/Rainbow validation | `./scripts/verify.sh` | exit 0; the one documented baseline test remains outside this script |
| Relay receipt unit tests | `GOCACHE=/tmp/hypercerts-relay-go-cache go test ./cmd/relay/relay -run 'TestRecoveryReceipt'` | exit 0 with all recovery receipt tests passing |
| Jetstream validation | `(cd jetstream && go test ./...)` | exit 0 |
| T19 qualification | `T19_PREDECESSOR_REVISION=<approved-predecessor> T19_CANDIDATE_REVISION=<candidate> ./tests/upgrade/t19 T19` | exit 0 and prints `T19 passed`; retain the sanitized evidence path |
| Administration static/test/build gate | `(cd administration && npm run check && npm test && npm run test:components && npm run build)` | exit 0 |
| Administration browser gate | `(cd administration && npm run test:browser)` | exit 0 |
| Whitespace gate | `git diff --check` | no output and exit 0 |

Do not install dependencies, publish images, change Railway configuration,
provision services, add a PDS, change a runtime rate limit, or publish a release
as part of this plan.

## Scope

**In scope**:

- `go.mod`, `go.sum` — only the minimal, proven compatibility correction for
  the PR #21 dependency update.
- `cmd/relay/relay/sources.go`, `cmd/relay/relay/models/source.go`,
  `cmd/relay/relay/sources_test.go`, `cmd/relay/control_test.go` — only after
  the receipt-authority decision below.
- The smallest necessary Jetstream receipt sender/control files and their tests
  — only if the approved authority design requires them.
- `docs/contracts/jobs.md` and `jetstream/README.md` — update the receipt
  contract if its durable authority changes.
- `administration/tests/browser-server.ts`, `administration/tests/control-fixtures.ts`,
  `administration/tests/t13-admin-acceptance.test.ts`, plus narrowly scoped
  tests needed to prove shutdown draining.
- `docs/upgrade-restore.md`, `tests/upgrade/t19-relay-state.go.tmpl`, and
  `tests/upgrade/t19-jetstream-state.go.tmpl`.

**Out of scope**:

- `cmd/relay/relay-admin-ui`, OAuth product scope, public control endpoints,
  and all production/Railway infrastructure.
- Rainbow activation, active/active Relay coordination, or changes to the
  single-active rate-admission lease.
- Any arbitrary `go mod tidy`, broad indirect-dependency upgrade, or replacement
  directive used only to make PR #21 compile.
- Changes to the already-correct Relay-control transport validator in
  `jetstream/internal/jetstreamd/runtime.go`.
- Manual resolution of GitHub review threads, pushing, merging, tagging, or
  release publication; a human owns those remote actions.

## Git workflow

- Work in separate branches because PR #21 targets `main` while #26–#32 are a
  dependent stack. Suggested names:
  `fix/pr21-go-car-compat`, `fix/tech-637-receipt-authority`,
  `fix/tech-588-admin-shutdown`, and `fix/tech-636-t19-assertions`.
- Preserve existing uncommitted files. Before each branch, confirm exact owners
  and use a clean worktree rather than stashing or resetting shared work.
- Keep one logical commit per remediation. Match the repository's conventional
  style, for example `fix: validate relay recovery control transport` and
  `test: harden administration fixture setup`.
- Do not push, create a pull request, resolve remote threads, or merge without
  explicit operator authorization.

## Steps

### Step 1: Reproduce and constrain the PR #21 dependency failure

Create an isolated worktree from PR #21's head or equivalent clean branch. Run
the exact focused verification under Go 1.26.1 before editing. Inspect the PR
diff and `go mod graph` to identify which `go-car v0.6.3` requirement advances
`go-log/v2` beyond compatibility with retained `go-log v1.0.5` consumers.

Choose one of these evidence-backed outcomes:

1. retain `go-car v0.6.3` and make the smallest compatible dependency selection
   that upstream modules permit; or
2. reject/defer the bump because a compatible resolution requires a larger,
   separately reviewed IPFS migration.

Do not pin a version by guesswork and do not add a replace directive unless the
upstream module graph documents it as the supported compatibility mechanism.

**Verify**: `./scripts/verify.sh && git diff --check` → exit 0. If retaining the
bump, `go test ./cmd/relay/relay -run '^TestClaimDueAccountLimitAlertsRepeatsAfterInterval$'`
must compile; record its pass/fail separately because the CI job is non-blocking.

### Step 2: Make the receipt's current-policy authority explicit before coding

Stop implementation and obtain a maintainer decision for how Relay can verify
the current Jetstream policy revision. Present these constraints:

- Jetstream is the owner of policy revisions and jobs; Relay does not currently
  persist a current policy version.
- A maximum of old Relay receipts proves only what was acknowledged before; it
  cannot prove the current Jetstream policy after a policy advance.
- The receiver must reject an obsolete receipt before clearing
  `RecoveryRequired`, while preserving retry idempotency for the same current
  completion coordinate.

The decision must name one durable authority and its update path, for example a
Relay-persisted policy-revision mirror updated atomically through a trusted
Jetstream control action, or a trusted current-policy assertion that Relay can
validate atomically at acknowledgement time. It must also define what re-arms
recovery when that policy advances. Document the chosen model in
`docs/contracts/jobs.md` before relying on it in code.

**Verify**: a written contract includes (a) authority owner, (b) durable storage
location, (c) update/re-arm event, (d) stale receipt result, and (e) retry
idempotency. If any are absent, STOP; do not implement a heuristic.

### Step 3: Implement and characterize the approved receipt-authority model

After Step 2, make the smallest cross-service change that persists the approved
current policy state and uses an exact comparison inside the same transaction
that creates the recovery receipt and clears `recovery_required`. Keep the
source revision predicate, private bearer boundary, bounded receipt fields, and
`ErrRecoveryReceiptConflict` behaviour for stale input.

Add focused tests beside `TestRecoveryReceiptRequiresCurrentEnabledSourceRevision`:

- current source + current policy clears recovery;
- current source + stale policy leaves recovery armed and adds no acknowledgement
  receipt;
- a policy advance re-arms recovery before an old job's receipt arrives;
- retry of the same current receipt remains idempotent;
- disabled, removed, and changed-source-revision paths retain their existing
  conflict behaviour.

If Step 2 requires Jetstream sender changes, add the matching integration test
through the private control seam; do not expose new browser routes.

**Verify**: `GOCACHE=/tmp/hypercerts-relay-go-cache go test ./cmd/relay/relay -run 'TestRecoveryReceipt'` → exit 0, followed by `./scripts/verify.sh && git diff --check` → exit 0.

### Step 4: Drain administration test work before closing dependencies

In `administration/tests/browser-server.ts`, replace the detached interval call
with a small tracked-tick wrapper. Every scheduled tick must be added to a set
of in-flight promises and removed in `finally`; shutdown must clear the timer,
await all tracked ticks, await the `server.close` callback, then close `store`
and fixtures. Preserve the existing `stopping` idempotency guard and do not
change production `Worker` semantics merely to make a fixture easier to stop.

In `administration/tests/control-fixtures.ts`, create the `once(child, "exit")`
promise after the existing `exitCode` check but before writing the stop file or
sending SIGTERM, then await that saved promise. Keep the five-second SIGKILL
fallback and cleanup semantics.

In `administration/tests/t13-admin-acceptance.test.ts`, retain `did` in the
session response type and assert it equals the administrator DID used in the
OAuth callback. Do not add a non-administrator operation case: callback rejects
that identity before it receives a session.

Add a deterministic shutdown test only if the existing fixture can exercise a
blocked `services.apply` without process timing. Prefer a test-owned deferred
promise/fake over real sleeps. Otherwise, rely on the existing T13 and browser
gates and state that the regression is covered by orderly shutdown assertions.

**Verify**: `(cd administration && npm run check && npm test && npm run test:components && npm run build)` → exit 0; then `(cd administration && npm run test:browser)` → exit 0; finally `git diff --check` → no output.

### Step 5: Align the T19 operator contract and strengthen its oracle

Decide the runbook's single restore policy and state it consistently. The
recommended initial-launch policy is: Relay state is part of the required
complete snapshot, because source admission and durable source coordinates must
be verified before resuming. Remove the sentence that allows Relay state to be
rebuilt/lost, unless the maintainer explicitly approves a separate recovery
procedure with equivalent source-admission evidence.

In `t19-relay-state.go.tmpl`, introduce constants for the seeded durable
boundary and raw event values if that makes exact comparison unambiguous. Check
the stored receipt's `DurableBoundary`; check exactly one raw account event with
the seeded DID, `Active`, sequence, and timestamp.

In `t19-jetstream-state.go.tmpl`, name the seeded archive event fields and
compare the decoded event's sequence, witnessed-at time, kind, DID, collection,
rkey, rev, and exact payload bytes. Keep count-one checking and do not print
payload bytes into T19 evidence.

Do not turn T19 into a production Railway/PostgreSQL test, a rollback test, or
an active/active/Rainbow qualification. Preserve its forward-only revision
ancestry check and its sanitized evidence manifest.

**Verify**: run `T19_PREDECESSOR_REVISION=<approved-predecessor> T19_CANDIDATE_REVISION=<candidate> ./tests/upgrade/t19 T19` using an approved forward revision pair → exit 0 and output contains `T19 passed`. Then run `(cd jetstream && go test ./...)` and `git diff --check` → exit 0.

### Step 6: Reconcile the review stack after local verification

For each changed branch, inspect the remote PR head and the exact review anchor
again before declaring a thread addressed. Confirm PR #28's transport validator
and unit test remain unchanged and mark that thread stale rather than changing
the validator. Re-run every applicable validation gate from this plan after
rebasing/reconciling the stack; a parent change invalidates child evidence.

Prepare, but do not send, a concise review disposition for each thread: affected
commit, validation command/result, and whether it was fixed, stale, or blocked
by the Step 2 architecture decision.

**Verify**: `git diff --check` → no output; `git status --short` lists only the
intended remediation and plan files; all validation gates for affected modules
have recorded exit-0 results.

## Test plan

- Relay: extend `cmd/relay/relay/sources_test.go` with the stale-policy and
  policy-advance cases defined in Step 3; retain existing source-lifecycle and
  idempotency cases.
- Relay control receiver: update `cmd/relay/control_test.go` only if the
  approved contract changes its trusted payload or conflict result.
- Administration: execute existing `node:test`, component, build, and browser
  gates. Add a deterministic drain test only when the fixture has an injectable
  blocked operation; never use sleep-based timing as the sole race oracle.
- T19: assert every field that seed creates for the recovery receipt, raw event,
  and selected archive event; then run the actual copied-state harness, not only
  compilation of its templates.
- Dependency: test the PR #21 resolved module graph with the repository's full
  Relay/Rainbow gate, not an isolated package compile alone.

## Done criteria

- [ ] PR #21 either has a minimal proven-compatible dependency resolution with
  `./scripts/verify.sh` passing, or is explicitly rejected/deferred with the
  incompatible module graph recorded.
- [ ] A maintainer-approved authoritative current-policy model exists before
  the PR #26 receipt code is changed.
- [ ] A stale current-source receipt cannot clear recovery after a Jetstream
  policy advance, and an exact current retry remains idempotent.
- [ ] Administration shutdown drains ticks and server requests before store or
  fixture closure; fixture exit waiting is registered before signalling.
- [ ] T13 asserts the authenticated administrator DID.
- [ ] The restore runbook has one consistent Relay-state policy.
- [ ] T19 exactly validates all seeded receipt, raw event, and archive fields
  without exposing archive payloads in evidence.
- [ ] Applicable Relay, Jetstream, administration, T19, and whitespace gates
  exit 0.
- [ ] No code changed for the already-fixed PR #28 transport issue.

## STOP conditions

- The worktree contains changes owned by another contributor and they overlap a
  proposed edit. Stop and coordinate; do not reset, overwrite, or stage them.
- No durable authority can identify the current Jetstream policy revision at
  receipt acknowledgement time. Stop rather than comparing against previous
  receipts or trusting an unverified caller field.
- A proposed receipt design adds a public endpoint, stores payloads/tokens, or
  moves Jetstream collection policy ownership into Relay.
- The dependency repair requires a broad indirect upgrade, a replacement
  directive without upstream support, or a Go/toolchain change outside PR #21's
  declared scope.
- T19 cannot run with an approved forward revision pair, or its test would need
  a real Railway resource, production data, credentials, or destructive state
  handling.
- Any validation gate fails twice after a reasonable scoped correction. Report
  the command, failure, and current diff instead of weakening a test or adding a
  skip.

## Maintenance notes

- Future Jetstream policy changes must preserve the chosen current-policy
  authority and re-arm semantics; reviewers should reject any receipt path that
  compares only a caller-supplied or historical value.
- The browser-server process is a test fixture. Its shutdown ordering must stay
  aligned with asynchronous `Worker.tick` behaviour if new background work is
  added.
- T19 is intentionally an upgrade/restore qualification oracle, not a full
  production disaster-recovery claim. Extend it only for durable state owners
  selected by the launch topology, and keep evidence bounded and non-payload.
