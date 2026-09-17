# Job and coverage contract

## Scope and owner

Jetstream owns durable direct-PDS source state, jobs, policy-scoped
reconciliation, checkpoints and current-state coverage. Relay source admission
and lifecycle remain Relay-owned. A [TECH-637 recovery receipt](https://linear.app/hypercerts/issue/TECH-637/persist-cross-service-recovery-receipts-for-admitted-pdss)
must bridge those owners before Relay clears a source recovery requirement.

After persisting a successful job completion, Jetstream's Plan 006 executor
submits Relay's
`POST /hypercerts/v1/source/recovery-receipt` with the exact admitted PDS,
Relay source revision, Jetstream policy revision, job ID, and bounded durable
completion boundary. Relay persists that composite receipt and clears
`RecoveryRequired` only while the enabled source still has that exact revision.
Stale, removed, or disabled sources reject the acknowledgement. Plan 006 owns
the private sender and completion proof; a submitted job is not itself a
receipt. The endpoint is a trusted internal Jetstream-to-Relay control seam,
not an administration or browser API.

The private Jetstream interface provides:

- `GET`, `POST` and `DELETE /hypercerts/v1/sources`;
- `GET /hypercerts/v1/jobs`, `POST /hypercerts/v1/jobs`, and
  `GET /hypercerts/v1/jobs/{id}`;
- `POST /hypercerts/v1/jobs/{id}/cancel` and `/retry`;
- `GET /hypercerts/v1/coverage`; and
- `GET /hypercerts/v1/snapshot-rejections`.

Job requests and retry/cancel actions use durable, separate receipt namespaces.
`POST /jobs` accepts an optional JSON `requestId` of at most 128 bytes. The same
nonempty request receipt returns its prior job; reuse with a different PDS or
reason returns conflict. `POST /jobs/{id}/cancel` and `/retry` accept an optional
`Idempotency-Key` header of at most 128 bytes. Reuse with a different job or
action returns conflict. Without a receipt, repeated job requests coalesce only
applicable pending or running work; a later unkeyed request after terminal work
may create a new job.

## Completion semantics

A completed job identifies the exact admitted PDS origin and policy revision
whose selected **current repository state** reached its durable boundary.
Completion does not establish historical creates, deletes or superseded values.
Those values are unavailable before the boundary, during an interruption, or
beyond raw retention, and must never be inferred from a current snapshot.

An unavailable source, changed DID hosting, stale snapshot or repository input
limit produces explicit incomplete coverage. Malformed or unverifiable input
fails. Neither result is completion. Cancellation and policy/source changes must
not allow obsolete work to report success.

Jobs report only `current_state` coverage. They never claim historical event
completeness or PDS attribution outside the completed snapshot boundary.

## Acceptance mapping

| Action or terminal outcome | Required evidence owner |
| --- | --- |
| Quiet/new admitted PDS reaches selected current state | [TECH-596](https://linear.app/hypercerts/issue/TECH-596/backfill-record-collections-for-a-pds) |
| Disabled source prevents pending/active job work | [TECH-637](https://linear.app/hypercerts/issue/TECH-637/persist-cross-service-recovery-receipts-for-admitted-pdss) and [TECH-596](https://linear.app/hypercerts/issue/TECH-596/backfill-record-collections-for-a-pds) |
| Adding a collection reconciles each enabled source | [TECH-596](https://linear.app/hypercerts/issue/TECH-596/backfill-record-collections-for-a-pds) |
| Policy/source changes cancel obsolete work; removal preserves archive | [TECH-596](https://linear.app/hypercerts/issue/TECH-596/backfill-record-collections-for-a-pds) |
| Crash, retry, receipt and checkpoint handling | [TECH-596](https://linear.app/hypercerts/issue/TECH-596/backfill-record-collections-for-a-pds) |
| Complete/incomplete/failed coverage wording | [TECH-597](https://linear.app/hypercerts/issue/TECH-597/expose-jetstream-backfill-progress-and-coverage) |
| Real-process direct-PDS verification/restart matrix | [TECH-634](https://linear.app/hypercerts/issue/TECH-634/unify-jetstream-acquisition-verification-and-durability-fault-coverage), `./tests/acceptance/run T15-core` |

`GET /hypercerts/v1/snapshot-rejections` accepts the normal private bearer
credential, optional repeated `pds` filters, `limit` (1–200), and an opaque
`after` cursor. It retains the most recent 1,000 rejections across all sources;
older rejections are evicted by rejection time (with a deterministic stored-key
tie-breaker). It returns only bounded non-payload rejection metadata:
origin, policy revision, DID, listed revision, rejection kind/code and timestamp.
It must not expose CAR bytes, records, repository tokens, or durable storage
keys. A permanent direct-PDS snapshot rejection is not inventory progress; it
survives restart and the job remains failed until an operator retry reaches a
changed or valid input.

`T15-core` is the real-process acceptance evidence owner for the current direct
PDS matrix. It authenticates and polls the private control `GET /policy` before
control calls, including retry phases after each Jetstream restart; it does not
use a public root-route response as readiness. It adds the source, proves a
selected record reaches the archive, injects targeted transient PLC-DID and
PDS-listRepos 5xx responses, and records bounded non-payload proxy hit evidence
for each configured route. It then substitutes the target DID signing key to
prove one durable non-payload rejection and no archive materialization. Archive
client output is temporary for the assertion and is deleted; retained archive
evidence is sanitized coordinates/count only. The runner also retains the base
revision and an executed-tree dirty manifest containing paths/statuses and
content digests, not source content, credentials, or payloads. Retained
per-phase artifacts are evidence of a run, not a claim that a run passed unless
the command exit/result says so.

The job reader accepts older persisted JSON which contains fields no longer
emitted by the control API. [TECH-635](https://linear.app/hypercerts/issue/TECH-635/validate-rainbow-raw-stream-recovery-and-retention)
owns complete-topology recovery evidence.
