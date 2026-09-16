# Job and coverage contract

## Scope and owner

Jetstream owns durable direct-PDS source state, jobs, policy-scoped
reconciliation, checkpoints and current-state coverage. Relay source admission
and lifecycle remain Relay-owned. A [TECH-637 recovery receipt](https://linear.app/hypercerts/issue/TECH-637/persist-cross-service-recovery-receipts-for-admitted-pdss)
must bridge those owners before Relay clears a source recovery requirement.

The private Jetstream interface provides:

- `GET`, `POST` and `DELETE /hypercerts/v1/sources`;
- `GET /hypercerts/v1/jobs`, `POST /hypercerts/v1/jobs`, and
  `GET /hypercerts/v1/jobs/{id}`;
- `POST /hypercerts/v1/jobs/{id}/cancel` and `/retry`; and
- `GET /hypercerts/v1/coverage`.

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

`historyComplete` is transitional baseline state only: it is always false and
does not communicate a useful operator choice. [TECH-597](https://linear.app/hypercerts/issue/TECH-597/expose-jetstream-backfill-progress-and-coverage)
removes it from persisted job state, Jetstream control responses, administration
contracts/UI, tests and documentation. Until then, no client may treat the field
as historical proof.

## Acceptance mapping

| Action or terminal outcome | Required evidence owner |
| --- | --- |
| Quiet/new admitted PDS reaches selected current state | [TECH-596](https://linear.app/hypercerts/issue/TECH-596/backfill-record-collections-for-a-pds) |
| Disabled source prevents pending/active job work | [TECH-637](https://linear.app/hypercerts/issue/TECH-637/persist-cross-service-recovery-receipts-for-admitted-pdss) and [TECH-596](https://linear.app/hypercerts/issue/TECH-596/backfill-record-collections-for-a-pds) |
| Adding a collection reconciles each enabled source | [TECH-596](https://linear.app/hypercerts/issue/TECH-596/backfill-record-collections-for-a-pds) |
| Policy/source changes cancel obsolete work; removal preserves archive | [TECH-596](https://linear.app/hypercerts/issue/TECH-596/backfill-record-collections-for-a-pds) |
| Crash, retry, receipt and checkpoint handling | [TECH-596](https://linear.app/hypercerts/issue/TECH-596/backfill-record-collections-for-a-pds) |
| Complete/incomplete/failed coverage wording | [TECH-597](https://linear.app/hypercerts/issue/TECH-597/expose-jetstream-backfill-progress-and-coverage) |

[TECH-597](https://linear.app/hypercerts/issue/TECH-597/expose-jetstream-backfill-progress-and-coverage)
also owns the compatibility test for reading older persisted job JSON while no
longer emitting `historyComplete`. [TECH-635](https://linear.app/hypercerts/issue/TECH-635/validate-rainbow-raw-stream-recovery-and-retention)
owns complete-topology recovery evidence.
