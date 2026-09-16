# Job and coverage contract

## Scope and owner

Jetstream owns durable direct-PDS source state, jobs, policy-scoped
reconciliation, checkpoints and current-state coverage. Relay source admission
and lifecycle remain Relay-owned. A Plan 004 recovery receipt must bridge those
owners before Relay clears a source recovery requirement.

The private Jetstream interface provides:

- `GET`, `POST` and `DELETE /hypercerts/v1/sources`;
- `GET /hypercerts/v1/jobs`, `POST /hypercerts/v1/jobs`, and
  `GET /hypercerts/v1/jobs/{id}`;
- `POST /hypercerts/v1/jobs/{id}/cancel` and `/retry`; and
- `GET /hypercerts/v1/coverage`.

Job requests and retry/cancel actions use durable, separate receipt namespaces.
The same nonempty request receipt returns its prior job. Without a receipt,
repeated requests coalesce only applicable pending or running work; a later
unkeyed request after terminal work may create a new job.

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
does not communicate a useful operator choice. Plan 006 removes it from persisted
job state, Jetstream control responses, administration contracts/UI, tests and
documentation. Until then, no client may treat the field as historical proof.

## Acceptance mapping

| Action or terminal outcome | Required evidence owner |
| --- | --- |
| Quiet/new admitted PDS reaches selected current state | Plan 006: T02 |
| Disabled source prevents pending/active job work | Plans 004 and 006: T03-lifecycle and T03-jobs |
| Adding a collection reconciles each enabled source | Plan 006: T05 |
| Policy/source changes cancel obsolete work; removal preserves archive | Plan 006: T06 and T09 |
| Crash, retry, receipt and checkpoint handling | Plan 006: T08 |
| Complete/incomplete/failed coverage wording | Plan 006: T02, T05, T08 and T09 |

Plan 006 also owns the compatibility test for reading older persisted job JSON
while no longer emitting `historyComplete`. Plan 009 owns complete-topology
recovery evidence.
