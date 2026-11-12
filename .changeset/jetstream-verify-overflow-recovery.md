---
'hypercerts-relay': minor
---

Jetstream now schedules repository-scoped current-state repair after
verify-queue loss. The live consumer caps each account's pending verification
queue, and an account that outruns it has the oldest queued events discarded
while the cursor advances past them. A verify-queue overflow now books a durable
recovery job covering exactly the affected DIDs, so only those repositories are
re-acquired rather than the whole PDS. The repair restores each repository's
current records; it does not replay the discarded events, so a record version
that was later updated or deleted is not recovered. Observations that have not yet been
booked are held in memory and can still be lost on process exit or when the
pending queue is full; check recovery warnings.

Recovery jobs are visible through the control API and the administration job
request, where `repository_recovery` accepts an explicit DID scope. They carry
no whole-source evidence, so they never appear as a source's current-state
coverage. Existing archive rows are not rewritten, and existing job identifiers
are unchanged.

For operators, unavailable latest revisions now leave only the affected DIDs
undiscovered while healthy repositories reconcile. Discovery failures and partial
progress survive restart without reporting complete recovery. Retry an incomplete
job after restoring PDS availability; valid inventory and completed work are
preserved, and automatic repository retries continue to respect cooldowns.
