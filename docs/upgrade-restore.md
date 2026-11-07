# Forward upgrade and restore runbook

This runbook qualifies the initial launch contract: **forward upgrades only**.
After a state migration, binary rollback is not supported; recover with a
consistent restore. The data that must be retained is the crawled/Jetstream
archive and its aligned durable cursor positions, Relay raw crawl and source
state, and the authorized administration database state. A valid restore keeps
those owners together so source admission can be verified before it resumes.

## Before a release candidate

1. Select the exact released predecessor and immutable candidate revisions.
2. Run the disposable copied-state qualification:

   ```sh
   T19_PREDECESSOR_REVISION=<released-commit> \
   T19_CANDIDATE_REVISION=<candidate-commit> \
   ./tests/upgrade/t19 T19
   ```

3. Retain the printed `tests/acceptance/results/T19.*` evidence directory with
   the release review. It records only revisions and bounded assertion
   coordinates, never credentials or archive payloads.
4. Confirm the candidate's migration/release notes state whether a compatible
   restore is required. Do not call an image build or a schema migration a
   successful upgrade without T19 evidence.

## Blue/green forward upgrade

The intended production approach is blue/green: start the candidate alongside
the old version, validate its health and restore assumptions, then ensure only
one Relay is active for source admission before stopping the old version. The
single-active Relay rate-admission lease is deliberately not a distributed
active/active protocol.

Before cutover, take a consistent snapshot that covers the actual owners of:

- Jetstream archive segments and Pebble metadata, including the durable Relay
  cursor and backfill/job coordinates;
- Relay raw crawl store, persisted source policy, and durable source cursor;
- the relevant Railway PostgreSQL state once that deployment is authorized.

Do not run old and new Jetstream writers against the same state directory, and
do not describe a PostgreSQL-only snapshot as a complete Jetstream restore.
Stop the old active Relay before allowing the candidate to acquire rate
admission and begin source processing. Record the selected revision, candidate
revision, snapshot identifier, owner paths/volumes, and cutover result.

## Restore after a failed migration or cutover

1. Stop the candidate writers and Relay source processing.
2. Restore the complete, matching snapshot of archive data, Jetstream metadata
   (including cursor/job state), Relay state, and authorized database state.
3. Start only the selected forward-compatible candidate; do not attempt a
   binary downgrade against migrated state.
4. Verify the restored archive and durable cursor/job/source coordinates using
   the same bounded checks represented by T19 before resuming source admission.
5. Record the observed recovery time and remaining gap/current-state limits.

## Upstream continuity loss with selected collections

When collection selection is enabled, a known upstream discontinuity is logged
at error level and recorded durably in the Jetstream metadata store at
`hypercerts/selected/upstream-loss/v1`, then skipped so ingestion continues.
This covers an upstream `OutdatedCursor` notice and a relay sequence gap. It
does not fail `/readyz`, close subscriptions, or block restart; the recorded loss
is logged again on the next start so it stays visible until addressed.

Nothing clears the record automatically, because the archive cannot reconstruct
events that were never acquired and a PDS current-state resync is not a
substitute for the missing raw history. Treat any recorded loss as a real
history gap for that interval and decide explicitly how to proceed:

1. When the upstream relay can still replay the required history, restore a
   coherent snapshot of the archive and Jetstream metadata into a separate data
   directory and reconcile from the truncated boundary.
2. Otherwise accept the gap, provision a new data directory and stream identity
   if a clean history claim is required, and retain the failed directory for
   investigation.

Consumers must not infer completeness for the skipped interval, and operators
should record which option was chosen. If writing the durable record fails,
ingestion still continues but the evidence is only in the logs — preserve those
logs, because the marker is the durable signal.

## Explicitly unqualified work

No real Railway resource, PostgreSQL snapshot, image publication, consumer, or
production workload is touched by this repository runbook. Capacity, lag,
storage headroom, RPO, RTO, and live blue/green timing are post-launch
measurements. Rainbow is deferred from the initial launch, so it is neither a
T19 state owner nor a recovery claim.
