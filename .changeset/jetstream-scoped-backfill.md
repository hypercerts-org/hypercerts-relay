---
'hypercerts-relay': minor
---

Add durable Jetstream jobs for explicitly configured PDS sources, collection policy
changes, backfill, and quota recovery. Initialize sources with the comma-separated
`JETSTREAM_PDS_SOURCES` setting on a new data directory. Jobs verify direct-PDS
snapshots, reconcile only selected collections, and resume persisted progress
after restart. Source removal cancels acquisition without purging archived rows.
Completion describes current state only; unavailable input remains explicitly
incomplete and historical completeness is never inferred from a snapshot.
