# Upgrade and restore qualification

`./tests/upgrade/t19 T19` is the disposable copied-state qualification for the
initial-launch forward-upgrade contract. It is deliberately separate from the
two-PDS live acceptance fixture: this selector creates stopped on-disk state,
copies it, and opens each copy through the selected candidate revision.

The default predecessor is `58d0d26b` (Plan 007's rate-policy-capable state)
and the default candidate is `2d8b9535` (the selected Plan 009 launch topology
revision). Both are full commit IDs in the retained JSON evidence. To qualify a
later release candidate, supply immutable revisions that retain a forward
ancestry relationship:

```sh
T19_PREDECESSOR_REVISION=<released-commit> \
T19_CANDIDATE_REVISION=<candidate-commit> \
./tests/upgrade/t19 T19
```

The predecessor creates real Relay SQLite state and real Jetstream Pebble plus
sealed-segment state. Only after both owners close does the selector copy the
entire bundle. The candidate must open the original forward-upgrade copy and a
separate restored copy. It asserts:

- Relay durable raw crawl event, admitted-source revision, desired state,
  durable source cursor, recovery receipt, global rate policy, and PDS rate
  policy;
- Jetstream exact selected collection policy, durable job/source-revision
  coordinate, persisted upstream cursor, and readable selected archive event.

This is **forward upgrade and restore-only** evidence. Binary rollback after a
state migration is not supported. Production recovery must restore a consistent
snapshot containing crawled/archive data and its corresponding durable cursor
positions; restoring only a future Railway PostgreSQL database is insufficient
to make that claim.

The test has no Railway login, provisioning, image publication, public network,
or long-running service. Its local SQLite and filesystem state are test-only;
the intended Railway PostgreSQL/database-snapshot direction must be exercised
later against its real state owners. Likewise, T17 capacity/lag and T20
blue/green overlap are runbook gates, not passing local selector aliases: no
production workload, capacity budget, or deployed Railway environment is
available here.
