# Initial launch topology contract

## Selected topology

Initial launch uses one active Hypercerts Relay, Jetstream v2, and the
administration application. Consumers of retained Hypercerts collections use
Jetstream archive, replay, and live interfaces directly.

Rainbow is **not** part of the initial launch topology. There are no selected
raw-stream consumers and no Rainbow public endpoint, retention guarantee, or
recovery claim in that launch. Its presence in this repository and its
container build are not authorization to run it.

Relay remains the complete raw `com.atproto.sync.subscribeRepos` source;
Jetstream remains the selected-record archive and current-state recovery owner.
Neither component makes a Rainbow replay claim on its behalf.

## Deferred Rainbow activation

[TECH-635](https://linear.app/hypercerts/issue/TECH-635/validate-rainbow-raw-stream-recovery-and-retention)
is backlog work, not a launch acceptance selector. Activating Rainbow later
requires all of the following before it is exposed to a consumer:

1. A named raw-stream consumer and its required cursor, gap, and slow-consumer
   behavior are approved.
2. Rainbow persistence failures are proven to stop cursor acknowledgement.
3. Replay before retention, future cursors, and upstream info/error frames have
   an explicit consumer-visible gap or repair outcome; a lower-bound replay is
   never described as gap-free history.
4. An owned fixture proves the selected raw routes, `#sync` behavior, restart,
   retention boundary, and consumer recovery against the actual deployed path.

Until those checks pass, `T15-rainbow` and `T16` are intentionally absent from
the local acceptance runner. Unknown selectors fail; their absence is not a
passing or skipped recovery result.

## Initial operational direction

The intended production shape is single-active Relay with PostgreSQL on Railway
and database-level snapshots. This is a deployment direction, not evidence that
the repository has provisioned or qualified Railway resources. Capacity,
resource, lag, and recovery targets will be measured after the service has real
production workload and then refined.

Database snapshots must be scoped to the actual durable state they protect.
They do not by themselves prove recovery of Jetstream archive data or its cursor
positions. Before a production recovery claim, the selected snapshot and restore
procedure must preserve the crawled/archive data and the corresponding durable
positions together, and its result must be recorded by the upgrade and restore
qualification work.
