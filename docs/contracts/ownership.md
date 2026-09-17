# Service ownership contract

## Durable state owners

| Owner | Durable responsibility | Does not own |
| --- | --- | --- |
| Relay | PDS admission and desired source lifecycle, source cursors, raw replay, verification rejections and raw-frame rate policy | Selected-record archive, collection backfill or browser authorization |
| Rainbow | Deferred raw-stream connection pooling, fan-out and raw replay; it is not in the initial launch topology | Selected-record retention, PDS backfill or Jetstream consumer archive |
| Jetstream v2 | Global collection policy, selected archive/replay, direct-PDS current-state jobs, checkpoints and current-state coverage | Relay admission state, raw Relay aggregation or browser OAuth |
| Administration | OAuth/session state, DID membership, operation journal and audit history | Owning Relay or Jetstream source/job/archive state |

Relay publishes raw `com.atproto.sync.subscribeRepos` events. Rainbow is held
from the initial launch; if it is later validated and deployed, it remains on
that raw path. Jetstream v2 owns archive, backfill and replay for launch
consumers; it is not replaced by Relay or Rainbow. The selected launch topology
and Rainbow activation gates are in [launch-topology.md](launch-topology.md).

## Control boundaries

Administration calls private bearer-token control interfaces to request owner
operations. Relay exposes `/hypercerts/v1` source, quota and rate-policy
operations. Jetstream exposes `/hypercerts/v1` policy, source, job and coverage
operations. Both interfaces require private transport and are not browser-facing
OAuth surfaces.

An operation journal records intent and observed result across those interfaces,
but does not make administration the source of truth for their state. A service
restart or unavailable owner produces an explicit incomplete result rather than
an assumed successful mutation.

## Cross-service boundaries

A Relay source remains recovery-required until a matching successful Jetstream
job crosses the durable receipt boundary defined by [TECH-637](https://linear.app/hypercerts/issue/TECH-637/persist-cross-service-recovery-receipts-for-admitted-pdss).
The receipt is keyed by admitted PDS, Relay source revision, Jetstream policy
revision, job ID and durable completion boundary, so a lifecycle change cannot
be acknowledged by earlier work.
Removing a PDS stops future acquisition and dependent work; it does not purge
already retained Jetstream archive data. An admitted migration target requires an explicit
Jetstream current-state job and does not restore unavailable history.

## Deferred evidence

[TECH-637](https://linear.app/hypercerts/issue/TECH-637/persist-cross-service-recovery-receipts-for-admitted-pdss)
owns the lifecycle recovery receipt. [TECH-596](https://linear.app/hypercerts/issue/TECH-596/backfill-record-collections-for-a-pds)
owns job cancellation/onboarding recovery and the stable-inventory contract.
[TECH-635](https://linear.app/hypercerts/issue/TECH-635/validate-rainbow-raw-stream-recovery-and-retention)
owns the deferred Rainbow persistence/recovery activation evidence, and
[TECH-636](https://linear.app/hypercerts/issue/TECH-636/qualify-hypercerts-relay-upgrade-and-restore)
owns upgrade/restore qualification. No deferred selector is passing merely
because its service owner exists.
