# Session handoff

## Current delivery state

- Branch: `plan/003-jetstream-verification`
- Working tree: clean at handoff
- Ready-for-review PR: [#25 — TECH-634: verify Jetstream acquisition paths](https://github.com/hypercerts-org/hypercerts-relay/pull/25)
- Base: `main`
- Head commit: `c6257a15 test: add verified direct PDS acceptance coverage`

PR #24 (contracts and Linear traceability) is merged into `main`.

## Delivered in TECH-634

- Verification for normal bootstrap, selected bootstrap, retry, and direct-PDS acquisition paths.
- Selected, retry, and direct-PDS paths use bounded DID refresh/reverification after a signature failure.
- Direct-PDS snapshots use complete-CAR loading and classify unavailable input as replayable/incomplete.
- Direct-PDS permanent rejections are durable, bounded, non-payload records written before source progress.
- Private authenticated rejection inspection: `GET /hypercerts/v1/snapshot-rejections`.
- Real-process `T15-core` acceptance coverage using disposable PDS/PLC fixtures and an internal pass-through fault proxy.

## Verified evidence

Run on the committed tree:

```sh
cd jetstream && go test ./...
./tests/acceptance/run T15-core
git diff --check
```

All passed.

`T15-core` proves:

- valid direct-PDS selected-record ingestion;
- PLC identity and PDS `listRepos` outages remain incomplete and recover through Jetstream restart plus same-job retry;
- a resolver-valid DID document with a real but wrong signing key produces a durable `verification_failed` rejection and no archive materialization.

T15 artifacts retain only sanitized coordinates, counters, fault-hit evidence, and an executed-tree digest. Raw archive output used during assertions is deleted.

## Known limitation

Atmos v0.3.6 normal bootstrap verification does not expose a hook to purge and refresh a stale DID cache after a signature failure. Do not claim normal-engine key-rotation retry parity. Selected, retry, and direct-PDS paths have bounded refresh/reverification. Resolving normal-engine parity requires an upstream Atmos hook or dependency update and should remain tracked under [TECH-634](https://linear.app/hypercerts/issue/TECH-634).

## Next delivery order

1. Review and merge PR #25 through the normal human review process. Do not merge it directly.
2. Create the next stacked branch above `plan/003-jetstream-verification` for [TECH-637](https://linear.app/hypercerts/issue/TECH-637): durable cross-service recovery receipts.
3. Continue the dependency chain for [TECH-595](https://linear.app/hypercerts/issue/TECH-595), [TECH-596](https://linear.app/hypercerts/issue/TECH-596), [TECH-602](https://linear.app/hypercerts/issue/TECH-602), and [TECH-588](https://linear.app/hypercerts/issue/TECH-588), creating one focused stacked PR per work item.
4. Do not begin [TECH-635](https://linear.app/hypercerts/issue/TECH-635) until D06 and D07 are decided. Do not begin [TECH-636](https://linear.app/hypercerts/issue/TECH-636) until D06, D07, and D10 are decided.

## Operational boundaries

- Indigo Relay publishes complete raw `subscribeRepos` events.
- Rainbow, if deployed, remains a raw-stream fan-out layer.
- Jetstream v2 owns selected-collection retention, PDS backfill, archive, and replay.
- The acceptance fixture is disposable and gated; it is not a deployment recipe.
- No production deployment, release, source admission, rate-limit change, or state rewrite was performed.
