# Selected Stream Contract v1

**Status:** implementation contract for the staged Hypercerts Jetstream work. This document specifies consumer-visible behavior; it does not claim these additions are already served. Existing upstream-compatible behavior remains in force until the relevant stage is deployed. A stage MUST NOT advertise this contract until all fields and guarantees mandatory for that stage are available and durable.

## 1. Scope and ownership

A selected stream is a Jetstream-owned, durable view of chosen record collections. It is not a filtered Relay or Rainbow stream. Relay continues to publish the complete admitted `com.atproto.sync.subscribeRepos` event stream, and Rainbow continues to proxy/fan out that raw stream. Jetstream applies selection when materializing its own stream. Raw consumers and selected-stream consumers therefore have different completeness claims.

The Jetstream data directory owns selected-stream metadata, policy history, sequence assignment, archive/replay state, and cursors. These records must share durable commit boundaries: a delivered selected event's sequence, stream identity, policy revision, and provenance must survive restart consistently with the event. Never infer history which was not observed or durably retained.

This contract applies to opted-in `network.bsky.jetstream.subscribeEvents` clients. Existing clients that omit the contract selector keep upstream semantics. No change to the Relay or Rainbow wire contract is implied.

## 2. Opt-in, request, and compatibility

A client opts in by supplying `contract=hypercerts-selected-v1` to the existing `network.bsky.jetstream.subscribeEvents` XRPC endpoint. The server MUST validate the selector before accepting the subscription. An absent selector MUST retain current upstream behavior. An unknown or unsupported selector MUST fail before streaming with the stable error `UnsupportedContract`; the server MUST NOT silently downgrade an opted-in client to upstream semantics.

For selected requests, `streamId` and `cursor` MUST be supplied together or both omitted. `streamId` is an opaque string. `cursor` is an integer selected-stream sequence and is an inclusive resume position: the event at that sequence may be replayed, and the client deduplicates it. Supplying only one member of the pair fails before streaming with `InvalidCursor`. A supplied stream identity that differs from the current stream fails with `StreamMismatch`; a cursor beyond the current sequence fails with `CursorOutOfRange`. If replay is disabled or the inclusive cursor precedes available replay, fail with `ReplayUnavailable`. Never silently begin at the tail.

The wire contract remains the existing typed `xrpc.v1.json` payload framed by its time envelope. A selected record is the existing `#commit` payload (including `$type` and `time`); selected metadata is added inside that typed payload, not as a new outer envelope or replacement event type. In abbreviated JSON form:

```json
{
  "$type": "com.atproto.sync.subscribeRepos#commit",
  "time": "2026-01-01T00:00:00Z",
  "seq": 123,
  "hypercerts": {
    "streamId": "opaque-stream-id",
    "policyRev": 12,
    "provenance": "live"
  }
}
```

All ordinary upstream fields and typed-payload behavior remain unchanged; the example omits the commit's other standard fields.

`streamId` is a non-empty opaque string stable across restart and changes only when the stream is deliberately reinitialized as a distinct stream. `policyRev` is a non-negative integer revision; `null` is reserved for legacy rows whose revision is unknown. `provenance` is one of the exact strings `live`, `snapshot`, `reconciliation`, or `legacy-unknown`. Live means observed live raw input; snapshot and reconciliation mean state materialized from those processes and MUST NOT imply historical raw-event verification. Legacy rows created before these fields were tracked use `policyRev: null` and `provenance: "legacy-unknown"`, never the current revision or fabricated raw attribution.

Every newly sequenced selected event MUST carry this metadata. If the implementation cannot supply the mandatory fields truthfully, it MUST fail the selected contract negotiation rather than emit a partial or misleading contract.

## 3. Selection and policy revisions

The selection policy is an exact set of collection NSIDs. A record event is selected only if its collection is enabled under the effective policy at that event's ordered position. A policy revision is immutable once committed. Persist its contents and adoption boundary before reporting adoption successful.

Policy adoption MUST be ordered with event ingestion and durably record `effectiveAfterSeq`, an integer sequence boundary. The new policy applies only to events with `seq > effectiveAfterSeq`, until the next revision's boundary. An adoption that occurs before the first event has `effectiveAfterSeq: null` and MUST be emitted as an unsequenced `info` frame before the first event governed by that revision. The frame uses the existing Jetstream info envelope (`kind: "info"`, `name`, optional `message`) in the existing `xrpc.v1.json` message framing. Keep the established `info.name: "PolicyAdopted"`; there is no new `initialPolicyAdopted` name. Selected-contract fields belong inside the typed info payload under `hypercerts`, for example:

```json
{
  "kind": "info",
  "name": "PolicyAdopted",
  "message": "Selected collection policy adopted",
  "hypercerts": {
    "contract": "hypercerts-selected-v1",
    "streamId": "opaque-stream-id",
    "policyRevision": 12,
    "collections": ["app.example.record"],
    "effectiveAfterSeq": null
  }
}
```

`policyRevision` is a non-negative integer and `effectiveAfterSeq` is the integer last sequence governed by the prior policy, or `null` before the first sequence. This frame has no `seq` and does not advance the cursor. Emit and retain historical policy boundaries on replay: for a resumed historical stream, emit the applicable `PolicyAdopted` frame before its governed data, and emit each subsequent policy boundary before data governed by that revision. This also applies when a revision changes only collections or is otherwise represented without a prior sequenced event. A crash during adoption MUST recover either the previous committed policy or the new committed policy and exact boundary; it MUST NOT expose a revision without a recoverable boundary or mix revisions at one boundary.

Removing a collection stops admitting future events for it. It does not delete previously retained history, manufacture delete events, or rewrite old event metadata. Re-adding a collection starts future admission under its new revision; it creates a gap for the time it was disabled. The service MUST NOT imply that this gap was filled. Current-state reconciliation may be separately initiated and labeled as reconciliation; it is not a reconstruction of missed event history.

No automatic archive purge may be triggered by policy removal or replacement. Retention is independently configured and, for this target, original selected-event retention is indefinite. Disk exhaustion MUST surface as an explicit ingestion/health failure and incomplete-history state. It MUST NOT silently trim, drop, renumber, or claim completion over missing data.

## 4. Event kinds and lifecycle semantics

Selected record create, update, and delete events follow the selected collection policy. Preserve lifecycle markers needed to maintain the consumer's selected-DID view: include an account or identity marker when the DID is currently or previously selected, and retain source-known lifecycle markers in the selected stream where necessary to preserve existing lifecycle behavior. A marker does not authorize excluded collection payloads to leak into the stream.

An identity refresh updates identity information; it MUST NOT purge the consumer's account state. An account-inactive marker hides the account from the current active view but MUST retain its prior history. A sync marker affects only the DID's selected scope and MUST state why reconciliation is needed. Selected subscriptions carry the sync event's DID, witnessed time, and revision, but MUST omit upstream CAR blocks and operation lists so excluded record payloads cannot cross the selected boundary. A sequenced sync marker carries `hypercerts.reconciliationRequired: true` and `hypercerts.scope` with the DID and exact collections admitted by the policy governing that marker; these fields are selected-contract metadata, not additions to ordinary upstream v2 events. It MUST NOT be presented as proof of a complete sync or trigger fabricated completion. Identity refresh remains a lifecycle update and MUST NOT purge account state. Consumers MUST NOT interpret lifecycle markers as permission to purge unrelated or unselected collections. Policy change is not a delete and MUST NOT synthesize per-record deletes.

Snapshot reconciliation is distinct from observed stream delivery. Snapshot-derived events MUST carry snapshot or reconciliation provenance and identify their reconciliation boundary. For a snapshot event the boundary is in `hypercerts.reconciliationBoundary`; it is a typed value whose JSON representation is either a non-negative selected-stream sequence integer or an object containing `seq` (non-negative integer or `null`) and/or `snapshotId` (non-empty string). The object MUST contain at least one of those fields. This reference identifies the source/durable reconciliation boundary; it is not itself a stream cursor or a substitute for the event's sequence. Arrival of an older snapshot after a newer live event MUST NOT roll back newer state. The implementation must fence snapshot publication against later live writes (or otherwise compare durable ordering) and preserve this rule across crashes.

## 5. Cursor, replay, errors, and gaps

A durable consumer cursor is the pair `(streamId, seq)`. It identifies the last successfully processed selected sequence; consumers should persist it atomically with their effects and process idempotently. Event sequence numbers are monotonic within a stream identity. Reconnect/replay from a cursor is inclusive; consumers deduplicate the boundary event. The service MUST NOT silently jump a cursor.

Archive range semantics are explicit: `after` is exclusive and `before` is inclusive. Live handoff MUST pin a sealed archive tip, replay through that tip, and continue from the matching live boundary using inclusive delivery plus deduplication. The existing archive planning/download APIs should be reused where their contract permits; selected replay must preserve stream identity and selection metadata. Archive authentication authorizes archive access only; it MUST NOT waive cursor validation, conceal an archive gap, or grant a completeness claim.

The stable selected-contract error names are `UnsupportedContract`, `InvalidCursor`, `StreamMismatch`, `CursorOutOfRange`, and `ReplayUnavailable` as defined above. Each is a terminal pre-stream error; none means successful resume. Timestamp-based old-cursor clamping such as upstream `OutdatedCursor` is not translated into successful selected-stream resume. Upstream warning/error frames MUST NOT be relabeled as selected contract errors or treated as successful replay.

Where history is discontinuous or unavailable, expose a `gaps` array. Each gap is an object with `fromSeq` and `throughSeq` (inclusive nullable integers) and a non-empty `reason` string; `collection` is a collection NSID or `null` when the gap applies to all selected collections. Null sequence bounds mean the exact boundary is unknown, not that no gap exists. Example:

```json
"gaps": [
  {"fromSeq": 401, "throughSeq": 428, "reason": "collection-policy-disabled", "collection": "app.example.record"},
  {"fromSeq": null, "throughSeq": 399, "reason": "legacy-coverage-unknown", "collection": null}
]
```

Implementations MUST NOT omit a known gap, represent a known gap with an empty array, or convert unknown boundaries into fabricated exact values.

## 6. First-frame capability and history disclosure

The first selected stream frame is `info` and MUST precede data. Its mandatory fields are:

```json
{
  "kind": "info",
  "name": "SelectedFeed",
  "message": "Selected stream capability",
  "hypercerts": {
    "contract": "hypercerts-selected-v1",
    "streamId": "opaque-stream-id",
    "policyRev": 12,
    "effectiveAfterSeq": 400,
    "collections": ["app.example.record"],
    "replayFloor": 350,
    "historyMode": "materialized",
    "originalHistoryFromSeq": null,
    "coverage": "unknown",
    "gaps": []
  }
}
```

Field types and semantics:

- `kind` and `name`: existing `xrpc.v1.json` info envelope fields (`info` and `SelectedFeed`); `hypercerts` carries the selected contract fields inside the typed info payload.
- `contract`: the negotiated contract identifier.
- `streamId`: current persistent stream identity.
- `policyRev`: current non-negative integer revision.
- `effectiveAfterSeq`: integer boundary for this revision, or `null` when adopted before any sequence; the separate `unsequencedPolicyAdopted` notice MUST precede its first event.
- `collections`: exact collection NSID array for the current policy revision.
- `replayFloor`: earliest selected sequence currently available for replay, or `null` if none is available.
- `historyMode`: `materialized` or `originalEvents`. Use `originalEvents` only when durable original-event capture can be substantiated from an explicit boundary.
- `originalHistoryFromSeq`: integer first sequence of substantiated original-event capture, otherwise `null`.
- `coverage`: `unknown` unless coverage can be substantiated; `complete` is permitted only with evidence for the declared scope and boundary.
- `gaps`: array of structured gap objects as specified above; `[]` means no known gaps, not proof of completeness.

At initial history materialization, the minimum truthful disclosure is `historyMode: "materialized"`, `originalHistoryFromSeq: null`, `coverage: "unknown"`, and `gaps: []` only when there are no known gaps (otherwise report them). These values MUST NOT be replaced with `false`/empty claims about historical completeness. A richer historical claim is not required for TECH670; claims that are made must be supported. `collections` describes current selection, not historical capture. If any mandatory field is unavailable, do not advertise/accept the selected contract.

## 7. Reconciliation scope and completion

Reconciliation status is an advisory, unsequenced `info` frame using the existing Jetstream info envelope (`kind: "info"`, `name`, optional `message`). A reconciliation notice uses `name: "Reconciliation"` and carries its typed contract fields under `hypercerts`, for example:

```json
{
  "kind": "info",
  "name": "Reconciliation",
  "message": "Reconciliation progress update",
  "hypercerts": {
    "scope": {"did": "did:plc:example", "collections": ["app.example.record"]},
    "status": "in_progress",
    "jobId": "reconcile-job-opaque",
    "sourceRevision": "source-boundary",
    "policyRevision": 12,
    "durableBoundary": {"seq": 428, "snapshotId": "source-boundary"}
  }
}
```

`scope` identifies the affected DID and/or collection set. `status` is one of `in_progress`, `completed`, or `incomplete`. `jobId` is a non-empty durable job identifier when the job is asynchronous or spans restart; it may be absent for a synchronous notice. `sourceRevision` is a non-empty source snapshot/version identifier when available, otherwise `null`. `policyRevision` is the non-negative policy revision used. `durableBoundary` is the durable sequence and/or source snapshot boundary it reconciles against, represented by `seq` (non-negative integer or `null`) and/or `snapshotId` (non-empty string); at least one MUST be present. This info frame has no `seq`, does not advance the cursor, and is advisory status—not a cursor, proof of completion, or modification of ordinary v2 events. `completed` is valid only when the durable job establishes completion for the stated scope and boundary. A sync marker, empty result, job start, or unsequenced completion message is not proof of complete reconciliation. If input is unavailable or the job cannot establish coverage, use `incomplete`; never fabricate completion. Reconciliation does not fill gaps in original event history.

## 8. Retention and honest completeness

Indefinite original-event retention begins only at an explicit, persisted capture boundary established by TECH669. TECH668's wire metadata work MUST NOT claim original-event capture or historical completeness. Before that boundary, the service reports only materialized history and its actual replay floor/boundaries. It cannot reconstruct unobserved events from snapshots, current state, or policy changes.

No maintenance process may silently purge selected history under this contract. Any future bounded-retention behavior requires a separately versioned operator/consumer contract that exposes its floor and loss boundary. Until then, inability to persist an event is an incomplete stream, not permission to advance the cursor.

## 9. Staged delivery and migration

- **TECH667 — documentation (this document):** establish the consumer contract; no operator runtime change or wire claim.
- **TECH668 — wire metadata:** negotiate the opt-in, emit mandatory selected metadata/info and lifecycle semantics, and preserve upstream-compatible behavior for clients without the selector. Do not claim indefinite original-event retention. The stage MUST NOT advertise the selected contract until every mandatory field described above is available.
- **TECH669 — original retention:** establish an explicit durable capture boundary and indefinite retention of original selected events, with recovery and disk-full behavior consistent with this contract.
- **TECH670 — historical boundaries:** expose defensible replay floors, legacy/unknown regions, structured gaps, and snapshot-vs-original history disclosures; never manufacture historical completeness. Richer historical completeness claims are optional, not a requirement to report the minimum truthful initial materialization state.

Stages are additive and must not retroactively upgrade old data's provenance. A deployment may advertise only capabilities it has implemented and durably verified. Consumer migration is opt-in: first continue existing unqualified subscriptions unchanged; then negotiate the named contract, validate the info frame and stream identity, store `(streamId, seq)`, and handle explicit replay/history errors. On stream identity change, do not reuse an old cursor; require explicit consumer reconciliation.

## 10. Acceptance scenarios

The implementation is accepted only when automated tests demonstrate all of the following:

1. **Compatibility and selection:** absent selector preserves existing upstream output; the supported selector yields only exact selected records with typed metadata; unsupported selectors fail with `UnsupportedContract` before data. The server does not advertise the contract if any mandatory info/event field is unavailable.
2. **Policy crash boundary:** crashes before, during, and after policy adoption recover one committed revision and its `effectiveAfterSeq`; no event has ambiguous policy attribution. First-event adoption sends `unsequencedPolicyAdopted` first; policy boundaries appear in replay.
3. **Removal and re-add:** removal halts future admission without purging retained rows or synthesizing deletes; re-add admits only from its new boundary and reports the intervening gap.
4. **Cursor validation and resume:** `streamId`/integer cursor pairing is enforced; inclusive cursor replay deduplicates correctly; wrong-stream, future, and replay-disabled/unavailable cursors produce their stable errors without tail-jump; old timestamp clamping is not reported as successful selected resume.
5. **Lifecycle scope:** current/previously selected and source-known lifecycle markers retain the defined selected-DID behavior; identity refresh does not purge, account inactive hides current state but retains history, and sync scope/reconciliation need is disclosed without fabricated completion or unselected payload leakage.
6. **Failed and stale snapshot:** failed reconciliation emits no false completion; durable scope, boundary, progress, and job reference are exposed as applicable; snapshot provenance is visible; an older snapshot cannot overwrite a newer live record, including after restart.
7. **Archive/live handoff:** archive replay pins the sealed tip and hands off without gaps or duplicates beyond the documented inclusive boundary; archive authorization does not bypass gap detection.
8. **Retention boundary:** original-event mode is not advertised before its explicit capture boundary; unobserved history is never reconstructed or claimed.
9. **Maintenance and disk full:** policy removal and routine maintenance do not purge history; disk-full/write failure leaves an explicit incomplete/error state and does not advance sequence/cursor as if successful.
10. **Metadata and history honesty:** legacy rows retain null/unknown attribution; snapshot/reconciliation state is distinguishable; every newly sequenced selected event has persistent `streamId`, integer `policyRev`, and valid provenance; initial materialization reports null original-history boundary and unknown coverage without asserting completeness.
11. **Gap schema:** gap bounds are nullable inclusive integers, reasons are populated, and collection scope is a NSID or null; known gaps are present in both initial info and replay disclosures where relevant.

## 11. Related operator documentation

See [Jetstream operation and selection policy](../../jetstream/README.md#hypercerts-policy-work) for current collection selection, durable policy, backfill, and the distinction between raw inputs and Jetstream materialization. See [the Jetstream package behavior](../../jetstream/doc.go) for the existing upstream client replay, cursor, and archive/live handoff semantics; those client semantics do not by themselves establish this selected-stream contract. The repository [agent guide](../../AGENTS.md#purpose-and-current-scope) describes Relay, Rainbow, and Jetstream ownership.
