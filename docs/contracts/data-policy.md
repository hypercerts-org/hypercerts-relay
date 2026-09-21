# Data policy contract

## Scope and owner

Jetstream owns durable selected-record materialization. Relay remains a complete
raw `com.atproto.sync.subscribeRepos` source. Rainbow is deferred from the
initial launch and, if later activated, remains a raw fan-out service. Neither is
a selected-record archive.

Consumers use Jetstream v2 archive, replay and subscription interfaces.

## Selected collection policy

The initial policy is global for every admitted PDS and contains these exact
NSIDs:

```text
app.certified.actor.organization
app.certified.actor.profile
app.certified.badge.award
app.certified.badge.definition
app.certified.badge.response
app.certified.graph.entityFollow
app.certified.graph.follow
app.certified.link.evm
app.certified.location
app.certified.signature.proof
org.hypercerts.claim.activity
org.hypercerts.claim.contribution
org.hypercerts.claim.contributorInformation
org.hypercerts.claim.rights
org.hypercerts.collection
org.hypercerts.context.acknowledgement
org.hypercerts.context.attachment
org.hypercerts.context.evaluation
org.hypercerts.context.measurement
org.hypercerts.funding.receipt
org.hypercerts.workscope.tag
```

`jetstream/internal/hypercerts/selection.DefaultCollections` is generated from
the approved Hypercerts lexicon revision and supplies this list to a new data
directory. The persisted policy is authoritative after initialization; a global
policy change is an explicit revision, not a restart-time replacement.

Collection matching is exact NSID matching. Prefixes are forbidden. Selection
validates NSID syntax, not record schemas. An empty policy retains no record
operations, but preserves protocol markers and source progress.

The private Jetstream interface is `GET /hypercerts/v1/policy` and
`PUT /hypercerts/v1/policy`. A write supplies the expected policy revision and
an exact collection list.

## Storage boundary

Jetstream archive, replay and indexes retain selected record collections only.
A direct-PDS full CAR may be read in process memory to recover selected current
state, but unrelated payloads must not be written to temporary files, queues,
logs, audit records, metrics, crash dumps or diagnostics. The current 64 MiB
per-repository input limit and two-minute repository operation deadline are
implementation limits; a limit breach must report non-payload incomplete
coverage rather than claim completion.

Relay raw replay uses its existing 72-hour window. Rainbow is not deployed at
initial launch; any later activation would use its existing 72-hour persistence
window unless that deferred contract changes. This is a time-only retention
policy: no byte cap is selected, and Rainbow's existing `persist-bytes=0`
behavior is not changed. Capacity planning will be measured and refined from
production workload rather than treated as a launch acceptance result.

### Persistence producer inventory

The selected-record boundary is enforced before each Jetstream writer below.
The inventory deliberately calls out stores that are allowed to carry raw or
transient payloads so an archive-only assertion is not overstated.

| Producer | Durable output | Selection rule | Allowed non-selected payload |
| --- | --- | --- | --- |
| Relay event manager | Raw subscribeRepos replay | Not a Jetstream archive; Relay remains complete | Raw frames for its documented 72-hour window |
| Rainbow event store, deferred from initial launch | Raw Relay fan-out replay if later activated | Not a Jetstream archive | Raw frames for its documented 72-hour window |
| `internal/ingest/backfill` bootstrap and retry | Jetstream segments and source progress | Exact policy before append or readable-log write | Direct-PDS CAR is process memory only; no CAR temp file is permitted |
| `internal/ingest/live` | Jetstream live segments, archive cursor and progress | Exact policy before append | Upstream frame is transient process memory only |
| `internal/ingest` snapshot reconciliation | Selected record replacements/deletes and protocol markers | Exact policy and per-collection reconciliation | Snapshot input is process memory only |
| Jetstream policy/job/control stores | Policy, job, cursor and bounded non-payload rejection metadata | They never materialize record payloads | No record payload is permitted |

`T04`, `T07`, and `T15-selection` exercise this inventory through
`./tests/acceptance/run`. They prove selected archive/replay data, empty-policy
progress and markers, selected deletes, restart durability, and absence of the
excluded direct-PDS fixture value from every durable file the writer creates.

## Deferred evidence

[TECH-595](https://linear.app/hypercerts/issue/TECH-595/store-only-enabled-record-collections-in-jetstream-v2)
owns selected-storage evidence. It must inspect archive, replay and permitted
temporary storage with excluded fixture values; selected-only durable storage
does not mean raw Relay frames or in-memory CAR input are filtered.
[TECH-635](https://linear.app/hypercerts/issue/TECH-635/validate-rainbow-raw-stream-recovery-and-retention)
owns any Rainbow raw-retention and recovery evidence.
