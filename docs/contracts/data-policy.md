# Data policy contract

## Scope and owner

Jetstream owns durable selected-record materialization. Relay remains a complete
raw `com.atproto.sync.subscribeRepos` source; Rainbow, if selected by D06,
remains a raw fan-out service. Neither is a selected-record archive.

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

Relay raw replay uses its existing 72-hour window. Rainbow, if deployed, uses
its existing 72-hour persistence window. This is a time-only retention policy:
no byte cap is selected, and Rainbow's existing `persist-bytes=0` behavior is
not changed. Capacity planning belongs to D07.

## Deferred evidence

Plan 005 owns T04, T07 and T15-selection. It must inspect archive, replay and
permitted temporary storage with excluded fixture values; selected-only durable
storage does not mean raw Relay frames or in-memory CAR input are filtered.
Plan 009 owns any Rainbow raw-retention and recovery evidence.
