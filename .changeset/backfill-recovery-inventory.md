---
'hypercerts-relay': minor
---

Freeze each direct-PDS backfill job's validated repository inventory before
reconciliation, so progress totals describe one durable current-state snapshot.
Lifecycle and quota recovery intents now carry the Relay source revision into
Jetstream jobs; a completed matching job durably retries its private Relay
recovery receipt after restart. Job and administration responses now report
only current-state coverage and no longer emit `historyComplete`.
