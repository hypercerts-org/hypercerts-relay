---
'hypercerts-relay': minor
---

Relay now records a versioned Jetstream recovery receipt before clearing an admitted source's recovery-required status. The private control caller must provide the exact source revision, collection-policy revision, job identifier, and durable completion boundary. Stale or disabled-source acknowledgements are rejected. No new operator configuration is required; source removal still stops acquisition without deleting retained archive data.
