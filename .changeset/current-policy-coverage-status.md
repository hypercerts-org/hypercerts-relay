---
'hypercerts-relay': minor
---

Expose a versioned current-policy coverage snapshot for every enabled PDS and an aggregate scoped to all enabled sources. Separate successful matching and no-match scans, unresolved inventory and attributable records, with unknown counts retained for older jobs. Historical coverage and live freshness remain independent and explicitly unknown at this private interface.

Private API consumers should use schema version 1 acquisition evidence and the aggregate instead of choosing the newest job or treating job-wide progress as collection attribution. Existing archived records and job history are preserved; this change does not deploy services or recover historical data.
