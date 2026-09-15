---
'hypercerts-relay': minor
---

Edit a PDS account quota from its detail view in administration. Requests are validated, durably audited and applied through the private Relay API. Stale quota edits are rejected, while retries of the same value are safe. Zero stops new account admission; reducing a quota does not remove existing accounts. Increasing it can reactivate quota-throttled accounts, with historical recovery tracked separately in backfill jobs.
