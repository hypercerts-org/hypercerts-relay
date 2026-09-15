---
'hypercerts-relay': patch
---

Update the administration UI to use operator-facing labels for PDS management, coverage, jobs, rate limits, audit logging, and administrator access. PDS origin inputs now assume HTTPS for bare hostnames, automatic polling is removed, coverage is grouped by PDS, audit history and requested changes are combined, and administrator rows show the last recorded login time. Backfill jobs now persist the active repository total before progress begins so in-progress status can report processed repositories out of that total.
