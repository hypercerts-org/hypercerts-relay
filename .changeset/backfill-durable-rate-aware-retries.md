---
'hypercerts-relay': minor
---

Jetstream now durably schedules direct-PDS repository retries, honors `Retry-After` and `RateLimit-Reset`, and applies persisted origin-wide cooldowns after 429 responses. Deferred work yields to other PDS sources. Restart and explicit Retry preserve archive-backed progress and the retry budget rules; canceled jobs stay canceled. No configuration change is required; unresolved coverage still requires explicit job Retry.
