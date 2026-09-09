---
'hypercerts-relay': minor
---

Add a separate OAuth administration application for source and collection policy,
backfill jobs, rate limits, scoped coverage and actor-attributed audit history.
Requested changes persist before service application and can be inspected or retried.

Operators can enable the private Relay API with RELAY_CONTROL_ADDR and
RELAY_CONTROL_TOKEN_FILE. Global and per-PDS events-per-second policies persist
across restart and apply backpressure before source events enter the scheduler.
Jetstream job requests and retry/cancel commands accept durable idempotency keys.
See administration/README.md for configuration and administrator enrollment.
