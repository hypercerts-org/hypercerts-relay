---
'hypercerts-relay': minor
---

Add a locally maintained Jetstream v2 runtime module for the Hypercerts Relay
archive, replay, live-subscription, and backfill service. Operators build and
run it from `jetstream/` with its own persistent data directory and configure
the owned Relay URL with `JETSTREAM_RELAY_URL`.

Jetstream's copied upstream runtime is a baseline, not an enabled
whole-network deployment. Hypercerts selected-collection policy and scoped
backfill jobs must be configured and reviewed before the service is exposed.
