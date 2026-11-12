---
'hypercerts-relay': patch
---

Add an acceptance scenario for verify-queue overflow recovery. `./tests/acceptance/run T20` stops Jetstream, writes a single-account backlog to the fixture PDS, then restarts Jetstream so the Relay replays the backlog at socket speed. The replay sheds events from atmos's per-DID verify queue, and the scenario fails unless that loss is booked as a durable, repository-scoped recovery job.

The acceptance PDS fixtures now run with `PDS_RATE_LIMITS_ENABLED=false`. They are disposable containers on a private network, and the write limiter otherwise paces the backlog far beyond the test budget. Existing scenarios do not depend on the limiter firing, and T01 was re-run green with it disabled.
