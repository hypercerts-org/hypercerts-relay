---
'hypercerts-relay': minor
---

Expose an authenticated private Jetstream service API for collection policy,
PDS sources, and durable backfill job progress, cancellation, and retry. Enable
it with a mounted `JETSTREAM_CONTROL_TOKEN_FILE` and a private
`JETSTREAM_DEBUG_ADDR`. Jobs identify their PDS and policy revision and separate
current-state completion from unproven historical coverage. Unreachable PDS
sources remain incomplete. Control routes are absent from the public listener.
