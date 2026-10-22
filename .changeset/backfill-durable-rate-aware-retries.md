---
'hypercerts-relay': minor
---

Jetstream now retries failed PDS downloads up to three times without losing
progress after a restart. Rate-limit waits are capped at 30 minutes, and other
PDS sources can continue while one waits. Jobs that still fail need a manual
Retry. No configuration changes are needed.

The cap only affects new responses. Previously saved longer waits remain
unchanged.

Older jobs without a saved inventory remain readable.
