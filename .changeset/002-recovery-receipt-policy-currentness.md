---
'hypercerts-relay': patch
---

Relay now durably mirrors Jetstream policy advances before accepting recovery work. An outdated backfill cannot acknowledge a newer source-recovery requirement; a policy update waits for Relay's private control acknowledgement. No operator action is required.
