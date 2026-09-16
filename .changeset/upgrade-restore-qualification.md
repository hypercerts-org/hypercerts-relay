---
'hypercerts-relay': minor
---

Document the supported forward-upgrade and restore-only procedure. Operators must preserve Jetstream archive data with its durable cursor positions and run the copied-state qualification before a release candidate; binary rollback after state migration is not supported.
