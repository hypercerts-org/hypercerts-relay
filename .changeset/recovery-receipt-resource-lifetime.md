---
'hypercerts-relay': patch
---

Jetstream operators: completed recovery-receipt attempts now release their background resources promptly, including failed attempts waiting for retry. Long-running services no longer accumulate receipt watchers. No configuration change is required.
