---
'hypercerts-relay': patch
---

Retry transient direct-PDS `getRepo` failures up to three times with bounded backoff before recording a repository as incomplete. After a failed or incomplete backfill outcome is durably recorded, emit a structured warning without logging raw errors or response bodies.
