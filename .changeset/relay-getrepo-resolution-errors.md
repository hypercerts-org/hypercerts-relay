---
'hypercerts-relay': patch
---

Relay getRepo now returns HTTP 429 when a DID lookup is rate limited and HTTP 503 when resolution fails. A confirmed missing DID returns HTTP 404.
