---
'hypercerts-relay': patch
---

Fix source enablement returning an error when Relay already has an active subscription. Repeated enable requests now preserve the connection and allow administration to complete Jetstream source registration. After upgrading, retry an incomplete source enable request before retrying its failed backfill request.
