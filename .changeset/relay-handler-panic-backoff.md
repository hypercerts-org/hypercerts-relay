---
'hypercerts-relay': patch
---

A Relay source whose event handler fails unexpectedly is retried with backoff and counted in `relay_handler_panics_total`.
