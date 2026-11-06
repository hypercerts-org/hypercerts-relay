---
'hypercerts-relay': patch
---

Selected-stream consumers: archive replay read failures now send a terminal `InternalError` before disconnecting instead of silently closing. Reconnect from the last successfully received cursor; failures do not skip archive rows. Cancellation remains quiet, and compressed connections receive the same error contract.
