---
'hypercerts-relay': patch
---

Selected-stream consumers: `maxMessageSizeBytes` now limits the entire uncompressed event frame, including selected-stream metadata, for both replay and live delivery. Oversized events are skipped without stopping subsequent delivery, including on compressed connections. A zero limit remains unlimited.
