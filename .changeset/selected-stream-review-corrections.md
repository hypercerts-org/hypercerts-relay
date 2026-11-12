---
'hypercerts-relay': patch
---

For selected-stream consumers, Jetstream now reports stalled replay and metadata failures with a terminal InternalError when the connection is usable, without advancing past failed delivery. Equal-boundary collection gaps have a deterministic order, and JSON clients preserve both integer and object reconciliation boundaries. Operators need no configuration changes; wrapped missing segment errors remain benign during attribution recovery.
