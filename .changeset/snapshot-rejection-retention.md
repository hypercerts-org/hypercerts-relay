---
'hypercerts-relay': patch
---

Direct-PDS snapshot rejection metadata now retains the most recent 1,000 permanent rejections. Older records are evicted, so a retry of an evicted listed revision can fetch it again; no operator action is required.
