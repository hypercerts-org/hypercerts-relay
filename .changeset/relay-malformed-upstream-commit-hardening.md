---
'hypercerts-relay': patch
---

Relay commit verification now rejects malformed upstream repository data instead of crashing. A malicious or misbehaving PDS could previously crash the whole Relay process (stopping ingestion and the firehose for every consumer) with a single crafted `#commit` event: repository CIDs using weak or truncated hashes (including a zero-length SHA-256 digest that matches any content), an MST that references the same subtree twice (a ~5 KB CAR could expand to gigabytes of memory), a `null` entry in the ops list (`malformed_commit`), an empty MST key, or an empty MST child node (both rejected as `invalid_mst` under the default strict validation). Spec-compliant repositories are unaffected: they already use 32-byte SHA-256 CIDs and never repeat subtrees. As defense in depth, a panic while processing a single event now stops only that source's scheduler instead of killing the process. No operator action is required.
