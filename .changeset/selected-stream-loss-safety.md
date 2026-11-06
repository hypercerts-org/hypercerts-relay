---
'hypercerts-relay': patch
---

Selected Jetstream subscriptions now stop instead of skipping a known upstream continuity loss. An upstream `OutdatedCursor` notice or relay sequence gap is recorded durably, fails `/readyz`, closes established selected subscriptions, and blocks restart until an operator restores a coherent pre-loss snapshot or provisions a new stream identity. Follow `docs/upgrade-restore.md`; do not delete the recorded loss and resume the same archive.
