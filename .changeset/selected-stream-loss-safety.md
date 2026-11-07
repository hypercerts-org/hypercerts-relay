---
'hypercerts-relay': patch
---

Selected Jetstream subscriptions now log and skip a known upstream continuity loss instead of stopping. An upstream `OutdatedCursor` notice or relay sequence gap is logged at error level and recorded durably, then ingestion continues; readiness, established subscriptions and restart are unaffected. The recorded loss is reported again on the next start. Treat the skipped interval as a real history gap and follow `docs/upgrade-restore.md`.
