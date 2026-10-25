---
"hypercerts-relay": patch
---

Relay now retries source lifecycle handoffs until Jetstream confirms the exact durable PDS revision. Jetstream treats repeated acceptance as idempotent across policy changes and restarts, while malformed or redirected acknowledgments remain retryable instead of silently dropping desired state.
