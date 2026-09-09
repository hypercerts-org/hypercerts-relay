---
'hypercerts-relay': minor
---

Make administration claims, retries and cancellation atomic, order operation pages
by creation time, and preserve Jetstream retry receipts in separate namespaces.
Existing job receipts migrate automatically; avoid downgrading Jetstream while
journal commands remain retryable. Operation pagination clients must use the new
opaque `next` cursor unchanged.

Correct applied-quota feedback and recovered polling errors. Secure SQLite sidecar
files, clear raw container secret inputs before starting services, and validate
control-service origins. HTTP control origins must be loopback, or single-service
`*.railway.internal` hosts with `HC_RAILWAY_STARTUP=1`; other hosts require HTTPS.
Railway startup now honors the edge's client IP for login
limits; other proxies can configure `ADMIN_TRUST_PROXY` with trusted peer CIDRs.

Keep Relay event admission responsive during policy persistence, apply rate limits
to all source frames, and open management only after subscription initialization.
