---
'hypercerts-relay': minor
---

Jetstream no longer exposes profiling endpoints merely because its private
control/metrics listener is enabled. Set `JETSTREAM_ENABLE_PPROF=true` or
`--enable-pprof` explicitly for a diagnostic session, together with
`JETSTREAM_DEBUG_ADDR`. Profiling remains unauthenticated on that private
listener; restrict access and disable the option after diagnostics.

Add accessible labels to the status page's host filter and account lookup.
