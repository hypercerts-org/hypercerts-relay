---
'hypercerts-relay': minor
---

Relay now acquires a renewable single-process rate-admission lease before it
opens managed PDS sockets. A second Relay using the same database exits rather
than splitting a global events-per-second policy. Graceful shutdown releases the
lease; a failed process can be recovered after its lease expires.

Private rate-policy responses now show bounded admitted-frame and waited-frame
counters, along with their process-local measurement scope. Configured limits
continue to persist across restart, while token balances and counters restart
with the documented one-second burst.
