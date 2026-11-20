---
'hypercerts-relay': patch
---

`did:web` document fetches are now rate-limited during identity resolution (10 per second, burst 30, process-wide). Identity documents are fetched from operator-uncontrolled hosts, kept in a 24-hour cache, and can be requested through unauthenticated endpoints, so the limit bounds how fast they can be fetched and cached in bulk. `did:plc` resolution is unchanged. Operators running a Relay that legitimately resolves very large numbers of `did:web` identities during a catch-up may see slower resolution; sustained demand above the limit is logged through the existing resolution logging.
