---
'hypercerts-relay': minor
---

`did:web` document fetches are now rate-limited during Relay identity resolution (10 per second, burst 30, process-wide), and `did:web` documents larger than 64 KiB are rejected instead of being truncated. Identity documents are fetched from hosts the operator does not control, kept in a 24-hour cache, and can be requested through unauthenticated endpoints, so the limit bounds how fast they can be fetched and cached in bulk. A fetch over the limit fails immediately rather than waiting, and that failure is not cached, so the same identity resolves normally on a later request. `did:plc` resolution is unchanged. Operators running a Relay that legitimately resolves very large numbers of `did:web` identities during a catch-up may see some of those lookups fail and succeed on retry. This bounds the rate of growth; it does not by itself cap the total memory the identity cache can hold.
