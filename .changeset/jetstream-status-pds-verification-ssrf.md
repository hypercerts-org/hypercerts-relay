---
'hypercerts-relay': patch
---

The Jetstream `/status` repo-verification lookup no longer sends requests into internal networks. The status page is unauthenticated and both the DID document and the PDS endpoint it names are chosen by the account, so a viewer could previously make the service contact internal or loopback addresses. The status page's handle and `did:web` lookups and the PDS request now use a client that refuses non-public and special-use addresses at connection time and uses no proxy, and the PDS endpoint is reduced to a bare origin. `did:plc` lookups still go to the configured PLC directory, which may be private, but without following redirects. Anonymous viewers see a generic "repo verification failed" message while the detail goes to the server log. No operator action is required.
