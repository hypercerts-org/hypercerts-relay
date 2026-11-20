---
'hypercerts-relay': patch
---

The Jetstream `/status` repo-verification lookup no longer fetches an account's PDS over internal networks. The PDS endpoint named in a DID document is account-controlled and the status page is unauthenticated, so it previously allowed any viewer to make the service send requests to internal or loopback addresses chosen through that document. The endpoint is now reduced to a bare public origin and every dial refuses non-public addresses; anonymous viewers see a generic "repo verification failed" message while the detail goes to the server log. No operator action is required.
