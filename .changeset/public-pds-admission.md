---
'hypercerts-relay': minor
---

Allow public `requestCrawl` to synchronously validate and admit a new PDS. Failed validation is retained with a one-minute retry cooldown. Concurrent or superseded admission checks return HTTP 409; clients should retry the request. The daily new-host limit applies when creating a source, returns HTTP 429 when reached, and does not apply to existing sources. Sibling relays are notified only after successful admission. Missing host checkers are reported as unavailable rather than as host-check failures.

Source sockets now use a public-network-only dialer in normal builds, including for administratively added sources. Operators using localhost or private-network PDS hosts must make those PDS instances reachable at a public address. Other operators need no action.
