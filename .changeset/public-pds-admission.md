---
'hypercerts-relay': minor
---

Allow public `requestCrawl` to synchronously validate and admit a new PDS. Failed validation is retained with a one-minute retry cooldown. Concurrent or superseded admission checks return HTTP 409; clients should retry the request. The daily new-host limit applies when creating a source, returns HTTP 429 when reached, and does not apply to existing sources. Sibling relays are notified only after successful admission. Missing host checkers are reported as unavailable rather than as host-check failures.

Source sockets now use a public-network-only dialer in normal builds, including for administratively added sources. HTTP and WS PDS sources are rejected even for administrators; source checks cannot downgrade HTTPS through redirects. Operators using plaintext, localhost, or private-network PDS hosts must move them to public HTTPS with valid TLS certificates and explicitly migrate any persisted `NoSSL` source state through their approved state-migration procedure. Existing insecure rows fail closed on validation and restart without rewriting their stored scheme or cursor. Remove `--allow-insecure-hosts` or `RELAY_ALLOW_INSECURE_HOSTS=true`, which now fails startup clearly. Operators already using public HTTPS hosts need no action.
