---
'hypercerts-relay': patch
---

Direct-PDS bootstrap now refreshes one stale DID signing-key cache entry after a signature mismatch, so a valid repository can complete after key rotation. No operator action is required.
