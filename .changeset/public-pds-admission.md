---
'hypercerts-relay': minor
---

Allow public `requestCrawl` to synchronously validate and admit a new PDS. Failed validation is retained with a one-minute retry cooldown, concurrent checks return a retryable conflict, and sibling relays are notified only after successful admission.
