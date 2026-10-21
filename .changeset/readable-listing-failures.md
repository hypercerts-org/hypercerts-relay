---
'hypercerts-relay': patch
---

Explain repository listing failures in the administration jobs UI with concise messages that distinguish PDS HTTP errors (including HTTP 429 rate limiting), transport failures, timeouts, canceled requests, and unreadable responses. Retry behavior and error codes are unchanged. No operator action is required.
