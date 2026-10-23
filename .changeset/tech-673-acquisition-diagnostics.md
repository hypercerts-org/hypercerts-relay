---
'hypercerts-relay': patch
---

Correct current-state coverage to distinguish automatic retry waits from running work, with recorded retry and PDS cooldown deadlines. Coverage now uses one coherent evidence capture and shows classified listing failures and the latest durable progress date without inventing legacy attribution counts. Existing job execution contracts are unchanged. No operator migration is required.
