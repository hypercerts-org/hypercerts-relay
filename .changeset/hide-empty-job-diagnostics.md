---
'hypercerts-relay': patch
---

Simplify the administration Backfill jobs table: omit unrecorded listing-failure and last-progress lines, show run counts, unresolved counts and attempt limits only when they are notable, shorten progress to "N of M repositories processed", and stop offering Retry job on complete jobs. No operator action is required.
