---
'hypercerts-relay': minor
---

The administration Backfill jobs table now lists the most recently created jobs first, shows each job's creation time, and has status links to show all, active (pending or running), pending, running, incomplete, failed, canceled or complete jobs. Ordering and filtering apply across pages and combine with the PDS filter. Jetstream's private `GET /hypercerts/v1/jobs` returns jobs newest first (job ID breaks ties), accepts an optional `state` filter, and rejects an unknown `after` cursor with 400 `invalid_cursor` or an unknown `state` with 400 `invalid_state`. Retrying a job does not change its position. No operator action is required.
