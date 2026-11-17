---
'hypercerts-relay': minor
---

Jetstream PDS backfill jobs download repositories concurrently and no longer
rewrite all job state after every repository. Each job now downloads two
repositories at once by default, which raises per-PDS request concurrency from
one to two on upgrade. Set `JETSTREAM_PDS_JOB_REPO_WORKERS` (or
`--pds-job-repo-workers`, 1–32, default 2) to change it; set `1` to keep one
request at a time. After an HTTP 429 the job waits for the PDS cooldown and
resumes with fewer workers, and it pauses before the limit when the PDS reports
its remaining request budget. Each worker can hold one repository of up to
64 MiB in memory. A downgraded release downloads repositories checkpointed by
this release since the job last saved its state again.
