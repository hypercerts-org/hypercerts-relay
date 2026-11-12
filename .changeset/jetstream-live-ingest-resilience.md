---
'hypercerts-relay': patch
---

Jetstream live ingestion no longer stops silently when the Relay restarts.
Previously, a single HTTP 502 or 503 from the Relay or its proxy during a
reconnect ended the upstream stream. Jetstream then kept running without
ingesting until its next restart. Transient rejections (408, 425, 429 and 5xx)
are now retried with backoff from the same cursor. Any other end of the stream
exits the process with an `upstream stream ended unexpectedly` error so it is
restarted. Successful connections are logged as `upstream connected`.

Catching up after a restart no longer overflows the per-account verify queue as
easily. Verification now uses 128 workers by default, up from 32, so each
account's queue holds 256 events instead of 64. Upstream reads are paced at 500
frames per second by default. Tune these with `JETSTREAM_LIVE_VERIFY_PARALLELISM`,
`JETSTREAM_LIVE_UPSTREAM_READ_RATE` and `JETSTREAM_LIVE_UPSTREAM_READ_BURST`.
Pacing limits how fast Jetstream replays a large backlog.

Repeated `verification failure` warnings for the same account are now logged
once per minute, followed by a `verification failures repeated` summary with
the count and the last error. The verifier failure metric is unchanged.
