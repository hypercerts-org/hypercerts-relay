---
'hypercerts-relay': minor
---

Add optional Jetstream consumer archive keys with per-key request and archive MB/minute limits. Operators can set `JETSTREAM_ARCHIVE_KEY_AUTH_ENABLED=true` alongside a private Jetstream control token and listener, issue keys through the private `/hypercerts/v1/archive-keys` API, then give each consumer its one-time token for archive replay. The flag defaults to off, so existing public archive access continues until operators enable it.
