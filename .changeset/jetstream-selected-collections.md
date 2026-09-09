---
'hypercerts-relay': minor
---

Jetstream now persists a versioned global list of enabled collections and filters
record writes during bootstrap, live ingestion, sync replacement, and retries.
For a new data directory, set `JETSTREAM_COLLECTIONS` to comma-separated exact
NSIDs. The default empty list stores no record payloads. Restart reuses the
persisted policy; changing this environment variable does not overwrite it.
Existing archived data is not purged. Identity, account, and sync markers remain
available and source cursors continue advancing for excluded-only commits.
