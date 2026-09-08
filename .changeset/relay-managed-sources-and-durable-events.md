---
'hypercerts-relay': minor
---

Add managed PDS sources and durable raw-event processing to Hypercerts Relay. Administrators can admit, validate, enable, disable, and remove sources independently of transport state; public crawl requests cannot add or re-enable a source. Source status reports durable cursor, validation, quota exclusions, and observed DID migrations without automatically enrolling a migration target.

Relay saves raw output before source progress advances. Processing, cursor, disk-write, and sync failures leave work replayable; permanent verification rejections contain no record payload. Retained output can repeat after a failed progress update, so consumers must tolerate duplicates.

Back up the Relay database and event store together before upgrading. This does not add Jetstream retention/backfill, an OAuth administration interface, or a deployment.
