---
'hypercerts-relay': minor
---

Persist PDS admission, validation, disablement, and removal independently from transport status. Sources require validation before connection, and public crawl requests cannot admit or re-enable a source. Existing block/unblock handlers use the durable policy; no new administration UI or HTTP API is added.

Startup adds source policy and account-source observation tables. Existing hosts remain admitted, existing bans remain disabled, and later restarts preserve managed state. Back up the database and event store together before upgrading.

Keep retained replay data when acquisition stops. Record DID migrations without automatically enrolling the target PDS. Bounded status queries expose quiet sources, durable cursors, account quota exclusions, and incomplete or unknown target coverage. Jetstream v2 reconciliation remains separate work.
