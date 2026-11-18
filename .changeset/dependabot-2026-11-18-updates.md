---
'hypercerts-relay': patch
---

Build Relay, Rainbow, the collection directory, PDS discovery, Jetstream, and Administration with Go 1.27.4, and update the command-line library, OpenTelemetry tracing libraries, and GORM tracing plugin. No operator configuration changes are required. When `--enable-db-tracing` is set, database spans now use current OpenTelemetry names (for example `select repos` instead of `gorm.Query`, and `db.query.text` instead of `db.statement`), so trace dashboards filtering on the old names need updating.
