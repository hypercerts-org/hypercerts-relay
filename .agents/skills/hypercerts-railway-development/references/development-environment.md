# Development environment procedure

Use the private infrastructure repository and identify the exact Railway project
before making changes. Inspect the current environment/service configuration
first. Create an isolated `development` environment by duplicating only the
reviewed service shape, then review the staged plan before applying it.

Provision Relay, Jetstream, Administration, and any selected Relay PostgreSQL
dependency. Give every stateful service its own development storage:

- Relay raw-event persistence and metadata database;
- Jetstream archive and Pebble directory; and
- Administration SQLite/OAuth state.

Never attach a staging or production volume, database, OAuth encryption key, seed
DID, or control secret to development. Generate distinct runtime secrets in the
platform secret store. When the Railway bootstrap is selected, set
`HC_RAILWAY_STARTUP=1` and provide `RAILWAY_VOLUME_MOUNT_PATH=/data`; keep raw
`HC_*_SECRET` values out of image builds, source control, and logs.

Configure Relay-to-Jetstream and Administration-to-control-plane connections only
over Railway private networking. Keep Relay control, Jetstream debug/control, and
metrics listeners private. Administration needs its own public HTTPS origin for
its OAuth metadata/callbacks; it must not expose private management origins.

Deploy explicitly selected immutable images, then confirm every deployment reaches
`SUCCESS`. Check Relay `/xrpc/_health`, Jetstream `/status`, and Administration
`/health`, followed by an authenticated private-control and persistence smoke test
appropriate to the environment. Health endpoints alone prove only availability.

When retiring development, first confirm its exact environment, services, and
state-retention decision. Deleting the environment or its volumes is destructive
and requires separate explicit approval; preserve or export needed diagnostic data
before any deletion.
