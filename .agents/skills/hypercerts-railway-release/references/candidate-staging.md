# Candidate staging

Merging a reviewed pull request into `dev` starts `Publish Railway release
candidate`. It builds each launch image once, pushes `sha-<commit>` tags to GHCR,
captures the OCI digest, and dispatches `hypercerts-release-candidate` to the
private infrastructure repository.

The staging GitHub Environment must contain:

- `INFRA_REPOSITORY` as the private `owner/repository` receiver; and
- `INFRA_DISPATCH_TOKEN` as a narrowly scoped credential permitted only to create
  repository-dispatch events on that receiver.

It must also contain `ADMIN_PUBLIC_ORIGIN` and may contain
`RELAY_PUBLIC_ORIGIN`, `RAINBOW_PUBLIC_ORIGIN`, and `JETSTREAM_PUBLIC_ORIGIN`.
The receiver applies the first to the Administration service and maps the optional
values to the Administration overview's display-only public-service settings.
Reject a missing administration origin; omit empty optional origins. Never use a
control/debug origin as a public-service URL, and do not treat a Rainbow display
URL as authorization to deploy Rainbow.

The private receiver must store a candidate receipt keyed by source repository and
commit. The receipt contains the three `image@sha256:...` values, staging
deployment IDs, and validation evidence. It deploys those digest references to
the staging Railway environment; it must not rebuild from Git, resolve a mutable
tag, or use a production volume or secret.

Before accepting the candidate, verify all three submitted Railway deployments
are `SUCCESS`, then run the agreed staging checks. Relay `/xrpc/_health`,
Jetstream `/status`, and Administration `/health` establish availability only;
they do not prove source ingestion, archive completeness, or OAuth qualification.

Keep state separate: Relay raw-event storage, Jetstream archive/Pebble storage,
Administration SQLite/OAuth state, and any Relay PostgreSQL dependency are all
environment-specific. Management/debug ports remain private.
