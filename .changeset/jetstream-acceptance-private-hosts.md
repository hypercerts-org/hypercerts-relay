---
'hypercerts-relay': minor
---

Rename the disposable Jetstream fixture opt-in from `JETSTREAM_DEVELOPMENT_MODE` to `JETSTREAM_ACCEPTANCE_PRIVATE_HOSTS` (or `--acceptance-private-hosts`). Use the new setting only with an acceptance-tag build; production Jetstream images continue to reject private fixture hosts.
