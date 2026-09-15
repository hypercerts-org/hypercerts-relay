---
'hypercerts-relay': patch
---

Fix Jetstream container builds on Railway by using ordinary Docker layer caching instead of cache mounts that require deployment-specific service identifiers. Continue building the canonical Dockerfile from the `jetstream/` module context.
