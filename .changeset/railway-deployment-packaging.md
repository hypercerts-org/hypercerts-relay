---
'hypercerts-relay': minor
---

Support optional Railway runtime-secret bootstrap in the canonical container
images. Keep infrastructure configuration in a private repository.
HC_RELAY_CONTROL_SECRET, HC_JETSTREAM_CONTROL_SECRET and HC_ADMIN_ENCRYPTION_SECRET
are converted to restricted files when HC_RAILWAY_STARTUP=1 enables the shared
bootstrap. Existing file-based credential interfaces remain supported.

The administration image includes its production TypeScript loader. See
docs/railway.md for runtime credentials, private ports, storage ownership and
administrator setup.
