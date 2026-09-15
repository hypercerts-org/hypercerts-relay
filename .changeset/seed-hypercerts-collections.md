---
'hypercerts-relay': minor
---

Seed new Jetstream collection policies with the bundled Hypercerts and Certified record lexicons. Disable this default with `JETSTREAM_DISABLE_COLLECTION_SEED=true` or `--disable-collection-seed`; an explicit `JETSTREAM_COLLECTIONS` / `--collections` list overrides the seed. Persisted policies, including administrator removals and empty lists, are never overwritten on restart. Existing deployments can apply the desired list through the administration Collections screen.
