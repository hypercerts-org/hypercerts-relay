---
'hypercerts-relay': minor
---

Add an optional collection directory for discovering candidate repositories from Relay events and PDS crawls. Operators must configure `COLLECTIONS_ADMIN_TOKEN` and restrict the API and metrics listeners; shared rate-limit bypass credentials are not forwarded. Directory results are candidates, not proof of current records or complete coverage.
