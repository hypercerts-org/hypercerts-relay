---
'hypercerts-relay': patch
---

Jetstream now holds the Relay control credential needed to advance collection
policy. Set `JETSTREAM_RELAY_CONTROL_URL` to the Relay's private control listener
(`http://relay.railway.internal:2472` on Railway); the token file is provided by
the container bootstrap as `JETSTREAM_RELAY_CONTROL_TOKEN_FILE`. When an enabled
source requires that mirror and it is missing or unreachable, the collection
change is rejected with `policy_mirror_unavailable` (HTTP 503) instead of failing
as an unspecified persistence error. Existing archives are unaffected, and no
stored data is rewritten.

**Required on staging and production before deploying.** Jetstream must have
`HC_RELAY_CONTROL_SECRET` (same value as Relay) and
`JETSTREAM_RELAY_CONTROL_URL=http://relay.railway.internal:2472`. Relay must have
`RELAY_CONTROL_ADDR=[::]:2472`. A public Relay domain does not serve the control
API and returns 404. The full per-service list is in `docs/railway.md` under
"Required variables per environment".
