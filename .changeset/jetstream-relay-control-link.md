---
'hypercerts-relay': patch
---

Jetstream now holds the Relay control credential needed to advance collection
policy. Set `JETSTREAM_RELAY_CONTROL_URL` to the Relay's private control listener
(`http://relay.railway.internal:2472` on Railway); the token file is provided by
the container bootstrap as `JETSTREAM_RELAY_CONTROL_TOKEN_FILE`. Without both, a
collection change is rejected with `policy_mirror_unavailable` (HTTP 503) instead
of failing as an unspecified persistence error. Existing archives are unaffected,
and no stored data is rewritten.
