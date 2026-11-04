---
'hypercerts-relay': patch
---

Control API operators: a policy change attempted before required policy adoption is initialized now returns HTTP 503 with `policy_adoption_not_ready`, rather than reporting a persistence failure. The saved policy and jobs remain unchanged; retry after initialization completes.
