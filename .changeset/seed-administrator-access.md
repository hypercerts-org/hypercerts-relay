---
'hypercerts-relay': minor
---

Set `ADMIN_SEED_DID` at runtime to grant the initial administrator once on an uninitialized administration database. Seed access is ordinary membership: another administrator can revoke it and restart cannot restore it. Existing access history prevents re-seeding.

The Administrators screen lets authenticated admins grant and remove other administrators with actor-attributed audit records and immediate session revocation. Keep the access CLI for recovery when no administrator can sign in.
