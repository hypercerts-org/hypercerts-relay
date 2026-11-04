---
'hypercerts-relay': patch
---

For selected-stream consumers, correct the subscription schema and generated Go types to expose existing event provenance, reconciliation scope and boundary metadata, plus capability, policy-adoption and history-gap notices. Typed JSON round trips preserve explicit null boundaries and empty collection and gap lists without adding capability fields to policy-adoption notices. The existing wire format is unchanged; ordinary subscriptions still omit this metadata. Operators do not need to change configuration.
