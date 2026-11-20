---
'hypercerts-relay': patch
---

Live ingestion no longer crash-loops on upstream `#sync` events it cannot reconcile. A PDS sending a resync with a spec-valid record key of 256–512 bytes (wider than the archive's column), replacement records whose DID or revision do not match the `#sync`, or the same record key twice, previously ended the live consumer with a fatal error and the process restart looped on the same event until an operator intervened — stopping archival for every account. These are now treated as hostile or racing upstream input: the unarchivable rows are dropped (counted under `field_too_long`), the whole event is dropped where it cannot be reconciled (counted under `invalid_rev`), and the upstream cursor advances past it. No operator action is required.
