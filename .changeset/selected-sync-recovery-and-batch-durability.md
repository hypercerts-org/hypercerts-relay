---
'hypercerts-relay': patch
---

Correct selected-stream sync recovery during bootstrap and asynchronous ingestion, and prevent stale syncs for inactive accounts from stopping ingestion or publishing a misleading sync marker. Preserve prepared durable writes when a later event in an asynchronous batch fails, without hanging shutdown.

Bootstrap sync recovery reconciles missing selected records against the destination archive during merge. Scoped sync markers survive compaction without deleting their replacements, unrelated collections, or newer records; ordinary full-sync tombstones remain unchanged.

Selected subscriptions now derive their replay floor from the first durably attributed event, not initial policy history, and disclose a `legacy-scope-unknown` gap for earlier archive rows. Resumes in that region fail with `ReplayUnavailable` before streaming, regardless of filters. Until attribution activates, selected replay is unavailable and its floor is null; live-only subscriptions remain available. Empty archives do not advertise a fabricated legacy gap. Consumers upgrading an existing archive must explicitly reconcile selected state before using that floor or choosing live-only delivery. Ordinary subscriptions and raw Relay/Rainbow streams are unchanged.
