---
'hypercerts-relay': minor
---

Keep source events replayable when processing or persistence fails. Relay acknowledges output only after durable storage and records permanent verification rejections without record contents. Unavailable identities remain retryable; signature failures require an identity refresh before rejection.

Startup adds the rejected-event table. Back up Relay state before upgrading. Replays can repeat already stored output after a failed account revision update; consumers must tolerate duplicates.

Disk write and sync failures require restart. Startup recovers incomplete trailing writes without removing complete events. Per-event synchronization can reduce throughput compared with buffered persistence.
