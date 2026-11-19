---
'hypercerts-relay': patch
---

A subscriber that connects to `com.atproto.sync.subscribeRepos` with a cursor and disconnects during replay can no longer crash the Relay: the dead subscriber was re-registered for live events after its channel had been closed, and the next broadcast panicked on the closed channel. Cursor subscribers are now registered only if their cleanup has not already run. No operator action is required.
