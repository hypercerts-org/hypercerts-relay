---
name: hypercerts-relay
description: Build, review, or operate the Hypercerts Indigo Relay and Rainbow fork. Use when work involves approved PDS sources, raw Relay events, Rainbow fan-out, upstream Indigo synchronization, or the service boundary with Hypercerts Jetstream v2.
---

# Hypercerts Relay

Use this skill for work in the Hypercerts Relay fork. Start with `AGENTS.md`; it contains the repository's current scope and safety rules.

Keep the component boundary explicit:

- Indigo Relay connects approved PDS instances and publishes raw `com.atproto.sync.subscribeRepos` events.
- Rainbow pools and fans out raw Relay connections.
- Jetstream v2 owns selected-collection retention and PDS/collection backfill in its separate repository.
- The administration control plane owns operator access, PDS and collection policy, rate limits, jobs, and telemetry.

Do not implement Jetstream filtering/backfill or the planned OAuth administration UI in Relay or Rainbow merely because the components connect to each other.

## Fork work

`origin` is the Hypercerts fork and `upstream` is Bluesky Indigo. Keep new behavior isolated where practical. Mark unavoidable edits to upstream-owned Go files with `// hypercerts:` and a short reason. Never resolve upstream merge conflicts automatically or commit conflict markers.

For an upstream update, branch from current `main`, merge `upstream/main`, inspect `git log upstream/main..main` and every marked Hypercerts edit, run `./scripts/verify.sh`, then open a review pull request. Do not rebase `main` onto upstream.

## Validation and release work

Run `./scripts/verify.sh` and `git diff --check` for Relay or Rainbow changes. The script excludes one documented local baseline test failure, while CI runs it separately as a visible non-blocking signal. Do not suppress additional tests without a documented observed behavior and removal condition.

A release is a reviewed version tag and GitHub Release, created only through the manual workflow from `main` after `CHANGELOG.md` is updated. It is not authority to deploy or change a running relay.
