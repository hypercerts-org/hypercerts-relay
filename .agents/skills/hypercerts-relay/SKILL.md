---
name: hypercerts-relay
description: Build, review, or operate the Hypercerts Indigo Relay, Rainbow, and copied Jetstream v2 runtime. Use when work involves approved PDS sources, raw Relay events, Rainbow fan-out, Jetstream archive/backfill policy, or upstream synchronization.
---

# Hypercerts Relay

Use this skill for work in the Hypercerts Relay fork. Start with `AGENTS.md`; it contains the repository's current scope and safety rules.

Keep the component boundary explicit:

- Indigo Relay connects approved PDS instances and publishes raw `com.atproto.sync.subscribeRepos` events.
- Rainbow pools and fans out raw Relay connections.
- The `jetstream/` module owns selected-collection retention, archive replay/live APIs, and PDS/collection backfill. It is a copied Jetstream v2 runtime with its own Go module, not part of the Indigo fork closure.
- The administration control plane owns operator access, PDS and collection policy, rate limits, jobs, and telemetry.

Do not implement Jetstream filtering/backfill in Relay or Rainbow merely because the components connect to each other. Do not implement the planned OAuth administration UI in any inherited Relay admin surface.

## Jetstream v2 work

Read `jetstream/README.md` before changing the copied runtime. It records the exact source commit, the included closure, its license, and the update procedure. The Indigo upstream script and `.hypercerts/upstream-paths` do not govern this directory.

Keep the Hypercerts policy boundary explicit: a versioned collection policy must be applied before durable record payload materialization on bootstrap, live, retry, sync-replacement, and restart paths. Preserve identity, account, sync, delete, verification, archive cursor, and progress behavior even where a commit has no selected record. Backfill jobs must be durable and idempotent, identify the PDS and policy revision they cover, and surface unavailable input as incomplete.

For Jetstream changes, run the module-local suite from `jetstream/` with `go test ./...`, plus `git diff --check`. Build images from the module context with `docker build -f jetstream/Dockerfile jetstream` when Docker daemon access is available.

## Fork work

Read `FORK.md` before any upstream update. It gives the exact Indigo remote URL, how to add and verify it when missing, and the required review process. Keep new behavior isolated where practical. Mark unavoidable edits to upstream-owned Go files with `// hypercerts:` and a short reason. Never resolve upstream merge conflicts automatically or commit conflict markers.

For an upstream update, branch from current `main`, run `./scripts/apply-upstream-update.sh`, inspect its staged allowlisted diff, `git log upstream/main..main`, and every marked Hypercerts edit, then run `./scripts/verify.sh` and open a review pull request. Do not merge `upstream/main` directly.
Only a human may merge a pull request. Never push, fast-forward, reset, or force-update `main`.

## Validation and release work

Run `./scripts/verify.sh` and `git diff --check` for Relay or Rainbow changes. The script excludes one documented local baseline test failure, while CI runs it separately as a visible non-blocking signal. Do not suppress additional tests without a documented observed behavior and removal condition.

A release is a reviewed version tag and GitHub Release, created only through the manual workflow from `main` after `CHANGELOG.md` is updated. It is not authority to deploy or change a running relay.
