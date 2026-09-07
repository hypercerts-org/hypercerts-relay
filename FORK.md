<!-- hypercerts: project-owned fork policy; review this file during every Indigo upstream sync. -->

# Maintaining the Indigo fork

Hypercerts Relay is a deliberately small fork of [Bluesky Indigo](https://github.com/bluesky-social/indigo). It contains the Relay, Rainbow, and their Go dependency closure. This document owns the fork provenance, upstream setup, and synchronization process.

## Module path

The Go module path remains `github.com/bluesky-social/indigo`. Relay and Rainbow are built from this repository; Hypercerts does not publish these packages as a standalone Go library. Keeping the upstream import path avoids a broad import rewrite and keeps upstream changes reviewable.

Do not change the module path without an explicit decision to publish and support a Hypercerts Go module.

## Configure the upstream remote

`origin` is the Hypercerts repository:

```text
git@github.com:hypercerts-org/hypercerts-relay.git
```

A fresh clone normally has no `upstream` remote. Before any Indigo synchronization work, add and verify this exact remote:

```bash
if ! git remote get-url upstream >/dev/null 2>&1; then
  git remote add upstream https://github.com/bluesky-social/indigo.git
fi
git remote get-url upstream
```

The final command must print:

```text
https://github.com/bluesky-social/indigo.git
```

If `upstream` already points elsewhere, stop and resolve that repository identity before fetching or merging.

## Keep changes easy to merge

Every changed upstream line can become a future merge conflict.

- Put new Hypercerts behavior in clearly owned packages or configuration whenever practical.
- For an unavoidable edit to an upstream-owned Go file, add a short `// hypercerts:` comment explaining why the fork diverges.
- Keep the raw Relay stream, Jetstream retention and backfill, Rainbow fan-out, and the administration control plane as separate responsibilities.
- Do not resolve upstream merge conflicts automatically or commit conflict markers.

## Synchronize from Indigo

The `Check Indigo upstream` workflow checks weekly whether `upstream/main` is ahead. When Git can merge cleanly, it opens a review pull request. It never resolves conflicts or merges the pull request.

For a manual update:

1. Start a dedicated branch from current `main`.
2. Fetch `upstream/main` and merge it with a merge commit. Do not rebase `main` onto upstream.
3. Run `git log upstream/main..main` to identify fork-only commits, then review every affected `// hypercerts:` edit.
4. Run `./scripts/verify.sh` and open a review pull request.
5. Merge the reviewed synchronization pull request into `main`.

A merge conflict is a deliberate stop. Resolve it manually with the affected component owners, then repeat verification before requesting review.
