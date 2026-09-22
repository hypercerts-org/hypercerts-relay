<!-- hypercerts: project-owned release procedure; review this file during every Indigo upstream sync. -->

# Releasing Hypercerts Relay

Hypercerts Relay uses [Changesets](https://github.com/changesets/changesets), consistent with the other maintained Hypercerts services. Each release is a versioned source tag and GitHub Release. It does not deploy a relay, publish a container, change PDS admission, or alter production rate limits.

## Record a release-worthy change

For an operator-visible Relay or Rainbow change, add a named file under `.changeset/` before requesting review. Use the `writing-changesets` project skill for the required format and version choice. The release note must say what changed and what an operator needs to do.

Do not hand-edit `CHANGELOG.md` or create a tag. Changesets updates the changelog from merged release notes, and the `Release` workflow creates the tag.

## Release workflow

The `Release` workflow uses the standard Hypercerts release bot credentials, `RELEASE_BOT_APP_ID` and `RELEASE_BOT_APP_PRIVATE_KEY`. A maintainer starts it from `main` after one or more Changesets have been merged.

1. Open **Actions → Release → Run workflow** on `main`.
2. The workflow verifies Relay and Rainbow, then opens or updates a `prepare for release vX.Y.Z` pull request. That pull request updates the root package, Administration package, Relay/Rainbow and Jetstream build versions, Docker build defaults, and `CHANGELOG.md` together.
3. Review and merge the version pull request normally.
4. Its merge triggers the workflow again. Before Changesets creates `vX.Y.Z`, the workflow checks that the source tag name, root package, Administration package, Relay/Rainbow runtime metadata, Jetstream runtime metadata, and all four Docker build defaults use the same version. It then verifies that the tag resolves to the merged release commit.
5. Use the `hypercerts-github-release` skill to create the GitHub Release from that existing validated tag with GitHub CLI generated notes. Review the generated notes for accurate operator impact before creating it.

For the one-time initial tag of an already-versioned tree, a maintainer can dispatch the workflow from `main` with **Create the missing tag for the checked-out package version** enabled. It validates and creates only the current `vX.Y.Z` tag; it does not bypass review for later Changesets releases or create a GitHub Release.

The workflow refuses to run a stable release from any branch other than `main`. It does not publish to npm, publish a container, deploy a service, or alter a running relay because the release package is private.

## Correcting a release

Do not move or force-push a release tag. Create a corrective Changeset, merge its release pull request, and publish the next version. If a version must be withdrawn from normal use, add a clear withdrawal notice to its GitHub Release and publish the corrective version.

## Upstream synchronization is not a release

An Indigo upstream-sync pull request can alter runtime behavior. It still follows normal review and verification, but it gets a Hypercerts version only when a maintainer adds a Changeset and completes this process.
