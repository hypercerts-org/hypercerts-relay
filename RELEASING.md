<!-- hypercerts: project-owned release procedure; review this file during every Indigo upstream sync. -->

# Releasing Hypercerts Relay

Hypercerts Relay uses [Changesets](https://github.com/changesets/changesets), consistent with the other maintained Hypercerts services. Each release is a versioned source tag and GitHub Release. It does not deploy a relay, publish a container, change PDS admission, or alter production rate limits.

## Record a release-worthy change

For an operator-visible Relay or Rainbow change, add a named file under `.changeset/` before requesting review. Use the `writing-changesets` project skill for the required format and version choice. The release note must say what changed and what an operator needs to do.

Do not hand-edit `CHANGELOG.md` or create a tag. Changesets creates both from the merged release notes.

## Release workflow

The `Release` workflow uses the standard Hypercerts release bot credentials, `RELEASE_BOT_APP_ID` and `RELEASE_BOT_APP_PRIVATE_KEY`. A maintainer starts it from `main` after one or more Changesets have been merged.

1. Open **Actions → Release → Run workflow** on `main`.
2. The workflow verifies Relay and Rainbow, then opens or updates a `prepare for release vX.Y.Z` pull request. That pull request updates `package.json`, `package-lock.json`, and `CHANGELOG.md`.
3. Review and merge the version pull request normally.
4. Its merge triggers the workflow again. Changesets creates the annotated `vX.Y.Z` tag and GitHub Release from the generated changelog.
5. Confirm that the tag points to the merged release pull request and that the release notes state the operator impact accurately.

The workflow refuses to run a stable release from any branch other than `main`. It does not publish to npm because the release package is private.

## Correcting a release

Do not move or force-push a release tag. Create a corrective Changeset, merge its release pull request, and publish the next version. If a version must be withdrawn from normal use, add a clear withdrawal notice to its GitHub Release and publish the corrective version.

## Upstream synchronization is not a release

An Indigo upstream-sync pull request can alter runtime behavior. It still follows normal review and verification, but it gets a Hypercerts version only when a maintainer adds a Changeset and completes this process.
