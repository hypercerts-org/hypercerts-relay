<!-- hypercerts: project-owned release procedure; review this file during every Indigo upstream sync. -->

# Releasing Hypercerts Relay

Hypercerts Relay releases are versioned source tags and GitHub Releases. A release records code that has passed the Relay and Rainbow verification gate; it does not deploy a relay, publish an image, change PDS admission, or alter production rate limits.

## Prepare a release

1. Work from an up-to-date `main` branch with the intended pull requests merged.
2. Add a `## [X.Y.Z] - YYYY-MM-DD` section to `CHANGELOG.md`. Describe observable operator or consumer changes in plain language. Do not copy a raw commit list.
3. Run `./scripts/verify.sh` and `git diff --check` locally.
4. Open and merge the changelog pull request through the normal review process.

Use Semantic Versioning while the project is pre-1.0:

- `0.x.y` patch: a relay or Rainbow correction that does not add configuration or a public behavior.
- `0.x.0` minor: a new configuration option, capability, administration operation, or other operator-visible behavior.
- `1.0.0` and later: use a major version for incompatible configuration or external behavior.

## Create the release

1. In GitHub, open **Actions → Release → Run workflow**.
2. Select the `main` branch and enter the version without the `v` prefix, for example `0.1.0`.
3. Watch the run. It validates the selected branch, verifies the version and changelog section, runs `./scripts/verify.sh`, creates the annotated `vX.Y.Z` tag, pushes it, and creates the GitHub Release using that changelog section.
4. Confirm that the tag points to the reviewed `main` commit and that the generated release notes match the changelog.

The workflow rejects duplicate tags and runs no deployment steps. Source archives are supplied by GitHub with the tag. Publishing containers or architecture-specific binaries requires a separate, reviewed delivery decision.

## Correcting a release

Do not move or force-push a release tag. If a released version is unsuitable, publish a corrective version with a changelog entry that explains the operator impact and required action. If a release must be withdrawn from normal use, mark the GitHub Release as a pre-release or add a clear withdrawal notice, then publish the correction.

## Upstream synchronization is not a release

An Indigo upstream-sync pull request can contain meaningful runtime changes. It still follows normal review and verification, but it does not create a Hypercerts release until a maintainer updates `CHANGELOG.md` and runs this procedure.
