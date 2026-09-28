---
name: hypercerts-github-release
description: Create and verify a Hypercerts Relay GitHub Release with GitHub CLI generated notes after the reviewed Release workflow has created and validated its source tag. Use for source releases only, never image publishing or Railway deployment.
---

# Hypercerts GitHub Release

Read `AGENTS.md`, `RELEASING.md`, and `.github/workflows/release.yml` before acting.
Use the `writing-changesets` skill if an operator-visible change needs a
Changeset.

A release is a source tag and GitHub Release only. It does not publish images,
deploy services, alter PDS admission, or change production settings.

## Guardrails

- Require explicit maintainer authorization before creating a release.
- The `Release` workflow on `main` creates and validates the source tag; do not
  use `git tag`, `npm run release`, or `gh release create` to compensate for a
  missing tag.
- Do not edit `CHANGELOG.md` manually or merge the generated version pull
  request. A human reviews and merges that pull request.
- Do not run Railway, image-publishing, staging, or production-promotion
  workflows.
- Do not replace, retarget, delete, or recreate a release tag or GitHub Release.
  Publish a corrective version instead.

## Procedure

1. Confirm the repository, authenticated GitHub account, and checked-out `main`:

   ```sh
   gh auth status
   git fetch --tags origin
   git switch main
   git pull --ff-only origin main
   ```

2. Run the manual **Release** workflow from `main`. Its normal path creates or
   updates the reviewed Changesets version pull request. After a human merges
   that pull request, the merge-triggered run creates and validates `vX.Y.Z`.
   For the one-time initial tag of an already-versioned tree, dispatch it with
   `tag_current_version=true`; do not use that option for later Changesets
   releases.

   ```sh
   gh workflow run Release --ref main
   gh run list --workflow Release --limit 5
   ```

3. After the tag-producing run succeeds, fetch its tag and detach at that exact
   commit before reading the version. Set `RELEASE_TAG` to the tag reported by
   the successful workflow. The validation must run from the tag commit, not a
   later `main` commit:

   ```sh
   tag="${RELEASE_TAG:?set RELEASE_TAG to the validated source tag, e.g. v0.9.0}"
   git fetch --tags origin
   git switch --detach "$tag"
   version="$(node -p "require('./package.json').version")"
   test "$tag" = "v$version"
   npm ci --ignore-scripts
   npm run check-component-versions -- --require-tag --tag="$tag"
   ```

4. Confirm a GitHub Release does not already exist. Preview GitHub's generated
   title and notes, then review them for accurate operator impact before the
   creation command:

   ```sh
   repo="$(gh repo view --json nameWithOwner --jq .nameWithOwner)"
   commit="$(git rev-parse HEAD)"
   gh release view "$tag" --repo "$repo" >/dev/null 2>&1 && {
     echo "GitHub Release $tag already exists" >&2
     exit 1
   }
   gh api --method POST "repos/$repo/releases/generate-notes" \
     -f tag_name="$tag" \
     -f target_commitish="$commit" \
     --jq '"title: " + .name + "\n\n" + .body'
   ```

5. Create the GitHub Release from the existing validated tag with generated
   notes, then inspect the published result:

   ```sh
   gh release create "$tag" --repo "$repo" --verify-tag --generate-notes --title "$tag"
   gh release view "$tag" --repo "$repo"
   ```

## Completion

Report the source tag, target commit, GitHub Release URL, and successful Release
workflow URL. State explicitly that no container publication or deployment was
performed.
