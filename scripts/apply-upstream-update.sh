#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

base_file=.hypercerts/upstream-base
paths_file=.hypercerts/upstream-paths

if [[ ! -f "$base_file" || ! -f "$paths_file" ]]; then
  echo 'Missing .hypercerts upstream synchronization metadata.' >&2
  exit 1
fi

base=$(tr -d '[:space:]' < "$base_file")
target=$(git rev-parse upstream/main)

if ! git cat-file -e "$base^{commit}" || ! git cat-file -e "$target^{commit}"; then
  echo 'The recorded upstream baseline or upstream/main is unavailable locally.' >&2
  exit 1
fi

if ! git merge-base --is-ancestor "$base" "$target"; then
  echo 'The recorded upstream baseline is not an ancestor of upstream/main. Resolve the upstream history change manually.' >&2
  exit 1
fi

mapfile -t paths < <(sed -e '/^[[:space:]]*$/d' -e '/^[[:space:]]*#/d' "$paths_file")
if [[ "${#paths[@]}" -eq 0 ]]; then
  echo 'The upstream path allowlist is empty.' >&2
  exit 1
fi

patch=$(mktemp)
trap 'rm -f "$patch"' EXIT

git diff --binary --full-index "$base" "$target" -- "${paths[@]}" > "$patch"
if [[ ! -s "$patch" ]]; then
  echo 'No allowed upstream changes are pending.'
  exit 0
fi

# --3way preserves local Relay/Rainbow changes when possible and stops on a
# conflict instead of importing unrelated upstream paths or auto-resolving it.
git apply --3way --index "$patch"
printf '%s\n' "$target" > "$base_file"
git add "$base_file"

echo "Applied allowed changes from $base to $target."
