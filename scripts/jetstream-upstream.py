#!/usr/bin/env python3
"""Review/apply only the recorded Jetstream closure, never an upstream merge."""
import argparse
from pathlib import Path
import subprocess


def git(*args, data=None):
    return subprocess.check_output(["git", *args], input=data)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("target", help="commit fetched from bluesky-social/jetstream")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    root = Path(git("rev-parse", "--show-toplevel").decode().strip())
    import os
    os.chdir(root)
    base_file = root / ".hypercerts/jetstream-upstream-base"
    base = base_file.read_text().strip()
    target = git("rev-parse", "--verify", args.target + "^{commit}").decode().strip()
    git("merge-base", "--is-ancestor", base, target)
    paths = (root / ".hypercerts/jetstream-upstream-paths").read_text().splitlines()
    if not paths or any(not p or p.startswith(("/", ":")) or ".." in Path(p).parts for p in paths):
        raise SystemExit("Invalid Jetstream path allowlist")
    changed = git("diff", "--name-only", base, target, "--", *paths).decode().splitlines()
    print(f"Jetstream upstream: {base} -> {target}", flush=True)
    print("Changed included paths:", *changed, sep="\n", flush=True)
    marked = [p for p in changed if (root / "jetstream" / p).is_file()
              and b"hypercerts:" in (root / "jetstream" / p).read_bytes()]
    print("Affected Hypercerts markers:", *(marked or ["None"]), sep="\n", flush=True)
    print("Required: ./scripts/verify-jetstream.sh and git diff --check. "
          "The verification script includes the long-running oracle and restart checks. "
          "A maintainer must resolve conflicts and a human must merge the review PR.", flush=True)
    if not args.apply:
        return
    if git("branch", "--show-current").decode().strip() in ("", "main"):
        raise SystemExit("Apply requires a named review branch other than main")
    if git("status", "--porcelain"):
        raise SystemExit("Apply requires a clean working tree")
    patch = git("diff", "--binary", "--full-index", base, target, "--", *paths)
    if patch:
        # Leave conflicts for a maintainer. Never continue to baseline advancement.
        git("apply", "--3way", "--directory=jetstream", "-", data=patch)
    base_file.write_text(target + "\n")
    git("add", ".hypercerts/jetstream-upstream-base")


if __name__ == "__main__":
    main()
