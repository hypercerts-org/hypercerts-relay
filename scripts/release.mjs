#!/usr/bin/env node

// Tag the current Changesets version only after its component declarations agree.
// The second check proves the tag was created at this checked-out release commit.

import { execFileSync } from 'node:child_process'
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'

const root = resolve(import.meta.dirname, '..')
const version = JSON.parse(readFileSync(resolve(root, 'package.json'), 'utf8')).version
const tag = `v${version}`

// Preserve child output and exit status for the Release workflow log.
function run(command, args) {
  execFileSync(command, args, { cwd: root, stdio: 'inherit' })
}

// Changesets owns tag naming; this wrapper adds the pre/post consistency gates.
run('node', ['scripts/check-component-versions.mjs', `--tag=${tag}`])
run('node', ['node_modules/@changesets/cli/bin.js', 'git-tag'])
run('node', ['scripts/check-component-versions.mjs', `--tag=${tag}`, '--require-tag'])
