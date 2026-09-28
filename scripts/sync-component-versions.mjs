#!/usr/bin/env node

// Synchronize generated component-version declarations after Changesets updates
// the root package manifest. The root package is the semantic version source;
// this script intentionally fails when a known declaration changes shape.

import { readFileSync, writeFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { isSemver } from './semver.mjs'

const root = resolve(import.meta.dirname, '..')
const version = JSON.parse(readFileSync(resolve(root, 'package.json'), 'utf8')).version

// Keep release tags and image metadata valid SemVer, including prereleases.
if (!isSemver(version)) {
  throw new Error(`package.json has an invalid semantic version: ${version}`)
}

// Replace exactly one expected marker so a source-layout change cannot silently
// leave a component on a stale version.
function replace(path, pattern, replacement) {
  const file = resolve(root, path)
  const source = readFileSync(file, 'utf8')
  if (!pattern.test(source)) throw new Error(`expected version marker not found in ${path}`)
  writeFileSync(file, source.replace(pattern, replacement))
}

// Package manifests and lockfile headers carry an ordinary top-level version.
function setJsonVersion(path) {
  replace(path, /("name":\s*"[^"]+",\n)(?:\s*"version":\s*"[^"]+",\n)?/, `$1  "version": "${version}",\n`)
}

// npm lockfiles repeat the workspace package version under packages[""].
function setLockfileRootVersion(path) {
  const file = resolve(root, path)
  const source = readFileSync(file, 'utf8')
  const packagesStart = source.indexOf('"packages": {')
  const rootPackageStart = source.indexOf('"": {', packagesStart)
  const rootNameStart = source.indexOf('"name":', rootPackageStart)
  const rootNameEnd = source.indexOf('\n', rootNameStart)
  const rootVersionStart = source.indexOf('"version":', rootNameEnd)
  const rootVersionEnd = source.indexOf('\n', rootVersionStart)
  const rootPackageEnd = source.indexOf('\n    }', rootPackageStart)
  if (packagesStart === -1 || rootPackageStart === -1 || rootNameStart === -1 || rootNameEnd === -1 || rootVersionStart === -1 || rootVersionEnd === -1 || rootVersionStart > rootPackageEnd) {
    throw new Error(`expected lockfile root version marker not found in ${path}`)
  }
  const indentation = source.slice(source.lastIndexOf('\n', rootVersionStart) + 1, rootVersionStart)
  writeFileSync(file, `${source.slice(0, rootVersionStart)}${indentation}"version": "${version}",${source.slice(rootVersionEnd)}`)
}

// Keep Administration's manifest and both lockfiles aligned with the release.
setJsonVersion('administration/package.json')
setJsonVersion('package-lock.json')
setLockfileRootVersion('package-lock.json')
setJsonVersion('administration/package-lock.json')
setLockfileRootVersion('administration/package-lock.json')
// Relay/Rainbow and Jetstream expose versions at runtime; Docker defaults make
// local image builds report the same release value before CI stamps metadata.
replace('hypercerts/version/version.go', /Version = "[^"]+"/, `Version = "${version}"`)
replace('jetstream/internal/version/version.go', /Version = "[^"]+"/, `Version = "${version}"`)
for (const path of ['cmd/relay/Dockerfile', 'cmd/rainbow/Dockerfile', 'jetstream/Dockerfile', 'administration/Dockerfile']) {
  const file = resolve(root, path)
  const source = readFileSync(file, 'utf8')
  if (!/ARG VERSION=[^\s]+/.test(source)) throw new Error(`expected version marker not found in ${path}`)
  writeFileSync(file, source.replaceAll(/ARG VERSION=[^\s]+/g, `ARG VERSION=${version}`))
}
