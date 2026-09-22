#!/usr/bin/env node

// Verify that every checked-in component declaration agrees with the root
// release version. The Release workflow runs this before and after tag creation.

import { execFileSync } from 'node:child_process'
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'

// --require-tag also requires the tag to resolve to the checked-out release
// commit. --tag permits an explicit tag while still enforcing v<package version>.
const root = resolve(import.meta.dirname, '..')
const args = new Set(process.argv.slice(2))
const requireTag = args.delete('--require-tag')
const tagArg = [...args].find((arg) => arg.startsWith('--tag='))
if ([...args].length !== (tagArg ? 1 : 0)) {
  throw new Error('usage: node scripts/check-component-versions.mjs [--tag=vX.Y.Z] [--require-tag]')
}

// Read only from the repository root so the check behaves identically in CI.
function read(path) {
  return readFileSync(resolve(root, path), 'utf8')
}

function json(path) {
  return JSON.parse(read(path))
}

const expected = json('package.json').version
const errors = []
const semver = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*))?(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$/
if (!semver.test(expected)) errors.push(`package.json has an invalid semantic version: ${expected}`)
const tag = tagArg ? tagArg.slice('--tag='.length) : `v${expected}`
if (tag !== `v${expected}`) errors.push(`tag ${tag} does not match package.json version ${expected}`)

// Collect every mismatch to give the release operator one actionable failure.
function equal(label, actual) {
  if (actual !== expected) errors.push(`${label} is ${JSON.stringify(actual)}, expected ${JSON.stringify(expected)}`)
}

function match(label, path, pattern) {
  const result = read(path).match(pattern)
  if (!result) {
    errors.push(`${label} marker is missing from ${path}`)
    return
  }
  equal(label, result[1])
}

function matchAll(label, path, pattern) {
  const results = [...read(path).matchAll(pattern)]
  if (results.length === 0) {
    errors.push(`${label} marker is missing from ${path}`)
    return
  }
  for (const result of results) equal(label, result[1])
}

// npm manifests/locks, Go runtime build info, and Docker ARG defaults are all
// checked because each can otherwise drift independently.
equal('package-lock.json version', json('package-lock.json').version)
equal('package-lock.json root package version', json('package-lock.json').packages[''].version)
equal('administration/package.json version', json('administration/package.json').version)
equal('administration/package-lock.json version', json('administration/package-lock.json').version)
equal('administration/package-lock.json root package version', json('administration/package-lock.json').packages[''].version)
match('Relay and Rainbow runtime version', 'hypercerts/version/version.go', /Version = "([^"]+)"/)
match('Jetstream runtime version', 'jetstream/internal/version/version.go', /Version = "([^"]+)"/)
for (const path of ['cmd/relay/Dockerfile', 'cmd/rainbow/Dockerfile', 'jetstream/Dockerfile', 'administration/Dockerfile']) {
  matchAll(`${path} build version`, path, /ARG VERSION=([^\s]+)/g)
}

// A tag is valid only when it identifies this exact checked-out release commit.
if (requireTag && errors.length === 0) {
  try {
    const target = execFileSync('git', ['rev-parse', '--verify', `${tag}^{commit}`], { cwd: root, encoding: 'utf8' }).trim()
    const head = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8' }).trim()
    if (target !== head) errors.push(`${tag} points to ${target}, expected HEAD ${head}`)
  } catch {
    errors.push(`required tag ${tag} does not resolve to a commit`)
  }
}

if (errors.length > 0) {
  throw new Error(`component version validation failed:\n- ${errors.join('\n- ')}`)
}

console.log(`component versions match ${tag}`)
