import { readFile } from 'node:fs/promises'

function fail(message) {
  throw new Error(`T01 assertion failed: ${message}`)
}

async function readJSON(path) {
  return JSON.parse(await readFile(path, 'utf8'))
}

async function readEvents(path) {
  const text = await readFile(path, 'utf8')
  return text.split('\n').filter(Boolean).map((line, index) => {
    try {
      return JSON.parse(line)
    } catch (error) {
      fail(`${path}:${index + 1} is not JSON: ${error.message}`)
    }
  })
}

function assertStrictlyIncreasing(events, label) {
  let previous = -1
  for (const event of events) {
    if (!Number.isSafeInteger(event.cursor) || event.cursor <= previous) {
      fail(`${label} cursor is not globally unique and increasing: ${event.cursor} after ${previous}`)
    }
    previous = event.cursor
  }
}

function matchOperation(event, expected, collection) {
  return event.did === expected.did &&
    event.kind === 'commit' &&
    event.commit?.operation === expected.operation &&
    event.commit?.collection === collection &&
    event.commit?.rkey === expected.rkey
}

function assertOperation(events, expected, collection, label) {
  const matches = events.filter((event) => matchOperation(event, expected, collection))
  if (matches.length !== 1) {
    fail(`${label} expected exactly one ${expected.operation} for ${expected.did}/${expected.rkey}, got ${matches.length}`)
  }
  const event = matches[0]
  if (expected.operation === 'delete') {
    if (event.commit.record != null || event.commit.cid != null) {
      fail(`${label} delete for ${expected.did}/${expected.rkey} retained a record or CID`)
    }
  } else if (event.commit.record?.text !== expected.text) {
    fail(`${label} ${expected.operation} for ${expected.did}/${expected.rkey} has unexpected text`)
  }
  return event
}

function assertLifecycle(events, expected, label) {
  const identity = events.find((event) => event.did === expected.did && event.kind === 'identity' &&
    event.identity?.did === expected.did && Number.isSafeInteger(event.identity?.seq) &&
    event.identity.seq > 0 && typeof event.identity.time === 'string' && event.identity.time)
  if (!identity) fail(`${label} lacks a well-formed identity event for ${expected.did}`)

  const account = events.find((event) => event.did === expected.did && event.kind === 'account' &&
    event.account?.did === expected.did && event.account?.active === true && !event.account.status &&
    Number.isSafeInteger(event.account.seq) && event.account.seq > 0 &&
    typeof event.account.time === 'string' && event.account.time)
  if (!account) fail(`${label} lacks a well-formed active account event for ${expected.did}`)
}

function assertOperationOrdering(events, label) {
  const mutations = new Map()
  for (const event of events) {
    const key = `${event.did}/${event.commit.rkey}`
    const mutation = mutations.get(key) ?? {}
    mutation[event.commit.operation] = event.cursor
    mutations.set(key, mutation)
  }
  for (const [key, mutation] of mutations) {
    if (!(mutation.create < mutation.update && mutation.update < mutation.delete)) {
      fail(`${label} mutation order is not create < update < delete for ${key}`)
    }
  }
}

function assertSourceProof(proof, phase) {
  if (!proof || typeof proof !== 'object') fail(`${phase} has no source cursor proof`)
  if (phase === 'seed') {
    if (!(proof.aAfterAccount >= 0 && proof.aAfterSeed > proof.aAfterAccount &&
      proof.bAfterAccount > proof.bBefore && proof.bAfterSeed > proof.bAfterAccount &&
      proof.bAfterAAccount === proof.bBefore && proof.bAfterASeed === proof.bBefore &&
      proof.aAfterBAccount === proof.aAfterSeed && proof.aAfterBSeed === proof.aAfterSeed)) {
      fail('seed source cursors did not advance independently')
    }
  } else if (!(proof.aAfter > proof.aBefore && proof.bAfter > proof.bBefore &&
    proof.bAfterA === proof.bBefore && proof.aAfterB === proof.aAfter)) {
    fail('live source cursors did not advance independently')
  }
}

function assertExpected(events, expected, label, boundary, requireLifecycle, proofPhase) {
  assertSourceProof(expected.sourceProof, proofPhase)
  const found = expected.operations.map((operation) => assertOperation(events, operation, expected.collection, label))
  if (boundary != null) {
    for (const event of found) {
      if (event.cursor > boundary) fail(`${label} event cursor ${event.cursor} exceeds sealed boundary ${boundary}`)
    }
  }
  assertOperationOrdering(found, label)
  if (requireLifecycle) {
    for (const lifecycle of expected.lifecycle) assertLifecycle(events, lifecycle, label)
  }
  return found
}

async function main() {
  const [mode, bootstrapPath, eventPath, snapshotPath, livePath] = process.argv.slice(2)
  if (!mode || !bootstrapPath || !eventPath) {
    throw new Error('usage: assert.mjs archive|seed-ready|combined bootstrap.json events.ndjson [snapshot.json live.json]')
  }
  const bootstrap = await readJSON(bootstrapPath)
  const events = await readEvents(eventPath)
  assertStrictlyIncreasing(events, mode)

  if (mode === 'archive') {
    if (!snapshotPath) fail('archive requires snapshot.json')
    const snapshot = await readJSON(snapshotPath)
    const boundary = snapshot.sealedTipSeq
    if (!Number.isSafeInteger(boundary) || boundary <= 0) fail(`invalid sealedTipSeq ${boundary}`)
    assertExpected(events, bootstrap, 'archive', boundary, true, 'seed')
    return
  }

  if (mode === 'seed-ready') {
    assertExpected(events, bootstrap, 'combined archive prefix', null, true, 'seed')
    return
  }

  if (mode === 'combined') {
    if (!snapshotPath || !livePath) fail('combined requires snapshot.json and live.json')
    const snapshot = await readJSON(snapshotPath)
    const boundary = snapshot.sealedTipSeq
    if (!Number.isSafeInteger(boundary) || boundary <= 0) fail(`invalid sealedTipSeq ${boundary}`)
    const live = await readJSON(livePath)
    const seedEvents = assertExpected(events, bootstrap, 'combined seed archive', boundary, true, 'seed')
    const liveEvents = assertExpected(events, live, 'combined live', null, false, 'live')
    for (const event of liveEvents) {
      if (event.cursor <= boundary) fail(`live event cursor ${event.cursor} is not after sealed boundary ${boundary}`)
    }
    const expectedCount = seedEvents.length + liveEvents.length
    const actualExpectedCount = events.filter((event) =>
      [...bootstrap.operations, ...live.operations].some((operation) => matchOperation(event, operation, bootstrap.collection)),
    ).length
    if (actualExpectedCount !== expectedCount) {
      fail(`combined stream has duplicate expected operations: expected ${expectedCount}, got ${actualExpectedCount}`)
    }
    return
  }

  throw new Error(`unknown assertion mode: ${mode}`)
}

await main()
