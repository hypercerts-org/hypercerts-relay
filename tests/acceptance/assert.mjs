import { readFile } from 'node:fs/promises'

function fail(message) {
  throw new Error(`acceptance assertion failed: ${message}`)
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
  const identity = events.some((event) => event.did === expected.did && event.kind === 'identity' &&
    event.identity?.did === expected.did && Number.isSafeInteger(event.identity?.seq) &&
    event.identity.seq > 0 && typeof event.identity.time === 'string' && event.identity.time)
  if (!identity) fail(`${label} lacks a well-formed identity event for ${expected.did}`)

  const account = events.some((event) => event.did === expected.did && event.kind === 'account' &&
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

async function assertArchive(events, bootstrap, snapshotPath) {
  if (!snapshotPath) fail('archive requires snapshot.json')
  const snapshot = await readJSON(snapshotPath)
  const boundary = snapshot.sealedTipSeq
  if (!Number.isSafeInteger(boundary) || boundary <= 0) fail(`invalid sealedTipSeq ${boundary}`)
  assertExpected(events, bootstrap, 'archive', boundary, true, 'seed')
}

function assertSeedReady(events, bootstrap) {
  assertExpected(events, bootstrap, 'combined archive prefix', null, true, 'seed')
}

async function assertT15Archive(events, phase) {
  if (!phase?.valid?.did || !phase.valid.rkey || !phase?.identity?.did || !phase.identity.rkey ||
    !phase?.source?.did || !phase.source.rkey || !phase?.invalid?.did || !phase.invalid.rkey) {
    fail('T15 phase artifact is missing record coordinates')
  }
  // A direct-PDS snapshot materializes the current selected record as an update.
  for (const [label, record] of [['valid', phase.valid], ['identity retry', phase.identity], ['source retry', phase.source]]) {
    const selected = events.filter((event) => event.did === record.did && event.kind === 'commit' &&
      event.commit?.operation === 'update' && event.commit?.collection === 'app.bsky.feed.post' && event.commit?.rkey === record.rkey)
    if (selected.length !== 1) fail(`T15 archive expected exactly one selected ${label} record, got ${selected.length}`)
  }
  const invalid = events.filter((event) => event.did === phase.invalid.did && event.kind === 'commit' && event.commit?.rkey === phase.invalid.rkey)
  if (invalid.length !== 0) fail(`T15 archive contains invalid-signature record (${invalid.length} events)`)
}

async function assertCombined(events, bootstrap, snapshotPath, livePath) {
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
}

// T20 proves that a verify-queue overflow becomes durable, repository-scoped
// recovery rather than silent loss. The scenario reads the driver's evidence
// file directly: a backlog that never overflowed proves nothing, so that case
// fails explicitly instead of passing an empty assertion.
async function assertT20Recovery(statePath) {
  const state = await readJSON(statePath)
  const backlog = state.backlog
  if (!backlog || typeof backlog.did !== 'string' || !backlog.did.startsWith('did:')) {
    fail('T20 has no recorded backlog DID; the driver did not run t20-backlog')
  }
  // The queue holds Parallelism*2 = 64 events per DID, so a smaller backlog
  // cannot overflow it and the scenario would prove nothing.
  if (!Number.isSafeInteger(backlog.count) || backlog.count < 65) {
    fail(`T20 backlog was too small to exceed the per-DID verify queue: ${backlog.count}`)
  }

  const jobs = state.recoveryJobs
  if (!Array.isArray(jobs) || jobs.length === 0) {
    fail(`T20 booked no repository_recovery job for ${backlog.did}; the replayed backlog was silently lost`)
  }
  const scoped = jobs.filter((item) => Number(item.recoveryDidCount ?? 0) > 0)
  if (scoped.length === 0) {
    fail(`T20 booked recovery jobs but none carried a DID scope: ${JSON.stringify(jobs)}`)
  }
  // One PDS-wide backfill would re-download every repository. Exactly the
  // requested DIDs must be in scope instead.
  for (const item of jobs) {
    if (item.reason !== undefined && item.reason !== 'repository_recovery') {
      fail(`T20 booked a job with unexpected reason ${item.reason}`)
    }
  }

  const job = state.job
  if (!job || typeof job.id !== 'string') {
    fail('T20 completed no recovery job; the driver did not run t20-complete')
  }
  if (!['complete', 'incomplete'].includes(job.state)) {
    fail(`T20 recovery job ended in ${job.state}, expected complete or incomplete`)
  }

  // A repository-scoped job must never be presented as whole-source coverage.
  const items = state.coverage?.items ?? state.coverage?.sources ?? []
  const row = Array.isArray(items) ? items.find((item) => item?.pds === job.pds) : undefined
  if (row) {
    if (row.matchingJob === true && row.jobId === job.id) {
      fail(`T20 recovery job ${job.id} was published as matching current-state coverage for ${job.pds}`)
    }
    const coverageLabel = row.acquisition?.state ?? row.legacyProgress?.coverage ?? row.coverage
    if (coverageLabel === 'current_state' && row.jobId === job.id) {
      fail(`T20 recovery job ${job.id} claimed current_state coverage for ${job.pds}`)
    }
  }
}

async function main() {
  const [mode, bootstrapPath, eventPath, snapshotPath, livePath] = process.argv.slice(2)

  // t20-recovery reads one state document rather than an event stream, so it is
  // dispatched before the stream-shaped argument check below.
  if (mode === 't20-recovery') {
    if (!bootstrapPath) {
      throw new Error('usage: assert.mjs t20-recovery state.json')
    }
    await assertT20Recovery(bootstrapPath)
    return
  }

  if (!mode || !bootstrapPath || !eventPath) {
    throw new Error('usage: assert.mjs archive|seed-ready|combined|t15-archive input.json events.ndjson [snapshot.json live.json] (t20-recovery takes a state path as its first argument)')
  }
  const bootstrap = await readJSON(bootstrapPath)
  const events = await readEvents(eventPath)
  assertStrictlyIncreasing(events, mode)

  if (mode === 'archive') {
    await assertArchive(events, bootstrap, snapshotPath)
    return
  }

  if (mode === 'seed-ready') {
    assertSeedReady(events, bootstrap)
    return
  }

  if (mode === 'combined') {
    await assertCombined(events, bootstrap, snapshotPath, livePath)
    return
  }

  if (mode === 't15-archive') {
    await assertT15Archive(events, bootstrap)
    return
  }

  throw new Error(`unknown assertion mode: ${mode}`)
}

await main()
