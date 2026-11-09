import { mkdir, readFile, writeFile } from 'node:fs/promises'

const ca = process.env.NODE_EXTRA_CA_CERTS
if (!ca) throw new Error('NODE_EXTRA_CA_CERTS is required')

const relayControlURL = 'http://relay:2471/hypercerts/v1/source'
const relayToken = process.env.RELAY_CONTROL_TOKEN
if (!relayToken || relayToken.length < 32) throw new Error('RELAY_CONTROL_TOKEN is required')
const jetstreamControlURL = 'http://jetstream:8081/hypercerts/v1'
const faultURL = 'http://fault-proxy:3000/hypercerts/acceptance/faults'
const statePath = '/state/accounts.json'
const t15StatePath = '/state/t15-state.json'
const password = 'acceptance-only-password'
const collection = 'app.bsky.feed.post'
const origins = { a: 'https://pds-a.test', b: 'https://pds-b.test' }
const directPDS = origins.a
const plcURL = 'https://plc.test'
const acceptanceOrigins = new Set([
  'http://relay:2471',
  'http://jetstream:8081',
  'http://fault-proxy:3000',
  origins.a,
  origins.b,
  plcURL,
])

async function request(url, options = {}) {
  const target = new URL(url)
  if (!acceptanceOrigins.has(target.origin)) throw new TypeError(`unexpected acceptance request origin: ${target.origin}`)
  const response = await fetch(target, options)
  const text = await response.text()
  let body
  try {
    body = text ? JSON.parse(text) : null
  } catch {
    body = text
  }
  if (!response.ok) throw new Error(`${options.method ?? 'GET'} ${url}: ${response.status} ${typeof body === 'string' ? body.slice(0, 400) : JSON.stringify(body).slice(0, 400)}`)
  return body
}

async function waitFor(url, label) {
  const deadline = Date.now() + 90_000
  let lastError = 'not attempted'
  while (Date.now() < deadline) {
    try {
      await request(url)
      return
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error)
      await new Promise((resolve) => setTimeout(resolve, 500))
    }
  }
  throw new Error(`${label} did not become ready: ${lastError}`)
}

async function admit(origin) {
  await request(relayControlURL, {
    method: 'PUT',
    headers: { authorization: `Bearer ${relayToken}`, 'content-type': 'application/json' },
    body: JSON.stringify({ pds: origin, state: 'enabled' }),
  })
}

async function source(origin) {
  return request(`${relayControlURL}?pds=${encodeURIComponent(origin)}`, {
    headers: { authorization: `Bearer ${relayToken}` },
  })
}

function cursor(view) {
  if (!Number.isSafeInteger(view.LastDurableCursor)) throw new TypeError('source response has no safe LastDurableCursor')
  return view.LastDurableCursor
}

function writeJSON(value) {
  process.stdout.write(`${JSON.stringify(value)}\n`)
}

async function waitForCursorAdvance(origin, previous, label) {
  const deadline = Date.now() + 90_000
  let observed = previous
  while (Date.now() < deadline) {
    const view = await source(origin)
    observed = cursor(view)
    if (observed > previous) return view
    await new Promise((resolve) => setTimeout(resolve, 250))
  }
  throw new Error(`${label} did not advance Relay source cursor beyond ${previous}; last=${observed}`)
}

async function createAccount(origin, handle, email) {
  const account = await request(`${origin}/xrpc/com.atproto.server.createAccount`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ handle, email, password }),
  }).catch((error) => {
    throw new Error(`createAccount(${origin}, ${handle}): ${error.message}`)
  })
  if (!account.did || !account.accessJwt) throw new Error('invalid createAccount response')
  return account
}

async function didDocument(did) {
  const document = await request(`${plcURL}/${encodeURIComponent(did)}`)
  if (document?.id !== did || !Array.isArray(document.verificationMethod)) throw new Error('invalid PLC DID document')
  return document
}

function signingKey(document, did) {
  const method = document.verificationMethod.find((item) => item?.id === `${did}#atproto` && item.controller === did && item.type === 'Multikey')
  if (typeof method?.publicKeyMultibase !== 'string') throw new Error('PLC DID document has no Multikey atproto signing method')
  return method.publicKeyMultibase
}

function recordInput(account, text) {
  return { repo: account.did, collection, record: { $type: collection, text, createdAt: new Date().toISOString() } }
}

async function createRecord(origin, account, text) {
  const result = await request(`${origin}/xrpc/com.atproto.repo.createRecord`, {
    method: 'POST',
    headers: { authorization: `Bearer ${account.accessJwt}`, 'content-type': 'application/json' },
    body: JSON.stringify(recordInput(account, text)),
  })
  const rkey = result.uri?.split('/').at(-1)
  if (!rkey || !result.uri) throw new Error('invalid createRecord response')
  return { rkey, uri: result.uri }
}

async function updateRecord(origin, account, rkey, text) {
  const result = await request(`${origin}/xrpc/com.atproto.repo.putRecord`, {
    method: 'POST',
    headers: { authorization: `Bearer ${account.accessJwt}`, 'content-type': 'application/json' },
    body: JSON.stringify({ ...recordInput(account, text), rkey }),
  })
  if (!result.uri) throw new Error('invalid putRecord response')
}

async function deleteRecord(origin, account, rkey) {
  await request(`${origin}/xrpc/com.atproto.repo.deleteRecord`, {
    method: 'POST',
    headers: { authorization: `Bearer ${account.accessJwt}`, 'content-type': 'application/json' },
    body: JSON.stringify({ repo: account.did, collection, rkey }),
  })
}

async function mutateRecord(origin, account, phase) {
  const createText = `acceptance-${phase}-${account.did}-create`
  const updateText = `acceptance-${phase}-${account.did}-update`
  const { rkey } = await createRecord(origin, account, createText)
  await updateRecord(origin, account, rkey, updateText)
  await deleteRecord(origin, account, rkey)
  return [
    { did: account.did, operation: 'create', rkey, text: createText },
    { did: account.did, operation: 'update', rkey, text: updateText },
    { did: account.did, operation: 'delete', rkey },
  ]
}

function assertCursorUnchanged(view, expected, label) {
  if (cursor(view) !== expected) throw new Error(`${label} changed unexpectedly: expected ${expected}, got ${cursor(view)}`)
}

async function bootstrap() {
  await Promise.all([waitFor(`${origins.a}/xrpc/com.atproto.server.describeServer`, 'PDS A'), waitFor(`${origins.b}/xrpc/com.atproto.server.describeServer`, 'PDS B')])
  await admit(origins.a)
  await admit(origins.b)

  const bBefore = await source(origins.b)
  const accountA = await createAccount(origins.a, 'alice.pds-a.test', 'alice@example.test')
  const aAfterAccount = await waitForCursorAdvance(origins.a, -1, 'PDS A account lifecycle')
  const bAfterAAccount = await source(origins.b)
  assertCursorUnchanged(bAfterAAccount, cursor(bBefore), 'PDS B cursor after PDS A account creation')

  const seedA = await mutateRecord(origins.a, accountA, 'seed')
  const aAfterSeed = await waitForCursorAdvance(origins.a, cursor(aAfterAccount), 'PDS A seed mutations')
  const bAfterASeed = await source(origins.b)
  assertCursorUnchanged(bAfterASeed, cursor(bBefore), 'PDS B cursor after PDS A seed mutations')

  const accountB = await createAccount(origins.b, 'bob.pds-b.test', 'bob@example.test')
  const bAfterAccount = await waitForCursorAdvance(origins.b, cursor(bBefore), 'PDS B account lifecycle')
  const aAfterBAccount = await source(origins.a)
  assertCursorUnchanged(aAfterBAccount, cursor(aAfterSeed), 'PDS A cursor after PDS B account creation')

  const seedB = await mutateRecord(origins.b, accountB, 'seed')
  const bAfterSeed = await waitForCursorAdvance(origins.b, cursor(bAfterAccount), 'PDS B seed mutations')
  const aAfterBSeed = await source(origins.a)
  assertCursorUnchanged(aAfterBSeed, cursor(aAfterSeed), 'PDS A cursor after PDS B seed mutations')

  await mkdir('/state', { recursive: true })
  await writeFile(statePath, JSON.stringify({ a: accountA, b: accountB }), { mode: 0o600 })
  writeJSON({ collection, lifecycle: [{ did: accountA.did }, { did: accountB.did }], operations: [...seedA, ...seedB], sourceProof: { bBefore: cursor(bBefore), aAfterAccount: cursor(aAfterAccount), bAfterAAccount: cursor(bAfterAAccount), aAfterSeed: cursor(aAfterSeed), bAfterASeed: cursor(bAfterASeed), bAfterAccount: cursor(bAfterAccount), aAfterBAccount: cursor(aAfterBAccount), bAfterSeed: cursor(bAfterSeed), aAfterBSeed: cursor(aAfterBSeed) } })
}

async function live() {
  const accounts = JSON.parse(await readFile(statePath, 'utf8'))
  const aBefore = await source(origins.a)
  const bBefore = await source(origins.b)
  const liveA = await mutateRecord(origins.a, accounts.a, 'live')
  const aAfter = await waitForCursorAdvance(origins.a, cursor(aBefore), 'PDS A live mutations')
  const bAfterA = await source(origins.b)
  assertCursorUnchanged(bAfterA, cursor(bBefore), 'PDS B cursor after PDS A live mutations')
  const liveB = await mutateRecord(origins.b, accounts.b, 'live')
  const bAfter = await waitForCursorAdvance(origins.b, cursor(bBefore), 'PDS B live mutations')
  const aAfterB = await source(origins.a)
  assertCursorUnchanged(aAfterB, cursor(aAfter), 'PDS A cursor after PDS B live mutations')
  writeJSON({ collection, operations: [...liveA, ...liveB], sourceProof: { aBefore: cursor(aBefore), aAfter: cursor(aAfter), bAfterA: cursor(bAfterA), bBefore: cursor(bBefore), bAfter: cursor(bAfter), aAfterB: cursor(aAfterB) } })
}

async function jetstreamToken() {
  const token = (await readFile('/run/acceptance/jetstream-control-token', 'utf8')).trim()
  if (token.length < 32) throw new Error('mounted Jetstream control token is invalid')
  return token
}

async function privatePolicy() {
  const token = await jetstreamToken()
  const policy = await request(`${jetstreamControlURL}/policy`, { headers: { authorization: `Bearer ${token}` } })
  if (!Number.isSafeInteger(policy?.revision) || policy.revision < 1) throw new Error('private Jetstream policy response is invalid')
}

async function waitForPrivatePolicy(label) {
  const deadline = Date.now() + 90_000
  let lastError = 'not attempted'
  while (Date.now() < deadline) {
    try {
      await privatePolicy()
      return
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error)
      await new Promise((resolve) => setTimeout(resolve, 500))
    }
  }
  throw new Error(`${label} did not become ready through authenticated private /policy: ${lastError}`)
}

async function control(path, options = {}) {
  // Every private control call first proves the authenticated listener is ready.
  // This also handles a Jetstream restart between driver phases without using a
  // public listener (whose root route may legitimately be 404).
  await waitForPrivatePolicy(`Jetstream control ${options.method ?? 'GET'} ${path}`)
  const token = await jetstreamToken()
  return request(`${jetstreamControlURL}${path}`, {
    ...options,
    headers: { authorization: `Bearer ${token}`, ...options.headers },
  })
}

function assertFaultStatus(value, label, configured) {
  if (value?.configured !== configured || typeof value?.hitCounters !== 'object' || value.hitCounters === null) {
    throw new Error(`${label} returned an invalid fault status`)
  }
  for (const route of ['plcDIDResolution5xx', 'pdsListRepos5xx', 'plcSigningKeySubstitution']) {
    const hits = value.hitCounters[route]
    if (!Number.isSafeInteger(hits) || hits < 0 || hits > 10_000) throw new Error(`${label} returned an invalid ${route} hit count`)
  }
  if (value.lastMatchedFaultRoute !== null && !Object.hasOwn(value.hitCounters, value.lastMatchedFaultRoute)) {
    throw new Error(`${label} returned an invalid last matched fault route`)
  }
}

async function configureFaults(value) {
  const status = await request(faultURL, { method: 'PUT', headers: { 'content-type': 'application/json' }, body: JSON.stringify(value) })
  assertFaultStatus(status, 'fault reset/configuration', Object.keys(value).length > 0)
  if (Object.values(status.hitCounters).some((hits) => hits !== 0) || status.lastMatchedFaultRoute !== null) {
    throw new Error('fault reset/configuration did not clear fault hit status')
  }
}

async function assertFaultHit(route, label) {
  const status = await request(faultURL)
  assertFaultStatus(status, label, true)
  if (status.hitCounters[route] < 1 || status.lastMatchedFaultRoute !== route) {
    throw new Error(`${label} did not hit configured ${route} target`)
  }
  return { route, hits: status.hitCounters[route] }
}

async function job(id) {
  return control(`/jobs/${encodeURIComponent(id)}`)
}

async function waitForJob(id, states, label) {
  const deadline = Date.now() + 120_000
  let latest
  while (Date.now() < deadline) {
    latest = await job(id)
    if (states.includes(latest.state)) return latest
    if (['complete', 'failed', 'incomplete', 'canceled'].includes(latest.state)) {
      throw new Error(`${label} reached unexpected terminal job view ${JSON.stringify(jobEvidence(latest))}`)
    }
    await new Promise((resolve) => setTimeout(resolve, 250))
  }
  throw new Error(`${label} did not reach ${states.join(',')}; last=${JSON.stringify(jobEvidence(latest))}`)
}

async function requestDirectJob(requestId) {
  return control('/jobs', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ pds: directPDS, reason: 'backfill', requestId }) })
}

async function retryJob(id, requestId) {
  return control(`/jobs/${encodeURIComponent(id)}/retry`, { method: 'POST', headers: { 'Idempotency-Key': requestId } })
}

async function listRejections() {
  return control(`/snapshot-rejections?pds=${encodeURIComponent(directPDS)}`)
}

async function readT15State() {
  return JSON.parse(await readFile(t15StatePath, 'utf8'))
}

async function saveT15State(state) {
  await mkdir('/state', { recursive: true })
  await writeFile(t15StatePath, JSON.stringify(state), { mode: 0o600 })
}

function jobEvidence(value) {
  if (!value || typeof value !== 'object') return { available: false }
  return {
    id: value.id,
    pds: value.pds,
    reason: value.reason,
    state: value.state,
    attempts: value.attempts,
    completedRepos: value.completedRepos,
    totalRepos: value.totalRepos,
    totalReposKnown: value.totalReposKnown,
    cursor: value.cursor,
    errorCode: value.errorCode ?? '',
    coverage: value.coverage,
    recoveryDidCount: value.recoveryDidCount,
  }
}

async function assertNoRejection(label) {
  const result = await listRejections()
  if (!Array.isArray(result.rejections) || result.rejections.length !== 0) throw new Error(`${label} created a permanent rejection`)
}

async function t15Valid() {
  await waitFor(`${directPDS}/xrpc/com.atproto.server.describeServer`, 'T15 PDS')
  await configureFaults({})
  const account = await createAccount(directPDS, 'valid.pds-a.test', 'valid@example.test')
  const record = await createRecord(directPDS, account, 't15-valid-selected-record')
  const created = await control('/sources', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ pds: directPDS }) })
  const completed = await waitForJob(created.id, ['complete'], 'valid direct-PDS job')
  await assertNoRejection('valid direct-PDS job')
  const state = { valid: { did: account.did, rkey: record.rkey, job: jobEvidence(completed) } }
  await saveT15State(state)
  writeJSON(state)
}

async function t15IdentityFault() {
  const state = await readT15State()
  const account = await createAccount(directPDS, 'identity.pds-a.test', 'identity@example.test')
  const record = await createRecord(directPDS, account, 't15-identity-transient-record')
  await configureFaults({ targetDID: account.did, plcDIDResolution5xx: true })
  const created = await requestDirectJob('t15-identity-fault')
  const incomplete = await waitForJob(created.id, ['incomplete'], 'identity fault job')
  const fault = await assertFaultHit('plcDIDResolution5xx', 'identity fault job')
  await assertNoRejection('identity fault job')
  state.identity = { did: account.did, rkey: record.rkey, fault, job: jobEvidence(incomplete) }
  await saveT15State(state)
  writeJSON(state.identity)
}

async function t15IdentityRetry() {
  const state = await readT15State()
  await configureFaults({})
  await retryJob(state.identity.job.id, 't15-identity-retry')
  const completed = await waitForJob(state.identity.job.id, ['complete'], 'identity retry job')
  await assertNoRejection('identity retry job')
  state.identity.retry = jobEvidence(completed)
  await saveT15State(state)
  writeJSON(state.identity.retry)
}

async function t15SourceFault() {
  const state = await readT15State()
  const account = await createAccount(directPDS, 'recovered.pds-a.test', 'recovered@example.test')
  const record = await createRecord(directPDS, account, 't15-source-transient-record')
  await configureFaults({ targetPDS: 'pds-a', pdsListRepos5xx: true })
  const created = await requestDirectJob('t15-source-fault')
  const incomplete = await waitForJob(created.id, ['incomplete'], 'source fault job')
  const fault = await assertFaultHit('pdsListRepos5xx', 'source fault job')
  await assertNoRejection('source fault job')
  state.source = { did: account.did, rkey: record.rkey, fault, job: jobEvidence(incomplete) }
  await saveT15State(state)
  writeJSON(state.source)
}

async function t15SourceRetry() {
  const state = await readT15State()
  await configureFaults({})
  await retryJob(state.source.job.id, 't15-source-retry')
  const completed = await waitForJob(state.source.job.id, ['complete'], 'source retry job')
  await assertNoRejection('source retry job')
  state.source.retry = jobEvidence(completed)
  await saveT15State(state)
  writeJSON(state.source.retry)
}

async function t15InvalidSignature() {
  const state = await readT15State()
  const account = await createAccount(directPDS, 'invalid.pds-a.test', 'invalid@example.test')
  const record = await createRecord(directPDS, account, 't15-invalid-signature-record')
  const substitute = await createAccount(directPDS, 'substitute.pds-a.test', 'substitute@example.test')
  const targetKey = signingKey(await didDocument(account.did), account.did)
  const substituteKey = signingKey(await didDocument(substitute.did), substitute.did)
  if (targetKey === substituteKey) throw new Error('target and substitute signing keys unexpectedly match')
  await configureFaults({ targetDID: account.did, substituteSigningKey: substituteKey })
  const created = await requestDirectJob('t15-invalid-signature')
  let terminal
  try {
    terminal = await waitForJob(created.id, ['failed'], 'invalid signature job')
  } catch (error) {
    terminal = await job(created.id).catch(() => terminal)
    state.invalid = { did: account.did, rkey: record.rkey, substituteDID: substitute.did, job: jobEvidence(terminal) }
    await saveT15State(state)
    writeJSON(state.invalid)
    throw error
  }
  const fault = await assertFaultHit('plcSigningKeySubstitution', 'invalid signature job')
  state.invalid = { did: account.did, rkey: record.rkey, substituteDID: substitute.did, fault, job: jobEvidence(terminal) }
  await saveT15State(state)
  writeJSON(state.invalid)
  if (terminal.errorCode !== 'verification_failed') throw new Error(`invalid signature job view ${JSON.stringify(jobEvidence(terminal))}`)
}

async function t15CheckRejection() {
  const state = await readT15State()
  await configureFaults({})
  const result = await listRejections()
  if (!Array.isArray(result.rejections) || result.rejections.length !== 1) throw new Error('invalid signature rejection was not singular and durable')
  const rejection = result.rejections[0]
  if (rejection.did !== state.invalid.did || rejection.code !== 'verification_failed' || rejection.kind !== 'direct_pds_snapshot') throw new Error('invalid signature rejection metadata is wrong')
  for (const forbidden of ['payload', 'record', 'accessJwt', 'refreshJwt']) {
    if (Object.hasOwn(rejection, forbidden) || JSON.stringify(rejection).includes(forbidden)) throw new Error(`rejection exposes ${forbidden}`)
  }
  state.invalid.rejection = { did: rejection.did, code: rejection.code, kind: rejection.kind, policyRevision: rejection.policyRevision }
  await saveT15State(state)
  writeJSON(state.invalid.rejection)
}

// T20: Jetstream is stopped while the Relay accumulates events for one account.
// When Jetstream restarts it resumes from its persisted cursor and the Relay
// replays the whole backlog as fast as the socket allows, with no write pacing.
// That backlog is what overflows the per-DID verify queue -- driving a live PDS
// burst cannot, because a PDS emits only ~80 signed commits/second while a
// replay is bounded only by the socket. The overflow must be booked as a
// durable, repository-scoped recovery job rather than lost silently.
const t20StatePath = '/state/t20-state.json'
const t20BacklogCount = Number(process.env.T20_BACKLOG_COUNT ?? '400')

async function t20State() {
  try {
    return JSON.parse(await readFile(t20StatePath, 'utf8'))
  } catch {
    return {}
  }
}

async function saveT20State(state) {
  await mkdir('/state', { recursive: true })
  await writeFile(t20StatePath, `${JSON.stringify(state)}\n`)
}

// Admits the direct-PDS source to the job manager. A recovery job is booked
// against a source that manager already knows, and the request is idempotent when
// it is already admitted, so this runs once before Jetstream is stopped.
async function t20AdmitSource() {
  await control('/sources', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ pds: origins.a }),
  })
  writeJSON({ pds: origins.a })
}

// Runs while Jetstream is stopped. Each await is a distinct signed commit, so
// the Relay persists one subscribeRepos event per record for this DID.
async function t20ReleaseBacklog() {
  await waitFor(`${origins.a}/xrpc/com.atproto.server.describeServer`, 'T20 PDS')
  await admit(origins.a)
  // Handles have a strict length budget, so the unique segment stays short.
  const suffix = Date.now().toString(32).slice(-6)
  const account = await createAccount(origins.a, `b${suffix}.pds-a.test`, `b${suffix}@example.test`)
  const started = Date.now()
  const concurrency = 16
  let lastRkey = ''
  for (let offset = 0; offset < t20BacklogCount; offset += concurrency) {
    const batch = []
    for (let index = offset; index < Math.min(offset + concurrency, t20BacklogCount); index += 1) {
      batch.push(createRecord(origins.a, account, `t20-backlog-${index}`))
    }
    const results = await Promise.all(batch)
    lastRkey = results.at(-1)?.rkey ?? lastRkey
  }
  // The Relay must have persisted this DID's events before Jetstream resumes,
  // otherwise the replay carries fewer events than the scenario assumes.
  await waitForCursorAdvance(origins.a, -1, 'T20 backlog ingestion')
  const state = await t20State()
  state.backlog = {
    did: account.did,
    count: t20BacklogCount,
    elapsedMs: Date.now() - started,
    lastRkey,
  }
  await saveT20State(state)
  writeJSON(state.backlog)
}

// Wait for the restarted consumer to book a DID-scoped recovery job. No booking
// after a proven replay backlog is exactly the silent-loss failure T20 exists
// to catch, so this fails rather than passing empty.
async function t20WaitJob() {
  const state = await t20State()
  const did = state.backlog?.did
  if (!did) throw new Error('t20-wait-job requires a prior t20-backlog')
  const deadline = Date.now() + 240_000
  let latest = []
  while (Date.now() < deadline) {
    const listing = await control(`/jobs?pds=${encodeURIComponent(origins.a)}`)
    latest = (listing?.jobs ?? []).filter((item) => item.reason === 'repository_recovery')
    const match = latest.find((item) => Number(item.recoveryDidCount ?? 0) > 0)
    if (match) {
      state.job = jobEvidence({ ...(await job(match.id)), recoveryDidCount: match.recoveryDidCount })
      state.recoveryJobs = latest.map((item) => ({ id: item.id, state: item.state, recoveryDidCount: item.recoveryDidCount }))
      await saveT20State(state)
      writeJSON(state.job)
      return
    }
    await new Promise((resolve) => setTimeout(resolve, 1000))
  }
  state.recoveryJobs = latest.map((item) => ({ id: item.id, state: item.state, recoveryDidCount: item.recoveryDidCount }))
  await saveT20State(state)
  writeJSON({ did, recoveryJobs: state.recoveryJobs })
  throw new Error(`no repository_recovery job was booked for ${did} after the replay backlog; a verify-queue overflow was silently lost`)
}

// The recovery job must reach a terminal state without claiming whole-source
// coverage over a PDS it never scanned.
async function t20Complete() {
  const state = await t20State()
  if (!state.job?.id) throw new Error('t20-complete requires a prior t20-wait-job')
  const terminal = await waitForJob(state.job.id, ['complete', 'incomplete'], 'T20 recovery job')
  state.job = jobEvidence({ ...terminal, recoveryDidCount: state.job.recoveryDidCount })
  state.coverage = await control(`/coverage?pds=${encodeURIComponent(origins.a)}`)
  await saveT20State(state)
  writeJSON({ job: state.job, coverage: state.coverage })
}

const command = process.argv[2]
if (command === 'bootstrap') await bootstrap()
else if (command === 'live') await live()
else if (command === 't15-valid') await t15Valid()
else if (command === 't15-identity-fault') await t15IdentityFault()
else if (command === 't15-identity-retry') await t15IdentityRetry()
else if (command === 't15-source-fault') await t15SourceFault()
else if (command === 't15-source-retry') await t15SourceRetry()
else if (command === 't15-invalid-signature') await t15InvalidSignature()
else if (command === 't15-check-rejection') await t15CheckRejection()
else if (command === 't20-admit') await t20AdmitSource()
else if (command === 't20-backlog') await t20ReleaseBacklog()
else if (command === 't20-wait-job') await t20WaitJob()
else if (command === 't20-complete') await t20Complete()
else if (command === 'idle') await new Promise(() => setInterval(() => {}, 2 ** 30))
else throw new Error('usage: driver.mjs bootstrap|live|t15-valid|t15-identity-fault|t15-identity-retry|t15-source-fault|t15-source-retry|t15-invalid-signature|t15-check-rejection|t20-admit|t20-backlog|t20-wait-job|t20-complete|idle')
