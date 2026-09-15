import { mkdir, readFile, writeFile } from 'node:fs/promises'

const ca = process.env.NODE_EXTRA_CA_CERTS
if (!ca) throw new Error('NODE_EXTRA_CA_CERTS is required')

const controlURL = 'http://relay:2471/hypercerts/v1/source'
const token = process.env.RELAY_CONTROL_TOKEN
if (!token || token.length < 32) throw new Error('RELAY_CONTROL_TOKEN is required')
const statePath = '/state/accounts.json'
const password = 'acceptance-only-password'
const collection = 'app.bsky.feed.post'
const origins = { a: 'https://pds-a.test', b: 'https://pds-b.test' }

async function request(url, options = {}) {
  const response = await fetch(url, options)
  const text = await response.text()
  let body
  try {
    body = text ? JSON.parse(text) : null
  } catch {
    body = text
  }
  if (!response.ok) {
    throw new Error(`${options.method ?? 'GET'} ${url}: ${response.status} ${text}`)
  }
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
  await request(controlURL, {
    method: 'PUT',
    headers: {
      authorization: `Bearer ${token}`,
      'content-type': 'application/json',
    },
    body: JSON.stringify({ pds: origin, state: 'enabled' }),
  })
}

async function source(origin) {
  return request(`${controlURL}?pds=${encodeURIComponent(origin)}`, {
    headers: { authorization: `Bearer ${token}` },
  })
}

function cursor(view) {
  if (!Number.isSafeInteger(view.LastDurableCursor)) {
    throw new TypeError(`source response has no safe LastDurableCursor: ${JSON.stringify(view)}`)
  }
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
  })
  if (!account.did || !account.accessJwt) throw new Error(`invalid createAccount response from ${origin}`)
  return account
}

function recordInput(account, text) {
  return {
    repo: account.did,
    collection,
    record: {
      $type: collection,
      text,
      createdAt: new Date().toISOString(),
    },
  }
}

async function createRecord(origin, account, text) {
  const result = await request(`${origin}/xrpc/com.atproto.repo.createRecord`, {
    method: 'POST',
    headers: {
      authorization: `Bearer ${account.accessJwt}`,
      'content-type': 'application/json',
    },
    body: JSON.stringify(recordInput(account, text)),
  })
  const rkey = result.uri?.split('/').at(-1)
  if (!rkey || !result.uri) throw new Error(`invalid createRecord response: ${JSON.stringify(result)}`)
  return { rkey, uri: result.uri }
}

async function updateRecord(origin, account, rkey, text) {
  const result = await request(`${origin}/xrpc/com.atproto.repo.putRecord`, {
    method: 'POST',
    headers: {
      authorization: `Bearer ${account.accessJwt}`,
      'content-type': 'application/json',
    },
    body: JSON.stringify({ ...recordInput(account, text), rkey }),
  })
  if (!result.uri) throw new Error(`invalid putRecord response: ${JSON.stringify(result)}`)
}

async function deleteRecord(origin, account, rkey) {
  await request(`${origin}/xrpc/com.atproto.repo.deleteRecord`, {
    method: 'POST',
    headers: {
      authorization: `Bearer ${account.accessJwt}`,
      'content-type': 'application/json',
    },
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
  if (cursor(view) !== expected) {
    throw new Error(`${label} changed unexpectedly: expected ${expected}, got ${cursor(view)}`)
  }
}

async function bootstrap() {
  await Promise.all([
    waitFor(`${origins.a}/xrpc/com.atproto.server.describeServer`, 'PDS A'),
    waitFor(`${origins.b}/xrpc/com.atproto.server.describeServer`, 'PDS B'),
  ])
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
  writeJSON({
    collection,
    lifecycle: [
      { did: accountA.did },
      { did: accountB.did },
    ],
    operations: [...seedA, ...seedB],
    sourceProof: {
      bBefore: cursor(bBefore),
      aAfterAccount: cursor(aAfterAccount),
      bAfterAAccount: cursor(bAfterAAccount),
      aAfterSeed: cursor(aAfterSeed),
      bAfterASeed: cursor(bAfterASeed),
      bAfterAccount: cursor(bAfterAccount),
      aAfterBAccount: cursor(aAfterBAccount),
      bAfterSeed: cursor(bAfterSeed),
      aAfterBSeed: cursor(aAfterBSeed),
    },
  })
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

  writeJSON({
    collection,
    operations: [...liveA, ...liveB],
    sourceProof: {
      aBefore: cursor(aBefore),
      aAfter: cursor(aAfter),
      bAfterA: cursor(bAfterA),
      bBefore: cursor(bBefore),
      bAfter: cursor(bAfter),
      aAfterB: cursor(aAfterB),
    },
  })
}

const command = process.argv[2]
if (command === 'bootstrap') await bootstrap()
else if (command === 'live') await live()
else if (command === 'idle') await new Promise(() => setInterval(() => {}, 2 ** 30))
else throw new Error('usage: driver.mjs bootstrap|live|idle')
