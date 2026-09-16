import http from 'node:http'

const port = Number(process.env.PORT ?? 3000)
const upstreams = {
  plc: { host: 'plc', port: 2582 },
  'pds-a': { host: 'pds-a', port: 3000 },
  'pds-b': { host: 'pds-b', port: 3000 },
}

const maxFaultHits = 10_000
let faults = {}
let hitCounters = emptyHitCounters()
let lastMatchedFaultRoute = null

function emptyHitCounters() {
  return {
    plcDIDResolution5xx: 0,
    pdsListRepos5xx: 0,
    plcSigningKeySubstitution: 0,
  }
}

function recordFault(route) {
  hitCounters[route] = Math.min(maxFaultHits, hitCounters[route] + 1)
  lastMatchedFaultRoute = route
}

function faultStatus() {
  return {
    configured: Object.keys(faults).length > 0,
    hitCounters,
    lastMatchedFaultRoute,
  }
}

function sendJSON(response, status, value) {
  response.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'no-store' })
  response.end(JSON.stringify(value))
}

async function readJSON(request) {
  const chunks = []
  let length = 0
  for await (const chunk of request) {
    length += chunk.length
    if (length > 16 << 10) throw new Error('fault configuration is too large')
    chunks.push(chunk)
  }
  const value = JSON.parse(Buffer.concat(chunks).toString('utf8'))
  if (value === null || Array.isArray(value) || typeof value !== 'object') throw new Error('fault configuration must be an object')
  return value
}

const maxDIDLength = 128
const maxMultikeyLength = 128
const base58btc = /^z[1-9A-HJ-NP-Za-km-z]+$/

function validMultikey(value) {
  return typeof value === 'string' && value.length >= 2 && value.length <= maxMultikeyLength && base58btc.test(value)
}

function validFaults(value) {
  const allowed = new Set(['targetDID', 'targetPDS', 'plcDIDResolution5xx', 'pdsListRepos5xx', 'substituteSigningKey'])
  if (Object.keys(value).some((key) => !allowed.has(key))) return false
  if (Object.hasOwn(value, 'targetDID') && (typeof value.targetDID !== 'string' || value.targetDID.length === 0 || value.targetDID.length > maxDIDLength)) return false
  if (Object.hasOwn(value, 'targetPDS') && value.targetPDS !== 'pds-a' && value.targetPDS !== 'pds-b') return false
  if (Object.hasOwn(value, 'plcDIDResolution5xx') && typeof value.plcDIDResolution5xx !== 'boolean') return false
  if (Object.hasOwn(value, 'pdsListRepos5xx') && typeof value.pdsListRepos5xx !== 'boolean') return false
  if (Object.hasOwn(value, 'substituteSigningKey')) {
    return typeof value.targetDID === 'string' && validMultikey(value.substituteSigningKey)
  }
  return true
}

function targetDID(pathname) {
  const parts = pathname.split('/').filter(Boolean)
  return parts.at(-1) ?? ''
}

function shouldRejectPLC(request, upstream) {
  return upstream === 'plc' && faults.plcDIDResolution5xx && targetDID(request.url) === faults.targetDID
}

function shouldRejectListRepos(request, upstream) {
  return faults.pdsListRepos5xx && upstream === faults.targetPDS && request.url.startsWith('/xrpc/com.atproto.sync.listRepos')
}

function substituteDocument(body) {
  const document = JSON.parse(body)
  const replacement = faults.substituteSigningKey
  if (document?.id !== faults.targetDID || !validMultikey(replacement) || !Array.isArray(document.verificationMethod)) return { body, substituted: false }
  const signingMethodID = `${faults.targetDID}#atproto`
  for (const method of document.verificationMethod) {
    if (method?.id === signingMethodID && method.controller === faults.targetDID && method.type === 'Multikey' && typeof method.publicKeyMultibase === 'string') {
      // Preserve the resolver-valid target document while substituting only its
      // atproto signing method with another fixture account's real Multikey.
      method.publicKeyMultibase = replacement
      return { body: JSON.stringify(document), substituted: true }
    }
  }
  return { body, substituted: false }
}

function proxy(request, response, upstream) {
  const target = upstreams[upstream]
  if (!target) {
    response.writeHead(502)
    response.end()
    return
  }
  if (shouldRejectPLC(request, upstream)) {
    recordFault('plcDIDResolution5xx')
    sendJSON(response, 503, { error: 'acceptance_fault' })
    return
  }
  if (shouldRejectListRepos(request, upstream)) {
    recordFault('pdsListRepos5xx')
    sendJSON(response, 503, { error: 'acceptance_fault' })
    return
  }
  const headers = { ...request.headers }
  delete headers['x-acceptance-upstream']
  const upstreamRequest = http.request({
    host: target.host,
    port: target.port,
    method: request.method,
    path: request.url,
    headers,
  }, (upstreamResponse) => {
    const rewriteDocument = upstream === 'plc' && faults.substituteSigningKey && targetDID(request.url) === faults.targetDID
    if (!rewriteDocument) {
      response.writeHead(upstreamResponse.statusCode ?? 502, upstreamResponse.headers)
      upstreamResponse.pipe(response)
      return
    }
    const chunks = []
    let length = 0
    upstreamResponse.on('data', (chunk) => {
      length += chunk.length
      if (length <= 1 << 20) chunks.push(chunk)
    })
    upstreamResponse.on('end', () => {
      if (length > 1 << 20) {
        response.writeHead(502)
        response.end()
        return
      }
      try {
        const rewritten = substituteDocument(Buffer.concat(chunks).toString('utf8'))
        if (rewritten.substituted) recordFault('plcSigningKeySubstitution')
        const headers = { ...upstreamResponse.headers, 'content-length': Buffer.byteLength(rewritten.body) }
        delete headers['content-encoding']
        response.writeHead(upstreamResponse.statusCode ?? 502, headers)
        response.end(rewritten.body)
      } catch {
        response.writeHead(502)
        response.end()
      }
    })
  })
  upstreamRequest.on('error', () => {
    if (!response.headersSent) response.writeHead(502)
    response.end()
  })
  request.pipe(upstreamRequest)
}

const server = http.createServer(async (request, response) => {
  if (request.method === 'PUT' && request.url === '/hypercerts/acceptance/faults') {
    try {
      const next = await readJSON(request)
      if (!validFaults(next)) throw new Error('invalid fault configuration')
      faults = next
      hitCounters = emptyHitCounters()
      lastMatchedFaultRoute = null
      sendJSON(response, 200, faultStatus())
    } catch {
      sendJSON(response, 400, { error: 'invalid_fault_configuration' })
    }
    return
  }
  if (request.method === 'GET' && request.url === '/hypercerts/acceptance/faults') {
    sendJSON(response, 200, faultStatus())
    return
  }
  proxy(request, response, request.headers['x-acceptance-upstream'])
})

server.listen(port, '0.0.0.0')
