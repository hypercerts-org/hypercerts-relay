import http from 'node:http'

const port = Number(process.env.PORT ?? 3000)
const plcUpstream = Object.freeze({ name: 'plc', host: 'plc', port: 2582 })
const pdsAUpstream = Object.freeze({ name: 'pds-a', host: 'pds-a', port: 3000 })
const pdsBUpstream = Object.freeze({ name: 'pds-b', host: 'pds-b', port: 3000 })

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
  const parts = pathname.split('?', 1)[0].split('/').filter(Boolean)
  return parts.at(-1) ?? ''
}

function selectedUpstream(value) {
  switch (value) {
    case 'plc': return plcUpstream
    case 'pds-a': return pdsAUpstream
    case 'pds-b': return pdsBUpstream
    default: return null
  }
}

function forwardedPath(value) {
  if (typeof value !== 'string' || !value.startsWith('/') || value.startsWith('//')) return null
  return value
}

function forwardedHeaders(rawHeaders) {
  const headers = []
  for (let index = 0; index < rawHeaders.length; index += 2) {
    if (rawHeaders[index].toLowerCase() !== 'x-acceptance-upstream') headers.push(rawHeaders[index], rawHeaders[index + 1])
  }
  return headers
}

function requestTarget(request) {
  const upstream = selectedUpstream(request.headers['x-acceptance-upstream'])
  const path = forwardedPath(request.url)
  if (!upstream || !path) return null
  return { upstream, path }
}

function shouldRejectPLC(path, upstream) {
  return upstream.name === 'plc' && faults.plcDIDResolution5xx && targetDID(path) === faults.targetDID
}

function shouldRejectListRepos(path, upstream) {
  return faults.pdsListRepos5xx && upstream.name === faults.targetPDS && path.startsWith('/xrpc/com.atproto.sync.listRepos')
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

function upstreamRequest(request, target, callback) {
  return http.request({
    host: target.upstream.host,
    port: target.upstream.port,
    method: request.method,
    path: target.path,
    headers: forwardedHeaders(request.rawHeaders),
  }, callback)
}

function proxy(request, response) {
  const target = requestTarget(request)
  if (!target) {
    response.writeHead(502)
    response.end()
    return
  }
  if (shouldRejectPLC(target.path, target.upstream)) {
    recordFault('plcDIDResolution5xx')
    sendJSON(response, 503, { error: 'acceptance_fault' })
    return
  }
  if (shouldRejectListRepos(target.path, target.upstream)) {
    recordFault('pdsListRepos5xx')
    sendJSON(response, 503, { error: 'acceptance_fault' })
    return
  }
  const outbound = upstreamRequest(request, target, (upstreamResponse) => {
    const rewriteDocument = target.upstream.name === 'plc' && faults.substituteSigningKey && targetDID(target.path) === faults.targetDID
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
  outbound.on('error', () => {
    if (!response.headersSent) response.writeHead(502)
    response.end()
  })
  request.pipe(outbound)
}

function rejectUpgrade(socket) {
  socket.end('HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\n\r\n')
}

function writeUpgradeResponse(socket, response) {
  const status = response.statusCode ?? 502
  const message = response.statusMessage ?? 'Bad Gateway'
  socket.write(`HTTP/${response.httpVersion} ${status} ${message}\r\n`)
  for (let index = 0; index < response.rawHeaders.length; index += 2) {
    socket.write(`${response.rawHeaders[index]}: ${response.rawHeaders[index + 1]}\r\n`)
  }
  socket.write('\r\n')
}

function proxyUpgrade(request, socket, head) {
  const target = requestTarget(request)
  if (!target) {
    rejectUpgrade(socket)
    return
  }
  let upgraded = false
  const outbound = upstreamRequest(request, target, (response) => {
    response.resume()
    rejectUpgrade(socket)
  })
  outbound.on('upgrade', (response, upstreamSocket, upstreamHead) => {
    upgraded = true
    writeUpgradeResponse(socket, response)
    if (head.length > 0) upstreamSocket.write(head)
    if (upstreamHead.length > 0) socket.write(upstreamHead)
    socket.on('error', () => upstreamSocket.destroy())
    upstreamSocket.on('error', () => socket.destroy())
    socket.pipe(upstreamSocket)
    upstreamSocket.pipe(socket)
  })
  outbound.on('error', () => {
    if (!upgraded) rejectUpgrade(socket)
  })
  outbound.end()
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
  proxy(request, response)
})

server.on('upgrade', (request, socket, head) => proxyUpgrade(request, socket, head))

server.listen(port, '0.0.0.0')
