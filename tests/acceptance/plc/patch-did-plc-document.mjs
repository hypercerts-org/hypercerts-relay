import { readFile, writeFile } from 'node:fs/promises'

// @did-plc/server 0.0.1 emits legacy ECDSA verification-method metadata and
// decompressed key bytes. Current plc.directory emits Multikey methods with
// absolute IDs and the did:key multicodec payload. The fixture must use the
// current shape so Indigo Relay and Jetstream validate the same identity.
const paths = [
  'node_modules/@did-plc/lib/dist/index.js',
  'node_modules/@did-plc/server/dist/bin.js',
  'node_modules/@did-plc/server/dist/index.js',
  'node_modules/@did-plc/server/dist/db/index.js',
]
const replacements = [
  ['id: `#${keyid}`', 'id: `${data.did}#${keyid}`', 1],
  ['context: "https://w3id.org/security/suites/ecdsa-2019/v1"', 'context: "https://w3id.org/security/multikey/v1"', 1],
  ['context: "https://w3id.org/security/suites/secp256k1-2019/v1"', 'context: "https://w3id.org/security/multikey/v1"', 1],
  ['type: "EcdsaSecp256r1VerificationKey2019"', 'type: "Multikey"', 1],
  ['type: "EcdsaSecp256k1VerificationKey2019"', 'type: "Multikey"', 1],
  ['publicKeyMultibase: `z${toString5(keyBytes, "base58btc")}`', 'publicKeyMultibase: key.slice("did:key:".length)', 2],
]

for (const path of paths) {
  let source = await readFile(path, 'utf8')
  for (const [oldText, newText, expected] of replacements) {
    const occurrences = source.split(oldText).length - 1
    if (occurrences !== expected) {
      throw new Error(`${path}: expected ${expected} occurrences of ${oldText}, found ${occurrences}`)
    }
    source = source.replaceAll(oldText, newText)
  }
  await writeFile(path, source)
}
