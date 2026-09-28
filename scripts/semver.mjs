// Validate the SemVer form used by release tags and image metadata without a
// high-complexity regular expression.

function validIdentifier(identifier, { numericOnly = false } = {}) {
  if (identifier.length === 0) return false
  if (!/^[0-9A-Za-z-]+$/.test(identifier)) return false
  return !numericOnly || identifier === '0' || identifier[0] !== '0'
}

function validIdentifierList(value, options) {
  return value.split('.').every((identifier) => validIdentifier(identifier, options))
}

export function isSemver(value) {
  const buildSeparator = value.indexOf('+')
  const withoutBuild = buildSeparator === -1 ? value : value.slice(0, buildSeparator)
  const build = buildSeparator === -1 ? '' : value.slice(buildSeparator + 1)
  if ((buildSeparator !== -1 && (build.includes('+') || !validIdentifierList(build, {})))) return false

  const prereleaseSeparator = withoutBuild.indexOf('-')
  const core = prereleaseSeparator === -1 ? withoutBuild : withoutBuild.slice(0, prereleaseSeparator)
  const prerelease = prereleaseSeparator === -1 ? '' : withoutBuild.slice(prereleaseSeparator + 1)
  if (prereleaseSeparator !== -1 && !validIdentifierList(prerelease, { numericOnly: true })) return false

  const coreParts = core.split('.')
  return coreParts.length === 3 && coreParts.every((part) => validIdentifier(part, { numericOnly: true }))
}
