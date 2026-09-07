---
name: writing-changesets
description: Write a named Changeset for an operator-visible change to Hypercerts Relay or Rainbow. Use when changing PDS admission, rate limits, stored event behavior, configuration, release behavior, or a service interface.
---

# Writing Changesets for Hypercerts Relay

Add a Changeset for an observable Relay or Rainbow change. The release process tracks the single private `hypercerts-relay` package and publishes source tags and GitHub Releases; it does not publish to npm.

Create a descriptive file in `.changeset/`, such as `pds-admission-policy.md`. Do not use a random generated filename. Use this form:

```markdown
---
'hypercerts-relay': minor
---

Plain-language summary of what changes and the action an operator must take, if any.
```

Use `patch` for a correction that operators can observe, `minor` for a new configuration option, PDS or rate-limit behavior, retained-record capability, or administration operation. Before 1.0, use `minor` for incompatible operator-facing changes. Do not add a Changeset for tests, internal refactoring, CI, skills, or documentation that does not change the running service.

Describe the visible behavior and migration action. Include exact configuration keys, limits, or commands when an operator must act. Avoid commit-message language and implementation detail that does not affect an operator.
