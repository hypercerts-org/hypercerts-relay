---
name: hypercerts-railway-release
description: Publish Hypercerts Relay images, deploy an immutable candidate to Railway staging, or promote that exact candidate to production. Use for release-candidate, staging, production-promotion, and local staging-deploy work; not for provisioning a new development environment.
---

# Hypercerts Railway release and promotion

Read `AGENTS.md`, `docs/deployment.md`, and `docs/railway.md` before acting.
This public repository builds and publishes images only. Railway environment
configuration, service IDs, volumes, domains, secrets, and deployment receipts
belong in the private infrastructure repository.

The release unit is three immutable images built from one `dev` commit:

- `relay` from `cmd/relay/Dockerfile` with repository-root context;
- `jetstream` from `jetstream/Dockerfile` with `jetstream/` context; and
- `administration` from `administration/Dockerfile` with repository-root context.

Rainbow is deliberately excluded from release candidates and all initial launch
environments. Do not make it part of a deployment without the separate activation
evidence and authorization.

## Select the operation

- For a `dev` merge, read [candidate staging](references/candidate-staging.md).
- For a `dev` to `production` promotion, read [production promotion](references/production-promotion.md).
- For a deliberate local upload to staging, read [local staging deployment](references/local-staging.md).

Do not infer authorization to deploy from an image build, a Changeset release, or
a passing health endpoint. Observe the submitted deployment reach `SUCCESS` and
perform the environment-specific checks before reporting deployment success.
