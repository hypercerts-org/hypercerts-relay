---
name: hypercerts-railway-development
description: Provision, validate, or retire an isolated all-services Railway development environment for Hypercerts Relay. Use for Railway development environments containing Relay, Jetstream, and Administration; not for staging release promotion or production deployment.
---

# Hypercerts Railway development environment

Read `AGENTS.md`, `docs/deployment.md`, and `docs/railway.md` before acting.
Project/environment bindings, volumes, domains, variables, and credentials are
private-infrastructure concerns; do not create Railway configuration files in this
public repository.

An initial all-services development environment contains:

- one active Relay;
- Jetstream v2; and
- Administration, plus the Relay metadata database where the selected setup uses
  PostgreSQL.

Rainbow is not part of this environment. Its container being buildable is not
authorization to provision it.

Read [development environment procedure](references/development-environment.md)
when creating, changing, validating, or retiring the environment. Require explicit
authorization before creating Railway resources, setting variables, or deleting
the development environment.
