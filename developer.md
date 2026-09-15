# Developer fixture: two tunneled PDS containers

`docker-compose.pds-fixture.yml` runs two separately stateful official AT Protocol
PDS containers and two Cloudflare Tunnel sidecars. It is a developer fixture for
constructing the controlled two-PDS topology required by Plan 001. It is **not** a
production deployment, a Relay/Rainbow/Jetstream Compose stack, or an acceptance
pass by itself.

The manifest pins the upstream
`ghcr.io/bluesky-social/atproto:pds-2e1787c2bf5bd47b55c3df930d688bb40b5ae63d`
OCI index and the Docker Hub
[`cloudflare/cloudflared`](https://hub.docker.com/r/cloudflare/cloudflared) OCI
index by digest. The PDS tag is the source revision published from upstream `main`
when this fixture was reconciled; renew both tag and digest together during an
upstream review. It does not require `cloudflared` to be installed on the host.

## Safety and identity contract

Each PDS needs its own stable public HTTPS hostname before any account is created:

- `PDS_A_HOSTNAME` and `PDS_B_HOSTNAME` must be distinct names in a Cloudflare
  managed zone.
- Configure a separate **named** Cloudflare Tunnel and public-hostname route for
  each PDS. The token configures `cloudflared` to route that hostname to the
  adjacent PDS on `127.0.0.1:3000`.
- Do not use Quick Tunnels: their hostnames change and cannot safely anchor PDS
  identity.
- Do not put Cloudflare Access, browser challenges, or an incompatible proxy in
  front of the PDS API/WebSocket paths. Relay, DID resolvers, and AT Protocol
  clients must reach them.

The official PDS creates a `did:plc` account by default. A public
`https://plc.directory` registration is persistent even after local volumes are
destroyed. For a disposable fixture, `PDS_*_DID_PLC_URL` must identify an isolated
PLC service that supports PDS account/DID creation **and** subsequent resolution.
This repository does not currently provide that service; do not point the fixture
at the public PLC directory merely to make a test start.

`PDS_CRAWLERS` is deliberately unset. The controlled Relay admits each public PDS
origin after its identity resolves; the PDSs do not independently announce
execution to any crawler. Never point fixture PDSs at the running production
Relay.

## Configure

Copy the tracked placeholder file to the ignored local file:

```sh
cp pds-fixture.env.example .env.pds.fixture
chmod 600 .env.pds.fixture
```

Fill in the two Cloudflare hostnames, isolated PLC origin, two named-tunnel
tokens, and different secrets for each PDS. The upstream PDS
installer generates compatible secret material with:

```sh
openssl rand --hex 16
openssl ecparam --name secp256k1 --genkey --noout --outform DER \
  | tail --bytes=+8 | head --bytes=32 | xxd --plain --cols 32
```

Write command output only into the ignored local environment file or an approved
secret manager. Do not put secrets in shell history, issue comments, logs, or this
repository.

## Start and validate

First render the manifest without starting any container:

```sh
docker compose --env-file .env.pds.fixture \
  -f docker-compose.pds-fixture.yml config --quiet
```

Then start the PDSs and their tunnel sidecars:

```sh
docker compose --env-file .env.pds.fixture \
  -f docker-compose.pds-fixture.yml up -d

docker compose --env-file .env.pds.fixture \
  -f docker-compose.pds-fixture.yml ps

docker compose --env-file .env.pds.fixture \
  -f docker-compose.pds-fixture.yml logs --tail=100 \
  pds-a pds-a-cloudflared pds-b pds-b-cloudflared
```

After both named tunnels report connected, verify the public PDS endpoints. Replace
the example names with the configured hostnames:

```sh
curl --fail --show-error \
  https://pds-a.fixture.example/xrpc/com.atproto.server.describeServer
curl --fail --show-error \
  https://pds-b.fixture.example/xrpc/com.atproto.server.describeServer
curl --fail --show-error https://pds-a.fixture.example/.well-known/did.json
curl --fail --show-error https://pds-b.fixture.example/.well-known/did.json
```

Only after the isolated PLC can resolve both created DIDs and the PDS APIs are
reachable should the controlled Relay admit the two public PDS origins. Jetstream
must retain its raw upstream from that Relay (or the controlled Rainbow) and add
those PDS origins separately for direct-PDS current-state backfill. Use disposable
Relay, Rainbow, and Jetstream data directories for the acceptance runner.

## Stop and clean up

Stop the fixture without removing state:

```sh
docker compose --env-file .env.pds.fixture \
  -f docker-compose.pds-fixture.yml down
```

For an authorized fixture reset, remove the two local PDS volumes:

```sh
docker compose --env-file .env.pds.fixture \
  -f docker-compose.pds-fixture.yml down --volumes
```

Removing volumes does not erase public PLC registrations, Cloudflare tunnels, DNS
routes, or any data already delivered to another service. Remove named tunnels and
DNS routes through the private Cloudflare configuration after the test, and record
that cleanup separately.

## Plan 001 local acceptance topology

The Cloudflare fixture above is for an explicitly requested public-PDS exercise.
Plan 001's disposable T01 topology instead uses
[`docker-compose.acceptance.yml`](docker-compose.acceptance.yml) and
[`tests/acceptance/run`](tests/acceptance/run). It starts two real PDS containers,
a local `@did-plc/server`, local internal TLS/DNS, an acceptance-tag Relay, and
Jetstream v2 without Cloudflare, Railway, public DNS, or a persistent public DID.
Run only its declared case:

```sh
./tests/acceptance/run T01
```

See [`tests/acceptance/README.md`](tests/acceptance/README.md) for boundaries and
requirements. T01 does not replace T15-core fault characterization, Rainbow
recovery work, or the account-alert baseline diagnosis. A Plan 001 completion
record still needs the actual runner result, exact commands/revisions, and the
documented baseline outcome.
