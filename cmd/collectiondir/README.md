# Collection directory: operator guide

The directory indexes candidate DID/collection pairs from raw Relay `subscribeRepos` events and PDS `listRepos` / `describeRepo` crawls. It is not an authoritative inventory or a backfill service. Consumers must resolve each DID and verify current records themselves. Entries can be stale after deletes, account changes, migrations, or failed crawls; missing entries do not prove absence. The index is limited by configured sources, upstream stream retention, the per-DID collection limit, and failures. It does not record coverage or guarantee crash-consistent stream progress.

## Run and bootstrap

Build with `go build -o collectiondir ./cmd/collectiondir`. Set `COLLECTIONS_ADMIN_TOKEN` to a nonempty secret through your secret manager, not a shell command or source file. Start with persistent storage paths and a raw Relay endpoint:

```sh
./collectiondir serve --pebble /path/to/directory.pebble --dau-directory /path/to/daily --upstream wss://your-relay --api-listen 127.0.0.1:2510 --metrics-listen 127.0.0.1:2511
```

Use your actual Relay address; do not embed credentials in URLs. Supply a nonempty token even when no crawl is planned. The service does not automatically discover or bootstrap PDSes. Once the stream is running, submit each approved public PDS to `POST /admin/pds/requestCrawl` with `Authorization: Bearer <token>` and JSON `{"hosts":["https://your-pds"]}`. Protect the token and transport; use a secure authenticated proxy for remote access. Check `GET /admin/crawlStatus` with the same bearer authorization and review logs for progress and errors; a successful submission is not proof of complete coverage. Choose `--crawl-qps` appropriate for each PDS. The service-side crawl transport blocks private addresses; offline crawls have different network protections and should only target trusted hosts.

Query `GET /xrpc/com.atproto.sync.listReposByCollection?collection=<nsid>&limit=100` and follow the returned cursor. `/v1/listCollections` provides approximate, cached counts; its default threshold excludes collections with fewer than five DIDs. Do not treat counts or candidates as verified records.

## Exposure and limitations

The API and metrics listeners default to all interfaces. The query endpoints are unauthenticated; crawl status requires the admin bearer token. The the metrics listener also exposes pprof. Bind both to trusted interfaces and restrict access. The admin endpoint can start network crawls; protect its bearer token and restrict access independently. Do not put credentials in PDS or upstream URLs, which may appear in logs. The imported `--ratelimit-header` option and its environment variables are unsupported: neither online nor offline crawls forward shared rate-limit bypass secrets. PDSes may rate-limit or reject crawls.

The directory records observed create/update operations and crawl results, not deletions or authoritative membership. It does not provide completeness guarantees, automatic source discovery, durable crawl jobs, or a replacement for Jetstream retention and backfill. Upstream stream gaps, failed descriptions, and restarts can leave omissions; monitor and reconcile independently. During shutdown the service drains accepted events, but it does not advance the stored stream cursor: queued writes and write failures cannot yet be tied to a durable event checkpoint. Restarts may replay from an older cursor, or start at the current stream position if no cursor was previously stored; upstream retention limits replay.

## Development and provenance

Imported from Indigo `cmd/collectiondir` at `41278964ec8e3253e70d4e919dfb8e34211c543d`; fork-specific Go changes carry `hypercerts` comments. Run `go test ./cmd/collectiondir` and `./scripts/verify.sh` from the repository root. Future Indigo synchronization includes this directory through `.hypercerts/upstream-paths` and requires review of fork divergences.
