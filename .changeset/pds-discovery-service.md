---
'hypercerts-relay': minor
---

Add the optional `pdsdiscovery` service. It watches a public Bluesky Jetstream
for records in the retained collections. When such a record comes from a PDS
that is not yet a source, it asks administration to enable that PDS. It never
enables a PDS that already exists as a source in any state, including one an
operator disabled or removed.

Before adding a PDS, the service queries the Bluesky relay with
`com.atproto.sync.getHostStatus` and skips hosts it reports as banned. Once the
new PDS is an enabled source, if the Bluesky relay reported more than 100
accounts and did not report the host as throttled, the service raises the
source's account limit to the count rounded up to the next thousand. For
example, 1,400 accounts gives a limit of 2,000. Administrators set the maximum
account limit discovery may request in the new "Automatic PDS discovery" panel
on the Sources screen; it defaults to 100,000 and changes are audited.

There is no cap on how many PDSes are added and no built-in denylist;
`DISCOVERY_DENY_HOSTS` can exclude hosts. `DISCOVERY_DRY_RUN=true` logs
candidates without requesting anything. See `cmd/pdsdiscovery/README.md`.

Administration gains discovery routes at `/api/v1/sources/discovery`. The
`state` and `operations` routes are mounted only when
`ADMIN_DISCOVERY_TOKEN_FILE` is set, or `HC_DISCOVERY_SECRET` under Railway
startup. They accept only the discovery token, and can only enable sources and
raise account limits up to the maximum. Their operations are journaled and
audited under the actor `service:pds-discovery`.
