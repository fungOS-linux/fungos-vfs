# Deployment: studio and agora-one

Both **studio** and **agora-one** are good full nodes:

- full `fungos-vfs` replica
- local CAS
- content provider
- optional Kubo/iroh-blobs provider
- Unibus client over the local hub or authenticated peer transport

A laptop/phone is the pull-through cache: it keeps placeholders and hydrates
ranges from the nearest full node.

## Full node

```sh
fungos-vfs-provider \
  --root /srv/fungos \
  --config /etc/unibus/studio-content.json
```

Example client config:

```json
{
  "installation_id": "home",
  "producer_id": "studio-content",
  "hub_endpoint": "127.0.0.1:7878",
  "token_file": "/etc/unibus/studio-content.token",
  "zones": ["default"],
  "capabilities": ["content.provider"],
  "schema_version": 2,
  "read_timeout_ms": 1000
}
```

The Unibus router needs explicit routes for the L2 kinds. The provider publishes
data only to the requesting producer:

```text
unibus.content.fetch.v1  -> capability content.provider
unibus.content.data.v1   -> producer
unibus.content.error.v1  -> producer
unibus.content.available.v1 -> capability content.provider
```

`agora-one` runs the same provider with its own producer id, token, and
`producer_id` in routes. It can also pin long-lived content while studio keeps
the active working set.

## Iroh peer mode

`ContentService::serve_peer(session, local_producer)` accepts any authenticated
`unibus::PeerSession`, including `unibus/peer/1` over Iroh. The integration test
in `crates/fungos-vfs-unibus/tests/iroh.rs` drives a real encrypted loopback
Iroh session, a signed direct-session claim, a bounded fetch, and a ranged
response.

For studio/agora-one, provision:

- separate persistent Iroh endpoint keys
- Mycelium-discovered endpoint hints
- a trusted direct-session issuer and scoped claims for
  `unibus.content.fetch.v1`, `unibus.content.data.v1`, and
  `unibus.content.error.v1`
- local allowlists for relays and peer producer ids

Do not reuse the Mycelium daemon key. The Unibus peer endpoint is a separate
application identity.

## Cache node

```sh
fungos-vfs --root ~/fungos-cache --role cache placeholder \
  docs/report.pdf blake3:<hash> 1048576 --media-type application/pdf
```

Then hydrate through the Unibus adapter:

```rust
let source = UnibusHydrator::new(client, Some("studio-content".into()));
let bytes = vfs.get_or_hydrate("docs/report.pdf", &source)?;
```

Leases prevent eviction while an app has a file open.
