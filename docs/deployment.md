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

## Current deployment

As of 2026-11-04:

- **studio** (`100.108.9.93`) runs `dev.fungos.vfs-content` as a launchd job.
- **agora-one** (`100.80.85.86`) runs `fungos-vfs-content.service` as a systemd
  user service.
- Both providers connect through the existing SSH tunnel to the home-pi
  `unibus-router` on `127.0.0.1:18790`.
- Central router nodes: `studio-content`, `agora-content`, `studio-cache`,
  `agora-cache`.
- Routes: `unibus.content.{fetch,data,error,available}.v1` all use
  `{"mode":"producer"}`. Clients name the provider explicitly.

Full-node roots:

```text
studio:   /Users/ajmwagar/fungos-node
agora:    /home/ajmwagar/fungos-node
```

Cache roots:

```text
studio:   /Users/ajmwagar/fungos-cache
agora:    /home/ajmwagar/fungos-cache
```

Verified cross-node fetches:

```text
studio cache <- agora-content   65567 bytes   sha256 83d10e18...
agora cache  <- studio-content  65567 bytes   sha256 83d10e18...
agora cache  <- studio-content  16407 bytes   sha256 e9b998e3...
```

The Agora native mount is `~/fungOS`, served by `fungos-vfs-fuse.service`.
The Studio native mount is `~/fungOS`, served by `dev.fungos.vfs-fuse`; it
requires macFUSE to be loaded and approved by macOS.
