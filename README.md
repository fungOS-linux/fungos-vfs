# fungos-vfs

**Local-first content-addressed VFS for fungOS.**

A workspace is a SQLite namespace plus a content-addressed blob store. The
filesystem is local and fast; distribution is a separate sync/transport layer.

Status: **working slice.** Local full-node and pull-through cache behavior is
implemented. Two stores converge by pull-based anti-entropy over ssh or QUIC
(see [Sync](#sync)). Unibus ranged fetching and serving work over the TCP hub
and authenticated Iroh peer sessions. A FUSE adapter exposes the VFS as a
native filesystem on macOS and Linux.

## Model

- **Full node** — can write, retain versions, serve blobs, and pin content.
- **Cache node** — pull-through only: registers placeholders, hydrates blobs,
  leases open files, evicts old content, and refuses authoritative writes.
- File paths are workspace-relative and cannot escape the root.
- Each write creates a new immutable `ContentId: blake3:<hex>` version.
- Sync queue entries are durable and acknowledged explicitly.
- Leases protect open files from eviction.
- Eviction changes a file to `placeholder` but preserves namespace/history.

## CLI

```sh
cargo run -- --root /tmp/fungos put ./README.md docs/README.md
cargo run -- --root /tmp/fungos ls
cargo run -- --root /tmp/fungos stat docs/README.md
cargo run -- --root /tmp/fungos history docs/README.md
cargo run -- --root /tmp/fungos get docs/README.md
cargo run -- --root /tmp/fungos pending
cargo run -- --root /tmp/fungos evict --max-bytes 0
```

Pull-through cache:

```sh
cargo run -- --root /tmp/cache --role cache placeholder \
  big.bin blake3:<hex> 4096 --media-type application/octet-stream
cargo run -- --root /tmp/cache --role cache hydrate big.bin ./big.bin
cargo run -- --root /tmp/cache --role cache lease big.bin --owner app --ttl-ms 60000
```

## Pull-through hydration

`Hydrator` is the transport seam for cache nodes:

```rust
vfs.get_or_hydrate("big.bin", &source)?;
```

The VFS verifies the fetched bytes against the registered `ContentId` before
marking the file present. The first Unibus L2 wire types are included:

```text
unibus.content.fetch.v1
unibus.content.data.v1
unibus.content.error.v1
unibus.content.available.v1
```

The Unibus client adapter is `crates/fungos-vfs-unibus`. It sends bounded
`unibus.content.fetch.v1` frames and reconstructs from correlated
`unibus.content.data.v1` ranges:

```rust
use std::sync::Arc;
use fungos_vfs_unibus::UnibusHydrator;
use unibus::client::Client;

let client = Arc::new(Client::connect(config)?);
let source = UnibusHydrator::new(client, Some("content-node".into()));
let bytes = vfs.get_or_hydrate("big.bin", &source)?;
```

## Native filesystem

```sh
cargo build --release -p fungos-vfs-fuse --features fuse
~/.local/bin/fungos-vfs-fuse --root ~/fungos-node --mount ~/fungOS
```

See [`docs/native-fs.md`](docs/native-fs.md). macOS uses macFUSE; Linux uses
FUSE3.

## Sync

Two stores converge by exchanging **ops**, not files. The op log already records
who wrote, when, in what order, and against which head, so the exchange can say
"this write was made against a head you no longer have" — which is a fact a
file-level sync cannot express.

It is pull-based anti-entropy: a store asks a peer for what it is missing. The
carriers are ssh (default, no daemon, no port) and QUIC.

```sh
# On the peer: answer one request on stdin. This is what ssh gives it.
fungos-vfs --root ~/fungos-node sync serve

# Here: pull from that peer's store into this one.
fungos-vfs --root ~/fungos-local sync pull \
  --remote agora-one --remote-root /srv/fungos

# Same exchange over QUIC. `serve` prints the fingerprint to pin on the client.
fungos-vfs --root /srv/fungos sync serve --quic 0.0.0.0:7701 --cert /etc/fungos/quic.key
fungos-vfs --root ~/fungos-local sync pull --quic agora-one:7701 \
  --fingerprint <hex>
```

`pull` reports what happened to each op:

```json
{"applied": 12, "duplicate": 0, "diverged": 1, "snapshot": false}
```

- **`applied`** — the op is now in this store.
- **`duplicate`** — already had it, recognised by `(origin, hlc)` before any work.
- **`diverged`** — the op names a head this store has moved past. It is **not
  merged**. Applying it would discard the intervening write, so it is held as
  unresolved state instead. This is the deliberate difference from a
  last-writer-wins layer: a filesystem that silently picks a winner loses an
  edit, and the person who lost it is never told.
- **`snapshot`** — this store had fallen behind a compaction horizon, so it was
  handed the live namespace at a watermark rather than a delta it could not
  recognise as short.

Content follows the ops: an op names content ids this store may not hold, and
the bytes are fetched by id and verified against it before being stored.

### Compaction

An op log that only grows eventually costs more to scan than the work it
describes. `compact` drops rows older than a cutoff:

```sh
fungos-vfs --root ~/fungos-node sync compact --older-than 2592000  # 30 days
```

Dropping rows is the easy half. A peer whose cursor now sits below what was
dropped cannot be caught up from the log at all — the ops that would fill its
gap are the ones that are gone — so it is told so and given a snapshot instead.
Compaction also stops short of the earliest unresolved divergence for a store:
those rows stay until the divergence settles, because a retried write must not
come back as a duplicate of a write this store never got.

### What sync is not

- **Not push.** Nothing watches the filesystem, so convergence happens when a
  pull runs. A live-sync daemon is future work.
- **Not a merge.** Concurrent writes to the same path surface as `diverged` for
  a human or a policy to settle. There is no conflict copy and no automatic
  winner.
- **Not a server.** No node is authoritative; any store can pull from any other.

## Next

1. File Provider / Cloud Files adapter for a signed macOS app.
2. Sparse and page-granular hydration.
3. Blob provider adapters: local CAS, Kubo, iroh-blobs.
4. Conflict/version UI.
5. LOD-aware sync policy.
6. A live-sync daemon: watch the mount, push on change, so convergence is not
   something a person has to remember to ask for.

## License

GPL-3.0-or-later. See [LICENSE](LICENSE).

Deployment notes for studio and agora-one are in [`docs/deployment.md`](docs/deployment.md).
