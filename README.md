# fungos-vfs

**Local-first content-addressed VFS for fungOS.**

A workspace is a SQLite namespace plus a content-addressed blob store. The
filesystem is local and fast; distribution is a separate sync/transport layer.

Status: **working slice.** Local full-node and pull-through cache behavior is
implemented. Unibus ranged fetching and serving work over the TCP hub and
authenticated Iroh peer sessions. A FUSE adapter exposes the VFS as a native
filesystem on macOS and Linux.

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

## Next

1. File Provider / Cloud Files adapter for a signed macOS app.
2. Sparse and page-granular hydration.
3. Blob provider adapters: local CAS, Kubo, iroh-blobs.
4. Conflict/version UI.
5. LOD-aware sync policy.

## License

GPL-3.0-or-later. See [LICENSE](LICENSE).

Deployment notes for studio and agora-one are in [`docs/deployment.md`](docs/deployment.md).
