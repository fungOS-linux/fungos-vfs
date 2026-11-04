# fungos-vfs

**Local-first content-addressed VFS for fungOS.**

A workspace is a SQLite namespace plus a content-addressed blob store. The
filesystem is local and fast; distribution is a separate sync/transport layer.

Status: **first slice.** Local full-node and pull-through cache behavior is
implemented. Network mounts, File Provider, Cloud Files, FUSE, and Unibus
transport are not.

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

The actual Unibus client adapter must live outside this public crate until
Unibus is public/published.

## Next

1. File Provider / Cloud Files / FUSE adapters.
2. Unibus client adapter in the Unibus repository.
3. Range hydration and sparse files.
4. Blob provider adapters: local CAS, Kubo, iroh-blobs.
5. Conflict/version UI.
6. LOD-aware sync policy.

## License

GPL-3.0-or-later. See [LICENSE](LICENSE).
