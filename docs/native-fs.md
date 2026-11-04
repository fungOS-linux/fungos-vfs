# Native filesystem adapter

`fungos-vfs-fuse` projects a VFS namespace into a real filesystem:

- **macOS** uses macFUSE.
- **Linux** uses FUSE3.

The adapter is a view, not a second authority. Paths, content identity,
versions, leases and eviction remain owned by `fungos-vfs`. Reads hydrate
placeholders through Unibus when `--unibus-config` is supplied. Writes are
buffered for the open file and become a new immutable content version on
release.

## Build

macOS needs macFUSE and `pkg-config`:

```sh
export PATH="/opt/homebrew/bin:$PATH"
export PKG_CONFIG_PATH="/usr/local/lib/pkgconfig"
cargo build --release -p fungos-vfs-fuse --features fuse
```

Linux builds without libfuse when using the pure-Rust mount path:

```sh
cargo build --release -p fungos-vfs-fuse --features fuse
```

## Mount

```sh
mkdir -p ~/fungOS
~/.local/bin/fungos-vfs-fuse \
  --root ~/fungos-node \
  --mount ~/fungOS
```

Cache node with pull-through hydration:

```sh
~/.local/bin/fungos-vfs-fuse \
  --root ~/fungos-cache \
  --role cache \
  --unibus-config ~/.config/unibus/agora-cache.json \
  --content-provider studio-content \
  --mount ~/fungOS
```

## macOS permission

macFUSE is a kernel/system extension. The first mount requires the extension to
be loaded and approved in **System Settings → Privacy & Security**. A reboot is
normally required after installing macFUSE. Until then the mount process waits
or fails; the launchd job retries.

## Current limits

- namespaces are flat VFS paths; directories are synthesized
- no rename, symlinks, xattrs, or hard links
- writeback is whole-file on close, not sparse or page-granular
- one writer per file; last close wins
- macFUSE and FUSE3 are external requirements
