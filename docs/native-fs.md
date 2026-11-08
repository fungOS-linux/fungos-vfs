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

## Serving the mount to macOS over NFS

macOS's built-in NFS client can browse a Linux fungOS mount with no Mac-side
install at all — useful when macFUSE is unavailable or blocked. The mount is
served by `nfs-kernel-server` on the Linux node and mounted with `-t nfs` on the
Mac:

```sh
# on the Mac
mkdir -p ~/mnt/personal
mount -t nfs -o vers=3,nolock,resvport,rw \
  agora-one:/srv/fungos-export/personal ~/mnt/personal
```

Four things make this work, and each is easy to get wrong:

1. the FUSE mount needs `--allow-other` **and** `user_allow_other` in
   `/etc/fuse.conf`, because the NFS server runs as root;
2. each export needs an explicit `fsid=`, because a FUSE mount has no stable
   `st_dev` across remounts;
3. inodes must be derived from the namespace path, not from a counter — a counter
   renumbers the filesystem on every restart and every cached NFS file handle
   then goes `ESTALE`;
4. the bind mounts that place the FUSE mounts under `/srv/fungos-export` must be
   refreshed when a FUSE service restarts, otherwise the export keeps pointing at
   the dead mount.

[`deploy/agora-one/`](../deploy/agora-one/README.md) has the units, `/etc/exports`
and the refresh loop, with the reasoning for each.

## Current limits

- namespaces are flat VFS paths; ancestors are synthesized
- no symlinks, xattrs, or hard links; files can be renamed but directories cannot
- writeback is whole-file on close, not sparse or page-granular
- one writer per file; last close wins
- macFUSE and FUSE3 are external requirements
- over NFS, a create and the write that follows land as two versions (the create
  is committed empty; the kernel NFS server creates files without opening them)
