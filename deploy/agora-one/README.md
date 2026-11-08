# Serving a fungOS VFS Desktop to macOS over NFS

macOS can reach a `fungos-vfs-fuse` mount with the built-in NFS client — no
macFUSE, no FUSE-T, no third-party install on the Mac. This directory holds the
agora-one side of that arrangement.

The shape of it:

```
/srv/fungos-export/<desktop>   <- what the Mac mounts
  ^ bind mount
~/Desktop/<desktop>    <- fungos-vfs-fuse mount (fungos-vfs-<desktop>.service, user)
  ^ VFS
~/.local/share/fungos/desktops/<desktop>
```

The FUSE mounts are per-user and are exported through bind mounts under
`/srv/fungos-export`, so nothing in `/etc/exports` points at a home directory.

## Why each moving part exists

| Piece | Why |
| --- | --- |
| `user_allow_other` in `/etc/fuse.conf` | knfsd runs as root; without it the kernel refuses `-o allow_other` and root cannot traverse a user-owned FUSE mount. |
| `--allow-other` in the FUSE units | Same reason, from the mount side. Without it every read through the export is `EACCES`. |
| `fsid=` per export | A FUSE mount has no stable `st_dev` across remounts, so knfsd cannot derive a filesystem id; without an explicit one clients see `ESTALE`/`EXDEV`. |
| `all_squash,anonuid=1000` | Collapses every client identity onto the uid that owns the FUSE mount, which is the only identity `default_permissions` will grant. |
| `fungos-nfs-export` | A bind mount pins the mount it was made from: when a FUSE service restarts, the export keeps pointing at the dead superblock and every request fails with `EIO`. This loop re-binds when a mount id changes and reloads exports. |
| `loginctl enable-linger ajmwagar` | The FUSE mounts are systemd **user** units; without linger they are not started at boot and the export has nothing to bind. |
| inodes derived from the path (`crates/fungos-vfs-fuse`) | A runtime counter renumbers every inode on restart, so every cached NFS file handle resolves to the wrong file — `ESTALE`. This is the one that silently corrupts clients. |

## Install

```sh
sudo install -m 0755 deploy/agora-one/fungos-nfs-export /usr/local/sbin/fungos-nfs-export
sudo install -m 0644 deploy/agora-one/fungos-nfs-export.service \
  /etc/systemd/system/fungos-nfs-export.service

sudo apt-get install -y nfs-kernel-server
sudo sed -i 's/^#user_allow_other/user_allow_other/' /etc/fuse.conf

sudo mkdir -p /srv/fungos-export/{personal,fpl,puget-audio}
sudo tee /etc/exports < deploy/agora-one/exports

sudo systemctl enable --now rpcbind nfs-server fungos-nfs-export.service
loginctl enable-linger ajmwagar    # once, if not already enabled

systemctl --user daemon-reload     # after adding --allow-other to the units
systemctl --user restart fungos-vfs-personal fungos-vfs-fpl fungos-vfs-puget-audio
```

## macOS client (no installs)

```sh
mkdir -p ~/mnt/personal
mount -t nfs -o vers=3,nolock,resvport,rw agora-one:/srv/fungos-export/personal ~/mnt/personal
ls ~/mnt/personal
```

`-t nfs` is the built-in macOS NFS client. `resvport` is macOS's default anyway,
and `nfs-kernel-server` requires a privileged source port (`secure` is implicit).
`nolock` avoids `statd`/`lockd` chatter for a single-user share.

Unmount with `umount ~/mnt/personal`. To make it permanent, add to `/etc/fstab`
on the Mac:

```
agora-one:/srv/fungos-export/personal /Users/<you>/mnt/personal nfs rw,vers=3,nolock,resvport,nobrowse 0 0
```

## Verify

From any Linux box (or agora-one itself):

```sh
showmount -e agora-one
mkdir -p /mnt/t && sudo mount -t nfs -o vers=3,nolock agora-one:/srv/fungos-export/personal /mnt/t
head -3 /mnt/t/projects/fungos-vfs/docs/native-fs.md
echo hello > /mnt/t/notes/nfs-write-test.md
~/fungos-vfs/target/release/fungos-vfs \
  --root ~/.local/share/fungos/desktops/personal history notes/nfs-write-test.md
sudo umount /mnt/t
```

A mount that shows an empty directory is not a success: the FUSE mount answers
`getattr` on the root even when it is not serving the namespace you think, so
always read a known file and check `history` for a written one.

## Limits

- NFSv3 (`vers=3`). The v4 pseudo-root is exported read-only as `fsid=0`; child
  exports are the supported path.
- Directory rename is not implemented (`Vfs::rename` moves one namespace entry),
  so Finder's rename/move of a *file* works and of a folder returns `EOPNOTSUPP`.
- Whole-file writeback on close, same as the FUSE adapter itself.
