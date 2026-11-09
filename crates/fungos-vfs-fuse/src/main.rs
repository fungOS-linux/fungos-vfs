//! Native filesystem adapter for a fungOS VFS workspace.
//!
//! macOS uses macFUSE; Linux uses FUSE3. The adapter is deliberately a thin
//! namespace projection: content identity, history, leases and eviction remain
//! owned by `fungos-vfs`. Reads hydrate placeholders through Unibus when a
//! client config is supplied; writes become new immutable content versions.

use std::{
    collections::HashMap,
    ffi::{CString, OsStr},
    os::unix::ffi::OsStrExt,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use anyhow::{Context, bail};
use clap::{Parser, ValueEnum};
use fungos_vfs::{Expect, NodeRole, Vfs, VfsError, normalize_path};
use fungos_vfs_unibus::UnibusHydrator;
use fuser::{
    AccessFlags, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, Generation,
    INodeNo, MountOption, OpenFlags, RenameFlags, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request,
    SessionACL, TimeOrNow, WriteFlags, mount,
};
use unibus::client::{Client, ClientConfig};

/// The root is the only inode number fixed by convention; every other inode is
/// derived from its namespace path.
const ROOT: u64 = 1;
const FIRST_DERIVED_INO: u64 = 2;

/// 64-bit FNV-1a with a salt, finished with a finalizer.
///
/// `DefaultHasher` is documented as unspecified across Rust releases, so it
/// cannot back an inode number: a toolchain upgrade would renumber the
/// filesystem. This is a fixed, self-contained hash instead.
fn hash_path(path: &str, salt: u64) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325 ^ salt;
    for byte in path.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    hash ^= hash >> 29;
    hash
}

/// Inode numbers are a hash of the namespace path, not a runtime counter.
///
/// A counter renumbers every inode on each restart. NFS clients — and the kernel
/// NFS server sitting in front of this mount — cache file handles built from
/// inode numbers, so after a remount every cached handle resolved to a different
/// file or to nothing: the client got `ESTALE` until it remounted. Deriving the
/// inode from the path keeps it stable across restarts, so handles survive a
/// mount restart and only go stale when the path itself disappears.
fn inode_for_path(path: &str) -> u64 {
    const SPAN: u64 = u64::MAX - FIRST_DERIVED_INO;
    FIRST_DERIVED_INO + hash_path(path, 0) % SPAN
}

/// The per-path generation reported alongside an inode.
///
/// The kernel copies this into `inode->i_generation` and the NFS server mixes it
/// into file handles, so it must stay constant for as long as a path exists:
/// deriving it from content version would make every open client handle stale
/// on the next write. A different salt keeps it independent of the inode number
/// itself, and it is never zero so it cannot be mistaken for an unset value.
fn generation_for_path(path: &str) -> Generation {
    Generation(hash_path(path, 0x9e37_79b9_7f4a_7c15) | 1)
}

#[derive(Clone, Copy, ValueEnum)]
enum RoleArg {
    Full,
    Cache,
}

impl From<RoleArg> for NodeRole {
    fn from(value: RoleArg) -> Self {
        match value {
            RoleArg::Full => NodeRole::Full,
            RoleArg::Cache => NodeRole::Cache,
        }
    }
}

#[derive(Parser)]
#[command(version, about = "Mount a fungOS VFS as a native filesystem")]
struct Cli {
    #[arg(long)]
    root: PathBuf,
    #[arg(long)]
    mount: PathBuf,
    #[arg(long, value_enum, default_value_t = RoleArg::Full)]
    role: RoleArg,
    /// Optional Unibus client config used to hydrate cache placeholders.
    #[arg(long)]
    unibus_config: Option<PathBuf>,
    /// Optional destination producer for content fetches.
    #[arg(long)]
    content_provider: Option<String>,
    /// Expose the mount to other local users.
    #[arg(long)]
    allow_other: bool,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("fungos-vfs-fuse: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let vfs = Vfs::open_with_role(&cli.root, cli.role.into())
        .with_context(|| format!("open VFS at {}", cli.root.display()))?;
    let hydrator = match &cli.unibus_config {
        Some(path) => {
            let config: ClientConfig =
                serde_json::from_slice(&std::fs::read(path).context("read Unibus client config")?)
                    .context("parse Unibus client config")?;
            let client = Arc::new(Client::connect(config).context("connect Unibus hub")?);
            Some(Arc::new(UnibusHydrator::new(
                client,
                cli.content_provider.clone(),
            )))
        }
        None => None,
    };
    if !cli.mount.is_dir() {
        bail!("mount point is not a directory: {}", cli.mount.display());
    }

    let fs = FungosFs::new(vfs, hydrator);
    #[allow(unused_mut)]
    let mut options = vec![
        MountOption::RW,
        MountOption::FSName("fungos".into()),
        MountOption::DefaultPermissions,
    ];
    #[cfg(target_os = "macos")]
    options.push(MountOption::CUSTOM("volname=fungOS".into()));
    let mut config = fuser::Config::default();
    config.mount_options = options;
    config.acl = if cli.allow_other {
        SessionACL::All
    } else {
        SessionACL::Owner
    };
    mount(fs, &cli.mount, &config).with_context(|| format!("mount {}", cli.mount.display()))?;
    Ok(())
}

struct State {
    vfs: Vfs,
    hydrator: Option<Arc<UnibusHydrator>>,
    inodes: HashMap<String, u64>,
    paths: HashMap<u64, String>,
    dirs: HashMap<String, bool>,
    pending: HashMap<u64, Vec<u8>>,
}

impl State {
    fn new(vfs: Vfs, hydrator: Option<Arc<UnibusHydrator>>) -> Self {
        let mut state = Self {
            vfs,
            hydrator,
            inodes: HashMap::from([(String::new(), ROOT)]),
            paths: HashMap::from([(ROOT, String::new())]),
            dirs: HashMap::from([(String::new(), true)]),
            pending: HashMap::new(),
        };
        state.seed_namespace();
        state
    }

    /// Register every path already in the namespace up front.
    ///
    /// Inodes are recomputed identically after a restart, but the reverse map
    /// (inode -> path, used by `getattr`, `open`, `readdir` and friends) cannot
    /// be inverted out of a hash. Seeding it means a client that arrives with a
    /// pre-restart file handle resolves to the same path without an intervening
    /// lookup.
    fn seed_namespace(&mut self) {
        for entry in self.vfs.list("").unwrap_or_default() {
            self.seed_path(&entry.path, false);
        }
        for directory in self.vfs.directories("").unwrap_or_default() {
            self.seed_path(&directory, true);
        }
    }

    fn seed_path(&mut self, path: &str, is_dir: bool) {
        let segments: Vec<&str> = path
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect();
        let mut current = String::new();
        for (index, segment) in segments.iter().enumerate() {
            current = join_path(&current, segment);
            let last = index + 1 == segments.len();
            self.ino_for(&current, is_dir || !last);
        }
    }

    fn ino_for(&mut self, path: &str, is_dir: bool) -> u64 {
        if let Some(ino) = self.inodes.get(path).copied() {
            if is_dir {
                self.dirs.entry(path.to_string()).or_insert(true);
            }
            return ino;
        }
        let mut ino = inode_for_path(path);
        // A collision would alias two paths onto one inode, so probe forward.
        // Deterministic for a given set of paths, and effectively never taken.
        while self
            .paths
            .get(&ino)
            .is_some_and(|existing| existing != path)
        {
            const SPAN: u64 = u64::MAX - FIRST_DERIVED_INO;
            ino = FIRST_DERIVED_INO + (ino - FIRST_DERIVED_INO + 1) % SPAN;
        }
        self.inodes.insert(path.to_string(), ino);
        self.paths.insert(ino, path.to_string());
        self.dirs.insert(path.to_string(), is_dir);
        ino
    }

    fn path_for(&self, ino: u64) -> Option<String> {
        self.paths.get(&ino).cloned()
    }

    fn is_dir(&self, ino: u64) -> bool {
        self.paths
            .get(&ino)
            .and_then(|path| self.dirs.get(path))
            .copied()
            .unwrap_or(false)
    }

    /// Forget a path and everything under it.
    ///
    /// Inodes are derived from the path, so a path that no longer exists must
    /// stop resolving: after an unlink or rename the old inode would otherwise
    /// keep answering `getattr` for content the namespace no longer has.
    fn forget(&mut self, path: &str) {
        let prefix = format!("{path}/");
        let stale: Vec<String> = self
            .inodes
            .keys()
            .filter(|candidate| candidate.as_str() == path || candidate.starts_with(&prefix))
            .cloned()
            .collect();
        for candidate in stale {
            if let Some(ino) = self.inodes.remove(&candidate) {
                self.paths.remove(&ino);
                self.pending.remove(&ino);
            }
            self.dirs.remove(&candidate);
        }
    }

    fn entry(&self, path: &str) -> Option<fungos_vfs::FileEntry> {
        self.vfs.file(path).ok().flatten()
    }

    fn data(&mut self, ino: u64) -> Result<Vec<u8>, VfsError> {
        if let Some(bytes) = self.pending.get(&ino) {
            return Ok(bytes.clone());
        }
        let path = self
            .paths
            .get(&ino)
            .cloned()
            .ok_or_else(|| VfsError::NotFound(format!("inode {ino}")))?;
        match self.vfs.get(&path) {
            Ok(bytes) => Ok(bytes),
            Err(VfsError::NotHydrated(_)) if self.hydrator.is_some() => {
                let hydrator = self.hydrator.clone().expect("checked");
                self.vfs.get_or_hydrate(&path, hydrator.as_ref())
            }
            Err(error) => Err(error),
        }
    }

    fn write_back(&mut self, ino: u64) -> Result<(), VfsError> {
        let Some(bytes) = self.pending.remove(&ino) else {
            return Ok(());
        };
        let path = self
            .paths
            .get(&ino)
            .cloned()
            .ok_or_else(|| VfsError::NotFound(format!("inode {ino}")))?;
        self.vfs
            .put(&path, &bytes, "application/octet-stream", Expect::Any)?;
        Ok(())
    }

    fn direct_children(&self, prefix: &str) -> Vec<(String, bool)> {
        let entries = self.vfs.list(prefix).unwrap_or_default();
        let mut children = HashMap::<String, bool>::new();
        for entry in entries {
            let rest = if prefix.is_empty() {
                entry.path.as_str()
            } else {
                entry.path.strip_prefix(prefix).unwrap_or(&entry.path)
            };
            let rest = rest.trim_start_matches('/');
            if rest.is_empty() {
                continue;
            }
            if let Some((head, _)) = rest.split_once('/') {
                children.insert(head.to_string(), true);
            } else {
                children.insert(rest.to_string(), false);
            }
        }
        let mut children: Vec<_> = children.into_iter().collect();
        children.sort();
        children
    }
}

struct FungosFs {
    state: Mutex<State>,
}

impl FungosFs {
    fn new(vfs: Vfs, hydrator: Option<Arc<UnibusHydrator>>) -> Self {
        Self {
            state: Mutex::new(State::new(vfs, hydrator)),
        }
    }

    fn attr(ino: u64, size: u64, is_dir: bool) -> FileAttr {
        let now = SystemTime::now();
        FileAttr {
            ino: INodeNo(ino),
            size: if is_dir { 4096 } else { size },
            blocks: size.div_ceil(512),
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: if is_dir {
                FileType::Directory
            } else {
                FileType::RegularFile
            },
            perm: if is_dir { 0o755 } else { 0o644 },
            nlink: if is_dir { 2 } else { 1 },
            uid: unsafe { libc::geteuid() },
            gid: unsafe { libc::getegid() },
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }
}

fn errno(error: &VfsError) -> Errno {
    match error {
        VfsError::NotFound(_) => Errno::ENOENT,
        VfsError::NotHydrated(_) | VfsError::Transport(_) => Errno::EIO,
        VfsError::ReadOnly => Errno::EROFS,
        VfsError::InvalidPath(_) => Errno::EINVAL,
        VfsError::Config(_) => Errno::EINVAL,
        VfsError::InvalidContentId(_) => Errno::EIO,
        VfsError::Conflict { .. } | VfsError::AlreadyExists(_) => Errno::EEXIST,
        // A fenced write means the caller wrote against a head that has moved;
        // re-reading and retrying is the only correct response, and EAGAIN says
        // so instead of pretending the file is gone or corrupt.
        VfsError::Fenced { .. } => Errno::EAGAIN,
        VfsError::NotEmpty(_) => Errno::ENOTEMPTY,
        VfsError::Leased { .. } => Errno::EBUSY,
        // Unsynced local work is holding the state that a snapshot would
        // replace. That is the same class as a lease: settle the work rather
        // than retry the mount operation, and say so instead of reporting the
        // file as gone or corrupt.
        VfsError::SnapshotRefused { .. } => Errno::EBUSY,
        // Unreadable store metadata is an I/O-class failure, not a policy one.
        VfsError::CorruptMeta { .. } => Errno::EIO,
        VfsError::RoleMismatch { .. } | VfsError::Io(_) | VfsError::Sql(_) => Errno::EIO,
    }
}

fn ttl() -> Duration {
    Duration::from_secs(1)
}

impl Filesystem for FungosFs {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let Ok(mut state) = self.state.lock() else {
            reply.error(Errno::EIO);
            return;
        };
        let Some(parent_path) = state.path_for(parent.0) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let name = name.to_string_lossy();
        if name == "." || name == ".." {
            reply.error(Errno::ENOENT);
            return;
        }
        let path = join_path(&parent_path, &name);
        if let Some(entry) = state.entry(&path) {
            let ino = state.ino_for(&path, false);
            reply.entry(
                &ttl(),
                &FungosFs::attr(ino, entry.size, false),
                generation_for_path(&path),
            );
            return;
        }
        // A freshly created file has no namespace entry until its first flush,
        // so consult the write buffer before declaring it missing.
        if let Some(ino) = state.inodes.get(&path).copied()
            && let Some(bytes) = state.pending.get(&ino)
        {
            reply.entry(
                &ttl(),
                &FungosFs::attr(ino, bytes.len() as u64, false),
                generation_for_path(&path),
            );
            return;
        }
        // Directories are namespace entries in their own right, so ask the VFS
        // rather than inferring from children. Inferring made an empty
        // directory unresolvable; git needs a .vfskeep placeholder for the same
        // reason, and here we have real entries instead.
        if state.dirs.get(&path).copied().unwrap_or(false)
            || state.vfs.is_directory(&path).unwrap_or(false)
        {
            let ino = state.ino_for(&path, true);
            reply.entry(
                &ttl(),
                &FungosFs::attr(ino, 4096, true),
                generation_for_path(&path),
            );
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let Ok(state) = self.state.lock() else {
            reply.error(Errno::EIO);
            return;
        };
        let Some(path) = state.path_for(ino.0) else {
            reply.error(Errno::ENOENT);
            return;
        };
        if state.is_dir(ino.0) {
            reply.attr(&ttl(), &FungosFs::attr(ino.0, 4096, true));
            return;
        }
        let size = state
            .pending
            .get(&ino.0)
            .map(|bytes| bytes.len() as u64)
            .or_else(|| state.entry(&path).map(|entry| entry.size));
        match size {
            Some(size) => reply.attr(&ttl(), &FungosFs::attr(ino.0, size, false)),
            None => reply.error(Errno::ENOENT),
        }
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<fuser::BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let Ok(mut state) = self.state.lock() else {
            reply.error(Errno::EIO);
            return;
        };
        if let Some(size) = size {
            let mut bytes = state.data(ino.0).unwrap_or_default();
            bytes.resize(size as usize, 0);
            state.pending.insert(ino.0, bytes);
        }
        let Some(path) = state.path_for(ino.0) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let size = state
            .pending
            .get(&ino.0)
            .map(|bytes| bytes.len() as u64)
            .or_else(|| state.entry(&path).map(|entry| entry.size))
            .unwrap_or(0);
        reply.attr(&ttl(), &FungosFs::attr(ino.0, size, false));
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let Ok(mut state) = self.state.lock() else {
            reply.error(Errno::EIO);
            return;
        };
        let Some(path) = state.path_for(ino.0) else {
            reply.error(Errno::ENOENT);
            return;
        };
        if flags.0 & libc::O_ACCMODE != libc::O_RDONLY {
            let bytes = state.data(ino.0).unwrap_or_default();
            state.pending.entry(ino.0).or_insert(bytes);
        }
        if state.entry(&path).is_none() && !state.pending.contains_key(&ino.0) {
            reply.error(Errno::ENOENT);
            return;
        }
        reply.opened(FileHandle(0), FopenFlags::empty());
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyData,
    ) {
        let Ok(mut state) = self.state.lock() else {
            reply.error(Errno::EIO);
            return;
        };
        match state.data(ino.0) {
            Ok(bytes) => {
                let start = (offset as usize).min(bytes.len());
                let end = start.saturating_add(size as usize).min(bytes.len());
                reply.data(&bytes[start..end]);
            }
            Err(error) => reply.error(errno(&error)),
        }
    }

    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyWrite,
    ) {
        let Ok(mut state) = self.state.lock() else {
            reply.error(Errno::EIO);
            return;
        };
        let mut bytes = state
            .pending
            .remove(&ino.0)
            .unwrap_or_else(|| state.data(ino.0).unwrap_or_default());
        let start = offset as usize;
        if start > bytes.len() {
            bytes.resize(start, 0);
        }
        let end = start.saturating_add(data.len());
        if end > bytes.len() {
            bytes.resize(end, 0);
        }
        bytes[start..end].copy_from_slice(data);
        state.pending.insert(ino.0, bytes);
        reply.written(data.len() as u32);
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _lock_owner: fuser::LockOwner,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn release(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let Ok(mut state) = self.state.lock() else {
            reply.error(Errno::EIO);
            return;
        };
        match state.write_back(ino.0) {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(errno(&error)),
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let Ok(mut state) = self.state.lock() else {
            reply.error(Errno::EIO);
            return;
        };
        let Some(path) = state.path_for(ino.0) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let parent_ino = parent_path(&path)
            .map(|parent| state.ino_for(&parent, true))
            .unwrap_or(ROOT);
        let mut entries = vec![
            (".".to_string(), ino.0, FileType::Directory),
            ("..".to_string(), parent_ino, FileType::Directory),
        ];
        for (name, is_dir) in state.direct_children(&path) {
            let child_path = join_path(&path, &name);
            let child_ino = state.ino_for(&child_path, is_dir);
            entries.push((
                name,
                child_ino,
                if is_dir {
                    FileType::Directory
                } else {
                    FileType::RegularFile
                },
            ));
        }
        for (index, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(INodeNo(entry.1), (index + 1) as u64, entry.2, &entry.0) {
                break;
            }
        }
        reply.ok();
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let Ok(mut state) = self.state.lock() else {
            reply.error(Errno::EIO);
            return;
        };
        let Some(parent_path) = state.path_for(parent.0) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let path = join_path(&parent_path, &name.to_string_lossy());
        // Buffer the new file instead of writing an empty version now. `release`
        // flushes once, so a file that is created and written lands as a single
        // version instead of an empty v1 followed by the real v2.
        let ino = state.ino_for(&path, false);
        state.pending.insert(ino, Vec::new());
        reply.created(
            &ttl(),
            &FungosFs::attr(ino, 0, false),
            generation_for_path(&path),
            FileHandle(0),
            FopenFlags::empty(),
        );
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let Ok(mut state) = self.state.lock() else {
            reply.error(Errno::EIO);
            return;
        };
        let Some(parent_path) = state.path_for(parent.0) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let path = join_path(&parent_path, &name.to_string_lossy());
        // A file that exists only in the write buffer — created but not yet
        // released — has no namespace entry to remove; dropping the buffer is the
        // whole deletion. Falling through to `remove` reported ENOENT for a file
        // the caller had just created and could read.
        let removed = match state.entry(&path) {
            Some(_) => state.vfs.remove(&path),
            None => Ok(()),
        };
        match removed {
            Ok(()) => {
                state.forget(&path);
                reply.ok();
            }
            Err(error) => reply.error(errno(&error)),
        }
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let Ok(mut state) = self.state.lock() else {
            reply.error(Errno::EIO);
            return;
        };
        let Some(parent_path) = state.path_for(parent.0) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let path = join_path(&parent_path, &name.to_string_lossy());
        match state.vfs.mkdir(&path) {
            Ok(()) => {
                let ino = state.ino_for(&path, true);
                reply.entry(
                    &ttl(),
                    &FungosFs::attr(ino, 4096, true),
                    generation_for_path(&path),
                );
            }
            Err(error) => reply.error(errno(&error)),
        }
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let Ok(mut state) = self.state.lock() else {
            reply.error(Errno::EIO);
            return;
        };
        let Some(parent_path) = state.path_for(parent.0) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let path = join_path(&parent_path, &name.to_string_lossy());
        match state.vfs.rmdir(&path) {
            Ok(()) => {
                state.forget(&path);
                reply.ok();
            }
            Err(error) => reply.error(errno(&error)),
        }
    }

    /// `mknod` is what the kernel uses when a filesystem is asked to create a
    /// file *without* opening it — which is exactly how the kernel NFS server
    /// creates files (`vfs_create`). Only a local `open(O_CREAT)` takes the
    /// atomic `create` path. Without this handler every `touch` through the
    /// export failed with EIO, because `ENOSYS` here reaches the client as a
    /// generic I/O error.
    fn mknod(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        let Ok(mut state) = self.state.lock() else {
            reply.error(Errno::EIO);
            return;
        };
        let Some(parent_path) = state.path_for(parent.0) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let path = join_path(&parent_path, &name.to_string_lossy());
        if state.entry(&path).is_some() || state.vfs.is_directory(&path).unwrap_or(false) {
            reply.error(Errno::EEXIST);
            return;
        }
        // Commit an empty version here rather than buffering like `create` does.
        // `create` is always followed by an open, so `release` flushes the
        // buffer; `mknod` is a create with no open — which is how the kernel NFS
        // server creates files — so a buffered file would never be flushed and
        // would stay invisible in `readdir` while `lookup` still answered for it.
        // A write that follows lands as the next version.
        match state
            .vfs
            .put(&path, &[], "application/octet-stream", Expect::Any)
        {
            Ok(_) => {}
            Err(error) => {
                reply.error(errno(&error));
                return;
            }
        }
        let ino = state.ino_for(&path, false);
        reply.entry(
            &ttl(),
            &FungosFs::attr(ino, 0, false),
            generation_for_path(&path),
        );
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        _flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        let Ok(mut state) = self.state.lock() else {
            reply.error(Errno::EIO);
            return;
        };
        let (Some(parent_path), Some(new_parent_path)) =
            (state.path_for(parent.0), state.path_for(newparent.0))
        else {
            reply.error(Errno::ENOENT);
            return;
        };
        let from = join_path(&parent_path, &name.to_string_lossy());
        let to = join_path(&new_parent_path, &newname.to_string_lossy());
        // `Vfs::rename` moves one namespace entry, so directories are not
        // renamable yet; say so instead of failing as if the source vanished.
        if state.vfs.is_directory(&from).unwrap_or(false) {
            reply.error(Errno::EOPNOTSUPP);
            return;
        }
        if state.entry(&from).is_none() {
            // The file exists only as a write buffer (created and not yet
            // released). Commit it at the new path so a rename cannot leave a
            // buffer that has no namespace entry left to flush it.
            let buffered = state
                .inodes
                .get(&from)
                .copied()
                .and_then(|ino| state.pending.remove(&ino));
            match buffered {
                Some(bytes) => {
                    match state
                        .vfs
                        .put(&to, &bytes, "application/octet-stream", Expect::Any)
                    {
                        Ok(_) => {
                            state.ino_for(&to, false);
                            state.forget(&from);
                            reply.ok();
                        }
                        Err(error) => reply.error(errno(&error)),
                    }
                }
                None => reply.error(Errno::ENOENT),
            }
            return;
        }
        match state.vfs.rename(&from, &to) {
            Ok(_) => {
                state.forget(&from);
                reply.ok();
            }
            Err(error) => reply.error(errno(&error)),
        }
    }

    fn access(&self, _req: &Request, _ino: INodeNo, _mask: AccessFlags, reply: ReplyEmpty) {
        reply.ok();
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        // The blobs live on the host filesystem under the VFS root, so that
        // filesystem is the real capacity constraint. Reporting zeros made
        // `df` and anything that checks free space before writing misbehave.
        let Ok(state) = self.state.lock() else {
            reply.error(Errno::EIO);
            return;
        };
        let root = state.vfs.root();
        let Ok(c_path) = CString::new(root.as_os_str().as_bytes()) else {
            reply.error(Errno::EINVAL);
            return;
        };
        // SAFETY: `c_path` is a valid NUL-terminated string and `st` is a
        // properly sized, zeroed statvfs that libc fills on success.
        let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::statvfs(c_path.as_ptr(), &mut st) } != 0 {
            reply.error(Errno::EIO);
            return;
        }
        let block = st.f_frsize as u64;
        reply.statfs(
            st.f_blocks as u64,
            st.f_bfree as u64,
            st.f_bavail as u64,
            st.f_files as u64,
            st.f_ffree as u64,
            block as u32,
            st.f_namemax as u32,
            block as u32,
        );
    }
}

fn parent_path(path: &str) -> Option<String> {
    path.rsplit_once('/').map(|(parent, _)| parent.to_string())
}

fn join_path(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        normalize_path(name).unwrap_or_else(|_| name.to_string())
    } else {
        normalize_path(&format!("{parent}/{name}")).unwrap_or_else(|_| format!("{parent}/{name}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn joins_root_and_nested_paths() {
        assert_eq!(join_path("", "a"), "a");
        assert_eq!(join_path("docs", "a.txt"), "docs/a.txt");
    }

    #[test]
    fn inodes_are_derived_from_the_path() {
        // Same path, same inode, in any process: this is what stops NFS file
        // handles from going stale across a remount.
        assert_eq!(inode_for_path("docs/a.txt"), inode_for_path("docs/a.txt"));
        assert_ne!(inode_for_path("docs/a.txt"), inode_for_path("docs/b.txt"));
        // Reserved numbers stay reserved.
        for path in ["", "a", "b", "docs/a.txt", "ä"] {
            let ino = inode_for_path(path);
            assert!(ino > ROOT, "{path} mapped to {ino}");
        }
        assert_eq!(generation_for_path("docs/a.txt").0 % 2, 1);
    }
}
