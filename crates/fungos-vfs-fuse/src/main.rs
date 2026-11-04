//! Native filesystem adapter for a fungOS VFS workspace.
//!
//! macOS uses macFUSE; Linux uses FUSE3. The adapter is deliberately a thin
//! namespace projection: content identity, history, leases and eviction remain
//! owned by `fungos-vfs`. Reads hydrate placeholders through Unibus when a
//! client config is supplied; writes become new immutable content versions.

use std::{
    collections::HashMap,
    ffi::OsStr,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use anyhow::{Context, bail};
use clap::{Parser, ValueEnum};
use fungos_vfs::{NodeRole, Vfs, VfsError, normalize_path};
use fungos_vfs_unibus::UnibusHydrator;
use fuser::{
    AccessFlags, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, Generation,
    INodeNo, MountOption, OpenFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request, SessionACL, TimeOrNow, WriteFlags,
    mount,
};
use unibus::client::{Client, ClientConfig};

const ROOT: u64 = 1;

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
    next_ino: u64,
    pending: HashMap<u64, Vec<u8>>,
}

impl State {
    fn new(vfs: Vfs, hydrator: Option<Arc<UnibusHydrator>>) -> Self {
        let mut inodes = HashMap::new();
        let mut paths = HashMap::new();
        inodes.insert(String::new(), ROOT);
        paths.insert(ROOT, String::new());
        Self {
            vfs,
            hydrator,
            inodes,
            paths,
            dirs: HashMap::from([(String::new(), true)]),
            next_ino: ROOT + 1,
            pending: HashMap::new(),
        }
    }

    fn ino_for(&mut self, path: &str, is_dir: bool) -> u64 {
        if let Some(ino) = self.inodes.get(path) {
            return *ino;
        }
        let ino = self.next_ino;
        self.next_ino += 1;
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
            .put(&path, &bytes, "application/octet-stream", None)?;
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
        VfsError::InvalidContentId(_) => Errno::EIO,
        VfsError::Conflict { .. } => Errno::EEXIST,
        VfsError::Leased { .. } => Errno::EBUSY,
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
                Generation(0),
            );
            return;
        }
        let is_dir = state.direct_children(&path).iter().any(|(_, dir)| *dir)
            || state.dirs.get(&path).copied().unwrap_or(false);
        if is_dir {
            let ino = state.ino_for(&path, true);
            reply.entry(&ttl(), &FungosFs::attr(ino, 4096, true), Generation(0));
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
        match state.vfs.put(&path, &[], "application/octet-stream", None) {
            Ok(entry) => {
                let ino = state.ino_for(&path, false);
                state.pending.insert(ino, Vec::new());
                reply.created(
                    &ttl(),
                    &FungosFs::attr(ino, entry.size, false),
                    Generation(0),
                    FileHandle(0),
                    FopenFlags::empty(),
                );
            }
            Err(error) => reply.error(errno(&error)),
        }
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
        match state.vfs.remove(&path) {
            Ok(()) => {
                if let Some(ino) = state.inodes.remove(&path) {
                    state.paths.remove(&ino);
                    state.pending.remove(&ino);
                }
                reply.ok();
            }
            Err(error) => reply.error(errno(&error)),
        }
    }

    fn access(&self, _req: &Request, _ino: INodeNo, _mask: AccessFlags, reply: ReplyEmpty) {
        reply.ok();
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        reply.statfs(0, 0, 0, 0, 0, 4096, 255, 0);
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
}
