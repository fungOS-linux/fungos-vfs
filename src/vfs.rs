use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    ContentId, normalize_path,
    store::{ContentStore, default_data_dir},
};

#[derive(Debug, Error)]
pub enum VfsError {
    #[error("invalid path: {0}")]
    InvalidPath(String),
    #[error("invalid content id: {0}")]
    InvalidContentId(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("already exists: {0}")]
    AlreadyExists(String),
    #[error("directory is not empty: {0}")]
    NotEmpty(String),
    #[error("not hydrated: {0}")]
    NotHydrated(String),
    #[error("version conflict: expected {expected}, current {current}")]
    Conflict { expected: u64, current: u64 },
    /// A write declared the head it was replacing and the head had moved.
    #[error("write fenced: {path} was based on {expected}, head is {current}")]
    Fenced {
        path: String,
        expected: String,
        current: String,
    },
    #[error("path is leased by {owner} until {expires_at_ms}")]
    Leased { owner: String, expires_at_ms: i64 },
    /// A snapshot replaces this store's namespace wholesale, so applying one
    /// while writes are still only local would discard them. Refused rather
    /// than applied: an empty answer can be retried, a discarded write cannot
    /// be recovered.
    #[error(
        "refusing snapshot: {pending} unsynced local ops would be discarded; sync or ack them first"
    )]
    SnapshotRefused { pending: usize },
    #[error("node is a pull-through cache; writes are disabled")]
    ReadOnly,
    /// Deployment configuration is missing or unusable. Distinct from
    /// [`Self::Transport`]: nothing was attempted on the wire.
    #[error("configuration error: {0}")]
    Config(String),
    #[error("no data directory: set FUNGOS_VFS_CAS or XDG_DATA_HOME")]
    NoDataDirectory,
    #[error("transport error: {0}")]
    Transport(String),
    #[error("corrupt store metadata: {key} = {value}")]
    CorruptMeta { key: String, value: String },
    #[error("workspace role mismatch: expected {expected}, found {found}")]
    RoleMismatch { expected: NodeRole, found: NodeRole },
    /// A generation is only ever advanced, never reused. Reaching the end of
    /// the counter refuses to serve rather than wrap onto values that have
    /// already been issued.
    #[error("handle generation exhausted; refusing to reuse one")]
    GenerationExhausted,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    /// Full replica: can write, serve, and retain content.
    Full,
    /// Pull-through cache: may hydrate and evict, but does not author writes.
    Cache,
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full => f.write_str("full"),
            Self::Cache => f.write_str("cache"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Hydration {
    Present,
    Placeholder,
}

impl std::fmt::Display for Hydration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Present => f.write_str("present"),
            Self::Placeholder => f.write_str("placeholder"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileEntry {
    pub path: String,
    pub content_id: ContentId,
    pub size: u64,
    pub media_type: String,
    pub version: u64,
    pub hydration: Hydration,
    pub pinned: bool,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Lease {
    pub path: String,
    pub owner: String,
    pub expires_at_ms: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncOpKind {
    Upsert,
    Remove,
    Mkdir,
    Rmdir,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncOp {
    pub id: i64,
    pub kind: SyncOpKind,
    pub path: String,
    pub version: u64,
    pub content_id: Option<ContentId>,
    pub created_at_ms: i64,
    /// Who produced the write. A write with no author is a write nobody can
    /// explain later, which is the difference between a history and a list.
    pub actor: String,
    /// Where it came from: host, device or process.
    pub origin: String,
    /// The worktree this op belongs to, empty when the store is unscoped.
    pub worktree: String,
    /// The store that produced it. Two stores on one host share an origin, so
    /// this is the identity a cursor has to key on.
    pub store: String,
    /// Total order across writers, not just within one.
    pub hlc: Hlc,
    /// The head this write replaced, when the write produced content.
    ///
    /// Without it a peer can see that a path changed, but not what the writer
    /// believed it was changing — which is the difference between replaying a
    /// change and guessing at one.
    pub base_content_id: Option<ContentId>,
}

/// One entry in the change feed.
///
/// The cursor is this store's own row id, not a clock: a feed reader is asking
/// "what happened after what I last saw here", and a local counter answers that
/// exactly, including for ops that arrived from a peer carrying a clock this
/// store never issued.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Change {
    pub id: i64,
    pub path: String,
    pub op: SyncOpKind,
    pub content_id: Option<ContentId>,
    pub actor: String,
    pub origin: String,
    /// The worktree the write was attributed to, empty when it was not.
    pub worktree: String,
    pub at_ms: i64,
}

/// One entry of the build cache: content in the store that this root may forget.
///
/// There is no version and no history here, because there is nothing to version:
/// the entry is a hint that some bytes exist, and losing it costs a rebuild
/// rather than information.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CacheEntry {
    pub content_id: ContentId,
    /// The path the content was last produced at. A label for a human reading
    /// `cache ls`, never an address: cache content is fetched by [`CacheEntry::content_id`].
    pub path: String,
    pub media_type: String,
    pub size: u64,
    /// Content a changeset names: eviction skips it.
    pub pinned: bool,
    pub created_at_ms: i64,
    pub last_used_ms: i64,
}

/// One revision in a file's history.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Version {
    pub version: u64,
    pub content_id: ContentId,
    pub size: u64,
    pub created_at_ms: i64,
    pub actor: String,
    pub origin: String,
}

/// What a write claims about the state it is replacing.
///
/// A version number fences writers on one store, because it is a local counter
/// that means nothing anywhere else. A content id means the same thing on every
/// store, so it is the fence that holds between devices — and the one the op log
/// records as `base_content_id`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Expect<'a> {
    /// Overwrite whatever is there.
    #[default]
    Any,
    /// Replace this version, or fail. Fences one store's writers.
    Version(u64),
    /// Replace a head holding this content, or fail. Fences across devices.
    Base(&'a ContentId),
}

/// A hybrid logical clock reading.
///
/// Wall-clock milliseconds alone are not ordered: two writers in the same
/// millisecond tie, and a clock that steps backwards makes a later write sort
/// earlier. Pairing the millisecond with a counter that increments on every
/// reading gives a total order that still means something in wall-clock terms,
/// which is what a log a human reads needs. Fields are ordered the way the sort
/// must be, so `Ord` is the log order.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Hlc {
    pub ms: i64,
    pub counter: u32,
}

impl Hlc {
    /// Advance past `now_ms`, never going backwards, and return the new reading.
    fn observe(&mut self, now_ms: i64) -> Hlc {
        if now_ms > self.ms {
            self.ms = now_ms;
            self.counter = 0;
        } else {
            self.counter = self.counter.saturating_add(1);
        }
        *self
    }
}

/// The identity stamped on every write this process makes.
///
/// This is deliberately an attribute of the process rather than an argument to
/// each call: attribution that a caller can forget is attribution that is
/// missing exactly where it matters. A demultiplexing daemon sets it once.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Writer {
    pub actor: String,
    pub origin: String,
}

impl Writer {
    /// Infer who and where from the process environment.
    ///
    /// Guessing here is better than an argument every caller forgets, but it is
    /// still a guess: a service should call [`Vfs::set_writer`] with a real
    /// identity rather than inherit `unknown` from an environment that has none.
    pub fn from_environment() -> Self {
        Self {
            actor: non_empty_env(&["USER", "LOGNAME", "USERNAME"])
                .unwrap_or_else(|| "unknown".to_string()),
            origin: host_name().unwrap_or_else(|| "unknown".to_string()),
        }
    }

    pub fn new(actor: impl Into<String>, origin: impl Into<String>) -> Self {
        Self {
            actor: actor.into(),
            origin: origin.into(),
        }
    }
}

fn non_empty_env(names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| std::env::var(name).ok().filter(|value| !value.is_empty()))
}

/// The machine this process runs on.
///
/// `HOSTNAME` is not exported by default on macOS, so reading the environment
/// alone reports `unknown` on the platform that matters most. `gethostname` is
/// the primitive every platform answers; the environment stays as a fallback
/// for environments where the call is unavailable.
#[cfg(unix)]
fn host_name() -> Option<String> {
    const MAX_HOST_NAME: usize = 255;
    let mut buffer = [0u8; MAX_HOST_NAME + 1];
    // SAFETY: `buffer` is a valid, writable slice of `buffer.len()` bytes and
    // the length passed matches it, which is the contract `gethostname`
    // requires. The call writes at most that many bytes and NUL-terminates when
    // the name fits, which is the case no larger than MAX_HOST_NAME.
    let result =
        unsafe { libc::gethostname(buffer.as_mut_ptr().cast::<libc::c_char>(), buffer.len()) };
    if result == 0 {
        let end = buffer
            .iter()
            .position(|byte| *byte == 0)
            .unwrap_or(buffer.len());
        if let Ok(name) = std::str::from_utf8(&buffer[..end])
            && !name.is_empty()
        {
            return Some(name.to_string());
        }
    }
    non_empty_env(&["HOSTNAME", "COMPUTERNAME"])
}

#[cfg(not(unix))]
fn host_name() -> Option<String> {
    non_empty_env(&["HOSTNAME", "COMPUTERNAME"])
}

#[cfg(test)]
mod writer_tests {
    use super::*;

    #[test]
    fn the_environment_yields_a_usable_identity() {
        let writer = Writer::from_environment();
        assert!(!writer.actor.is_empty());
        // A hostname, not the placeholder: this test runs on a real machine.
        assert_ne!(writer.origin, "unknown", "origin should be the hostname");
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct LodPolicy {
    pub max_cache_bytes: u64,
    pub evict_after_write: bool,
}

impl Default for LodPolicy {
    fn default() -> Self {
        Self {
            max_cache_bytes: 8 * 1024 * 1024 * 1024,
            evict_after_write: false,
        }
    }
}

/// The identity and clock reading stamped on one write.
struct Stamp {
    actor: String,
    origin: String,
    store: String,
    worktree: String,
    hlc: Hlc,
}

impl Stamp {
    /// Who this stamp attributes a write to, for the change feed.
    fn who(&self) -> Attribution<'_> {
        Attribution {
            actor: &self.actor,
            origin: &self.origin,
            worktree: &self.worktree,
        }
    }
}

/// The attribution a change carries: who, where, and for which worktree.
///
/// Borrowed rather than owned because it is passed straight through to a single
/// insert — and because a remote op's attribution arrives as borrowed fields of
/// the op, not as a stamp this store made.
struct Attribution<'a> {
    actor: &'a str,
    origin: &'a str,
    worktree: &'a str,
}

/// The directory that holds one root's own state.
///
/// `.fungos` was the old name: it described a product rather than the contents,
/// and it held the content too. A root now keeps only its namespace, op log,
/// versions, cursor, horizon and diverged records here — the bytes live in the
/// machine's shared store. Roots an older build wrote keep the old name until
/// [`migrate_storage_with_cas`] retires it.
pub const STATE_DIR: &str = ".vfs-state";
/// The legacy state directory, readable until a root is migrated.
pub const LEGACY_STATE_DIR: &str = ".fungos";
const NAMESPACE_DB: &str = "vfs.sqlite3";
const OBJECTS_DIR: &str = "objects";

/// Where one root's state lives, before anything is created.
#[derive(Clone, Debug)]
struct StateLayout {
    root: PathBuf,
}

impl StateLayout {
    fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The directory holding this root's state.
    ///
    /// The current name wins when it exists; a root an older build wrote keeps
    /// using the legacy name, so nothing has to be renamed before a migration
    /// runs. Only a root with neither gets a directory created, and it gets the
    /// current name.
    fn state_dir(&self) -> PathBuf {
        let current = self.root.join(STATE_DIR);
        if current.exists() {
            return current;
        }
        let legacy = self.root.join(LEGACY_STATE_DIR);
        if legacy.exists() {
            return legacy;
        }
        current
    }

    fn db_path(&self) -> PathBuf {
        self.state_dir().join(NAMESPACE_DB)
    }

    /// The root's own object directory, if an older layout left one behind.
    fn legacy_objects(&self) -> Option<PathBuf> {
        let objects = self.root.join(LEGACY_STATE_DIR).join(OBJECTS_DIR);
        objects.is_dir().then_some(objects)
    }

    /// Rename the legacy state directory to the current name, and retire the
    /// object directory that used to live inside it.
    ///
    /// Must run with no store open on this root: renaming a directory out from
    /// under an open SQLite connection leaves it writing WAL files next to a
    /// path that no longer names its database.
    fn retire_legacy_state_dir(&self) -> Result<bool, VfsError> {
        let legacy = self.root.join(LEGACY_STATE_DIR);
        let current = self.root.join(STATE_DIR);
        if !legacy.is_dir() {
            return Ok(false);
        }
        if current.exists() {
            return Err(VfsError::InvalidPath(format!(
                "both {} and {} exist; refusing to guess which holds this root's namespace",
                legacy.display(),
                current.display()
            )));
        }
        // The per-root object directory goes with the migration. It is removed
        // when empty; a file that is not an object stays visible in the renamed
        // directory rather than disappearing here.
        let objects = legacy.join(OBJECTS_DIR);
        if objects.is_dir() {
            let _ = std::fs::remove_dir(&objects);
        }
        std::fs::rename(&legacy, &current)?;
        Ok(true)
    }
}

/// What moving a root onto the shared content store did.
#[derive(Clone, Debug, Serialize)]
pub struct StorageMigration {
    /// The shared store the root now reads content from.
    pub content_store: String,
    /// The root's state directory, after the rename.
    pub state_dir: String,
    /// Objects written into the shared store.
    pub moved: usize,
    /// Objects the shared store already had, which therefore cost nothing.
    pub already_shared: usize,
    /// Files left in the legacy object directory because they are not objects.
    pub left_behind: usize,
    /// Whether `.fungos` was renamed to `.vfs-state`.
    pub renamed: bool,
}

/// Move a root onto the machine's shared content store.
///
/// A root that has never been migrated still works: reads fall back to its legacy
/// object directory. This is what stops that, moving the blobs into the shared
/// store and retiring `.fungos`. Running it twice moves nothing the second time.
pub fn migrate_storage(root: impl AsRef<Path>) -> Result<StorageMigration, VfsError> {
    migrate_storage_with_cas(root.as_ref(), None)
}

/// [`migrate_storage`] against an explicit content store instead of the
/// machine's default one.
pub fn migrate_storage_with_cas(
    root: &Path,
    cas: Option<&Path>,
) -> Result<StorageMigration, VfsError> {
    if !root.is_dir() {
        return Err(VfsError::NotFound(root.display().to_string()));
    }
    let root = std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
    let layout = StateLayout::new(&root);
    let data = match cas {
        Some(dir) => dir.to_path_buf(),
        None => default_data_dir()?,
    };
    let store = ContentStore::open(&data, layout.legacy_objects())?;
    // Registered before the objects move: the moment content starts living in the
    // shared store, a collection has to know this root names it.
    store.register_root(&root)?;
    let moved = store.migrate_legacy()?;
    let renamed = layout.retire_legacy_state_dir()?;
    Ok(StorageMigration {
        content_store: store.data_dir().display().to_string(),
        state_dir: layout.state_dir().display().to_string(),
        moved: moved.moved,
        already_shared: moved.already_shared,
        left_behind: moved.left_behind,
        renamed,
    })
}

pub struct Vfs {
    root: PathBuf,
    role: NodeRole,
    pub(crate) db: Connection,
    pub(crate) blobs: ContentStore,
    writer: Writer,
    hlc: Hlc,
    /// The worktree this store's writes belong to, when it serves one.
    worktree: String,
    /// This store's own identity.
    ///
    /// Distinct from `origin`, which names the machine and is therefore shared
    /// by every store on it. A cursor keyed on origin would let two stores on
    /// one host mask each other's newer ops, so the cursor keys on this.
    store: String,
    /// The generation this process's file handles are issued under.
    ///
    /// Inodes are deterministic, so a handle memoised by a kernel or NFS client
    /// survives a restart and still points at the same inode number — which may
    /// now mean different content. The generation is the part of a handle that
    /// changes when that mapping can change, so a stale handle is refused
    /// instead of resolved against whatever took the inode's place.
    generation: u64,
}

impl Vfs {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, VfsError> {
        Self::open_with_role(root, NodeRole::Full)
    }

    pub fn open_with_role(root: impl AsRef<Path>, role: NodeRole) -> Result<Self, VfsError> {
        Self::open_with(root, role, None)
    }

    /// Open a root against an explicit content store instead of the machine's
    /// default one. `None` means the shared store every other root on this
    /// machine uses; tests and services that own their own data directory pass
    /// one.
    pub fn open_with(
        root: impl AsRef<Path>,
        role: NodeRole,
        cas: Option<&Path>,
    ) -> Result<Self, VfsError> {
        let root = root.as_ref().to_path_buf();
        let layout = StateLayout::new(&root);
        std::fs::create_dir_all(layout.state_dir())?;
        let data = match cas {
            Some(dir) => dir.to_path_buf(),
            None => default_data_dir()?,
        };
        let blobs = ContentStore::open(data, layout.legacy_objects())?;
        let db = Connection::open(layout.db_path())?;
        let vfs = Self {
            root,
            role,
            db,
            blobs,
            writer: Writer::from_environment(),
            hlc: Hlc::default(),
            worktree: String::new(),
            store: String::new(),
            generation: 0,
        };
        let mut vfs = vfs;
        vfs.migrate()?;
        vfs.store = vfs.ensure_store_id()?;
        // The content this root names is shared with every other root on the
        // machine, so the machine has to know this root exists: `gc` collects
        // across the roots that registered, and a root nobody listed is a root
        // whose content would be deleted.
        vfs.blobs.register_root(&vfs.canonical_root())?;
        vfs.hlc = vfs.stored_hlc()?;
        vfs.hlc.observe(now_ms());
        vfs.generation = vfs.stored_generation()?;
        let stored = vfs.stored_role()?;
        if stored != role {
            return Err(VfsError::RoleMismatch {
                expected: role,
                found: stored,
            });
        }
        Ok(vfs)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn role(&self) -> NodeRole {
        self.role
    }

    /// The identity stamped on this process's writes.
    pub fn writer(&self) -> &Writer {
        &self.writer
    }

    /// Replace the inferred identity with a real one. A daemon serving several
    /// principals should set this per request rather than inherit the process
    /// user.
    pub fn set_writer(&mut self, writer: Writer) {
        self.writer = writer;
    }

    pub fn set_actor(&mut self, actor: impl Into<String>) {
        self.writer.actor = actor.into();
    }

    /// Scope every following write to a worktree.
    ///
    /// Deliberately a property of the store rather than an argument to each
    /// write: one VFS root serves one worktree, and an op log that mixed
    /// worktrees would have to be filtered to be replayed.
    pub fn set_worktree(&mut self, worktree: impl Into<String>) {
        self.worktree = worktree.into();
    }

    pub fn worktree(&self) -> &str {
        &self.worktree
    }

    /// This store's own identity, stable across restarts.
    pub fn store(&self) -> &str {
        &self.store
    }

    /// This store's last clock reading, as the previous process left it.
    ///
    /// The pair is written on every stamp, so it is the reading the store can
    /// guarantee it has used. A missing pair means a store from before the clock
    /// was persisted, which starts where it used to: at the wall clock.
    fn stored_hlc(&self) -> Result<Hlc, VfsError> {
        let read = |key: &str| -> Result<i64, VfsError> {
            let value: Option<String> = self
                .db
                .query_row("SELECT value FROM meta WHERE key=?1", params![key], |row| {
                    row.get(0)
                })
                .optional()?;
            match value {
                None => Ok(0),
                Some(value) => value.parse::<i64>().map_err(|_| VfsError::CorruptMeta {
                    key: key.to_string(),
                    value,
                }),
            }
        };
        let counter = u32::try_from(read("hlc_counter")?).map_err(|_| VfsError::CorruptMeta {
            key: "hlc_counter".to_string(),
            value: read("hlc_counter").unwrap_or_default().to_string(),
        })?;
        Ok(Hlc {
            ms: read("hlc_ms")?,
            counter,
        })
    }

    /// Write down the reading a stamp used.
    ///
    /// Deliberately not a cache and not best-effort: a clock that quietly
    /// forgets is the defect this recording exists to remove, so a failure here
    /// is the write's failure.
    fn persist_hlc(&self, hlc: Hlc) -> Result<(), VfsError> {
        let tx = self.db.unchecked_transaction()?;
        for (key, value) in [
            ("hlc_ms", hlc.ms.to_string()),
            ("hlc_counter", hlc.counter.to_string()),
        ] {
            tx.execute(
                "INSERT INTO meta(key,value) VALUES(?1,?2)
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                params![key, value],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// The generation a file handle has to carry to be accepted.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Advance and persist the generation handles are issued under.
    ///
    /// Called once by an adapter as it starts issuing handles. Deliberately not
    /// part of `open`: a CLI command sharing the root with a live mount must
    /// not invalidate that mount's handles.
    ///
    /// A read followed by a write would let two adapters open the same store and
    /// both claim generation *n*, so this takes the write lock before reading,
    /// the same way a stamp pins which clock reading it used.
    pub fn begin_generation(&mut self) -> Result<u64, VfsError> {
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current: Option<String> = tx
            .query_row("SELECT value FROM meta WHERE key='generation'", [], |row| {
                row.get(0)
            })
            .optional()?;
        let next = match current {
            Some(value) => parse_generation(&value)?.checked_add(1),
            // A store that has never issued a handle starts at one, so zero
            // stays reserved for "never issued" and is never a live handle.
            None => Some(1),
        }
        .ok_or(VfsError::GenerationExhausted)?;
        tx.execute(
            "INSERT INTO meta(key,value) VALUES('generation',?1)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![next.to_string()],
        )?;
        tx.commit()?;
        self.generation = next;
        Ok(next)
    }

    /// The persisted generation, or zero for a store that has never issued one.
    ///
    /// A missing row is a store from before handles carried a generation; it
    /// has issued none, so zero is the honest reading. A row that does not
    /// parse is corruption and refuses the open rather than restarting at zero.
    fn stored_generation(&self) -> Result<u64, VfsError> {
        let value: Option<String> = self
            .db
            .query_row("SELECT value FROM meta WHERE key='generation'", [], |row| {
                row.get(0)
            })
            .optional()?;
        match value {
            Some(value) => parse_generation(&value),
            None => Ok(0),
        }
    }

    /// The local identity for a write: who, where, and the clock reading it is
    /// ordered by.
    fn stamp(&mut self) -> Result<Stamp, VfsError> {
        let hlc = self.hlc.observe(now_ms());
        self.persist_hlc(hlc)?;
        Ok(Stamp {
            actor: self.writer.actor.clone(),
            origin: self.writer.origin.clone(),
            store: self.store.clone(),
            worktree: self.worktree.clone(),
            hlc,
        })
    }

    fn migrate(&self) -> Result<(), VfsError> {
        self.db.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS files(
               path TEXT PRIMARY KEY,
               content_id TEXT NOT NULL,
               size INTEGER NOT NULL,
               media_type TEXT NOT NULL,
               version INTEGER NOT NULL,
               hydration TEXT NOT NULL,
               pinned INTEGER NOT NULL,
               updated_at_ms INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS directories(
               path TEXT PRIMARY KEY,
               created_at_ms INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS versions(
               path TEXT NOT NULL,
               version INTEGER NOT NULL,
               content_id TEXT NOT NULL,
               size INTEGER NOT NULL,
               created_at_ms INTEGER NOT NULL,
               actor TEXT NOT NULL DEFAULT 'unattributed',
               origin TEXT NOT NULL DEFAULT 'unattributed',
               PRIMARY KEY(path, version)
             );
             CREATE TABLE IF NOT EXISTS sync_queue(
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               op TEXT NOT NULL,
               path TEXT NOT NULL,
               version INTEGER NOT NULL,
               content_id TEXT,
               created_at_ms INTEGER NOT NULL,
               actor TEXT NOT NULL DEFAULT 'unattributed',
               origin TEXT NOT NULL DEFAULT 'unattributed',
               hlc_ms INTEGER NOT NULL DEFAULT 0,
               hlc_counter INTEGER NOT NULL DEFAULT 0,
               base_content_id TEXT,
               worktree TEXT NOT NULL DEFAULT '',
               store TEXT NOT NULL DEFAULT ''
             );
             CREATE TABLE IF NOT EXISTS applied(
               store TEXT NOT NULL,
               origin TEXT NOT NULL,
               hlc_ms INTEGER NOT NULL,
               hlc_counter INTEGER NOT NULL,
               PRIMARY KEY(store, hlc_ms, hlc_counter)
             );
             CREATE TABLE IF NOT EXISTS horizon(
               store TEXT PRIMARY KEY,
               hlc_ms INTEGER NOT NULL,
               hlc_counter INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS diverged(
               store TEXT NOT NULL,
               hlc_ms INTEGER NOT NULL,
               hlc_counter INTEGER NOT NULL,
               path TEXT NOT NULL,
               detected_at_ms INTEGER NOT NULL,
               PRIMARY KEY(store, hlc_ms, hlc_counter)
             );
             CREATE TABLE IF NOT EXISTS leases(
               path TEXT PRIMARY KEY,
               owner TEXT NOT NULL,
               expires_at_ms INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS exports(
               content_id TEXT NOT NULL,
               path TEXT NOT NULL,
               destination TEXT NOT NULL,
               recorded_at_ms INTEGER NOT NULL,
               PRIMARY KEY(content_id, destination)
             );
             CREATE TABLE IF NOT EXISTS changes(
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               path TEXT NOT NULL,
               op TEXT NOT NULL,
               content_id TEXT,
               actor TEXT NOT NULL,
               origin TEXT NOT NULL,
               worktree TEXT NOT NULL DEFAULT '',
               at_ms INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS cache(
               content_id TEXT PRIMARY KEY,
               path TEXT NOT NULL,
               media_type TEXT NOT NULL,
               size INTEGER NOT NULL,
               pinned INTEGER NOT NULL DEFAULT 0,
               created_at_ms INTEGER NOT NULL,
               last_used_ms INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS worktrees(
               name TEXT PRIMARY KEY,
               path TEXT NOT NULL,
               prefix TEXT NOT NULL,
               base_commit TEXT NOT NULL,
               base_tree TEXT NOT NULL,
               created_at_ms INTEGER NOT NULL,
               updated_at_ms INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS worktree_overrides(
               worktree TEXT NOT NULL,
               path TEXT NOT NULL,
               op TEXT NOT NULL,
               content_id TEXT,
               media_type TEXT NOT NULL,
               size INTEGER NOT NULL,
               PRIMARY KEY(worktree, path)
             );
             CREATE TABLE IF NOT EXISTS worktree_journal(
               worktree TEXT NOT NULL,
               path TEXT NOT NULL,
               op TEXT NOT NULL,
               apply INTEGER NOT NULL DEFAULT 1,
               content_id TEXT,
               media_type TEXT NOT NULL,
               size INTEGER NOT NULL,
               base_content_id TEXT,
               PRIMARY KEY(worktree, path)
             );",
        )?;
        self.db.execute(
            "INSERT OR IGNORE INTO meta(key,value) VALUES('role', ?1)",
            params![self.role.to_string()],
        )?;
        // Attribution arrived after the first stores were written. Rows that
        // predate it keep the `unattributed` default rather than being
        // backfilled with a guess.
        self.ensure_column("versions", "actor", "TEXT NOT NULL DEFAULT 'unattributed'")?;
        self.ensure_column("versions", "origin", "TEXT NOT NULL DEFAULT 'unattributed'")?;
        self.ensure_column(
            "sync_queue",
            "actor",
            "TEXT NOT NULL DEFAULT 'unattributed'",
        )?;
        self.ensure_column(
            "sync_queue",
            "origin",
            "TEXT NOT NULL DEFAULT 'unattributed'",
        )?;
        self.ensure_column("sync_queue", "hlc_ms", "INTEGER NOT NULL DEFAULT 0")?;
        self.ensure_column("sync_queue", "hlc_counter", "INTEGER NOT NULL DEFAULT 0")?;
        self.ensure_column("sync_queue", "base_content_id", "TEXT")?;
        self.ensure_column("sync_queue", "worktree", "TEXT NOT NULL DEFAULT ''")?;
        self.ensure_column("sync_queue", "store", "TEXT NOT NULL DEFAULT ''")?;

        // Receipts were keyed on origin, which two stores on one host share.
        // The table is only a record of what has been applied, so rebuilding it
        // costs a possible re-delivery — which the base check turns into
        // divergence at worst, never a lost write.
        let has_store: bool = self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('applied') WHERE name='store')",
            [],
            |row| row.get(0),
        )?;
        if !has_store {
            self.db.execute_batch(
                "DROP TABLE IF EXISTS applied;
                 CREATE TABLE applied(
                   store TEXT NOT NULL,
                   origin TEXT NOT NULL,
                   hlc_ms INTEGER NOT NULL,
                   hlc_counter INTEGER NOT NULL,
                   PRIMARY KEY(store, hlc_ms, hlc_counter)
                 );",
            )?;
        }

        let store = self.ensure_store_id()?;
        // Every row in this log was written by this store.
        self.db.execute(
            "UPDATE sync_queue SET store=?1 WHERE store=''",
            params![store],
        )?;
        Ok(())
    }

    /// This root's path with symlinks and relative components resolved, so that
    /// two processes naming one root agree on its identity.
    fn canonical_root(&self) -> PathBuf {
        std::fs::canonicalize(&self.root).unwrap_or_else(|_| self.root.clone())
    }

    /// This store's identity: generated once, then read back.
    fn ensure_store_id(&self) -> Result<String, VfsError> {
        let canonical = self.canonical_root();
        let digest = blake3::hash(canonical.to_string_lossy().as_bytes()).to_hex();
        self.db.execute(
            "INSERT OR IGNORE INTO meta(key,value) VALUES('store_id',?1)",
            params![&digest[..12]],
        )?;
        Ok(self
            .db
            .query_row("SELECT value FROM meta WHERE key='store_id'", [], |row| {
                row.get(0)
            })?)
    }

    /// Add a column to an existing table if it is not there yet.
    ///
    /// The table and column names are compile-time constants, never user input.
    fn ensure_column(&self, table: &str, column: &str, declaration: &str) -> Result<(), VfsError> {
        let present: bool = self.db.query_row(
            &format!(
                "SELECT EXISTS(SELECT 1 FROM pragma_table_info('{table}') WHERE name='{column}')"
            ),
            [],
            |row| row.get(0),
        )?;
        if !present {
            self.db.execute_batch(&format!(
                "ALTER TABLE {table} ADD COLUMN {column} {declaration}"
            ))?;
        }
        Ok(())
    }

    fn stored_role(&self) -> Result<NodeRole, VfsError> {
        let value: String =
            self.db
                .query_row("SELECT value FROM meta WHERE key='role'", [], |row| {
                    row.get(0)
                })?;
        Ok(match value.as_str() {
            "full" => NodeRole::Full,
            "cache" => NodeRole::Cache,
            _ => return Err(VfsError::InvalidPath(format!("unknown role {value}"))),
        })
    }

    pub fn put(
        &mut self,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        expect: Expect<'_>,
    ) -> Result<FileEntry, VfsError> {
        if self.role == NodeRole::Cache {
            return Err(VfsError::ReadOnly);
        }
        let path = normalize_path(path)?;
        let current = self.file(&path)?;
        match expect {
            Expect::Any => {}
            Expect::Version(expected) => {
                let current_version = current.as_ref().map_or(0, |entry| entry.version);
                if current_version != expected {
                    return Err(VfsError::Conflict {
                        expected,
                        current: current_version,
                    });
                }
            }
            Expect::Base(expected) => {
                // A write that believes it is replacing content which is no
                // longer the head would silently discard whatever replaced it.
                let current_id = current.as_ref().map(|entry| &entry.content_id);
                if current_id != Some(expected) {
                    return Err(VfsError::Fenced {
                        path: path.clone(),
                        expected: expected.to_string(),
                        current: current_id
                            .map(ToString::to_string)
                            .unwrap_or_else(|| "nothing".to_string()),
                    });
                }
            }
        }
        let content_id = self.blobs.put(bytes)?;
        let version = self.next_version(&path)?;
        let now = now_ms();
        let stamp = self.stamp()?;
        let base = current.as_ref().map(|entry| entry.content_id.0.clone());
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO files(path,content_id,size,media_type,version,hydration,pinned,updated_at_ms)
             VALUES(?1,?2,?3,?4,?5,'present',0,?6)
             ON CONFLICT(path) DO UPDATE SET content_id=excluded.content_id,size=excluded.size,
               media_type=excluded.media_type,version=excluded.version,hydration='present',
               updated_at_ms=excluded.updated_at_ms",
            params![path, content_id.to_string(), bytes.len() as i64, media_type, version as i64, now],
        )?;
        tx.execute(
            "INSERT INTO versions(path,version,content_id,size,created_at_ms,actor,origin)
             VALUES(?1,?2,?3,?4,?5,?6,?7)",
            params![
                path,
                version as i64,
                content_id.to_string(),
                bytes.len() as i64,
                now,
                stamp.actor,
                stamp.origin
            ],
        )?;
        tx.execute(
            "INSERT INTO sync_queue(op,path,version,content_id,created_at_ms,actor,origin,
                                    hlc_ms,hlc_counter,base_content_id,worktree,store)
             VALUES('upsert',?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
            params![
                path,
                version as i64,
                content_id.to_string(),
                now,
                stamp.actor,
                stamp.origin,
                stamp.hlc.ms,
                i64::from(stamp.hlc.counter),
                base,
                stamp.worktree,
                stamp.store,
            ],
        )?;
        note_change(&tx, &path, "upsert", Some(&content_id), &stamp.who(), now)?;
        tx.commit()?;
        self.file(&path)?.ok_or_else(|| VfsError::NotFound(path))
    }

    /// The next version number for a path, taken from its history.
    ///
    /// A version belongs to the path's history, not to whether the path exists
    /// at this moment. Reading it off the `files` row instead hands out 1 again
    /// to a path that was removed and written back, which collides with the
    /// history still on disk and fails the insert.
    fn next_version(&self, path: &str) -> Result<u64, VfsError> {
        let version: i64 = self.db.query_row(
            "SELECT COALESCE(MAX(version),0)+1 FROM versions WHERE path=?1",
            params![path],
            |row| row.get(0),
        )?;
        Ok(version as u64)
    }

    /// Pull-through cache fill. The bytes must match the registered content id.
    pub fn hydrate(&mut self, path: &str, bytes: &[u8]) -> Result<FileEntry, VfsError> {
        let path = normalize_path(path)?;
        let entry = self
            .file(&path)?
            .ok_or_else(|| VfsError::NotFound(path.clone()))?;
        if ContentId::from_bytes(bytes) != entry.content_id {
            return Err(VfsError::InvalidContentId(entry.content_id.to_string()));
        }
        self.blobs.put(bytes)?;
        self.db.execute(
            "UPDATE files SET hydration='present' WHERE path=?1",
            params![path],
        )?;
        self.file(&path)?.ok_or_else(|| VfsError::NotFound(path))
    }

    pub fn register_placeholder(
        &mut self,
        path: &str,
        content_id: ContentId,
        size: u64,
        media_type: &str,
    ) -> Result<FileEntry, VfsError> {
        let path = normalize_path(path)?;
        let now = now_ms();
        let version = self.next_version(&path)?;
        self.db.execute(
            "INSERT INTO files(path,content_id,size,media_type,version,hydration,pinned,updated_at_ms)
             VALUES(?1,?2,?3,?4,?5,'placeholder',0,?6)
             ON CONFLICT(path) DO UPDATE SET content_id=excluded.content_id,size=excluded.size,
               media_type=excluded.media_type,hydration='placeholder',updated_at_ms=excluded.updated_at_ms",
            params![
                path,
                content_id.to_string(),
                size as i64,
                media_type,
                version as i64,
                now
            ],
        )?;
        self.file(&path)?.ok_or_else(|| VfsError::NotFound(path))
    }

    pub fn get(&mut self, path: &str) -> Result<Vec<u8>, VfsError> {
        let path = normalize_path(path)?;
        let entry = self.file(&path)?.ok_or_else(|| VfsError::NotFound(path))?;
        if entry.hydration == Hydration::Placeholder {
            return Err(VfsError::NotHydrated(entry.content_id.to_string()));
        }
        self.blobs.get(&entry.content_id)
    }

    pub fn file(&self, path: &str) -> Result<Option<FileEntry>, VfsError> {
        let path = normalize_path(path)?;
        self.db
            .query_row(
                "SELECT path,content_id,size,media_type,version,hydration,pinned,updated_at_ms
                 FROM files WHERE path=?1",
                params![path],
                row_to_entry,
            )
            .optional()
            .map_err(VfsError::from)
    }

    pub fn find_by_content(&self, content_id: &ContentId) -> Result<Option<FileEntry>, VfsError> {
        self.db
            .query_row(
                "SELECT path,content_id,size,media_type,version,hydration,pinned,updated_at_ms
                 FROM files WHERE content_id=?1 LIMIT 1",
                params![content_id.to_string()],
                row_to_entry,
            )
            .optional()
            .map_err(VfsError::from)
    }

    pub fn list(&self, prefix: &str) -> Result<Vec<FileEntry>, VfsError> {
        let prefix = if prefix.is_empty() {
            String::new()
        } else {
            normalize_path(prefix)?
        };
        let mut statement = self.db.prepare(
            "SELECT path,content_id,size,media_type,version,hydration,pinned,updated_at_ms
             FROM files WHERE path LIKE ?1 ORDER BY path",
        )?;
        let rows = statement.query_map(params![format!("{prefix}%")], row_to_entry)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(VfsError::from)
    }

    pub fn history(&self, path: &str) -> Result<Vec<Version>, VfsError> {
        let path = normalize_path(path)?;
        let mut statement = self.db.prepare(
            "SELECT version,content_id,size,created_at_ms,actor,origin
             FROM versions WHERE path=?1 ORDER BY version",
        )?;
        let rows = statement.query_map(params![path], |row| {
            Ok(Version {
                version: row.get::<_, i64>(0)? as u64,
                content_id: ContentId(row.get(1)?),
                size: row.get::<_, i64>(2)? as u64,
                created_at_ms: row.get(3)?,
                actor: row.get(4)?,
                origin: row.get(5)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(VfsError::from)
    }

    pub fn remove(&mut self, path: &str) -> Result<(), VfsError> {
        if self.role == NodeRole::Cache {
            return Err(VfsError::ReadOnly);
        }
        let path = normalize_path(path)?;
        let current = self
            .file(&path)?
            .ok_or_else(|| VfsError::NotFound(path.clone()))?;
        let stamp = self.stamp()?;
        let tx = self.db.transaction()?;
        tx.execute("DELETE FROM files WHERE path=?1", params![path])?;
        tx.execute(
            "INSERT INTO sync_queue(op,path,version,content_id,created_at_ms,actor,origin,
                                    hlc_ms,hlc_counter,worktree,store)
             VALUES('remove',?1,?2,NULL,?3,?4,?5,?6,?7,?8,?9)",
            params![
                path,
                current.version as i64,
                now_ms(),
                stamp.actor,
                stamp.origin,
                stamp.hlc.ms,
                i64::from(stamp.hlc.counter),
                stamp.worktree,
                stamp.store,
            ],
        )?;
        note_change(&tx, &path, "remove", None, &stamp.who(), now_ms())?;
        tx.commit()?;
        Ok(())
    }

    /// Queue a removal for a path this namespace does not hold.
    ///
    /// A removal is a fact whether or not the file was here: Git's tree pointer
    /// covers files that never became blobs, so a file deleted from a worktree has
    /// nothing in the namespace to delete. Dropping the op would leave every
    /// reader believing the file is still there, so the op is recorded and peers
    /// apply it as the no-op it is for them.
    pub fn tombstone(&mut self, path: &str) -> Result<(), VfsError> {
        if self.role == NodeRole::Cache {
            return Err(VfsError::ReadOnly);
        }
        let path = normalize_path(path)?;
        let now = now_ms();
        let stamp = self.stamp()?;
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO sync_queue(op,path,version,content_id,created_at_ms,actor,origin,
                                    hlc_ms,hlc_counter,worktree,store)
             VALUES('remove',?1,0,NULL,?2,?3,?4,?5,?6,?7,?8)",
            params![
                path,
                now,
                stamp.actor,
                stamp.origin,
                stamp.hlc.ms,
                i64::from(stamp.hlc.counter),
                stamp.worktree,
                stamp.store,
            ],
        )?;
        note_change(&tx, &path, "remove", None, &stamp.who(), now)?;
        tx.commit()?;
        Ok(())
    }

    /// Create an explicit directory. Directories are namespace nodes with no
    /// content; they exist so empty directories survive a round trip.
    pub fn mkdir(&mut self, path: &str) -> Result<(), VfsError> {
        if self.role == NodeRole::Cache {
            return Err(VfsError::ReadOnly);
        }
        let path = normalize_path(path)?;
        if path.is_empty() || self.file(&path)?.is_some() || self.is_directory(&path)? {
            return Err(VfsError::AlreadyExists(path));
        }
        let parent = path
            .rsplit_once('/')
            .map(|(parent, _)| parent)
            .unwrap_or("");
        if !self.is_directory(parent)? {
            return Err(VfsError::NotFound(parent.to_string()));
        }
        let stamp = self.stamp()?;
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO directories(path,created_at_ms) VALUES(?1,?2)",
            params![path, now_ms()],
        )?;
        tx.execute(
            "INSERT INTO sync_queue(op,path,version,content_id,created_at_ms,actor,origin,
                                    hlc_ms,hlc_counter,worktree,store)
             VALUES('mkdir',?1,1,NULL,?2,?3,?4,?5,?6,?7,?8)",
            params![
                path,
                now_ms(),
                stamp.actor,
                stamp.origin,
                stamp.hlc.ms,
                i64::from(stamp.hlc.counter),
                stamp.worktree,
                stamp.store,
            ],
        )?;
        note_change(&tx, &path, "mkdir", None, &stamp.who(), now_ms())?;
        tx.commit()?;
        Ok(())
    }

    /// Remove an empty explicit directory. Implicit directories disappear when
    /// their last descendant file is removed.
    pub fn rmdir(&mut self, path: &str) -> Result<(), VfsError> {
        if self.role == NodeRole::Cache {
            return Err(VfsError::ReadOnly);
        }
        let path = normalize_path(path)?;
        if path.is_empty() || !self.is_directory(&path)? {
            return Err(VfsError::NotFound(path));
        }
        let exists: bool = self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM files WHERE path LIKE ?1
              UNION ALL SELECT 1 FROM directories WHERE path LIKE ?1)",
            params![format!("{path}/%")],
            |row| row.get(0),
        )?;
        if exists {
            return Err(VfsError::NotEmpty(path));
        }
        let stamp = self.stamp()?;
        let tx = self.db.transaction()?;
        tx.execute("DELETE FROM directories WHERE path=?1", params![path])?;
        tx.execute(
            "INSERT INTO sync_queue(op,path,version,content_id,created_at_ms,actor,origin,
                                    hlc_ms,hlc_counter,worktree,store)
             VALUES('rmdir',?1,1,NULL,?2,?3,?4,?5,?6,?7,?8)",
            params![
                path,
                now_ms(),
                stamp.actor,
                stamp.origin,
                stamp.hlc.ms,
                i64::from(stamp.hlc.counter),
                stamp.worktree,
                stamp.store,
            ],
        )?;
        note_change(&tx, &path, "rmdir", None, &stamp.who(), now_ms())?;
        tx.commit()?;
        Ok(())
    }

    pub fn is_directory(&self, path: &str) -> Result<bool, VfsError> {
        if path.is_empty() {
            return Ok(true);
        }
        let path = normalize_path(path)?;
        let exists: bool = self.db.query_row(
            "SELECT EXISTS(
               SELECT 1 FROM directories WHERE path=?1
               UNION ALL SELECT 1 FROM files WHERE path LIKE ?2
             )",
            params![path, format!("{path}/%")],
            |row| row.get(0),
        )?;
        Ok(exists)
    }

    /// Explicit directory entries. Implicit ancestors of files are not included.
    pub fn directories(&self, prefix: &str) -> Result<Vec<String>, VfsError> {
        let prefix = if prefix.is_empty() {
            String::new()
        } else {
            normalize_path(prefix)?
        };
        let mut statement = self
            .db
            .prepare("SELECT path FROM directories WHERE path LIKE ?1 ORDER BY path")?;
        let rows = statement.query_map(params![format!("{prefix}%")], |row| row.get(0))?;
        rows.collect::<Result<Vec<_>, _>>().map_err(VfsError::from)
    }

    /// Move one namespace entry and its history. Content is not rewritten:
    /// the ContentId is unchanged and removal/addition are queued for sync.
    pub fn rename(&mut self, from: &str, to: &str) -> Result<FileEntry, VfsError> {
        if self.role == NodeRole::Cache {
            return Err(VfsError::ReadOnly);
        }
        let from = normalize_path(from)?;
        let to = normalize_path(to)?;
        if from == to {
            return self
                .file(&from)?
                .ok_or_else(|| VfsError::NotFound(from.clone()));
        }
        let current = self
            .file(&from)?
            .ok_or_else(|| VfsError::NotFound(from.clone()))?;
        if self.file(&to)?.is_some() {
            return Err(VfsError::AlreadyExists(to));
        }
        let now = now_ms();
        let stamp = self.stamp()?;
        let tx = self.db.transaction()?;
        tx.execute("UPDATE files SET path=?1 WHERE path=?2", params![to, from])?;
        tx.execute(
            "UPDATE versions SET path=?1 WHERE path=?2",
            params![to, from],
        )?;
        tx.execute(
            "INSERT INTO sync_queue(op,path,version,content_id,created_at_ms,actor,origin,
                                    hlc_ms,hlc_counter,worktree,store)
             VALUES('remove',?1,?2,NULL,?3,?4,?5,?6,?7,?8,?9)",
            params![
                from,
                current.version as i64,
                now,
                stamp.actor,
                stamp.origin,
                stamp.hlc.ms,
                i64::from(stamp.hlc.counter),
                stamp.worktree,
                stamp.store,
            ],
        )?;
        tx.execute(
            "INSERT INTO sync_queue(op,path,version,content_id,created_at_ms,actor,origin,
                                    hlc_ms,hlc_counter,worktree,store)
             VALUES('upsert',?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![
                to,
                current.version as i64,
                current.content_id.0,
                now,
                stamp.actor,
                stamp.origin,
                stamp.hlc.ms,
                i64::from(stamp.hlc.counter),
                stamp.worktree,
                stamp.store,
            ],
        )?;
        // Two changes, not one: the feed answers "what happened to this path",
        // and a reader watching the destination and a reader watching the source
        // are both right.
        note_change(&tx, &from, "remove", None, &stamp.who(), now)?;
        note_change(
            &tx,
            &to,
            "upsert",
            Some(&current.content_id),
            &stamp.who(),
            now,
        )?;
        tx.commit()?;
        let mut entry = current;
        entry.path = to;
        Ok(entry)
    }

    pub fn set_pinned(&self, path: &str, pinned: bool) -> Result<(), VfsError> {
        let path = normalize_path(path)?;
        self.db.execute(
            "UPDATE files SET pinned=?1 WHERE path=?2",
            params![i64::from(pinned), path],
        )?;
        Ok(())
    }

    /// Record that what is under `prefix` was published to `destination`.
    ///
    /// An export is a promise that this content exists somewhere outside the
    /// namespace — a release directory, a tarball, a build artifact. Those
    /// content ids stay reachable for `gc` even after the namespace moves on,
    /// because a collection that deletes what a release was built from leaves the
    /// release pointing at nothing. Returns the number of entries recorded.
    pub fn record_export(&self, prefix: &str, destination: &Path) -> Result<usize, VfsError> {
        let destination = destination.to_string_lossy().to_string();
        let now = now_ms();
        let mut recorded = 0;
        for entry in self.list(prefix)? {
            if entry.hydration != Hydration::Present {
                continue;
            }
            recorded += self.db.execute(
                "INSERT INTO exports(content_id,path,destination,recorded_at_ms)
                 VALUES(?1,?2,?3,?4)
                 ON CONFLICT(content_id,destination) DO UPDATE SET path=excluded.path,
                   recorded_at_ms=excluded.recorded_at_ms",
                params![entry.content_id.to_string(), entry.path, destination, now],
            )?;
        }
        Ok(recorded)
    }

    pub fn lease(&self, path: &str, owner: &str, ttl_ms: u64) -> Result<Lease, VfsError> {
        let path = normalize_path(path)?;
        let now = now_ms();
        if let Some((lease_owner, expires)) = self
            .db
            .query_row(
                "SELECT owner,expires_at_ms FROM leases WHERE path=?1 AND expires_at_ms>?2",
                params![path, now],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()?
        {
            return Err(VfsError::Leased {
                owner: lease_owner,
                expires_at_ms: expires,
            });
        }
        let expires_at_ms = now + ttl_ms as i64;
        self.db.execute(
            "INSERT INTO leases(path,owner,expires_at_ms) VALUES(?1,?2,?3)
             ON CONFLICT(path) DO UPDATE SET owner=excluded.owner,expires_at_ms=excluded.expires_at_ms",
            params![path, owner, expires_at_ms],
        )?;
        Ok(Lease {
            path,
            owner: owner.into(),
            expires_at_ms,
        })
    }

    pub fn release(&self, path: &str, owner: &str) -> Result<(), VfsError> {
        let path = normalize_path(path)?;
        self.db.execute(
            "DELETE FROM leases WHERE path=?1 AND owner=?2",
            params![path, owner],
        )?;
        Ok(())
    }

    /// Record that a remote op has been applied. Returns 0 when it already was.
    ///
    /// This is the duplicate check: a sender that retries, or a second path
    /// delivering the same op, must not apply a write twice.
    pub(crate) fn mark_applied(&self, store: &str, hlc: Hlc) -> Result<usize, VfsError> {
        self.db
            .execute(
                "INSERT OR IGNORE INTO applied(store,origin,hlc_ms,hlc_counter) VALUES(?1,?2,?3,?4)",
                params![store, store, hlc.ms, i64::from(hlc.counter)],
            )
            .map_err(VfsError::from)
    }

    /// Settle a divergence by taking the peer's version.
    ///
    /// The copy becomes the file and the record stops holding compaction back.
    /// The other answer — keep ours — is `resolve_divergence` with the copy left
    /// in place, because it is then the only remaining record of what arrived.
    ///
    /// Lives here rather than beside the record because it is a local write,
    /// and a local write needs this store's stamp.
    pub fn accept_divergence(&mut self, store: &str, hlc: Hlc) -> Result<bool, VfsError> {
        let found = self
            .divergences()?
            .into_iter()
            .find(|entry| entry.store == store && entry.hlc == hlc);
        let Some(divergence) = found else {
            return Ok(false);
        };
        let copy = divergence.copy_path();
        if self.file(&copy)?.is_none() {
            // Without the bytes there is nothing to accept, and dropping the
            // record would erase the only trace that a write was refused.
            return Err(VfsError::NotFound(copy));
        }
        let bytes = self.get(&copy)?;
        let current = self.file(&divergence.path)?;
        let stamp = self.stamp()?;
        let worktree = stamp.worktree.clone();
        self.apply_put(
            &divergence.path,
            &bytes,
            current.as_ref(),
            &stamp.actor,
            &stamp.origin,
            &worktree,
        )?;
        self.resolve_divergence(store, hlc)?;
        self.apply_remove(&copy, &stamp.actor, &stamp.origin, &worktree)?;
        Ok(true)
    }

    /// Record that an op was refused because the head had moved, and drop the
    /// receipt for it.
    ///
    /// The receipt goes because nothing landed, but the fact of the refusal is
    /// kept: "we deliberately deferred this write" and "we never saw it" are
    /// different facts, and what compaction does to the log must not be allowed
    /// to erase the difference. The record is removed when the divergence is
    /// resolved — either by the op finally fitting, or by a caller saying so.
    ///
    /// The refused bytes are written to a visible path *first*. Deleting the
    /// receipt is what makes that ordering mandatory rather than tidy: with no
    /// receipt, an op the horizon has passed reads as a duplicate, so a write
    /// that never landed would look like one that had. Keeping the copy is what
    /// lets the horizon advance over a refusal honestly, and it is why the cost
    /// of a divergence is disk instead of a log that cannot be compacted.
    pub(crate) fn mark_diverged(
        &mut self,
        store: &str,
        hlc: Hlc,
        path: &str,
        bytes: &[u8],
        actor: &str,
        origin: &str,
    ) -> Result<(), VfsError> {
        let detected_at_ms = now_ms();
        let copy = crate::sync::conflict_path(path, store, detected_at_ms);
        let current = self.file(&copy)?;
        let worktree = self.worktree.clone();
        self.apply_put(&copy, bytes, current.as_ref(), actor, origin, &worktree)?;

        let tx = self.db.unchecked_transaction()?;
        tx.execute(
            "DELETE FROM applied WHERE store=?1 AND hlc_ms=?2 AND hlc_counter=?3",
            params![store, hlc.ms, i64::from(hlc.counter)],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO diverged(store,hlc_ms,hlc_counter,path,detected_at_ms)
             VALUES(?1,?2,?3,?4,?5)",
            params![store, hlc.ms, i64::from(hlc.counter), path, detected_at_ms],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// High-water marks for ops applied from other stores.
    pub(crate) fn applied_cursor(&self) -> Result<crate::SyncCursor, VfsError> {
        let mut cursor = crate::SyncCursor::new();
        let mut statement = self
            .db
            .prepare("SELECT store, hlc_ms, hlc_counter FROM applied")?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                Hlc {
                    ms: row.get(1)?,
                    counter: row.get::<_, i64>(2)? as u32,
                },
            ))
        })?;
        for row in rows {
            let (origin, hlc) = row?;
            cursor.observe(&origin, hlc);
        }
        Ok(cursor)
    }

    pub(crate) fn has_content(&self, content_id: &ContentId) -> Result<bool, VfsError> {
        Ok(self.blobs.get(content_id).is_ok())
    }

    /// Bytes for a content id, independent of any path.
    pub fn content(&self, content_id: &ContentId) -> Result<Vec<u8>, VfsError> {
        self.blobs.get(content_id)
    }

    /// Apply a remote write without enqueueing a new local op.
    ///
    /// The op is already in the origin's log; recording it again here would
    /// make every store re-broadcast every op it ever received, and the log
    /// would grow with echoes instead of events.
    pub(crate) fn apply_put(
        &mut self,
        path: &str,
        bytes: &[u8],
        current: Option<&FileEntry>,
        actor: &str,
        origin: &str,
        worktree: &str,
    ) -> Result<(), VfsError> {
        let path = normalize_path(path)?;
        let content_id = self.blobs.put(bytes)?;
        let version = self.next_version(&path)?;
        let media_type = current
            .map(|entry| entry.media_type.clone())
            .unwrap_or_else(|| "application/octet-stream".to_string());
        let now = now_ms();
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO files(path,content_id,size,media_type,version,hydration,pinned,updated_at_ms)
             VALUES(?1,?2,?3,?4,?5,'present',0,?6)
             ON CONFLICT(path) DO UPDATE SET content_id=excluded.content_id,size=excluded.size,
               media_type=excluded.media_type,version=excluded.version,hydration='present',
               updated_at_ms=excluded.updated_at_ms",
            params![
                path,
                content_id.to_string(),
                bytes.len() as i64,
                media_type,
                version as i64,
                now
            ],
        )?;
        tx.execute(
            "INSERT INTO versions(path,version,content_id,size,created_at_ms,actor,origin)
             VALUES(?1,?2,?3,?4,?5,?6,?7)",
            params![
                path,
                version as i64,
                content_id.to_string(),
                bytes.len() as i64,
                now,
                actor,
                origin
            ],
        )?;
        // The local delivery queue stays untouched — this op is not ours to
        // forward — but the change feed is a record of what happened here, and a
        // peer's write happened here.
        note_change(
            &tx,
            &path,
            "upsert",
            Some(&content_id),
            &Attribution {
                actor,
                origin,
                worktree,
            },
            now,
        )?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn apply_remove(
        &mut self,
        path: &str,
        actor: &str,
        origin: &str,
        worktree: &str,
    ) -> Result<(), VfsError> {
        let path = normalize_path(path)?;
        let tx = self.db.transaction()?;
        tx.execute("DELETE FROM files WHERE path=?1", params![path])?;
        note_change(
            &tx,
            &path,
            "remove",
            None,
            &Attribution {
                actor,
                origin,
                worktree,
            },
            now_ms(),
        )?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn apply_mkdir(
        &mut self,
        path: &str,
        actor: &str,
        origin: &str,
        worktree: &str,
    ) -> Result<(), VfsError> {
        let path = normalize_path(path)?;
        let now = now_ms();
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT OR IGNORE INTO directories(path,created_at_ms) VALUES(?1,?2)",
            params![path, now],
        )?;
        note_change(
            &tx,
            &path,
            "mkdir",
            None,
            &Attribution {
                actor,
                origin,
                worktree,
            },
            now,
        )?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn apply_rmdir(
        &mut self,
        path: &str,
        actor: &str,
        origin: &str,
        worktree: &str,
    ) -> Result<(), VfsError> {
        let path = normalize_path(path)?;
        let now = now_ms();
        let tx = self.db.transaction()?;
        tx.execute("DELETE FROM directories WHERE path=?1", params![path])?;
        note_change(
            &tx,
            &path,
            "rmdir",
            None,
            &Attribution {
                actor,
                origin,
                worktree,
            },
            now,
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn pending(&self, limit: usize) -> Result<Vec<SyncOp>, VfsError> {
        let mut statement = self.db.prepare(
            "SELECT id,op,path,version,content_id,created_at_ms,actor,origin,
                    hlc_ms,hlc_counter,base_content_id,worktree,store
             FROM sync_queue ORDER BY hlc_ms, hlc_counter, id LIMIT ?1",
        )?;
        let rows = statement.query_map(params![limit as i64], |row| {
            let op: String = row.get(1)?;
            Ok(SyncOp {
                id: row.get(0)?,
                kind: match op.as_str() {
                    "upsert" => SyncOpKind::Upsert,
                    "remove" => SyncOpKind::Remove,
                    "mkdir" => SyncOpKind::Mkdir,
                    "rmdir" => SyncOpKind::Rmdir,
                    _ => return Err(rusqlite::Error::InvalidQuery),
                },
                path: row.get(2)?,
                version: row.get::<_, i64>(3)? as u64,
                content_id: row.get::<_, Option<String>>(4)?.map(ContentId),
                created_at_ms: row.get(5)?,
                actor: row.get(6)?,
                origin: row.get(7)?,
                hlc: Hlc {
                    ms: row.get(8)?,
                    counter: row.get::<_, i64>(9)? as u32,
                },
                base_content_id: row.get::<_, Option<String>>(10)?.map(ContentId),
                worktree: row.get(11)?,
                store: row.get(12)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(VfsError::from)
    }

    pub fn ack(&self, ids: &[i64]) -> Result<(), VfsError> {
        let tx = self.db.unchecked_transaction()?;
        for id in ids {
            tx.execute("DELETE FROM sync_queue WHERE id=?1", params![id])?;
        }
        tx.commit()?;
        Ok(())
    }

    /// What changed here after `since`, newest last.
    ///
    /// `prefix` scopes the answer to a subtree of the namespace and `worktree`
    /// to ops that were attributed to one; either matches, because a write made
    /// through the plain namespace API under a worktree's prefix is still a
    /// change to that worktree, and a worktree's record is still that worktree's
    /// even if a path moved out from under it. An empty prefix with an empty
    /// worktree asks for everything.
    pub fn changes(
        &self,
        since: i64,
        prefix: &str,
        worktree: &str,
        limit: usize,
    ) -> Result<Vec<Change>, VfsError> {
        let needle = if prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", prefix.trim_matches('/'))
        };
        let mut statement = self.db.prepare(
            "SELECT id,path,op,content_id,actor,origin,worktree,at_ms FROM changes
             WHERE id>?1 AND (?2<>'' AND worktree=?2 OR substr(path,1,?3)=?4)
             ORDER BY id LIMIT ?5",
        )?;
        let rows = statement.query_map(
            params![
                since,
                worktree,
                needle.chars().count() as i64,
                needle,
                limit as i64
            ],
            |row| {
                let op: String = row.get(2)?;
                Ok(Change {
                    id: row.get(0)?,
                    path: row.get(1)?,
                    op: match op.as_str() {
                        "upsert" => SyncOpKind::Upsert,
                        "remove" => SyncOpKind::Remove,
                        "mkdir" => SyncOpKind::Mkdir,
                        "rmdir" => SyncOpKind::Rmdir,
                        _ => return Err(rusqlite::Error::InvalidQuery),
                    },
                    content_id: row.get::<_, Option<String>>(3)?.map(ContentId),
                    actor: row.get(4)?,
                    origin: row.get(5)?,
                    worktree: row.get(6)?,
                    at_ms: row.get(7)?,
                })
            },
        )?;
        rows.collect::<Result<Vec<_>, _>>().map_err(VfsError::from)
    }

    pub fn cache_bytes(&self) -> Result<u64, VfsError> {
        let bytes: i64 = self.db.query_row(
            "SELECT COALESCE(SUM(size),0) FROM files WHERE hydration='present'",
            [],
            |row| row.get(0),
        )?;
        Ok(bytes.max(0) as u64)
    }

    /// File a build's output in the store, and nowhere else.
    ///
    /// Deliberately not [`Vfs::put`]: no namespace entry, no version, no op, and
    /// therefore no authorship. A build's intermediate output is not something
    /// anyone wrote, and putting it in the op log would make every peer replay a
    /// compiler's scratch files as if they were edits.
    ///
    /// What a reader gets back is the [`ContentId`], and that is the only handle:
    /// cache content is fetched by id, never by path. The path is recorded so a
    /// human reading `cache ls` can tell what produced the bytes.
    pub fn put_cache(
        &self,
        path: &str,
        bytes: &[u8],
        media_type: &str,
    ) -> Result<ContentId, VfsError> {
        if self.role == NodeRole::Cache {
            return Err(VfsError::ReadOnly);
        }
        let content_id = self.blobs.put(bytes)?;
        let now = now_ms();
        self.db.execute(
            "INSERT INTO cache(content_id,path,media_type,size,pinned,created_at_ms,last_used_ms)
             VALUES(?1,?2,?3,?4,0,?5,?5)
             ON CONFLICT(content_id) DO UPDATE SET path=excluded.path,
               last_used_ms=excluded.last_used_ms",
            params![
                content_id.to_string(),
                path,
                media_type,
                bytes.len() as i64,
                now
            ],
        )?;
        Ok(content_id)
    }

    /// Mark cache content as named by a changeset, so eviction leaves it alone.
    pub fn pin_cache(&self, content_id: &ContentId, pinned: bool) -> Result<(), VfsError> {
        self.db.execute(
            "UPDATE cache SET pinned=?1 WHERE content_id=?2",
            params![i64::from(pinned), content_id.to_string()],
        )?;
        Ok(())
    }

    pub fn cache_entries(&self) -> Result<Vec<CacheEntry>, VfsError> {
        let mut statement = self.db.prepare(
            "SELECT content_id,path,media_type,size,pinned,created_at_ms,last_used_ms
             FROM cache ORDER BY last_used_ms",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(CacheEntry {
                content_id: ContentId(row.get(0)?),
                path: row.get(1)?,
                media_type: row.get(2)?,
                size: row.get::<_, i64>(3)? as u64,
                pinned: row.get::<_, i64>(4)? != 0,
                created_at_ms: row.get(5)?,
                last_used_ms: row.get(6)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(VfsError::from)
    }

    pub fn cached_bytes(&self) -> Result<u64, VfsError> {
        let bytes: i64 =
            self.db
                .query_row("SELECT COALESCE(SUM(size),0) FROM cache", [], |row| {
                    row.get(0)
                })?;
        Ok(bytes.max(0) as u64)
    }

    /// Drop cache entries until the cache class fits in `max_bytes`, oldest first.
    ///
    /// Size is the axis, not age: a twelve-hour-old build tree may be the only
    /// copy of an hour of work. What makes that safe is the class — a cache entry
    /// is the one thing whose loss costs a rebuild and nothing else.
    ///
    /// Only the entry goes; the bytes stay until a later [`Vfs::gc`], because the
    /// store is shared and another root may hold the same content. Eviction is
    /// this root forgetting its claim, not the machine deleting data.
    ///
    /// Pinned entries are skipped, which is what lets a changeset name an artifact
    /// and have it survive the ceiling. A build in progress is *not* protected
    /// here yet: the tree-scoped lease that would do it is a follow-up, and until
    /// it exists eviction can drop the tree under a running build.
    pub fn evict_cache_to(&self, max_bytes: u64) -> Result<usize, VfsError> {
        let mut total = self.cached_bytes()?;
        if total <= max_bytes {
            return Ok(0);
        }
        let victims: Vec<(String, u64)> = {
            let mut statement = self.db.prepare(
                "SELECT content_id,size FROM cache WHERE pinned=0
                 ORDER BY last_used_ms, created_at_ms",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        let mut evicted = 0;
        for (content_id, size) in victims {
            if total <= max_bytes {
                break;
            }
            self.db
                .execute("DELETE FROM cache WHERE content_id=?1", params![content_id])?;
            total = total.saturating_sub(size);
            evicted += 1;
        }
        Ok(evicted)
    }

    /// Evict oldest unpinned, unleased present files until the cache fits.
    ///
    /// The bytes are not deleted: they are shared with every other root on the
    /// machine, so one root dropping its copy cannot speak for the rest. An
    /// eviction drops this root's claim on the content — the entry becomes a
    /// placeholder and stops counting against the cache — and [`Vfs::gc`]
    /// collects what nothing on the machine names any more.
    pub fn evict_to(&mut self, max_bytes: u64) -> Result<usize, VfsError> {
        let now = now_ms();
        let mut total = self.cache_bytes()?;
        if total <= max_bytes {
            return Ok(0);
        }
        let mut statement = self.db.prepare(
            "SELECT f.path FROM files f
             WHERE f.hydration='present' AND f.pinned=0
               AND NOT EXISTS(SELECT 1 FROM leases l WHERE l.path=f.path AND l.expires_at_ms>?1)
             ORDER BY f.updated_at_ms, f.path",
        )?;
        let paths = statement
            .query_map(params![now], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        let mut evicted = 0;
        for path in paths {
            if total <= max_bytes {
                break;
            }
            let entry = self
                .file(&path)?
                .ok_or_else(|| VfsError::NotFound(path.clone()))?;
            self.db.execute(
                "UPDATE files SET hydration='placeholder' WHERE path=?1",
                params![path],
            )?;
            total = total.saturating_sub(entry.size);
            evicted += 1;
        }
        Ok(evicted)
    }

    /// Collect shared content no root on this machine names any more.
    ///
    /// The reachable set is machine-wide, because the store is: this root's
    /// namespace, history, pins and exports, plus the same for every root
    /// registered with the store. Collecting against one root's namespace would
    /// delete content every other root points at, which is the shape of data loss
    /// a shared store makes possible.
    ///
    /// A root that no longer exists is stale: its marker is dropped, and the
    /// content it used to name is nobody's. A root that exists but cannot be read
    /// is an error rather than a skip — guessing that it names nothing would
    /// delete content the guess was wrong about.
    pub fn gc(&self) -> Result<usize, VfsError> {
        let own = self.canonical_root();
        let mut reachable = reachable_in(&self.db)?;
        for root in self.blobs.registered_roots()? {
            if root == own {
                continue;
            }
            match reachable_at(&root)? {
                Some(ids) => reachable.extend(ids),
                None => self.blobs.forget_root(&root)?,
            }
        }
        let reachable: Vec<ContentId> = reachable.into_iter().collect();
        self.blobs.gc(reachable.iter())
    }
}

/// Content a root's state says must survive collection: what its namespace names
/// now, what it pinned, what it has ever named, and what it published as an
/// artifact.
fn reachable_in(db: &Connection) -> Result<BTreeSet<ContentId>, VfsError> {
    let mut reachable = BTreeSet::new();
    let mut statement = db.prepare(
        "SELECT content_id FROM files WHERE hydration='present'
         UNION SELECT content_id FROM files WHERE pinned=1
         UNION SELECT content_id FROM versions",
    )?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    for row in rows {
        reachable.insert(ContentId(row?));
    }
    // A store written before exports existed simply has a smaller reachable set.
    if table_exists(db, "exports")? {
        let mut statement = db.prepare("SELECT content_id FROM exports")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        for row in rows {
            reachable.insert(ContentId(row?));
        }
    }
    // Cache content is reachable *to gc* and survivable only through eviction.
    // Collecting it here and evicting it there would be two collectors racing to
    // delete the same bytes, each undoing the other's policy: the ceiling is the
    // rule for cache content, and reachability is the rule for everything else.
    if table_exists(db, "cache")? {
        let mut statement = db.prepare("SELECT content_id FROM cache")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        for row in rows {
            reachable.insert(ContentId(row?));
        }
    }
    Ok(reachable)
}

fn table_exists(db: &Connection, table: &str) -> Result<bool, VfsError> {
    Ok(db.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
        params![table],
        |row| row.get(0),
    )?)
}

/// Another root's reachable set, read without taking its namespace over.
///
/// `None` means the root is gone, which is what a stale registration looks like.
fn reachable_at(root: &Path) -> Result<Option<BTreeSet<ContentId>>, VfsError> {
    if !root.is_dir() {
        return Ok(None);
    }
    let db_path = StateLayout::new(root).db_path();
    if !db_path.is_file() {
        return Ok(None);
    }
    let db = Connection::open_with_flags(
        &db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    reachable_in(&db).map(Some)
}

/// Append one entry to the change feed.
///
/// Written inside the same transaction as the mutation it describes, so the feed
/// and the namespace can never disagree about what happened. Deliberately not the
/// delivery queue: an op leaves `sync_queue` once a peer has it and the log is
/// compacted, while a reader of the feed is asking what changed here — a pull to
/// another machine is not an answer to that question, and a compaction must not
/// be one either.
fn note_change(
    db: &Connection,
    path: &str,
    op: &str,
    content_id: Option<&ContentId>,
    who: &Attribution<'_>,
    at_ms: i64,
) -> Result<(), VfsError> {
    db.execute(
        "INSERT INTO changes(path,op,content_id,actor,origin,worktree,at_ms)
         VALUES(?1,?2,?3,?4,?5,?6,?7)",
        params![
            path,
            op,
            content_id.map(ToString::to_string),
            who.actor,
            who.origin,
            who.worktree,
            at_ms
        ],
    )?;
    Ok(())
}

fn row_to_entry(row: &rusqlite::Row<'_>) -> rusqlite::Result<FileEntry> {
    let hydration: String = row.get(5)?;
    Ok(FileEntry {
        path: row.get(0)?,
        content_id: ContentId(row.get(1)?),
        size: row.get::<_, i64>(2)? as u64,
        media_type: row.get(3)?,
        version: row.get::<_, i64>(4)? as u64,
        hydration: if hydration == "present" {
            Hydration::Present
        } else {
            Hydration::Placeholder
        },
        pinned: row.get::<_, i64>(6)? != 0,
        updated_at_ms: row.get(7)?,
    })
}

/// A persisted generation either parses or the store refuses to open.
///
/// Falling back to zero would be the whole bug wearing a different hat: every
/// cached handle from the previous run would then be indistinguishable from a
/// handle issued now, and would resolve to whatever currently owns the inode.
fn parse_generation(value: &str) -> Result<u64, VfsError> {
    value.parse().map_err(|_| VfsError::CorruptMeta {
        key: "generation".to_string(),
        value: value.to_string(),
    })
}

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Pretend the store's written-down clock reading is ahead of the wall clock.
///
/// That is what a step-back looks like from inside the store: an RTC that ran
/// fast and was corrected, a VM restored from a snapshot, a laptop waking with a
/// stale clock. A test cannot move the wall clock, so the reading the store must
/// not fall below is moved instead.
#[cfg(test)]
pub(crate) fn set_stored_hlc(root: &Path, hlc: Hlc) {
    let db = Connection::open(StateLayout::new(root).db_path()).unwrap();
    for (key, value) in [
        ("hlc_ms", hlc.ms.to_string()),
        ("hlc_counter", hlc.counter.to_string()),
    ] {
        db.execute(
            "INSERT INTO meta(key,value) VALUES(?1,?2)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![key, value],
        )
        .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    /// A root on a machine whose content store this test owns.
    ///
    /// Tests pass the store explicitly rather than using the machine's real one:
    /// a test that collected against the store a developer's worktrees share
    /// would delete their content.
    fn workspace() -> (tempfile::TempDir, Vfs) {
        let temp = tempfile::tempdir().unwrap();
        let cas = temp.path().join("cas");
        let vfs = Vfs::open_with(temp.path().join("root"), NodeRole::Full, Some(&cas)).unwrap();
        (temp, vfs)
    }

    /// Open a root against a content store the test owns, under the root itself.
    ///
    /// No test may resolve the machine's default store: that one is shared with
    /// real roots, so a test writing to it would be a test writing outside
    /// itself.
    fn open_temp_root(root: &Path) -> Vfs {
        Vfs::open_with(root, NodeRole::Full, Some(&root.join(".cas"))).unwrap()
    }

    /// Every object file in a content store.
    fn object_files(cas: &Path) -> Vec<PathBuf> {
        let mut files = Vec::new();
        let Ok(shards) = fs::read_dir(cas.join("objects")) else {
            return files;
        };
        for shard in shards {
            let shard = shard.unwrap();
            if !shard.file_type().unwrap().is_dir() {
                continue;
            }
            for entry in fs::read_dir(shard.path()).unwrap() {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_file() {
                    files.push(entry.path());
                }
            }
        }
        files
    }

    /// Bytes the content store holds, which is what sharing has to reduce.
    fn object_bytes(cas: &Path) -> u64 {
        object_files(cas)
            .iter()
            .map(|path| fs::metadata(path).unwrap().len())
            .sum()
    }

    /// Write `bytes` into an object file the way the store files them.
    fn write_object(cas: &Path, id: &ContentId, bytes: &[u8]) -> PathBuf {
        let hex = id.hex();
        let path = cas.join("objects").join(&hex[..2]).join(hex);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, bytes).unwrap();
        path
    }

    /// Build a root the way the layout that kept a store per checkout left it:
    /// state in `.fungos`, bytes in `.fungos/objects` rather than shared.
    fn make_legacy_root(root: &Path, name: &str, bytes: &[u8], staging: &Path) -> ContentId {
        let mut vfs = Vfs::open_with(root, NodeRole::Full, Some(staging)).unwrap();
        let entry = vfs
            .put(name, bytes, "application/octet-stream", Expect::Any)
            .unwrap();
        drop(vfs);
        fs::rename(root.join(STATE_DIR), root.join(LEGACY_STATE_DIR)).unwrap();
        let hex = entry.content_id.hex();
        let from = staging.join("objects").join(&hex[..2]).join(hex);
        let to = root
            .join(LEGACY_STATE_DIR)
            .join(OBJECTS_DIR)
            .join(&hex[..2])
            .join(hex);
        fs::create_dir_all(to.parent().unwrap()).unwrap();
        fs::rename(from, to).unwrap();
        entry.content_id
    }

    #[test]
    fn cache_content_is_stored_but_never_versioned() {
        let (temp, mut vfs) = workspace();
        let bytes = b"compiled artifact\0\x01";

        let first = vfs
            .put_cache(
                "target/debug/libfoo.rlib",
                bytes,
                "application/octet-stream",
            )
            .expect("cache put");
        assert_eq!(first, ContentId::from_bytes(bytes));

        // Same bytes, same id, one row: a build that produces identical output
        // twice costs nothing the second time.
        let second = vfs
            .put_cache(
                "target/debug/libfoo.rlib",
                bytes,
                "application/octet-stream",
            )
            .expect("cache put again");
        assert_eq!(first, second);
        assert_eq!(vfs.cache_entries().unwrap().len(), 1);

        // None of the things a real write creates. This is the whole distinction
        // from `put`: no namespace entry, no version, no op for a peer to replay.
        assert!(
            vfs.file("target/debug/libfoo.rlib").unwrap().is_none(),
            "cache content is not namespace state"
        );
        assert!(vfs.history("target/debug/libfoo.rlib").unwrap().is_empty());
        assert!(vfs.pending(100).unwrap().is_empty(), "nothing to sync");
        assert!(vfs.changes(0, "", "", 100).unwrap().is_empty());

        // And it is a real object in the shared store.
        assert!(!object_files(&temp.path().join("cas")).is_empty());
        assert!(
            vfs.get("target/debug/libfoo.rlib").is_err(),
            "there is no path to read it back by"
        );
        assert!(vfs.content(&first).is_ok(), "the content id is the handle");
    }

    #[test]
    fn gc_keeps_cache_content_and_eviction_is_what_forgets_it() {
        let (temp, vfs) = workspace();
        let cas = temp.path().join("cas");
        let bytes = vec![7u8; 4096];
        let id = vfs
            .put_cache("target/debug/app", &bytes, "application/octet-stream")
            .expect("cache put");

        // Nothing in the namespace names it, but it is not garbage: the ceiling
        // is the rule for cache content, so gc leaves it alone.
        assert_eq!(vfs.gc().unwrap(), 0);
        assert!(!object_files(&cas).is_empty(), "gc kept the object");
        assert!(vfs.content(&id).is_ok());

        // Forgetting the entry makes the bytes collectable on the next pass.
        assert_eq!(vfs.evict_cache_to(0).unwrap(), 1);
        assert!(vfs.cache_entries().unwrap().is_empty());
        assert_eq!(vfs.cached_bytes().unwrap(), 0);
        assert_eq!(vfs.gc().unwrap(), 1);
        assert!(object_files(&cas).is_empty());
    }

    #[test]
    fn cache_eviction_is_by_size_and_spares_pinned_content() {
        let (temp, vfs) = workspace();
        let keep = vfs
            .put_cache("target/artifact.tar", &vec![1u8; 1000], "application/x-tar")
            .expect("put pinned");
        vfs.pin_cache(&keep, true).unwrap();
        let first = vfs
            .put_cache("target/one", &vec![2u8; 1000], "application/octet-stream")
            .expect("put one");
        std::thread::sleep(std::time::Duration::from_millis(3));
        let second = vfs
            .put_cache("target/two", &vec![3u8; 1000], "application/octet-stream")
            .expect("put two");
        assert_eq!(vfs.cached_bytes().unwrap(), 3000);

        // Down to 2000: one unpinned entry goes, and it is the least recently
        // used one, not the largest and not the oldest-created.
        assert_eq!(vfs.evict_cache_to(2000).unwrap(), 1);
        let left: Vec<ContentId> = vfs
            .cache_entries()
            .unwrap()
            .into_iter()
            .map(|entry| entry.content_id)
            .collect();
        assert_eq!(left.len(), 2);
        assert!(left.contains(&keep), "pinned content survives the ceiling");
        assert!(left.contains(&second), "the newer entry is used last");
        assert!(!left.contains(&first));

        // A ceiling below the pinned content cannot be met, and says so by not
        // deleting what it must not: the pin wins over the number.
        assert_eq!(vfs.evict_cache_to(0).unwrap(), 1);
        assert_eq!(vfs.cached_bytes().unwrap(), 1000);
        let _ = temp;
    }

    #[test]
    fn writes_carry_the_actor_and_origin_that_made_them() {
        let (_temp, mut vfs) = workspace();
        vfs.set_writer(Writer::new("ajmwagar", "studio"));
        vfs.put("notes/a.md", b"# a\n", "text/markdown", Expect::Any)
            .unwrap();
        vfs.set_writer(Writer::new("agent:reviewer", "agora-one"));
        vfs.put("notes/a.md", b"# a again\n", "text/markdown", Expect::Any)
            .unwrap();

        let history = vfs.history("notes/a.md").unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(
            (history[0].actor.as_str(), history[0].origin.as_str()),
            ("ajmwagar", "studio")
        );
        assert_eq!(
            (history[1].actor.as_str(), history[1].origin.as_str()),
            ("agent:reviewer", "agora-one")
        );

        // The replication log carries the same attribution, so a peer can tell
        // whose write it is applying.
        let ops = vfs.pending(10).unwrap();
        assert_eq!(ops[0].actor, "ajmwagar");
        assert_eq!(ops[1].origin, "agora-one");
    }

    #[test]
    fn the_op_log_is_totally_ordered_across_writers() {
        let (_temp, mut vfs) = workspace();
        vfs.set_writer(Writer::new("a", "studio"));
        vfs.put("f.txt", b"1", "text/plain", Expect::Any).unwrap();
        vfs.put("f.txt", b"2", "text/plain", Expect::Any).unwrap();
        vfs.set_writer(Writer::new("b", "agora-one"));
        vfs.put("f.txt", b"3", "text/plain", Expect::Any).unwrap();

        let ops = vfs.pending(10).unwrap();
        assert_eq!(ops.len(), 3);
        // Writes inside one millisecond still order, and a later write never
        // sorts before an earlier one.
        for pair in ops.windows(2) {
            assert!(
                pair[0].hlc < pair[1].hlc,
                "{:?} !< {:?}",
                pair[0].hlc,
                pair[1].hlc
            );
        }
        assert_eq!(ops[2].actor, "b");
    }

    #[test]
    fn ops_are_scoped_to_the_worktree_the_store_serves() {
        let (_temp, mut vfs) = workspace();
        assert_eq!(vfs.worktree(), "", "an unscoped store stamps nothing");
        vfs.put("a.txt", b"a", "text/plain", Expect::Any).unwrap();
        assert_eq!(vfs.pending(10).unwrap()[0].worktree, "");

        vfs.set_worktree("wt_fywy");
        vfs.put("a.txt", b"b", "text/plain", Expect::Any).unwrap();
        let ops = vfs.pending(10).unwrap();
        assert_eq!(ops[1].worktree, "wt_fywy");
    }

    #[test]
    fn a_write_based_on_stale_content_is_fenced() {
        let (_temp, mut vfs) = workspace();
        let first = vfs
            .put("doc.md", b"v1", "text/markdown", Expect::Any)
            .unwrap();

        // The writer that read v1 may replace it.
        let second = vfs
            .put(
                "doc.md",
                b"v2",
                "text/markdown",
                Expect::Base(&first.content_id),
            )
            .unwrap();
        assert_eq!(vfs.get("doc.md").unwrap(), b"v2");

        // A writer still holding v1 would discard v2, so it is refused — and the
        // error names both heads rather than just saying no.
        let error = vfs
            .put(
                "doc.md",
                b"v1b",
                "text/markdown",
                Expect::Base(&first.content_id),
            )
            .unwrap_err();
        match &error {
            VfsError::Fenced {
                path,
                expected,
                current,
            } => {
                assert_eq!(path, "doc.md");
                assert_eq!(expected, &first.content_id.to_string());
                assert_eq!(current, &second.content_id.to_string());
            }
            other => panic!("expected a fence, got {other:?}"),
        }
        assert_eq!(vfs.get("doc.md").unwrap(), b"v2", "nothing was written");
    }

    #[test]
    fn a_fence_on_a_missing_path_says_nothing_is_there() {
        let (_temp, mut vfs) = workspace();
        let other = ContentId::from_bytes(b"somewhere else");
        let error = vfs
            .put("absent.md", b"x", "text/markdown", Expect::Base(&other))
            .unwrap_err();
        assert!(format!("{error}").contains("nothing"), "{error}");
    }

    #[test]
    fn a_write_records_the_head_it_replaced() {
        let (_temp, mut vfs) = workspace();
        let first = vfs.put("d.txt", b"one", "text/plain", Expect::Any).unwrap();
        assert_eq!(
            vfs.pending(10).unwrap()[0].base_content_id,
            None,
            "a create replaces nothing"
        );

        vfs.put("d.txt", b"two", "text/plain", Expect::Any).unwrap();
        let ops = vfs.pending(10).unwrap();
        assert_eq!(ops[1].base_content_id.as_ref(), Some(&first.content_id));
    }

    #[test]
    fn structural_ops_are_attributed_too() {
        let (_temp, mut vfs) = workspace();
        vfs.set_writer(Writer::new("ajmwagar", "studio"));
        vfs.mkdir("docs").unwrap();
        vfs.rmdir("docs").unwrap();
        let ops = vfs.pending(10).unwrap();
        assert!(ops.iter().all(|op| op.actor == "ajmwagar"));
        assert!(ops.iter().all(|op| op.origin == "studio"));
    }

    /// Attribution arrived after the first stores existed. An older database
    /// must open, gain the columns, and keep its rows — marked honestly rather
    /// than backfilled with a guess. It also uses the legacy `.fungos` name, so
    /// this is the path a root that has not been migrated takes.
    #[test]
    fn a_store_from_before_attribution_upgrades_in_place() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        let cas = temp.path().join("cas");
        std::fs::create_dir_all(root.join(LEGACY_STATE_DIR)).unwrap();
        {
            let db = Connection::open(root.join(LEGACY_STATE_DIR).join(NAMESPACE_DB)).unwrap();
            db.execute_batch(
                "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO meta VALUES('role','full');
                 CREATE TABLE versions(
                   path TEXT NOT NULL, version INTEGER NOT NULL, content_id TEXT NOT NULL,
                   size INTEGER NOT NULL, created_at_ms INTEGER NOT NULL,
                   PRIMARY KEY(path, version));
                 INSERT INTO versions VALUES('a.txt',1,'blake3:aaaa',7,1);
                 CREATE TABLE sync_queue(
                   id INTEGER PRIMARY KEY AUTOINCREMENT, op TEXT NOT NULL, path TEXT NOT NULL,
                   version INTEGER NOT NULL, content_id TEXT, created_at_ms INTEGER NOT NULL);
                 INSERT INTO sync_queue(op,path,version,content_id,created_at_ms)
                   VALUES('upsert','a.txt',1,'blake3:aaaa',1);",
            )
            .unwrap();
        }

        let vfs = Vfs::open_with(&root, NodeRole::Full, Some(&cas)).unwrap();
        let history = vfs.history("a.txt").unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].size, 7);
        assert_eq!(history[0].actor, "unattributed");
        let ops = vfs.pending(10).unwrap();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].origin, "unattributed");
        // The clock and base columns arrive with defaults too, rather than the
        // migration having to invent a reading for old rows.
        assert_eq!(ops[0].hlc, Hlc::default());
        assert_eq!(ops[0].base_content_id, None);
    }

    #[test]
    fn rename_preserves_identity_and_history() {
        let (_temp, mut vfs) = workspace();
        let entry = vfs
            .put("a.txt", b"same bytes", "text/plain", Expect::Any)
            .unwrap();
        let renamed = vfs.rename("a.txt", "b.txt").unwrap();
        assert_eq!(renamed.path, "b.txt");
        assert_eq!(renamed.content_id, entry.content_id);
        assert!(vfs.file("a.txt").unwrap().is_none());
        assert_eq!(vfs.get("b.txt").unwrap(), b"same bytes");
        assert_eq!(vfs.history("b.txt").unwrap().len(), 1);
    }

    #[test]
    fn put_get_history_and_conflict() {
        let (_temp, mut vfs) = workspace();
        let first = vfs
            .put("docs/a.md", b"one", "text/markdown", Expect::Any)
            .unwrap();
        assert_eq!(first.version, 1);
        assert_eq!(vfs.get("docs/a.md").unwrap(), b"one");
        let second = vfs
            .put("docs/a.md", b"two", "text/markdown", Expect::Version(1))
            .unwrap();
        assert_eq!(second.version, 2);
        assert_eq!(vfs.history("docs/a.md").unwrap().len(), 2);
        let conflict = vfs.put("docs/a.md", b"three", "text/markdown", Expect::Version(1));
        assert!(matches!(conflict, Err(VfsError::Conflict { .. })));
        assert_eq!(vfs.list("docs").unwrap().len(), 1);
    }

    #[test]
    fn cache_node_hydrates_and_evicts_without_writing() {
        let temp = tempfile::tempdir().unwrap();
        let cas = temp.path().join("cas");
        let mut full =
            Vfs::open_with(temp.path().join("node"), NodeRole::Full, Some(&cas)).unwrap();
        full.put(
            "big.bin",
            &[7u8; 4096],
            "application/octet-stream",
            Expect::Any,
        )
        .unwrap();
        let entry = full.file("big.bin").unwrap().unwrap();

        let mut cache =
            Vfs::open_with(temp.path().join("cache"), NodeRole::Cache, Some(&cas)).unwrap();
        cache
            .register_placeholder(
                "big.bin",
                entry.content_id.clone(),
                entry.size,
                "application/octet-stream",
            )
            .unwrap();
        assert!(matches!(
            cache.get("big.bin"),
            Err(VfsError::NotHydrated(_))
        ));
        cache.hydrate("big.bin", &[7u8; 4096]).unwrap();
        assert_eq!(cache.get("big.bin").unwrap(), vec![7u8; 4096]);
        assert!(matches!(
            cache.put("new.bin", b"no", "application/octet-stream", Expect::Any),
            Err(VfsError::ReadOnly)
        ));
        cache.lease("big.bin", "reader", 60_000).unwrap();
        assert_eq!(cache.evict_to(0).unwrap(), 0);
        cache.release("big.bin", "reader").unwrap();
        assert_eq!(cache.evict_to(0).unwrap(), 1);
        assert!(matches!(
            cache.get("big.bin"),
            Err(VfsError::NotHydrated(_))
        ));
    }

    #[test]
    fn explicit_directories_round_trip() {
        let (_temp, mut vfs) = workspace();
        vfs.mkdir("projects").unwrap();
        vfs.mkdir("projects/soil").unwrap();
        assert!(vfs.is_directory("projects").unwrap());
        assert_eq!(
            vfs.directories("").unwrap(),
            vec!["projects".to_string(), "projects/soil".to_string()]
        );
        assert!(matches!(vfs.rmdir("projects"), Err(VfsError::NotEmpty(_))));
        vfs.rmdir("projects/soil").unwrap();
        vfs.rmdir("projects").unwrap();
        assert!(!vfs.is_directory("projects").unwrap());
        let ops = vfs.pending(10).unwrap();
        assert_eq!(ops[0].kind, SyncOpKind::Mkdir);
        assert_eq!(ops[3].kind, SyncOpKind::Rmdir);
    }

    #[test]
    fn generation_advances_across_reopens_without_repeating() {
        let temp = tempfile::tempdir().unwrap();
        let mut seen = Vec::new();
        for _ in 0..4 {
            let mut vfs = open_temp_root(temp.path());
            seen.push(vfs.begin_generation().unwrap());
            assert_eq!(vfs.generation(), *seen.last().unwrap());
        }
        assert_eq!(seen, vec![1, 2, 3, 4]);
    }

    #[test]
    fn opening_without_issuing_handles_leaves_the_generation_alone() {
        let temp = tempfile::tempdir().unwrap();
        let mut vfs = open_temp_root(temp.path());
        assert_eq!(vfs.generation(), 0, "a fresh store has issued nothing");
        assert_eq!(vfs.begin_generation().unwrap(), 1);
        drop(vfs);

        // A reader that shares the root with a live mount must not invalidate
        // the handles that mount already issued.
        let vfs = open_temp_root(temp.path());
        assert_eq!(vfs.generation(), 1);
    }

    #[test]
    fn an_unreadable_generation_refuses_to_open() {
        let temp = tempfile::tempdir().unwrap();
        {
            let mut vfs = open_temp_root(temp.path());
            vfs.begin_generation().unwrap();
            vfs.db
                .execute(
                    "UPDATE meta SET value='not-a-number' WHERE key='generation'",
                    [],
                )
                .unwrap();
        }

        match Vfs::open_with(temp.path(), NodeRole::Full, Some(&temp.path().join(".cas"))) {
            Ok(_) => panic!("a corrupt generation opened as if it were zero"),
            Err(VfsError::CorruptMeta { key, value }) => {
                assert_eq!(key, "generation");
                assert_eq!(value, "not-a-number");
            }
            Err(error) => panic!("unexpected error: {error}"),
        }
    }

    #[test]
    fn sync_queue_round_trip() {
        let (_temp, mut vfs) = workspace();
        vfs.put("a.txt", b"a", "text/plain", Expect::Any).unwrap();
        vfs.remove("a.txt").unwrap();
        let ops = vfs.pending(10).unwrap();
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0].kind, SyncOpKind::Upsert);
        assert_eq!(ops[1].kind, SyncOpKind::Remove);
        vfs.ack(&[ops[0].id]).unwrap();
        assert_eq!(vfs.pending(10).unwrap().len(), 1);
    }

    #[test]
    fn a_restart_with_a_clock_that_stepped_back_still_stamps_above_the_last_write() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        let cas = temp.path().join("cas");

        let mut a = Vfs::open_with(&root, NodeRole::Full, Some(&cas)).unwrap();
        a.put("one.md", b"one", "text/markdown", Expect::Any)
            .unwrap();
        let first = a.pending(10).unwrap().last().unwrap().hlc;
        drop(a);

        // The reading this store has already used is an hour ahead of anything
        // the reopened process will read from the clock.
        let ahead = Hlc {
            ms: first.ms + 3_600_000,
            counter: 0,
        };
        set_stored_hlc(&root, ahead);

        let mut a = Vfs::open_with(&root, NodeRole::Full, Some(&cas)).unwrap();
        a.put("two.md", b"two", "text/markdown", Expect::Any)
            .unwrap();
        let second = a.pending(10).unwrap().last().unwrap().hlc;
        assert!(second > ahead, "{second:?} must resume above {ahead:?}");
        assert!(second > first);

        // A peer sees two writes, not a write and a duplicate: a clock that
        // restarted at zero would have made the second sort under the first.
        let mut b = Vfs::open_with(temp.path().join("peer"), NodeRole::Full, Some(&cas)).unwrap();
        let ops = a.pending(10).unwrap();
        let outcomes: Vec<_> = ops
            .iter()
            .map(|op| {
                let bytes = a.get(&op.path).unwrap();
                b.apply_remote(op, Some(&bytes)).unwrap()
            })
            .collect();
        assert_eq!(outcomes, vec![crate::ApplyOutcome::Applied; 2]);
    }

    #[test]
    fn put_after_remove_continues_the_path_history() {
        let (_temp, mut vfs) = workspace();
        vfs.put("notes/a.md", b"one", "text/markdown", Expect::Any)
            .unwrap();
        vfs.remove("notes/a.md").unwrap();

        // Recreating a path is an ordinary thing to do. The version used to be
        // read off the now-absent `files` row, so it started at 1 again and the
        // insert collided with history that was still there.
        let entry = vfs
            .put("notes/a.md", b"two", "text/markdown", Expect::Any)
            .unwrap();
        assert_eq!(entry.version, 2);

        // A version is the path's, not the current file's: both writes are still
        // in the history, in order. (The removal itself is not a version — this
        // crate has no tombstone — which is why the numbers continue rather than
        // restarting.)
        let history = vfs.history("notes/a.md").unwrap();
        assert_eq!(
            history
                .iter()
                .map(|version| version.version)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(history[1].content_id, entry.content_id);
    }

    #[test]
    fn a_directory_can_be_recreated_after_removal() {
        let (_temp, mut vfs) = workspace();
        vfs.mkdir("projects").unwrap();
        vfs.rmdir("projects").unwrap();
        // Directories carry no version rows at all, so the shape that broke `put`
        // does not exist here. The test pins that they stay recreatable.
        vfs.mkdir("projects").unwrap();
        assert!(vfs.is_directory("projects").unwrap());
    }

    /// The point of the shared store: two roots naming the same bytes cost one
    /// object, not two.
    #[test]
    fn two_roots_on_one_machine_share_the_same_object() {
        let temp = tempfile::tempdir().unwrap();
        let cas = temp.path().join("cas");
        let bytes = vec![9u8; 64 * 1024];

        let mut a = Vfs::open_with(temp.path().join("a"), NodeRole::Full, Some(&cas)).unwrap();
        a.put("big.bin", &bytes, "application/octet-stream", Expect::Any)
            .unwrap();
        let after_first = object_bytes(&cas);
        assert_eq!(object_files(&cas).len(), 1);
        assert_eq!(after_first, bytes.len() as u64);

        let mut b = Vfs::open_with(temp.path().join("b"), NodeRole::Full, Some(&cas)).unwrap();
        b.put("copy.bin", &bytes, "application/octet-stream", Expect::Any)
            .unwrap();

        // Not just behaviour: the second write added no object and no bytes.
        assert_eq!(
            object_files(&cas).len(),
            1,
            "the second root wrote a second object"
        );
        assert_eq!(object_bytes(&cas), after_first);
        // And a root holds no objects of its own at all.
        assert!(
            !temp
                .path()
                .join("a")
                .join(STATE_DIR)
                .join(OBJECTS_DIR)
                .exists()
        );
        assert!(
            !temp
                .path()
                .join("b")
                .join(STATE_DIR)
                .join(OBJECTS_DIR)
                .exists()
        );
        assert_eq!(a.get("big.bin").unwrap(), bytes);
        assert_eq!(b.get("copy.bin").unwrap(), bytes);
    }

    /// A root an older layout wrote still works: reads fall back to its own
    /// object directory, and opening it moves nothing.
    #[test]
    fn a_root_with_a_legacy_store_reads_before_migration() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("legacy");
        let staging = temp.path().join("staging");
        let bytes = b"stated in .fungos".to_vec();
        let id = make_legacy_root(&root, "notes/a.md", &bytes, &staging);

        // A store this machine owns but which has never seen these bytes: only
        // the legacy directory can answer.
        let cas = temp.path().join("cas");
        let mut vfs = Vfs::open_with(&root, NodeRole::Full, Some(&cas)).unwrap();
        assert!(
            object_files(&cas).is_empty(),
            "a read must not copy into the shared store"
        );
        assert_eq!(vfs.get("notes/a.md").unwrap(), bytes);
        assert_eq!(vfs.file("notes/a.md").unwrap().unwrap().content_id, id);
        assert!(
            root.join(LEGACY_STATE_DIR).is_dir(),
            "opening a root must not migrate it"
        );
    }

    /// The migration is a command: it moves the blobs into the shared store,
    /// retires `.fungos`, and a second run has nothing left to do.
    #[test]
    fn migration_moves_blobs_into_the_shared_store_once() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("legacy");
        let staging = temp.path().join("staging");
        let bytes = b"migrate me".to_vec();
        let id = make_legacy_root(&root, "notes/a.md", &bytes, &staging);

        let cas = temp.path().join("cas");
        let report = migrate_storage_with_cas(&root, Some(&cas)).unwrap();
        assert_eq!(report.moved, 1);
        assert_eq!(report.already_shared, 0);
        assert_eq!(report.left_behind, 0);
        assert!(report.renamed);
        assert!(
            report.state_dir.ends_with(STATE_DIR),
            "{}",
            report.state_dir
        );
        assert!(
            !root.join(LEGACY_STATE_DIR).exists(),
            "the old state directory is gone"
        );
        assert_eq!(object_files(&cas).len(), 1);
        assert_eq!(object_bytes(&cas), bytes.len() as u64, "moved, not copied");

        // The root reads from the shared store now, and only from there: the
        // object is the one thing standing between it and the bytes.
        let mut vfs = Vfs::open_with(&root, NodeRole::Full, Some(&cas)).unwrap();
        assert_eq!(vfs.get("notes/a.md").unwrap(), bytes);
        let hex = id.hex();
        fs::remove_file(cas.join("objects").join(&hex[..2]).join(hex)).unwrap();
        assert!(matches!(
            vfs.get("notes/a.md"),
            Err(VfsError::NotHydrated(_))
        ));
        drop(vfs);

        // Idempotent: nothing left to move, nothing left to rename.
        let again = migrate_storage_with_cas(&root, Some(&cas)).unwrap();
        assert_eq!(
            (again.moved, again.already_shared, again.renamed),
            (0, 0, false)
        );
        assert_eq!(object_files(&cas).len(), 0);
    }

    /// While a root is migrating, the same content id can be in both places.
    /// Moving it must not make a second copy, and nothing may be lost.
    #[test]
    fn migration_does_not_duplicate_content_already_in_the_shared_store() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("legacy");
        let staging = temp.path().join("staging");
        let bytes = b"shared and legacy".to_vec();
        make_legacy_root(&root, "notes/a.md", &bytes, &staging);

        let cas = temp.path().join("cas");
        let mut other =
            Vfs::open_with(temp.path().join("other"), NodeRole::Full, Some(&cas)).unwrap();
        other
            .put("copy.bin", &bytes, "application/octet-stream", Expect::Any)
            .unwrap();
        assert_eq!(object_files(&cas).len(), 1);

        let report = migrate_storage_with_cas(&root, Some(&cas)).unwrap();
        assert_eq!((report.moved, report.already_shared), (0, 1));
        assert_eq!(object_files(&cas).len(), 1, "one object, not two");
        assert!(!root.join(LEGACY_STATE_DIR).exists());
        let mut legacy = Vfs::open_with(&root, NodeRole::Full, Some(&cas)).unwrap();
        assert_eq!(legacy.get("notes/a.md").unwrap(), bytes);
    }

    /// A collection keeps what a namespace names and drops what nothing does.
    #[test]
    fn gc_drops_objects_no_namespace_names() {
        let temp = tempfile::tempdir().unwrap();
        let cas = temp.path().join("cas");
        let mut vfs = Vfs::open_with(temp.path().join("root"), NodeRole::Full, Some(&cas)).unwrap();
        vfs.put("notes/a.md", b"# a\n", "text/markdown", Expect::Any)
            .unwrap();

        // What a deleted root leaves behind: bytes, and nothing naming them.
        let orphan = ContentId::from_bytes(b"orphan");
        let path = write_object(&cas, &orphan, b"orphan");
        assert_eq!(object_files(&cas).len(), 2);

        assert_eq!(vfs.gc().unwrap(), 1);
        assert!(!path.exists());
        assert_eq!(object_files(&cas).len(), 1);
        assert_eq!(vfs.get("notes/a.md").unwrap(), b"# a\n");
    }

    /// An exported artifact outlives the path that produced it: a collection
    /// that deleted it would leave a release pointing at nothing.
    #[test]
    fn gc_keeps_exported_content_that_no_live_path_names() {
        let temp = tempfile::tempdir().unwrap();
        let cas = temp.path().join("cas");
        let mut vfs = Vfs::open_with(temp.path().join("root"), NodeRole::Full, Some(&cas)).unwrap();
        vfs.put(
            "release/app.tar",
            b"artifact",
            "application/x-tar",
            Expect::Any,
        )
        .unwrap();
        vfs.put("notes/a.md", b"# a\n", "text/markdown", Expect::Any)
            .unwrap();

        let dest = temp.path().join("out");
        assert_eq!(vfs.record_export("release", &dest).unwrap(), 1);
        let artifact = vfs.file("release/app.tar").unwrap().unwrap();

        // Strip the references a collection would otherwise count, leaving the
        // export record as the only reason to keep the artifact.
        vfs.remove("release/app.tar").unwrap();
        vfs.db
            .execute("DELETE FROM versions WHERE path='release/app.tar'", [])
            .unwrap();

        assert_eq!(vfs.gc().unwrap(), 0, "a published artifact is not garbage");
        assert_eq!(vfs.content(&artifact.content_id).unwrap(), b"artifact");
        assert_eq!(object_files(&cas).len(), 2);

        // With the record gone, the same bytes are collectable: the export is
        // what kept them, not some other reference.
        vfs.db.execute("DELETE FROM exports", []).unwrap();
        assert_eq!(vfs.gc().unwrap(), 1);
        assert!(matches!(
            vfs.content(&artifact.content_id),
            Err(VfsError::NotHydrated(_))
        ));
        assert_eq!(vfs.get("notes/a.md").unwrap(), b"# a\n");
    }

    /// Two roots, one store: a collection in one must speak for both.
    #[test]
    fn one_roots_gc_keeps_content_another_root_references() {
        let temp = tempfile::tempdir().unwrap();
        let cas = temp.path().join("cas");
        let mut a = Vfs::open_with(temp.path().join("a"), NodeRole::Full, Some(&cas)).unwrap();
        let mut b = Vfs::open_with(temp.path().join("b"), NodeRole::Full, Some(&cas)).unwrap();
        a.put("a.txt", b"a-owned", "text/plain", Expect::Any)
            .unwrap();
        b.put("b.txt", b"b-owned", "text/plain", Expect::Any)
            .unwrap();
        assert_eq!(object_files(&cas).len(), 2);
        // Never opened by the collector: only the registration makes it known.
        drop(b);

        assert_eq!(a.gc().unwrap(), 0, "b's content is not a's garbage");
        assert_eq!(object_files(&cas).len(), 2);
        let mut b = Vfs::open_with(temp.path().join("b"), NodeRole::Full, Some(&cas)).unwrap();
        assert_eq!(b.get("b.txt").unwrap(), b"b-owned");
        assert_eq!(a.get("a.txt").unwrap(), b"a-owned");
    }

    /// A root still holding a legacy store is registered like any other, so its
    /// content survives a collection run from a root that shares the store.
    #[test]
    fn a_collection_keeps_content_a_legacy_root_still_needs() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("legacy");
        let staging = temp.path().join("staging");
        let bytes = b"legacy bytes".to_vec();
        make_legacy_root(&root, "notes/a.md", &bytes, &staging);

        let cas = temp.path().join("cas");
        // The same bytes reach the shared store through another root.
        let mut other =
            Vfs::open_with(temp.path().join("other"), NodeRole::Full, Some(&cas)).unwrap();
        other
            .put("copy.bin", &bytes, "application/octet-stream", Expect::Any)
            .unwrap();
        // Opening the legacy root is what registers it; it stays unmigrated.
        let legacy = Vfs::open_with(&root, NodeRole::Full, Some(&cas)).unwrap();
        assert!(root.join(LEGACY_STATE_DIR).is_dir());
        assert_eq!(object_files(&cas).len(), 1);

        assert_eq!(other.gc().unwrap(), 0, "the legacy root names these bytes");
        assert_eq!(object_files(&cas).len(), 1);
        drop(legacy);
        let mut legacy = Vfs::open_with(&root, NodeRole::Full, Some(&cas)).unwrap();
        assert_eq!(legacy.get("notes/a.md").unwrap(), bytes);
    }
}
