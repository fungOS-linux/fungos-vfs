use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ContentId, normalize_path, store::BlobStore};

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
    #[error("transport error: {0}")]
    Transport(String),
    #[error("corrupt store metadata: {key} = {value}")]
    CorruptMeta { key: String, value: String },
    #[error("workspace role mismatch: expected {expected}, found {found}")]
    RoleMismatch { expected: NodeRole, found: NodeRole },
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

pub struct Vfs {
    root: PathBuf,
    role: NodeRole,
    pub(crate) db: Connection,
    pub(crate) blobs: BlobStore,
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
}

impl Vfs {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, VfsError> {
        Self::open_with_role(root, NodeRole::Full)
    }

    pub fn open_with_role(root: impl AsRef<Path>, role: NodeRole) -> Result<Self, VfsError> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(root.join(".fungos"))?;
        let blobs = BlobStore::open(root.join(".fungos/objects"))?;
        let db = Connection::open(root.join(".fungos/vfs.sqlite3"))?;
        let vfs = Self {
            root,
            role,
            db,
            blobs,
            writer: Writer::from_environment(),
            hlc: Hlc::default(),
            worktree: String::new(),
            store: String::new(),
        };
        let mut vfs = vfs;
        vfs.migrate()?;
        vfs.store = vfs.ensure_store_id()?;
        // The clock resumes where the last process left it, and only then
        // advances to the wall clock if that is ahead. Starting at zero would
        // make every reading depend on the wall clock being monotonic across
        // restarts, which it is not: a restored VM, NTP correcting downwards,
        // or a laptop waking with a skewed RTC all hand a fresh process a
        // reading below ones this store has already used — and every rule
        // keyed on "at or below" would then treat a new write as an old one.
        vfs.hlc = vfs.stored_hlc()?;
        vfs.hlc.observe(now_ms());
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

    /// This store's identity: generated once, then read back.
    fn ensure_store_id(&self) -> Result<String, VfsError> {
        let canonical = std::fs::canonicalize(&self.root).unwrap_or_else(|_| self.root.clone());
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

    /// Record that an op was refused because the head had moved, and drop the
    /// receipt for it.
    ///
    /// The receipt goes because nothing landed, but the fact of the refusal is
    /// kept: "we deliberately deferred this write" and "we never saw it" are
    /// different facts, and what compaction does to the log must not be allowed
    /// to erase the difference. The record is removed when the divergence is
    /// resolved — either by the op finally fitting, or by a caller saying so.
    pub(crate) fn mark_diverged(&self, store: &str, hlc: Hlc, path: &str) -> Result<(), VfsError> {
        let tx = self.db.unchecked_transaction()?;
        tx.execute(
            "DELETE FROM applied WHERE store=?1 AND hlc_ms=?2 AND hlc_counter=?3",
            params![store, hlc.ms, i64::from(hlc.counter)],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO diverged(store,hlc_ms,hlc_counter,path,detected_at_ms)
             VALUES(?1,?2,?3,?4,?5)",
            params![store, hlc.ms, i64::from(hlc.counter), path, now_ms()],
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
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn apply_remove(&mut self, path: &str) -> Result<(), VfsError> {
        let path = normalize_path(path)?;
        self.db
            .execute("DELETE FROM files WHERE path=?1", params![path])?;
        Ok(())
    }

    pub(crate) fn apply_mkdir(&mut self, path: &str) -> Result<(), VfsError> {
        let path = normalize_path(path)?;
        self.db.execute(
            "INSERT OR IGNORE INTO directories(path,created_at_ms) VALUES(?1,?2)",
            params![path, now_ms()],
        )?;
        Ok(())
    }

    pub(crate) fn apply_rmdir(&mut self, path: &str) -> Result<(), VfsError> {
        let path = normalize_path(path)?;
        self.db
            .execute("DELETE FROM directories WHERE path=?1", params![path])?;
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

    pub fn cache_bytes(&self) -> Result<u64, VfsError> {
        let bytes: i64 = self.db.query_row(
            "SELECT COALESCE(SUM(size),0) FROM files WHERE hydration='present'",
            [],
            |row| row.get(0),
        )?;
        Ok(bytes.max(0) as u64)
    }

    /// Evict oldest unpinned, unleased present files until the cache fits.
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
            self.blobs.remove(&entry.content_id)?;
            self.db.execute(
                "UPDATE files SET hydration='placeholder' WHERE path=?1",
                params![path],
            )?;
            total = total.saturating_sub(entry.size);
            evicted += 1;
        }
        Ok(evicted)
    }

    pub fn gc(&self) -> Result<usize, VfsError> {
        let mut statement = self.db.prepare(
            "SELECT content_id FROM files WHERE hydration='present'
             UNION SELECT content_id FROM versions",
        )?;
        let ids = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        let ids: Vec<ContentId> = ids.into_iter().map(ContentId).collect();
        self.blobs.gc(ids.iter())
    }
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
    let db = Connection::open(root.join(".fungos/vfs.sqlite3")).unwrap();
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
    use super::*;

    fn workspace() -> (tempfile::TempDir, Vfs) {
        let temp = tempfile::tempdir().unwrap();
        let vfs = Vfs::open(temp.path()).unwrap();
        (temp, vfs)
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
    /// than backfilled with a guess.
    #[test]
    fn a_store_from_before_attribution_upgrades_in_place() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        std::fs::create_dir_all(root.join(".fungos")).unwrap();
        {
            let db = Connection::open(root.join(".fungos/vfs.sqlite3")).unwrap();
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

        let vfs = Vfs::open(root).unwrap();
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
        let temp = tempfile::tempdir().unwrap();
        let mut vfs = Vfs::open(temp.path()).unwrap();
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
        let mut full = Vfs::open(temp.path().join("node")).unwrap();
        full.put(
            "big.bin",
            &[7u8; 4096],
            "application/octet-stream",
            Expect::Any,
        )
        .unwrap();
        let entry = full.file("big.bin").unwrap().unwrap();

        let mut cache = Vfs::open_with_role(temp.path().join("cache"), NodeRole::Cache).unwrap();
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
        let root = temp.path();

        let mut a = Vfs::open(root).unwrap();
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
        set_stored_hlc(root, ahead);

        let mut a = Vfs::open(root).unwrap();
        a.put("two.md", b"two", "text/markdown", Expect::Any)
            .unwrap();
        let second = a.pending(10).unwrap().last().unwrap().hlc;
        assert!(second > ahead, "{second:?} must resume above {ahead:?}");
        assert!(second > first);

        // A peer sees two writes, not a write and a duplicate: a clock that
        // restarted at zero would have made the second sort under the first.
        let mut b = Vfs::open(temp.path().join("peer")).unwrap();
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
}
