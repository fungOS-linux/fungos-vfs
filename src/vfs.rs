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
    #[error("not hydrated: {0}")]
    NotHydrated(String),
    #[error("version conflict: expected {expected}, current {current}")]
    Conflict { expected: u64, current: u64 },
    #[error("path is leased by {owner} until {expires_at_ms}")]
    Leased { owner: String, expires_at_ms: i64 },
    #[error("node is a pull-through cache; writes are disabled")]
    ReadOnly,
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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncOp {
    pub id: i64,
    pub kind: SyncOpKind,
    pub path: String,
    pub version: u64,
    pub content_id: Option<ContentId>,
    pub created_at_ms: i64,
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

pub struct Vfs {
    root: PathBuf,
    role: NodeRole,
    db: Connection,
    blobs: BlobStore,
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
        };
        vfs.migrate()?;
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
             CREATE TABLE IF NOT EXISTS versions(
               path TEXT NOT NULL,
               version INTEGER NOT NULL,
               content_id TEXT NOT NULL,
               size INTEGER NOT NULL,
               created_at_ms INTEGER NOT NULL,
               PRIMARY KEY(path, version)
             );
             CREATE TABLE IF NOT EXISTS sync_queue(
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               op TEXT NOT NULL,
               path TEXT NOT NULL,
               version INTEGER NOT NULL,
               content_id TEXT,
               created_at_ms INTEGER NOT NULL
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
        expected_version: Option<u64>,
    ) -> Result<FileEntry, VfsError> {
        if self.role == NodeRole::Cache {
            return Err(VfsError::ReadOnly);
        }
        let path = normalize_path(path)?;
        let current = self.file(&path)?;
        if let Some(expected) = expected_version {
            let current_version = current.as_ref().map_or(0, |entry| entry.version);
            if current_version != expected {
                return Err(VfsError::Conflict {
                    expected,
                    current: current_version,
                });
            }
        }
        let content_id = self.blobs.put(bytes)?;
        let version = current.as_ref().map_or(1, |entry| entry.version + 1);
        let now = now_ms();
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
            "INSERT INTO versions(path,version,content_id,size,created_at_ms) VALUES(?1,?2,?3,?4,?5)",
            params![path, version as i64, content_id.to_string(), bytes.len() as i64, now],
        )?;
        tx.execute(
            "INSERT INTO sync_queue(op,path,version,content_id,created_at_ms) VALUES('upsert',?1,?2,?3,?4)",
            params![path, version as i64, content_id.to_string(), now],
        )?;
        tx.commit()?;
        self.file(&path)?.ok_or_else(|| VfsError::NotFound(path))
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
        self.db.execute(
            "INSERT INTO files(path,content_id,size,media_type,version,hydration,pinned,updated_at_ms)
             VALUES(?1,?2,?3,?4,1,'placeholder',0,?5)
             ON CONFLICT(path) DO UPDATE SET content_id=excluded.content_id,size=excluded.size,
               media_type=excluded.media_type,hydration='placeholder',updated_at_ms=excluded.updated_at_ms",
            params![path, content_id.to_string(), size as i64, media_type, now],
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

    pub fn history(&self, path: &str) -> Result<Vec<(u64, ContentId, u64, i64)>, VfsError> {
        let path = normalize_path(path)?;
        let mut statement = self.db.prepare(
            "SELECT version,content_id,size,created_at_ms FROM versions WHERE path=?1 ORDER BY version",
        )?;
        let rows = statement.query_map(params![path], |row| {
            Ok((
                row.get::<_, i64>(0)? as u64,
                ContentId(row.get(1)?),
                row.get::<_, i64>(2)? as u64,
                row.get(3)?,
            ))
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
        let tx = self.db.transaction()?;
        tx.execute("DELETE FROM files WHERE path=?1", params![path])?;
        tx.execute(
            "INSERT INTO sync_queue(op,path,version,content_id,created_at_ms) VALUES('remove',?1,?2,NULL,?3)",
            params![path, current.version as i64, now_ms()],
        )?;
        tx.commit()?;
        Ok(())
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

    pub fn pending(&self, limit: usize) -> Result<Vec<SyncOp>, VfsError> {
        let mut statement = self.db.prepare(
            "SELECT id,op,path,version,content_id,created_at_ms FROM sync_queue ORDER BY id LIMIT ?1",
        )?;
        let rows = statement.query_map(params![limit as i64], |row| {
            let op: String = row.get(1)?;
            Ok(SyncOp {
                id: row.get(0)?,
                kind: match op.as_str() {
                    "upsert" => SyncOpKind::Upsert,
                    "remove" => SyncOpKind::Remove,
                    _ => return Err(rusqlite::Error::InvalidQuery),
                },
                path: row.get(2)?,
                version: row.get::<_, i64>(3)? as u64,
                content_id: row.get::<_, Option<String>>(4)?.map(ContentId),
                created_at_ms: row.get(5)?,
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

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
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
    fn put_get_history_and_conflict() {
        let (_temp, mut vfs) = workspace();
        let first = vfs.put("docs/a.md", b"one", "text/markdown", None).unwrap();
        assert_eq!(first.version, 1);
        assert_eq!(vfs.get("docs/a.md").unwrap(), b"one");
        let second = vfs
            .put("docs/a.md", b"two", "text/markdown", Some(1))
            .unwrap();
        assert_eq!(second.version, 2);
        assert_eq!(vfs.history("docs/a.md").unwrap().len(), 2);
        let conflict = vfs.put("docs/a.md", b"three", "text/markdown", Some(1));
        assert!(matches!(conflict, Err(VfsError::Conflict { .. })));
        assert_eq!(vfs.list("docs").unwrap().len(), 1);
    }

    #[test]
    fn cache_node_hydrates_and_evicts_without_writing() {
        let temp = tempfile::tempdir().unwrap();
        let mut full = Vfs::open(temp.path().join("node")).unwrap();
        full.put("big.bin", &[7u8; 4096], "application/octet-stream", None)
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
            cache.put("new.bin", b"no", "application/octet-stream", None),
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
    fn sync_queue_round_trip() {
        let (_temp, mut vfs) = workspace();
        vfs.put("a.txt", b"a", "text/plain", None).unwrap();
        vfs.remove("a.txt").unwrap();
        let ops = vfs.pending(10).unwrap();
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0].kind, SyncOpKind::Upsert);
        assert_eq!(ops[1].kind, SyncOpKind::Remove);
        vfs.ack(&[ops[0].id]).unwrap();
        assert_eq!(vfs.pending(10).unwrap().len(), 1);
    }
}
