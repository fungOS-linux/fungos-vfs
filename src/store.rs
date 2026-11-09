//! The machine's content-addressed store.
//!
//! One store holds the bytes for every VFS root on the machine. A root keeps only
//! what is per-root — its namespace (`path -> content id`), its op log, versions,
//! cursor, horizon and diverged records — so a hundred worktrees naming the same
//! content cost one copy of it rather than a hundred.
//!
//! Two things live here that are not objects:
//!
//! - `roots/`: one marker per root that references this store's content. `gc`
//!   collects across every registered root, so a root that is not registered is a
//!   root whose content would be collected.
//! - A root's legacy `objects/` directory, left by the layout that kept a store
//!   per checkout. It is read while it is there and emptied by
//!   [`ContentStore::migrate_legacy`].

use std::{
    collections::HashSet,
    ffi::OsString,
    fs, io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use serde::Serialize;

use crate::{ContentId, VfsError};

/// This application's directory under the platform data directory.
const APP_DIR: &str = "fungos-vfs";
/// Objects, sharded by the first two hex characters of their content id.
const OBJECTS: &str = "objects";
/// Markers naming the roots that reference this store.
const ROOTS: &str = "roots";
/// Point this at a directory to override where content lives.
const CAS_ENV: &str = "FUNGOS_VFS_CAS";

/// Where this machine keeps VFS content.
///
/// `$FUNGOS_VFS_CAS` wins, so a service, a test, or a user with an unusual disk
/// layout can name the directory outright. Otherwise the XDG data home is used —
/// machine-wide content belongs in the machine's data directory, not in whichever
/// checkout happens to be writing it — falling back to the platform convention
/// when `XDG_DATA_HOME` is unset.
pub fn default_data_dir() -> Result<PathBuf, VfsError> {
    data_dir_from(|name| std::env::var_os(name))
}

/// The precedence, as a function of an environment rather than of this process's
/// copy of it.
///
/// A test that wanted to pin the ordering by setting the variables would be
/// mutating state every other test in the process shares; passing the environment
/// in keeps the rule testable and the process untouched.
fn data_dir_from(env: impl Fn(&str) -> Option<OsString>) -> Result<PathBuf, VfsError> {
    let env = |name: &str| env(name).filter(|value| !value.is_empty());
    if let Some(dir) = env(CAS_ENV) {
        return Ok(PathBuf::from(dir));
    }
    if let Some(dir) = env("XDG_DATA_HOME") {
        return Ok(PathBuf::from(dir).join(APP_DIR));
    }
    if cfg!(windows) {
        if let Some(dir) = env("LOCALAPPDATA") {
            return Ok(PathBuf::from(dir).join(APP_DIR));
        }
    } else if let Some(home) = env("HOME") {
        return Ok(PathBuf::from(home).join(".local/share").join(APP_DIR));
    }
    Err(VfsError::NoDataDirectory)
}

/// A sibling of `path` to write first and rename into place.
///
/// The name has to be unique per write, not just per process: the store is
/// shared, so two threads or two roots can be filing the same object at the same
/// moment, and a name they both pick makes one of the two renames fail on a file
/// the other has already moved.
fn temporary_path(path: &Path) -> PathBuf {
    static NONCE: AtomicU64 = AtomicU64::new(0);
    let nonce = NONCE.fetch_add(1, Ordering::Relaxed);
    path.with_extension(format!("tmp-{}-{nonce}", std::process::id()))
}

/// What a root's legacy object directory held when it moved.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct LegacyMigration {
    /// Objects written into the shared store.
    pub moved: usize,
    /// Objects already there, which therefore cost nothing.
    pub already_shared: usize,
    /// Files left behind because they are not objects.
    pub left_behind: usize,
}

/// The content store every VFS root on this machine shares.
pub struct ContentStore {
    /// The data directory: `objects/` and `roots/` live under it.
    data: PathBuf,
    /// A root's old per-root object directory, read while it is still there.
    legacy: Option<PathBuf>,
}

impl ContentStore {
    /// Open the store, registering `legacy` as a second place to read from.
    pub fn open(data: impl Into<PathBuf>, legacy: Option<PathBuf>) -> Result<Self, VfsError> {
        let data = data.into();
        fs::create_dir_all(data.join(OBJECTS))?;
        fs::create_dir_all(data.join(ROOTS))?;
        Ok(Self { data, legacy })
    }

    pub fn data_dir(&self) -> &Path {
        &self.data
    }

    fn objects(&self) -> PathBuf {
        self.data.join(OBJECTS)
    }

    pub fn path_for(&self, id: &ContentId) -> PathBuf {
        let hex = id.hex();
        self.objects().join(&hex[..2]).join(hex)
    }

    fn legacy_path_for(&self, id: &ContentId) -> Option<PathBuf> {
        let legacy = self.legacy.as_deref()?;
        let hex = id.hex();
        Some(legacy.join(&hex[..2]).join(hex))
    }

    pub fn put(&self, bytes: &[u8]) -> Result<ContentId, VfsError> {
        let id = ContentId::from_bytes(bytes);
        let path = self.path_for(&id);
        if path.exists() {
            return Ok(id);
        }
        let Some(parent) = path.parent() else {
            return Err(VfsError::InvalidPath(path.display().to_string()));
        };
        fs::create_dir_all(parent)?;
        let temporary = temporary_path(&path);
        fs::write(&temporary, bytes)?;
        fs::rename(temporary, &path)?;
        Ok(id)
    }

    /// The bytes a content id names.
    ///
    /// The shared store answers first and a legacy per-root directory second, so
    /// a root that has not been migrated still reads. The two are the same store
    /// of the same bytes, keyed by the same digest: which one answers is not a
    /// fact any caller needs to know.
    pub fn get(&self, id: &ContentId) -> Result<Vec<u8>, VfsError> {
        if let Some(bytes) = self.read(&self.path_for(id))? {
            return Ok(bytes);
        }
        match self.legacy_path_for(id) {
            Some(path) => self
                .read(&path)?
                .ok_or_else(|| VfsError::NotHydrated(id.to_string())),
            None => Err(VfsError::NotHydrated(id.to_string())),
        }
    }

    /// The bytes at `path`, or `None` when nothing is there.
    fn read(&self, path: &Path) -> Result<Option<Vec<u8>>, VfsError> {
        match fs::read(path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(VfsError::Io(error)),
        }
    }

    /// Record that `root` has a namespace which may name this store's content.
    ///
    /// A root whose namespace lives here and is missing from this list is a root
    /// whose content `gc` would delete. The marker is named by the root's own
    /// path, so two processes opening one root agree on it, and written through a
    /// temporary file so a reader never sees half a path.
    pub fn register_root(&self, root: &Path) -> Result<(), VfsError> {
        let marker = self.root_marker(root);
        if marker.is_file() {
            return Ok(());
        }
        fs::create_dir_all(self.data.join(ROOTS))?;
        let temporary = temporary_path(&marker);
        fs::write(&temporary, root.to_string_lossy().as_bytes())?;
        fs::rename(temporary, &marker)?;
        Ok(())
    }

    /// Every root that has registered itself with this store.
    pub fn registered_roots(&self) -> Result<Vec<PathBuf>, VfsError> {
        let mut roots = Vec::new();
        for entry in fs::read_dir(self.data.join(ROOTS))? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            if let Ok(text) = fs::read_to_string(entry.path())
                && !text.is_empty()
            {
                roots.push(PathBuf::from(text));
            }
        }
        roots.sort();
        Ok(roots)
    }

    pub fn forget_root(&self, root: &Path) -> Result<(), VfsError> {
        match fs::remove_file(self.root_marker(root)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(VfsError::Io(error)),
        }
    }

    fn root_marker(&self, root: &Path) -> PathBuf {
        let digest = blake3::hash(root.to_string_lossy().as_bytes()).to_hex();
        self.data.join(ROOTS).join(&digest[..16])
    }

    /// Delete every object no id in `reachable` names.
    ///
    /// The caller owns the scope of `reachable`. Content is shared, so collecting
    /// against one root's namespace would delete what every other root points at:
    /// the set has to cover the machine.
    pub fn gc<'a>(
        &self,
        reachable: impl IntoIterator<Item = &'a ContentId>,
    ) -> Result<usize, VfsError> {
        let reachable: HashSet<&ContentId> = reachable.into_iter().collect();
        let mut removed = 0;
        for shard in fs::read_dir(self.objects())? {
            let shard = shard?;
            if !shard.file_type()?.is_dir() {
                continue;
            }
            for entry in fs::read_dir(shard.path())? {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().to_string();
                let Ok(id) = ContentId::parse(&format!("blake3:{name}")) else {
                    continue;
                };
                if !reachable.contains(&id) {
                    fs::remove_file(entry.path())?;
                    removed += 1;
                }
            }
            // An emptied shard goes with its last object; one that still holds
            // something is left alone.
            let _ = fs::remove_dir(shard.path());
        }
        Ok(removed)
    }

    /// Move a root's per-root objects into the shared store.
    ///
    /// Deliberately a command rather than something opening a store does: an open
    /// that rewrites gigabytes is a change nobody can attribute to anything.
    ///
    /// Interruptible and idempotent: each object is filed in the shared store and
    /// only then removed from the legacy directory, so a run that dies leaves the
    /// rest for the next one, and a run after a finished one has nothing to do.
    /// Nothing is deleted before its bytes are checked against the name they are
    /// filed under — moving a corrupt object would put bytes under a digest they
    /// do not have.
    pub fn migrate_legacy(&self) -> Result<LegacyMigration, VfsError> {
        let Some(legacy) = self.legacy.clone() else {
            return Ok(LegacyMigration::default());
        };
        if !legacy.is_dir() {
            return Ok(LegacyMigration::default());
        }
        let mut report = LegacyMigration::default();
        for shard in fs::read_dir(&legacy)? {
            let shard = shard?;
            if !shard.file_type()?.is_dir() {
                continue;
            }
            for entry in fs::read_dir(shard.path())? {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().to_string();
                let Ok(id) = ContentId::parse(&format!("blake3:{name}")) else {
                    // Not an object: a temporary file from an interrupted write,
                    // or something another tool left here. It stays visible
                    // rather than being deleted on a guess.
                    report.left_behind += 1;
                    continue;
                };
                let bytes = fs::read(entry.path())?;
                if ContentId::from_bytes(&bytes) != id {
                    return Err(VfsError::InvalidContentId(id.to_string()));
                }
                if self.path_for(&id).exists() {
                    report.already_shared += 1;
                } else {
                    self.put(&bytes)?;
                    report.moved += 1;
                }
                fs::remove_file(entry.path())?;
            }
            let _ = fs::remove_dir(shard.path());
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The environment as a lookup, so the rule can be tested without touching
    /// the one this process shares with every other test.
    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<OsString> {
        let pairs: Vec<(String, OsString)> = pairs
            .iter()
            .map(|(name, value)| ((*name).to_string(), OsString::from(value)))
            .collect();
        move |name: &str| {
            pairs
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.clone())
        }
    }

    #[test]
    fn the_override_wins_and_the_data_home_is_the_fallback() {
        let dir = data_dir_from(env(&[(CAS_ENV, "/cas"), ("XDG_DATA_HOME", "/xdg")])).unwrap();
        assert_eq!(dir, PathBuf::from("/cas"), "the override is not a hint");

        let dir = data_dir_from(env(&[("XDG_DATA_HOME", "/xdg")])).unwrap();
        assert_eq!(dir, PathBuf::from("/xdg").join(APP_DIR));

        // An empty variable is not a directory, and neither is a missing one:
        // failing loudly beats writing content somewhere nobody chose.
        assert!(matches!(
            data_dir_from(env(&[(CAS_ENV, ""), ("XDG_DATA_HOME", "")])),
            Err(VfsError::NoDataDirectory)
        ));
    }
}
