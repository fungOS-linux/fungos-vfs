//! Replicas: the other places a piece of content can be.
//!
//! Content is immutable and named by its own digest, which is what makes
//! replication both easy and safe: a copy is a copy, `put` is idempotent, there
//! is no order to agree on, and a source that is down is a source to skip rather
//! than a conflict to resolve. That is the whole difference between replicating
//! content and syncing ops — the log has an order and content does not.
//!
//! Three properties are load-bearing:
//!
//! - **Every byte is checked.** A replica that hands back the wrong block is
//!   worse than one that is down, because the byte is about to become a file.
//!   The digest is verified on read and a mismatch fails; it is never a
//!   substitute.
//! - **Reads race, writes mirror.** [`Replicas::fetch_any`] asks every source at
//!   once and takes the first verified answer, so four slow replicas add up to
//!   one fast one. [`Replicas::put_all`] writes everywhere it can and reports
//!   each outcome.
//! - **A source that fails says which one it was.** A backup that stopped
//!   working without saying so is the failure this module exists to prevent.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    thread,
};

use serde::{Deserialize, Serialize};

use crate::{ContentId, Hydration, Vfs, VfsError};

/// Whether a copy had to be written or was already there.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Stored {
    Added,
    AlreadyPresent,
}

/// One place content can live besides this machine's own store.
///
/// A disk, a RAID array, an R2 bucket and a Kubo node are all the same shape: a
/// name goes in, bytes come out, and the name is the digest of the bytes. `size`
/// is a hint for sources that can refuse an obviously wrong read before starting
/// it; a source that ignores it is still correct.
pub trait Replica: Send + Sync {
    fn name(&self) -> &str;

    /// The bytes, verified against `id` by the implementation.
    fn fetch(&self, id: &ContentId, size: u64) -> Result<Vec<u8>, VfsError>;

    /// File the bytes under their own digest. Must be idempotent.
    fn put(&self, id: &ContentId, bytes: &[u8]) -> Result<Stored, VfsError>;

    /// Whether writing here is allowed. A read-only replica is a peer's copy.
    fn writable(&self) -> bool {
        true
    }
}

/// What a source returned, and which one answered.
#[derive(Debug)]
pub struct Fetched {
    pub source: String,
    pub bytes: Vec<u8>,
}

/// What one replica said about one `put`.
#[derive(Debug, Serialize)]
pub struct Replicated {
    pub source: String,
    /// `added`, `already`, `read-only` or `failed`.
    pub outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// A set of replicas, read in parallel and written in full.
#[derive(Default, Clone)]
pub struct Replicas {
    sources: Vec<Arc<dyn Replica>>,
}

impl std::fmt::Debug for Replicas {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Replicas")
            .field(
                "sources",
                &self
                    .sources
                    .iter()
                    .map(|source| source.name().to_string())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl Replicas {
    #[must_use]
    pub fn new(sources: Vec<Arc<dyn Replica>>) -> Self {
        Self { sources }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    pub fn names(&self) -> Vec<&str> {
        self.sources.iter().map(|source| source.name()).collect()
    }

    /// Every source is asked at once and the first verified answer wins.
    ///
    /// Racing is the point: the replicas differ by orders of magnitude in latency
    /// (a RAID shelf, a bucket in another region, a node behind a tailnet) and
    /// asking them in turn makes every fetch cost the sum instead of the fastest.
    /// A source that fails is a `detail` on the way to the one that works, not
    /// the end of the attempt — and when nothing has it, every failure is named,
    /// because "not found" without the list is a shrug.
    pub fn fetch_any(&self, id: &ContentId, size: u64) -> Result<Fetched, VfsError> {
        if self.sources.is_empty() {
            return Err(VfsError::Config(
                "no replica is configured; nothing to fetch from".to_owned(),
            ));
        }
        let (sender, receiver) = mpsc::channel();
        for source in &self.sources {
            let sender = sender.clone();
            let source = Arc::clone(source);
            let id = id.clone();
            // Detached on purpose: the first answer ends the wait, and a source
            // that is still thinking should not hold up the caller.
            thread::spawn(move || {
                let outcome = source.fetch(&id, size);
                let _ = sender.send((source.name().to_owned(), outcome));
            });
        }
        drop(sender);

        let mut failures = Vec::new();
        for (name, outcome) in receiver {
            match outcome {
                Ok(bytes) => {
                    return Ok(Fetched {
                        source: name,
                        bytes,
                    });
                }
                Err(error) => failures.push(format!("{name}: {error}")),
            }
        }
        Err(VfsError::NotFound(format!(
            "{} from any replica ({})",
            id,
            failures.join("; ")
        )))
    }

    /// Write to every writable replica and report each one.
    pub fn put_all(&self, id: &ContentId, bytes: &[u8]) -> Vec<Replicated> {
        let mut outcomes = Vec::with_capacity(self.sources.len());
        for source in &self.sources {
            let mut entry = Replicated {
                source: source.name().to_owned(),
                outcome: "failed",
                detail: None,
            };
            if !source.writable() {
                entry.outcome = "read-only";
            } else {
                match source.put(id, bytes) {
                    Ok(Stored::Added) => entry.outcome = "added",
                    Ok(Stored::AlreadyPresent) => entry.outcome = "already",
                    Err(error) => entry.detail = Some(format!("{error}")),
                }
            }
            outcomes.push(entry);
        }
        outcomes
    }
}

/// A directory of objects, laid out the way the local store lays them out.
///
/// The layout is the same on purpose: a replica is a mirror, and a mirror that
/// sorts its files differently is a second format to debug at the worst moment.
/// An external disk, a RAID shelf, a network export and another machine's store
/// directory all take the same path.
pub struct DiskReplica {
    name: String,
    root: PathBuf,
    writable: bool,
}

impl DiskReplica {
    #[must_use]
    pub fn new(name: impl Into<String>, root: impl Into<PathBuf>) -> Self {
        Self {
            name: name.into(),
            root: root.into(),
            writable: true,
        }
    }

    /// A copy that is read from but never written to: a peer's store, or a
    /// snapshot of one taken before this machine existed.
    #[must_use]
    pub fn read_only(mut self) -> Self {
        self.writable = false;
        self
    }

    #[must_use]
    pub fn path_for(&self, id: &ContentId) -> PathBuf {
        let hex = id.hex();
        self.root.join(&hex[..2]).join(hex)
    }
}

impl Replica for DiskReplica {
    fn name(&self) -> &str {
        &self.name
    }

    fn fetch(&self, id: &ContentId, _size: u64) -> Result<Vec<u8>, VfsError> {
        let path = self.path_for(id);
        match fs::read(&path) {
            Ok(bytes) => {
                // The digest is the check, on every source. A mirror is the
                // likeliest place for a half-written file to be sitting.
                if ContentId::from_bytes(&bytes) != *id {
                    return Err(VfsError::InvalidContentId(format!(
                        "{} holds bytes that hash to something else",
                        path.display()
                    )));
                }
                Ok(bytes)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Err(VfsError::NotHydrated(id.to_string()))
            }
            Err(error) => Err(VfsError::Io(error)),
        }
    }

    fn put(&self, id: &ContentId, bytes: &[u8]) -> Result<Stored, VfsError> {
        if !self.writable {
            return Err(VfsError::ReadOnly);
        }
        let path = self.path_for(id);
        if path.is_file() {
            return Ok(Stored::AlreadyPresent);
        }
        let Some(parent) = path.parent() else {
            return Err(VfsError::InvalidPath(path.display().to_string()));
        };
        fs::create_dir_all(parent)?;
        // Written beside the destination and renamed over it, so a reader never
        // sees a partial object under a name that promises a digest.
        let temporary = parent.join(format!(
            ".{}.{}.{}",
            id.hex(),
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(&temporary, bytes)?;
        fs::rename(&temporary, &path)?;
        Ok(Stored::Added)
    }

    fn writable(&self) -> bool {
        self.writable
    }
}

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// One configured replica, as it is written down.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicaSpec {
    pub name: String,
    /// `disk` today. `kubo` and `r2` are adapters over the same trait, and a
    /// kind this build has no adapter for is refused rather than ignored.
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
    #[serde(default)]
    pub read_only: bool,
}

/// The replicas a root knows about. Device-local: a RAID path is not a fact that
/// travels with a namespace, and a bucket is reached with this machine's
/// credentials.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ReplicaConfig {
    #[serde(default)]
    pub replicas: Vec<ReplicaSpec>,
}

/// Where a root keeps its replica list.
#[must_use]
pub fn config_path(root: &Path) -> PathBuf {
    root.join(".fungos").join("replicas.json")
}

impl ReplicaConfig {
    pub fn load(root: &Path) -> Result<Self, VfsError> {
        let path = config_path(root);
        match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|error| VfsError::Config(format!("{}: {error}", path.display()))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(error) => Err(VfsError::Io(error)),
        }
    }

    pub fn save(&self, root: &Path) -> Result<(), VfsError> {
        let path = config_path(root);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec_pretty(self)
            .map_err(|error| VfsError::Config(format!("{}: {error}", path.display())))?;
        fs::write(&path, bytes)?;
        Ok(())
    }

    /// Add or replace a replica by name, so `add` is idempotent.
    pub fn upsert(&mut self, spec: ReplicaSpec) {
        match self
            .replicas
            .iter_mut()
            .find(|existing| existing.name == spec.name)
        {
            Some(existing) => *existing = spec,
            None => self.replicas.push(spec),
        }
    }

    pub fn remove(&mut self, name: &str) -> bool {
        let before = self.replicas.len();
        self.replicas.retain(|spec| spec.name != name);
        self.replicas.len() != before
    }

    /// Build the set, refusing a kind this build cannot reach.
    pub fn build(&self) -> Result<Replicas, VfsError> {
        let mut sources: Vec<Arc<dyn Replica>> = Vec::with_capacity(self.replicas.len());
        for spec in &self.replicas {
            let replica: Arc<dyn Replica> = match spec.kind.as_str() {
                "disk" => {
                    let path = spec.path.clone().ok_or_else(|| {
                        VfsError::Config(format!(
                            "replica {:?} is a disk and needs a path",
                            spec.name
                        ))
                    })?;
                    let disk = DiskReplica::new(spec.name.clone(), path);
                    if spec.read_only {
                        Arc::new(disk.read_only())
                    } else {
                        Arc::new(disk)
                    }
                }
                other => {
                    return Err(VfsError::Config(format!(
                        "replica {:?} is kind {other:?}, which this build has no adapter for \
                         (disk is built in)",
                        spec.name
                    )));
                }
            };
            sources.push(replica);
        }
        Ok(Replicas::new(sources))
    }
}

/// What a hydration did.
#[derive(Debug, Serialize)]
pub struct Hydrated {
    pub path: String,
    pub content_id: String,
    pub source: String,
    pub size: usize,
}

impl Vfs {
    /// Fill one placeholder from whichever replica answers first.
    pub fn hydrate_from_replicas(
        &mut self,
        path: &str,
        replicas: &Replicas,
    ) -> Result<Hydrated, VfsError> {
        let entry = self
            .file(path)?
            .ok_or_else(|| VfsError::NotFound(path.to_owned()))?;
        if entry.hydration == Hydration::Present {
            return Err(VfsError::Config(format!(
                "{path} is already present; nothing to hydrate"
            )));
        }
        let fetched = replicas.fetch_any(&entry.content_id, entry.size)?;
        self.hydrate(&entry.path, &fetched.bytes)?;
        Ok(Hydrated {
            path: entry.path,
            content_id: entry.content_id.to_string(),
            source: fetched.source,
            size: fetched.bytes.len(),
        })
    }

    /// Every placeholder under `prefix`, so a caller can hydrate a tree from the
    /// replicas without knowing which files are missing.
    pub fn placeholders(&self, prefix: &str) -> Result<Vec<(String, ContentId, u64)>, VfsError> {
        Ok(self
            .list(prefix)?
            .into_iter()
            .filter(|entry| entry.hydration == Hydration::Placeholder)
            .map(|entry| (entry.path, entry.content_id, entry.size))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::NodeRole;

    fn spec(name: &str, root: &Path, read_only: bool) -> ReplicaSpec {
        ReplicaSpec {
            name: name.to_owned(),
            kind: "disk".to_owned(),
            path: Some(root.to_path_buf()),
            read_only,
        }
    }

    /// A source that is down. Real ones fail this way, and the point is that a
    /// dead replica is a skip rather than the end of the attempt.
    struct Down(&'static str);

    impl Replica for Down {
        fn name(&self) -> &str {
            self.0
        }
        fn fetch(&self, id: &ContentId, _size: u64) -> Result<Vec<u8>, VfsError> {
            Err(VfsError::Transport(format!("{} cannot reach {id}", self.0)))
        }
        fn put(&self, _id: &ContentId, _bytes: &[u8]) -> Result<Stored, VfsError> {
            Err(VfsError::Transport(format!("{} is down", self.0)))
        }
    }

    /// A source that answers, but not soon enough to matter.
    struct Slow(Arc<DiskReplica>);

    impl Replica for Slow {
        fn name(&self) -> &str {
            "slow"
        }
        fn fetch(&self, id: &ContentId, size: u64) -> Result<Vec<u8>, VfsError> {
            thread::sleep(Duration::from_secs(2));
            self.0.fetch(id, size)
        }
        fn put(&self, id: &ContentId, bytes: &[u8]) -> Result<Stored, VfsError> {
            self.0.put(id, bytes)
        }
    }

    #[test]
    fn a_disk_replica_files_bytes_under_their_digest() {
        let temp = tempfile::tempdir().unwrap();
        let mirror = DiskReplica::new("raid", temp.path().join("mirror"));
        let bytes = b"a file worth keeping";
        let id = ContentId::from_bytes(bytes);

        assert_eq!(mirror.put(&id, bytes).unwrap(), Stored::Added);
        // The same layout as the local store: a mirror that sorts its objects
        // differently is a second format to debug at the worst moment.
        let expected = temp
            .path()
            .join("mirror")
            .join(&id.hex()[..2])
            .join(id.hex());
        assert_eq!(mirror.path_for(&id), expected);
        assert!(expected.is_file());

        // Idempotent, because a retried backup must not be a second copy.
        assert_eq!(mirror.put(&id, bytes).unwrap(), Stored::AlreadyPresent);
        assert_eq!(mirror.fetch(&id, bytes.len() as u64).unwrap(), bytes);
    }

    #[test]
    fn a_replica_holding_the_wrong_bytes_is_a_failure_not_a_substitute() {
        let temp = tempfile::tempdir().unwrap();
        let mirror = DiskReplica::new("raid", temp.path());
        let id = ContentId::from_bytes(b"what was asked for");
        let path = mirror.path_for(&id);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"something else entirely").unwrap();

        let error = mirror.fetch(&id, 0).unwrap_err();
        let message = format!("{error}");
        assert!(message.contains("hash to something else"), "{message}");
        assert!(message.contains(&id.hex()[..8]), "{message}");
    }

    #[test]
    fn writing_mirrors_and_reading_survives_one_copy_going_missing() {
        let temp = tempfile::tempdir().unwrap();
        let first = Arc::new(DiskReplica::new("raid", temp.path().join("raid")));
        let second = Arc::new(DiskReplica::new("nfs", temp.path().join("nfs")));
        let replicas = Replicas::new(vec![first.clone(), second.clone()]);
        let bytes = b"mirrored content";
        let id = ContentId::from_bytes(bytes);

        let outcomes = replicas.put_all(&id, bytes);
        assert_eq!(
            outcomes
                .iter()
                .map(|entry| entry.outcome)
                .collect::<Vec<_>>(),
            vec!["added", "added"]
        );
        assert_eq!(
            replicas
                .put_all(&id, bytes)
                .iter()
                .map(|entry| entry.outcome)
                .collect::<Vec<_>>(),
            vec!["already", "already"]
        );

        // One shelf loses it; the read still lands, and says where from.
        fs::remove_file(first.path_for(&id)).unwrap();
        let fetched = replicas.fetch_any(&id, bytes.len() as u64).unwrap();
        assert_eq!(fetched.source, "nfs");
        assert_eq!(fetched.bytes, bytes);
    }

    #[test]
    fn every_source_that_failed_is_named() {
        let temp = tempfile::tempdir().unwrap();
        let replicas = Replicas::new(vec![
            Arc::new(Down("bucket")) as Arc<dyn Replica>,
            Arc::new(DiskReplica::new("raid", temp.path().join("empty"))),
        ]);
        let id = ContentId::from_bytes(b"nobody has this");

        let error = replicas.fetch_any(&id, 0).unwrap_err();
        let message = format!("{error}");
        assert!(message.contains("bucket"), "{message}");
        assert!(message.contains("raid"), "{message}");
        assert!(message.contains("cannot reach"), "{message}");
    }

    #[test]
    fn the_answer_arrives_at_the_speed_of_the_fastest_source() {
        let temp = tempfile::tempdir().unwrap();
        let shelf = Arc::new(DiskReplica::new("shelf", temp.path().join("shelf")));
        let bytes = b"present on both, quick on one";
        let id = ContentId::from_bytes(bytes);
        shelf.put(&id, bytes).unwrap();
        // Two sources that both have it: one answers now, one after a two-second
        // pause. Racing is worth nothing unless the fast answer is the one used.
        let replicas = Replicas::new(vec![Arc::new(Slow(shelf.clone())), shelf]);

        let started = Instant::now();
        let fetched = replicas.fetch_any(&id, bytes.len() as u64).unwrap();
        assert!(
            started.elapsed() < Duration::from_millis(900),
            "waited {:?} for an answer that was already available",
            started.elapsed()
        );
        assert_eq!(fetched.source, "shelf");
        assert_eq!(fetched.bytes, bytes);
    }

    #[test]
    fn a_kind_without_an_adapter_is_refused_by_name() {
        let config = ReplicaConfig {
            replicas: vec![ReplicaSpec {
                name: "cold".to_owned(),
                kind: "r2".to_owned(),
                path: None,
                read_only: false,
            }],
        };
        let error = config.build().expect_err("an unknown kind is refused");
        let message = format!("{error}");
        assert!(message.contains("cold"), "{message}");
        assert!(message.contains("r2"), "{message}");
    }

    #[test]
    fn the_replica_list_round_trips_and_add_replaces_by_name() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        assert!(ReplicaConfig::load(root).unwrap().replicas.is_empty());

        let mut config = ReplicaConfig::default();
        config.upsert(spec("raid", Path::new("/Volumes/RAID/fungos"), false));
        config.save(root).unwrap();
        let loaded = ReplicaConfig::load(root).unwrap();
        assert_eq!(loaded.replicas.len(), 1);
        assert_eq!(loaded.build().unwrap().names(), vec!["raid"]);

        // `add` is idempotent: the same name is the same replica, corrected.
        config.upsert(spec("raid", Path::new("/Volumes/RAID/other"), true));
        assert_eq!(config.replicas.len(), 1);
        assert_eq!(
            config.replicas[0].path.as_deref(),
            Some(Path::new("/Volumes/RAID/other"))
        );
        assert!(config.replicas[0].read_only);
        assert!(config.remove("raid"));
        assert!(!config.remove("raid"));
    }

    #[test]
    fn a_placeholder_is_filled_from_whichever_replica_has_it() {
        let temp = tempfile::tempdir().unwrap();
        let cas = temp.path().join("cas");
        let bytes = b"a file this node only knows the name of";
        let id = ContentId::from_bytes(bytes);
        let mut node =
            Vfs::open_with(temp.path().join("node"), NodeRole::Cache, Some(&cas)).unwrap();
        node.register_placeholder(
            "docs/handoff.md",
            id.clone(),
            bytes.len() as u64,
            "text/markdown",
        )
        .unwrap();
        assert_eq!(node.placeholders("docs").unwrap().len(), 1);

        let mirror = Arc::new(DiskReplica::new("shelf", temp.path().join("shelf")));
        mirror.put(&id, bytes).unwrap();
        let replicas = Replicas::new(vec![Arc::new(Down("bucket")) as Arc<dyn Replica>, mirror]);

        let hydrated = node
            .hydrate_from_replicas("docs/handoff.md", &replicas)
            .unwrap();
        assert_eq!(hydrated.source, "shelf");
        assert_eq!(hydrated.size, bytes.len());
        assert_eq!(node.get("docs/handoff.md").unwrap(), bytes);
        assert!(node.placeholders("docs").unwrap().is_empty());

        // Hydrating what is already present is a mistake worth naming rather than
        // a silent no-op that hides a caller working from a stale list.
        let again = node.hydrate_from_replicas("docs/handoff.md", &replicas);
        assert!(format!("{}", again.unwrap_err()).contains("already present"));
    }

    #[test]
    fn a_read_only_replica_is_never_written_to() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("peer");
        let config = ReplicaConfig {
            replicas: vec![spec("peer", &root, true)],
        };
        let replicas = config.build().unwrap();
        let bytes = b"their copy, not ours to change";
        let id = ContentId::from_bytes(bytes);

        let outcomes = replicas.put_all(&id, bytes);
        assert_eq!(outcomes[0].outcome, "read-only");
        assert!(!root.exists(), "nothing was written there");
    }
}
