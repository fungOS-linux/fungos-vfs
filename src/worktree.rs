//! Worktrees on a real disk, recorded by the difference.
//!
//! A worktree is where work happens: a directory on the local filesystem, where
//! `cargo`, an editor, and `rg` behave exactly as they always have. The namespace
//! is where the work is *recorded*. Keeping those apart is the whole point — a
//! virtual filesystem under a build turns every compiler `stat` into a syscall
//! into our code, and it is still not the directory the editor has open.
//!
//! What ties the two together is a base and a diff:
//!
//! - [`materialize`] puts a recorded worktree on disk at its base commit, then
//!   places the recorded in-between on top. Files the commit covers come from
//!   Git; only the in-between is streamed out of the content store, and where the
//!   filesystem can copy on write it is a reflink rather than a copy.
//! - [`record`] asks Git what changed since the base commit, hashes *only* those
//!   files, and writes one op for each that differs from what the namespace
//!   already holds. A 678-file checkout with three edits costs three ops and zero
//!   bytes for the other 675: the rest is still the commit's tree, which Git
//!   already content-addresses, and re-deriving it would be the thing this whole
//!   module exists to avoid.
//! - [`changes`] answers "what happened after this cursor", so a harness watching
//!   an agent work can poll instead of walking the tree.
//!
//! Two rules make an interrupted recording safe:
//!
//! 1. **The plan is written before it is applied.** Every op lands in
//!    `worktree_journal` in one transaction, with its content already filed in the
//!    store. A run that dies leaves a plan the next run finishes.
//! 2. **Ops are committed before the base advances.** Each op is its own
//!    transaction; the recorded base moves only after they have all landed, in the
//!    same transaction that clears the journal. A base that moved without its ops
//!    loses an edit; ops without a moved base are a replay, and a replay of an op
//!    that already landed is recognised by looking at the namespace — not by
//!    trusting a flag that could itself be the thing that was lost.
//!
//! Deliberate limits, all of them names rather than surprises:
//!
//! - A worktree is a Git worktree. A plain directory has no cheap base, and
//!   inventing a per-file one would cost a hash of everything on every record —
//!   the cost this design exists to avoid. Refused rather than approximated.
//! - Ignored files are not recorded, because Git does not report them and the
//!   attached policy excludes build output anyway. Same rule as `project attach`,
//!   from the same policy.
//! - Renames are recorded as a removal and an upsert. That is one blob, which is
//!   what a rename costs to store, and it keeps the plan free of a two-path entry
//!   form.
//! - Symlinks and submodule entries are skipped: neither is content.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs, io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use rusqlite::OptionalExtension;
use serde::Serialize;

use crate::{
    ContentId, Expect, GitPointer, Hydration, IngestReport, PathClass, PathPolicy, SyncOpKind, Vfs,
    project::{attach_policy, git},
    transfer::media_type_for,
};

/// Leases are renewed this often while a plan is being applied, so a recording
/// that outlives its first deadline stays owned by the process doing the work.
const RENEW_EVERY: usize = 128;

/// Long enough for a large worktree's first record, short enough that a killed
/// process does not own the worktree for the rest of the day.
pub const DEFAULT_LEASE_MS: u64 = 120_000;

/// What this desktop knows about one worktree.
#[derive(Clone, Debug, Serialize)]
pub struct WorktreeState {
    pub name: String,
    /// Where the working directory is on this machine.
    pub path: PathBuf,
    /// The namespace prefix this worktree's in-between lands under.
    pub prefix: String,
    /// The commit the recorded state is relative to.
    pub base_commit: String,
    pub base_tree: String,
    /// How many paths the recording holds on top of that commit.
    pub overrides: usize,
    /// Ops of an interrupted record still waiting to be applied.
    pub journal: usize,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// What `worktree materialize` did.
#[derive(Debug, Default, Serialize)]
pub struct MaterializeReport {
    pub name: String,
    pub path: PathBuf,
    pub prefix: String,
    pub commit: String,
    pub tree: String,
    /// The directory was already there at this base; only the in-between moved.
    pub reused: bool,
    /// The directory was adopted rather than created: it was already a checkout
    /// of this repository at this commit, and nothing in it was written.
    pub adopted: bool,
    /// Paths placed from the content store.
    pub placed: usize,
    /// Of those, how many the filesystem copied on write instead of copying.
    pub cloned: usize,
    /// Paths the recording knows are absent and removed after the checkout.
    pub removed: usize,
    pub bytes: u64,
}

/// How one `record` behaves.
#[derive(Clone, Debug)]
pub struct RecordOptions {
    /// Who holds the lease while the record runs. Named in the refusal, so it
    /// should say which process it is rather than which human.
    pub owner: String,
    pub lease_ms: u64,
}

impl RecordOptions {
    /// An owner for this process: the actor, and the pid that is doing the work.
    pub fn new(actor: &str) -> Self {
        Self {
            owner: format!("{actor}:{}", std::process::id()),
            lease_ms: DEFAULT_LEASE_MS,
        }
    }
}

/// What `worktree checkout` did.
#[derive(Debug, Default, Serialize)]
pub struct CheckoutReport {
    pub name: String,
    pub path: PathBuf,
    /// The namespace the in-between came from.
    pub namespace: String,
    pub base_commit: String,
    /// Placed where nothing was.
    pub written: usize,
    /// Already held the recorded content.
    pub kept: usize,
    /// Were the base commit's version, so laying the in-between over them is not
    /// a loss.
    pub replaced: usize,
}

/// What `worktree prune` found, and what it did about it.
#[derive(Debug, Serialize)]
pub struct PruneEntry {
    pub name: String,
    pub path: PathBuf,
    /// Why this one can go, or why it stays.
    ///
    /// `clean` and `missing` are removable. `recorded` (the recording still
    /// holds an in-between), `dirty` (uncommitted work the recording has not
    /// seen), `leased` (work in progress), `unreadable` and `kept-by-git` are
    /// kept, each with its own word so a report can be read without guessing.
    pub state: &'static str,
    pub removed: bool,
    /// What went wrong, when something did: git refused, the row said one thing
    /// and the disk said another.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// What `worktree prune` looked at and did.
#[derive(Debug, Default, Serialize)]
pub struct PruneReport {
    pub applied: bool,
    pub entries: Vec<PruneEntry>,
}

/// How `worktree checkout` behaves.
#[derive(Clone, Debug)]
pub struct CheckoutOptions {
    pub name: String,
    pub path: PathBuf,
    /// The attached project to read the address from; defaults to the name of
    /// the repository directory at `path`.
    pub project: Option<String>,
    pub prefix: String,
    /// Lay the in-between over this base instead of the recorded one.
    pub commit: Option<String>,
    /// Write over files that are neither the recorded content nor the base's.
    pub force: bool,
    pub owner: String,
    pub lease_ms: u64,
}

/// What `worktree record` did.
#[derive(Debug, Default, Serialize)]
pub struct RecordReport {
    pub worktree: String,
    pub path: PathBuf,
    pub prefix: String,
    pub base_commit: String,
    /// The commit the base moved to, when the worktree had been committed since
    /// the recording was made.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_advanced_to: Option<String>,
    /// This run finished a plan an earlier run started.
    pub resumed: bool,
    /// Ops in the plan. This is the number the acceptance test is about.
    pub planned: usize,
    pub applied: usize,
    pub upserted: usize,
    pub removed: usize,
    /// Ops that were already in the namespace — a replay of an interrupted run.
    pub replayed: usize,
    /// Candidate paths that needed no op because the namespace already matched.
    pub unchanged: usize,
    /// Symlinks, submodules, and excluded paths.
    pub skipped: usize,
    /// Bytes read to decide. Proof that the diff, not the tree, was hashed.
    pub hashed_bytes: u64,
    /// Bytes newly filed in the content store, so `0` for a repeat record.
    pub written_bytes: u64,
}

/// One page of the change feed, relative to the worktree's own root.
#[derive(Clone, Debug, Serialize)]
pub struct ChangePage {
    pub worktree: String,
    pub prefix: String,
    /// Pass back as `since` to continue from where this page ends.
    pub cursor: i64,
    pub changes: Vec<WorktreeChange>,
}

/// One change to a path inside a worktree.
#[derive(Clone, Debug, Serialize)]
pub struct WorktreeChange {
    pub cursor: i64,
    /// Relative to the worktree root, which is what a reader of the worktree
    /// wants; the namespace path is an implementation detail of the recording.
    pub path: String,
    pub op: SyncOpKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_id: Option<ContentId>,
    pub author: String,
    pub origin: String,
    pub at_ms: i64,
}

/// Where a worktree's recording lives in the namespace.
///
/// Deliberately the same address `project attach` gives it, and the same
/// function decides both: the project's main worktree *is* the project's
/// namespace, and every other worktree hangs under it. Two functions naming one
/// worktree is how a recording and an attach end up writing the same files to
/// two addresses.
///
/// `label` is the recording's own name; `main` says the directory is the
/// repository's main worktree, which cannot be inferred from the label.
#[must_use]
pub fn namespace_for(prefix: &str, project: &Path, label: &str, main: bool) -> String {
    let project = project
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "project".to_string());
    let prefix = prefix.trim_matches('/');
    if main {
        return format!("{prefix}/{project}");
    }
    let label = crate::project::label(Some(label), Path::new(label));
    format!("{prefix}/{project}/worktrees/{label}")
}

/// Every worktree this store has recorded.
pub fn worktrees(vfs: &Vfs) -> Result<Vec<WorktreeState>> {
    let mut statement = vfs.db.prepare(
        "SELECT name,path,prefix,base_commit,base_tree,created_at_ms,updated_at_ms,
                (SELECT COUNT(*) FROM worktree_overrides o WHERE o.worktree=w.name),
                (SELECT COUNT(*) FROM worktree_journal j WHERE j.worktree=w.name)
         FROM worktrees w ORDER BY name",
    )?;
    let rows = statement.query_map([], |row| {
        Ok(WorktreeState {
            name: row.get(0)?,
            path: PathBuf::from(row.get::<_, String>(1)?),
            prefix: row.get(2)?,
            base_commit: row.get(3)?,
            base_tree: row.get(4)?,
            created_at_ms: row.get(5)?,
            updated_at_ms: row.get(6)?,
            overrides: row.get::<_, i64>(7)? as usize,
            journal: row.get::<_, i64>(8)? as usize,
        })
    })?;
    Ok(rows.collect::<Result<Vec<_>, _>>()?)
}

fn state(vfs: &Vfs, name: &str) -> Result<Option<WorktreeState>> {
    Ok(worktrees(vfs)?.into_iter().find(|state| state.name == name))
}

/// The recording for the worktree whose directory is `path`, if there is one.
///
/// Matched on the resolved path, because the same checkout is named `/src/soil`
/// by one caller and `/private/src/soil` by another and they are one directory.
pub fn state_for_path(vfs: &Vfs, path: &Path) -> Result<Option<WorktreeState>> {
    let wanted = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    Ok(worktrees(vfs)?.into_iter().find(|state| {
        fs::canonicalize(&state.path).unwrap_or_else(|_| state.path.clone()) == wanted
    }))
}

/// Write a recording into the namespace under `prefix`.
///
/// The namespace for a recorded worktree is the **in-between**, not the checkout:
/// the base commit covers every file the work has not touched, through one
/// pointer, so writing those files again would store what Git has already
/// content-addressed and make attaching a dirty worktree cost its whole tree.
///
/// It also reconciles both ways. Anything under `prefix` that the recording does
/// not hold is removed, which is what repairs a namespace an earlier `attach`
/// filled with a whole dirty checkout.
pub fn project(
    vfs: &mut Vfs,
    known: &WorktreeState,
    prefix: &str,
    dry_run: bool,
) -> Result<IngestReport> {
    let prefix = prefix.trim_matches('/');
    let mut report = IngestReport {
        prefix: prefix.to_string(),
        ..IngestReport::default()
    };
    let overlay = overrides(vfs, &known.name)?;
    let needle = format!("{prefix}/");

    // `list` matches by prefix, so a sibling whose name starts the same way
    // (`main` and `main-2`) comes back too; the separator is what tells them
    // apart, and pruning the wrong one would delete another worktree's files.
    let existing: BTreeMap<String, ContentId> = vfs
        .list(prefix)?
        .into_iter()
        .filter(|entry| entry.path.starts_with(&needle))
        .map(|entry| (entry.path, entry.content_id))
        .collect();

    for (rel, override_) in &overlay {
        let path = format!("{needle}{rel}");
        match override_.op {
            Planned::Upsert => {
                let content_id = override_.content_id.clone().ok_or_else(|| {
                    anyhow!("the recording for {rel} says it has content but names none")
                })?;
                match existing.get(&path) {
                    Some(current) if *current == content_id => report.unchanged += 1,
                    Some(_) => {
                        if !dry_run {
                            let bytes = vfs.content(&content_id)?;
                            vfs.put(&path, &bytes, &override_.media_type, Expect::Any)?;
                        }
                        report.changed += 1;
                        report.bytes_written += override_.size;
                    }
                    None => {
                        if !dry_run {
                            let bytes = vfs.content(&content_id)?;
                            vfs.put(&path, &bytes, &override_.media_type, Expect::Any)?;
                        }
                        report.added += 1;
                        report.bytes_written += override_.size;
                    }
                }
            }
            Planned::Remove => {
                if existing.contains_key(&path) {
                    if !dry_run {
                        vfs.remove(&path)?;
                    }
                    report.removed += 1;
                }
            }
            Planned::Forget => {
                // Never stored: a forgotten path is one the base commit covers.
            }
        }
    }

    for path in existing.keys() {
        let rel = path.strip_prefix(&needle).unwrap_or_default();
        if overlay.contains_key(rel) {
            continue;
        }
        if !dry_run {
            vfs.remove(path)?;
        }
        report.removed += 1;
    }

    report.git = Some(GitPointer {
        commit: known.base_commit.clone(),
        tree: known.base_tree.clone(),
        // What the tree id does not describe is exactly the in-between.
        dirty: !overlay.is_empty(),
    });
    Ok(report)
}

/// Put a recorded worktree on disk at its base commit.
///
/// The directory is a real Git worktree, registered with the repository like any
/// other, because `record` reads `git status` inside it and a person expects
/// `git worktree list` to show what is checked out. Files the commit covers come
/// from Git's own checkout; the recorded in-between is placed on top.
///
/// Safe to run twice: a second run against the same directory re-places the
/// in-between and changes nothing else.
pub fn materialize(
    vfs: &mut Vfs,
    project: &Path,
    name: &str,
    dest: &Path,
    commit: Option<&str>,
    prefix: &str,
) -> Result<MaterializeReport> {
    if name.trim().is_empty() {
        bail!("a worktree needs a name");
    }
    let root = PathBuf::from(git(project, &["rev-parse", "--show-toplevel"])?.trim());
    let commit = match commit {
        Some(spec) => git(&root, &["rev-parse", &format!("{spec}^{{commit}}")])?
            .trim()
            .to_string(),
        None => git(&root, &["rev-parse", "HEAD"])?.trim().to_string(),
    };
    let tree = git(&root, &["rev-parse", &format!("{commit}^{{tree}}")])?
        .trim()
        .to_string();
    // The repository's main worktree has a different address from every other
    // one, and the directory that is about to exist is the only thing that can
    // say which this is.
    let resolved = fs::canonicalize(dest).ok();
    let root_resolved = fs::canonicalize(&root).ok();
    let main = matches!((&resolved, &root_resolved), (Some(dest), Some(root)) if dest == root);
    let namespace = namespace_for(prefix, &root, name, main);

    let existing = state(vfs, name)?;
    if let Some(known) = &existing
        && known.path != dest
    {
        bail!(
            "worktree {name} is recorded at {}; \
             materialize restores a worktree rather than moving it",
            known.path.display()
        );
    }

    let mut report = MaterializeReport {
        name: name.to_string(),
        path: dest.to_path_buf(),
        prefix: namespace.clone(),
        commit: commit.clone(),
        tree: tree.clone(),
        ..MaterializeReport::default()
    };

    let present = dest.is_dir();
    let empty = present
        && fs::read_dir(dest)
            .with_context(|| format!("reading {}", dest.display()))?
            .next()
            .is_none();

    if present && !empty && existing.is_none() {
        // A checkout somebody already has is the common case: a worktree exists
        // because an agent or an editor made it, and the recording arrives after.
        // Adopting it writes nothing — the row says "this directory, at this
        // commit" and the next `record` describes whatever is already different.
        // Refusing instead would mean the only way into the recording was to
        // throw away the work in the directory first.
        let head = git(dest, &["rev-parse", "--show-toplevel"])
            .map(|top| top.trim().to_string())
            .ok()
            .is_some_and(|top| Path::new(&top) == root.as_path());
        if !head {
            bail!(
                "{} is not a worktree of {}; materialize writes into a directory \
                 it creates, or adopts one of this repository's",
                dest.display(),
                root.display()
            );
        }
        let on = git(dest, &["rev-parse", "HEAD"])
            .map(|value| value.trim().to_string())
            .unwrap_or_default();
        if on != commit {
            bail!(
                "{} is checked out at {on}, not {commit}; pass --commit {on} to \
                 record what is there",
                dest.display()
            );
        }
        report.adopted = true;
        report.reused = true;
    } else if present && !empty {
        // Already checked out at this base: only the in-between has to be put back.
        report.reused = true;
    } else {
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
        }
        // A recorded worktree whose directory was deleted leaves an entry in the
        // repository's own admin directory, and `worktree add` refuses while it
        // is there. Pruning drops entries whose directory is gone and nothing
        // else, which is exactly the dead entry this is removing.
        if existing.is_some() {
            git(&root, &["worktree", "prune"]).ok();
        }
        let dest_str = dest.to_string_lossy().into_owned();
        git(&root, &["worktree", "add", "--detach", &dest_str, &commit])
            .with_context(|| format!("checking out {commit} into {}", dest.display()))?;
    }

    for (rel, override_) in overrides(vfs, name)? {
        let target = dest.join(&rel);
        match override_.op {
            Planned::Upsert => {
                let content_id = override_.content_id.ok_or_else(|| {
                    anyhow!("the recording for {rel} says it has content but names none")
                })?;
                let cloned = place(vfs, &content_id, &target)?;
                report.placed += 1;
                report.cloned += usize::from(cloned);
                // Logical size of the in-between. What it cost to move is
                // `cloned` against `placed`: a reflink moves no bytes at all.
                report.bytes += override_.size;
            }
            _ => match fs::remove_file(&target) {
                Ok(()) => report.removed += 1,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error).context(target.display().to_string()),
            },
        }
    }

    let now = now_ms();
    vfs.db.execute(
        "INSERT INTO worktrees(name,path,prefix,base_commit,base_tree,created_at_ms,updated_at_ms)
         VALUES(?1,?2,?3,?4,?5,?6,?6)
         ON CONFLICT(name) DO UPDATE SET path=excluded.path, prefix=excluded.prefix,
           base_commit=excluded.base_commit, base_tree=excluded.base_tree,
           updated_at_ms=excluded.updated_at_ms",
        rusqlite::params![name, dest.to_string_lossy(), namespace, commit, tree, now],
    )?;

    Ok(report)
}

/// Lay a recorded worktree's in-between over a checkout on **this** machine.
///
/// The other half of a handoff. [`record`] writes what changed into the
/// namespace; this puts it back into a working tree somewhere else, so the agent
/// on the far side has files instead of a namespace. It is deliberately not
/// `materialize`: that one makes a checkout from a local recording, and the whole
/// point here is that the local recording is the thing that did not travel.
///
/// What travels is the project marker — `project attach` records where each
/// worktree lives and the commit it is based on — plus the in-between, as ops.
/// So the address and the base come from the marker, and the bytes come from the
/// content store.
///
/// Three outcomes per path, and one refusal:
///
/// - nothing there — write it;
/// - already the recorded content — nothing to do;
/// - the base commit's content — replace it, because that is what "the worktree
///   changed this file" means;
/// - anything else — **refuse**, naming the paths, and write nothing at all. A
///   file that is neither what was recorded nor what the commit says is somebody
///   else's edit, and laying a recording over it loses work silently.
///
/// It writes files, not namespace entries: the content is already in the
/// namespace, and a checkout that enqueued an op per file would tell every peer
/// the work had changed again.
pub fn checkout(vfs: &mut Vfs, options: &CheckoutOptions) -> Result<CheckoutReport> {
    let path = &options.path;
    if !path.is_dir() {
        bail!("{} is not a directory", path.display());
    }
    let project = match &options.project {
        Some(name) => name.clone(),
        None => path
            .canonicalize()
            .unwrap_or_else(|_| path.clone())
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .ok_or_else(|| anyhow!("{} has no name to attach to", path.display()))?,
    };

    let marker = format!("{}/{project}.json", crate::project::PROJECTS_DIR);
    let bytes = vfs
        .get(&marker)
        .with_context(|| format!("no attached project {project:?} here ({marker})"))?;
    let record: serde_json::Value = serde_json::from_slice(&bytes)?;
    let namespace = recorded_namespace(&record, &project, &options.name, &options.prefix)?;

    let head = git(path, &["rev-parse", "HEAD"])?.trim().to_string();
    let base = match &options.commit {
        Some(commit) => git(path, &["rev-parse", &format!("{commit}^{{commit}}")])?
            .trim()
            .to_string(),
        None => recorded_head(&record, &namespace)?,
    };
    if head != base && options.commit.is_none() {
        bail!(
            "{} is checked out at {head}, and the recording is based on {base}; \
             check out the recording's base first, or pass --commit {head} to lay \
             it over what is there",
            path.display()
        );
    }

    // The same lease a record takes: a checkout writes the working tree, and a
    // record that ran alongside it would describe a half-applied tree.
    vfs.lease(&namespace, &options.owner, options.lease_ms)?;
    let outcome = checkout_leased(vfs, options, &namespace, &base);
    let released = vfs.release(&namespace, &options.owner);
    let report = outcome?;
    released?;
    Ok(report)
}

fn checkout_leased(
    vfs: &mut Vfs,
    options: &CheckoutOptions,
    namespace: &str,
    base: &str,
) -> Result<CheckoutReport> {
    let needle = format!("{}/", namespace.trim_matches('/'));
    let entries: Vec<_> = vfs
        .list(namespace)?
        .into_iter()
        .filter(|entry| entry.path.starts_with(&needle) && entry.hydration == Hydration::Present)
        .collect();

    let mut plan: Vec<(String, ContentId, PathBuf)> = Vec::new();
    let mut conflicts = Vec::new();
    let mut report = CheckoutReport {
        name: options.name.clone(),
        path: options.path.clone(),
        namespace: namespace.to_string(),
        base_commit: base.to_string(),
        ..CheckoutReport::default()
    };

    for entry in entries {
        let rel = entry.path[needle.len()..].to_string();
        let target = options.path.join(&rel);
        match fs::read(&target) {
            Ok(bytes) if ContentId::from_bytes(&bytes) == entry.content_id => {
                report.kept += 1;
            }
            Ok(_) => {
                // It exists and it is not what was recorded. Whether replacing it
                // loses work is not a guess: the base commit already says what
                // this path should be, so a file matching the base is pristine.
                let pristine = is_pristine(&options.path, base, &rel);
                if pristine || options.force {
                    plan.push((rel, entry.content_id, target));
                    report.replaced += 1;
                } else {
                    conflicts.push(rel);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                plan.push((rel, entry.content_id, target));
                report.written += 1;
            }
            Err(error) => {
                return Err(error).with_context(|| format!("reading {}", target.display()));
            }
        }
    }

    if !conflicts.is_empty() {
        conflicts.sort();
        bail!(
            "{} path(s) in {} hold changes the recording does not know about: {}\n\
             nothing was written; move them aside, or pass --force to replace them",
            conflicts.len(),
            options.path.display(),
            conflicts.join(", ")
        );
    }

    for (_rel, content_id, target) in plan {
        place(vfs, &content_id, &target)?;
    }

    // This machine now has a recording of its own: the same address, the same
    // base, and an overlay that is empty because the in-between is already in the
    // namespace. Its first `record` adopts that state in one pass and writes no
    // ops, which is what makes the far side a peer rather than a reader.
    let tree = git(&options.path, &["rev-parse", &format!("{base}^{{tree}}")])?
        .trim()
        .to_string();
    let now = now_ms();
    vfs.db.execute(
        "INSERT INTO worktrees(name,path,prefix,base_commit,base_tree,created_at_ms,updated_at_ms)
         VALUES(?1,?2,?3,?4,?5,?6,?6)
         ON CONFLICT(name) DO UPDATE SET path=excluded.path, prefix=excluded.prefix,
           base_commit=excluded.base_commit, base_tree=excluded.base_tree,
           updated_at_ms=excluded.updated_at_ms",
        rusqlite::params![
            options.name,
            options.path.to_string_lossy(),
            namespace,
            base,
            tree,
            now
        ],
    )?;
    Ok(report)
}

/// Whether the file at `rel` still matches the base commit.
///
/// An untracked file never matches: the commit has no opinion about it, so
/// overwriting it would be overwriting something nobody has a copy of.
fn is_pristine(path: &Path, base: &str, rel: &str) -> bool {
    if git(path, &["ls-files", "--error-unmatch", "--", rel]).is_err() {
        return false;
    }
    git(path, &["diff", "--quiet", base, "--", rel]).is_ok()
}

/// The namespace a marker says this worktree lives at.
fn recorded_namespace(
    record: &serde_json::Value,
    project: &str,
    name: &str,
    prefix: &str,
) -> Result<String> {
    let main = namespace_for(prefix, Path::new(project), project, true);
    let wanted = crate::project::label(Some(name), Path::new(name));
    let mut found: Vec<String> = Vec::new();
    for entry in record["worktrees"].as_array().into_iter().flatten() {
        let Some(namespace) = entry["namespace"].as_str() else {
            continue;
        };
        let last = namespace.rsplit('/').next().unwrap_or_default();
        if name == "main" || name == project {
            if namespace == main {
                found.push(namespace.to_string());
            }
        } else if last == wanted {
            found.push(namespace.to_string());
        }
    }
    match found.len() {
        0 => bail!(
            "the attached project {project:?} has no worktree named {name:?}; \
             its marker names {:?}",
            record["worktrees"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|entry| entry["namespace"].as_str())
                .collect::<Vec<_>>()
        ),
        1 => Ok(found.remove(0)),
        _ => bail!("{name:?} matches more than one worktree: {found:?}"),
    }
}

fn recorded_head(record: &serde_json::Value, namespace: &str) -> Result<String> {
    for entry in record["worktrees"].as_array().into_iter().flatten() {
        if entry["namespace"].as_str() == Some(namespace) {
            let head = entry["head"].as_str().unwrap_or_default();
            if head.is_empty() {
                bail!("the marker has no commit for {namespace}");
            }
            return Ok(head.to_string());
        }
    }
    bail!("the marker does not name {namespace}")
}

/// Forget worktrees whose work is safe elsewhere, and say why for the rest.
///
/// Only two states are removable, and both are decided by evidence, never by age:
///
/// - **clean** — the recording holds no in-between and git reports nothing
///   uncommitted, so every file in the checkout is described by a commit and the
///   row is bookkeeping;
/// - **missing** — the directory is gone, so the row is a promise about nothing.
///
/// Everything else is kept and named: **recorded** (the recording still holds an
/// in-between, which exists nowhere else), **dirty** (the disk holds work the
/// recording has not seen), **leased** (something is working in it right now),
/// **unreadable**, **kept-by-git**. A prune that guesses is a prune that loses an
/// afternoon.
///
/// Clean is deliberately not the same question as *merged*. A removed worktree
/// loses its directory and its row, not its commits: they stay in the repository
/// behind their branch, and `worktree checkout` rebuilds the directory from the
/// row's commit. Whether a branch has landed is `git branch --merged`'s answer,
/// asked by the merge that lands it — not something to re-derive here.
///
/// What clean cannot see is *ignored* files: git reports nothing for a `target/`
/// and removes it without complaint, which is right for build output and is why
/// `--name` exists for the one worktree holding something a commit does not
/// describe.
///
/// A dry run by default: it prints what it would forget and changes nothing. The
/// directory is removed through `git worktree remove`, so git's own refusals — the
/// main worktree, the one you are standing in, a `.env` it does not know about —
/// are the guard, rather than a copy of those rules kept here.
pub fn prune(vfs: &mut Vfs, only: Option<&str>, apply: bool) -> Result<PruneReport> {
    let mut report = PruneReport {
        applied: apply,
        ..PruneReport::default()
    };
    for known in worktrees(vfs)? {
        if only.is_some_and(|name| name != known.name) {
            continue;
        }
        let mut entry = PruneEntry {
            name: known.name.clone(),
            path: known.path.clone(),
            state: "clean",
            removed: false,
            detail: None,
        };
        // A recording with an in-between is the one thing this must never forget:
        // the work exists only as ops, and dropping the row drops the pointer to it.
        if known.overrides > 0 || known.journal > 0 {
            entry.state = "recorded";
            report.entries.push(entry);
            continue;
        }
        // A live lease means a `record` or a `checkout` is running here: removing
        // the rows underneath it would turn its next commit into a failure.
        if let Some(owner) = lease_holder(vfs, &known.prefix)? {
            entry.state = "leased";
            entry.detail = Some(format!("{owner} holds {}", known.prefix));
            report.entries.push(entry);
            continue;
        }
        let missing = !known.path.is_dir();
        if !missing {
            match uncommitted(&known.path) {
                // Clean means the commit describes everything in the checkout, so
                // there is nothing here that a fresh materialize would not rebuild.
                Ok(false) => {}
                Ok(true) => {
                    entry.state = "dirty";
                    report.entries.push(entry);
                    continue;
                }
                Err(error) => {
                    entry.state = "unreadable";
                    entry.detail = Some(format!("{error:#}"));
                    report.entries.push(entry);
                    continue;
                }
            }
        } else {
            entry.state = "missing";
        }

        if apply {
            if !missing {
                let path = known.path.to_string_lossy().into_owned();
                match crate::project::git(&known.path, &["worktree", "remove", &path]) {
                    Ok(_) => {}
                    Err(error) => {
                        // Git refuses the main worktree, a dirty one, and the one
                        // the caller is standing in. Keeping the row on a refusal
                        // is the difference between "not yet" and "gone".
                        entry.state = "kept-by-git";
                        entry.detail = Some(format!("{error:#}"));
                        report.entries.push(entry);
                        continue;
                    }
                }
            }
            let tx = vfs.db.transaction()?;
            for statement in [
                "DELETE FROM worktree_overrides WHERE worktree=?1",
                "DELETE FROM worktree_journal WHERE worktree=?1",
                "DELETE FROM worktrees WHERE name=?1",
            ] {
                tx.execute(statement, rusqlite::params![known.name])?;
            }
            // A lease that has already lapsed is litter for a namespace that no
            // longer has a row; a live one was refused above.
            tx.execute(
                "DELETE FROM leases WHERE path=?1 AND expires_at_ms<=?2",
                rusqlite::params![known.prefix, now_ms()],
            )?;
            tx.commit()?;
            entry.removed = true;
        }
        report.entries.push(entry);
    }
    Ok(report)
}

/// Who holds the lease over `prefix` right now, if anyone.
fn lease_holder(vfs: &Vfs, prefix: &str) -> Result<Option<String>> {
    Ok(vfs
        .db
        .query_row(
            "SELECT owner FROM leases WHERE path=?1 AND expires_at_ms>?2",
            rusqlite::params![prefix, now_ms()],
            |row| row.get::<_, String>(0),
        )
        .optional()?)
}

/// Whether a checkout holds anything a commit does not describe.
fn uncommitted(path: &Path) -> Result<bool> {
    let listing = crate::project::git(
        path,
        &["status", "--porcelain=v1", "-z", "--untracked-files=all"],
    )?;
    Ok(!listing.trim_matches('\0').trim().is_empty())
}

/// Record what changed in a worktree's directory since its recorded base.
///
/// Only paths Git reports as differing from the base commit are read. Everything
/// else is still the commit's tree and costs nothing to leave alone.
pub fn record(vfs: &mut Vfs, name: &str, options: &RecordOptions) -> Result<RecordReport> {
    let known = state(vfs, name)?
        .ok_or_else(|| anyhow!("no worktree named {name}; materialize it first"))?;
    let root = PathBuf::from(&known.path);
    if !root.is_dir() {
        bail!("{} is not a directory", root.display());
    }

    // The lease is the worktree's namespace address: it says which worktree is
    // being recorded, in the same vocabulary as everything else that names one.
    vfs.lease(&known.prefix, &options.owner, options.lease_ms)?;

    // Every op this run writes is attributed to this worktree, so the change
    // feed can answer for it. Restored afterwards, because a caller may reuse the
    // store for something else and a scope that outlives its work is a quiet lie.
    let previous = vfs.worktree().to_string();
    vfs.set_worktree(&known.name);
    let outcome = record_leased(vfs, &known, options);
    vfs.set_worktree(previous);
    let released = vfs.release(&known.prefix, &options.owner);

    let report = outcome?;
    released?;
    Ok(report)
}

fn record_leased(
    vfs: &mut Vfs,
    known: &WorktreeState,
    options: &RecordOptions,
) -> Result<RecordReport> {
    let mut report = RecordReport {
        worktree: known.name.clone(),
        path: known.path.clone(),
        prefix: known.prefix.clone(),
        base_commit: known.base_commit.clone(),
        ..RecordReport::default()
    };

    // A plan already on disk is the authority: recomputing it would read the
    // working tree a second time and, worse, take the partially-applied
    // namespace as the new base — which is how a resumed run silently forgets the
    // edits it had already recorded.
    let plan = load_journal(vfs, &known.name)?;
    let rows = if plan.is_empty() {
        plan_record(vfs, known, &mut report)?
    } else {
        report.resumed = true;
        plan
    };
    report.planned = rows.iter().filter(|row| row.apply).count();

    if !rows.is_empty() {
        save_journal(vfs, &known.name, &rows)?;
    }
    apply_journal(vfs, known, &rows, options, &mut report)?;
    report.base_advanced_to = advance(vfs, known, &rows)?;
    Ok(report)
}

/// One op, as written down before it is applied.
#[derive(Clone, Debug)]
struct JournalRow {
    path: String,
    op: Planned,
    /// Whether this row also writes the namespace. Claiming an override the
    /// namespace already holds is bookkeeping; removing a trace of one is not.
    apply: bool,
    content_id: Option<ContentId>,
    media_type: String,
    size: u64,
    /// The head this op replaces, so a replay is fenced instead of clobbering a
    /// write that arrived in between.
    base_content_id: Option<ContentId>,
}

/// What a record decided to do about one path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Planned {
    /// The worktree holds content the commit does not.
    Upsert,
    /// The worktree lacks a file the commit has.
    Remove,
    /// The path is back to what the commit says, so the recording lets go of it.
    Forget,
}

/// Decide the ops a record has to make, and file their content.
///
/// The candidate set is "paths Git reports as dirty" plus "paths the recording
/// already holds", and nothing else. That is what makes the cost the diff: a
/// clean file is never opened, so a large checkout costs the same as its edits.
fn plan_record(
    vfs: &Vfs,
    known: &WorktreeState,
    report: &mut RecordReport,
) -> Result<Vec<JournalRow>> {
    let root = Path::new(&known.path);
    let policy = PathPolicy::parse(attach_policy());
    let dirty = dirty_paths(root)?;
    let overlay = overrides(vfs, &known.name)?;

    let mut targets: BTreeMap<String, Planned> = BTreeMap::new();
    for rel in &dirty {
        if policy.classify(rel) == PathClass::Exclude {
            report.skipped += 1;
            continue;
        }
        let absolute = root.join(rel);
        match fs::symlink_metadata(&absolute) {
            Ok(meta) if meta.file_type().is_symlink() || meta.is_dir() => {
                // Neither a symlink nor a submodule is content this store can
                // hold; following either would import bytes from outside the tree.
                report.skipped += 1;
            }
            Ok(_) => {
                targets.insert(rel.clone(), Planned::Upsert);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                targets.insert(rel.clone(), Planned::Remove);
            }
            Err(error) => {
                return Err(error).with_context(|| format!("inspecting {}", absolute.display()));
            }
        }
    }

    // A path the recording holds but Git no longer reports is a path that went
    // back to what the commit says — a revert, or a file deleted and re-added.
    // The recording has to let go, or a later materialize would restore content
    // the worktree has deliberately dropped.
    for rel in overlay.keys() {
        if !dirty.contains(rel) {
            targets.insert(rel.clone(), Planned::Forget);
        }
    }

    let mut rows = Vec::with_capacity(targets.len());
    for (rel, planned) in targets {
        let absolute = root.join(&rel);
        let namespace = format!("{}/{}", known.prefix, rel);
        let current = vfs.file(&namespace)?;
        let base = current.as_ref().map(|entry| entry.content_id.clone());
        let prior = overlay.get(&rel);
        match planned {
            Planned::Upsert => {
                // Read once: the hash, the blob, and the row all come from these
                // bytes, so a file is opened because it changed and for no other
                // reason.
                let bytes = match fs::read(&absolute) {
                    Ok(bytes) => bytes,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        report.skipped += 1;
                        continue;
                    }
                    Err(error) => {
                        return Err(error)
                            .with_context(|| format!("reading {}", absolute.display()));
                    }
                };
                report.hashed_bytes += bytes.len() as u64;
                let content_id = ContentId::from_bytes(&bytes);
                let matches = base.as_ref() == Some(&content_id);
                let recorded = matches
                    && prior.is_some_and(|entry| {
                        entry.op == Planned::Upsert
                            && entry.content_id.as_ref() == Some(&content_id)
                    });
                if matches {
                    report.unchanged += 1;
                }
                if recorded {
                    continue;
                }
                if !matches {
                    if !vfs.blobs.has(&content_id) {
                        report.written_bytes += bytes.len() as u64;
                    }
                    vfs.blobs.put(&bytes)?;
                }
                rows.push(JournalRow {
                    path: rel.clone(),
                    op: Planned::Upsert,
                    apply: !matches,
                    content_id: Some(content_id),
                    media_type: media_type_for(&rel).to_string(),
                    size: bytes.len() as u64,
                    base_content_id: base,
                });
            }
            Planned::Remove => {
                // Already written down as absent: Git keeps reporting a deletion
                // until it is committed, and re-recording it every run would make
                // the feed say the same thing forever.
                if current.is_none() && prior.is_some_and(|entry| entry.op == Planned::Remove) {
                    report.unchanged += 1;
                    continue;
                }
                rows.push(JournalRow {
                    path: rel.clone(),
                    op: Planned::Remove,
                    apply: true,
                    content_id: None,
                    media_type: media_type_for(&rel).to_string(),
                    size: 0,
                    base_content_id: base,
                });
            }
            Planned::Forget => {
                rows.push(JournalRow {
                    path: rel.clone(),
                    op: Planned::Forget,
                    // A revert that the namespace never recorded is nothing but a
                    // row to delete; one it did record is a change to report.
                    apply: base.is_some(),
                    content_id: None,
                    media_type: media_type_for(&rel).to_string(),
                    size: 0,
                    base_content_id: base,
                });
            }
        }
    }
    Ok(rows)
}

/// Apply the plan, one op per transaction.
///
/// An op that already landed is recognised from the namespace rather than from a
/// flag: the namespace is the thing the op was supposed to change, so it is the
/// only witness that cannot be out of date about it.
fn apply_journal(
    vfs: &mut Vfs,
    known: &WorktreeState,
    rows: &[JournalRow],
    options: &RecordOptions,
    report: &mut RecordReport,
) -> Result<()> {
    for (index, row) in rows.iter().enumerate() {
        if index > 0 && index % RENEW_EVERY == 0 {
            vfs.lease(&known.prefix, &options.owner, options.lease_ms)?;
        }
        if !row.apply {
            continue;
        }
        let namespace = format!("{}/{}", known.prefix, row.path);
        let current = vfs.file(&namespace)?;
        match row.op {
            Planned::Upsert => {
                let content_id = row
                    .content_id
                    .clone()
                    .ok_or_else(|| anyhow!("journal row for {} has no content", row.path))?;
                if current
                    .as_ref()
                    .is_some_and(|entry| entry.content_id == content_id)
                {
                    report.replayed += 1;
                    report.applied += 1;
                    continue;
                }
                let expect = match &row.base_content_id {
                    Some(base) => Expect::Base(base),
                    // A path with no head must still not exist; anything else there
                    // arrived after the plan was made, and overwriting it would
                    // discard a write nobody asked us to reconcile.
                    None => Expect::Version(0),
                };
                let bytes = vfs.content(&content_id)?;
                vfs.put(&namespace, &bytes, &row.media_type, expect)?;
                report.upserted += 1;
            }
            _ => {
                if current.is_some() {
                    vfs.remove(&namespace)?;
                } else {
                    // Git's tree covered this path, so the namespace never held it.
                    // The removal is still a change, and the feed is what a reader
                    // of this worktree is watching.
                    vfs.tombstone(&namespace)?;
                }
                report.removed += 1;
            }
        }
        report.applied += 1;
    }
    Ok(())
}

/// Move the recorded base onto the ops that just landed, and clear the plan.
///
/// One transaction for all three facts: the overlay, the base, and the journal.
/// Anything earlier and an op would survive as a duplicate; anything later and a
/// crash would leave the plan replaying against a base that already moved.
fn advance(vfs: &mut Vfs, known: &WorktreeState, rows: &[JournalRow]) -> Result<Option<String>> {
    let root = Path::new(&known.path);
    let mut base_commit = known.base_commit.clone();
    let mut base_tree = known.base_tree.clone();
    // Committing the work moves the base without changing a single file. Saying
    // so here is what keeps a handoff honest: the next materialize checks out the
    // commit the work is actually on, not the one it started from.
    if let Ok(head) = git(root, &["rev-parse", "HEAD"])
        && head.trim() != base_commit
        && let Ok(tree) = git(root, &["rev-parse", &format!("{}^{{tree}}", head.trim())])
    {
        base_commit = head.trim().to_string();
        base_tree = tree.trim().to_string();
    }

    let now = now_ms();
    let tx = vfs.db.transaction()?;
    for row in rows {
        tx.execute(
            "DELETE FROM worktree_overrides WHERE worktree=?1 AND path=?2",
            rusqlite::params![known.name, row.path],
        )?;
        if row.op == Planned::Forget {
            // Back to what the commit says: the base now covers it, so the
            // recording has nothing left to say about this path.
            continue;
        }
        tx.execute(
            "INSERT INTO worktree_overrides(worktree,path,op,content_id,media_type,size)
             VALUES(?1,?2,?3,?4,?5,?6)",
            rusqlite::params![
                known.name,
                row.path,
                match row.op {
                    Planned::Upsert => "upsert",
                    _ => "remove",
                },
                row.content_id.as_ref().map(ToString::to_string),
                row.media_type,
                row.size as i64
            ],
        )?;
    }
    tx.execute(
        "DELETE FROM worktree_journal WHERE worktree=?1",
        rusqlite::params![known.name],
    )?;
    tx.execute(
        "UPDATE worktrees SET base_commit=?1, base_tree=?2, updated_at_ms=?3 WHERE name=?4",
        rusqlite::params![base_commit, base_tree, now, known.name],
    )?;
    tx.commit()?;
    Ok((base_commit != known.base_commit).then_some(base_commit))
}

/// What changed inside a worktree after `since`, oldest first.
///
/// `since` is the `cursor` a previous page returned, and `0` starts from the
/// beginning. The page is bounded so a harness polls a fixed amount no matter
/// how much happened while it was away.
pub fn changes(vfs: &Vfs, name: &str, since: i64, limit: usize) -> Result<ChangePage> {
    let known = state(vfs, name)?
        .ok_or_else(|| anyhow!("no worktree named {name}; materialize it first"))?;
    let entries = vfs.changes(since, &known.prefix, &known.name, limit)?;
    let cursor = entries.last().map_or(since, |entry| entry.id);
    let prefix = format!("{}/", known.prefix);
    let changes = entries
        .into_iter()
        .map(|entry| WorktreeChange {
            cursor: entry.id,
            path: entry
                .path
                .strip_prefix(&prefix)
                .unwrap_or(&entry.path)
                .to_string(),
            op: entry.op,
            content_id: entry.content_id,
            author: entry.actor,
            origin: entry.origin,
            at_ms: entry.at_ms,
        })
        .collect();
    Ok(ChangePage {
        worktree: known.name,
        prefix: known.prefix,
        cursor,
        changes,
    })
}

/// The override the recording holds for a path: content the base commit does not
/// carry, or the absence of a file it does.
struct Override {
    op: Planned,
    content_id: Option<ContentId>,
    media_type: String,
    size: u64,
}

fn overrides(vfs: &Vfs, name: &str) -> Result<BTreeMap<String, Override>> {
    let mut map = BTreeMap::new();
    let mut statement = vfs.db.prepare(
        "SELECT path,op,content_id,media_type,size FROM worktree_overrides
         WHERE worktree=?1 ORDER BY path",
    )?;
    let rows = statement.query_map(rusqlite::params![name], |row| {
        let op: String = row.get(1)?;
        Ok((
            row.get::<_, String>(0)?,
            op,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, i64>(4)? as u64,
        ))
    })?;
    for row in rows {
        let (path, op, content_id, media_type, size) = row?;
        let op = match op.as_str() {
            "upsert" => Planned::Upsert,
            "remove" => Planned::Remove,
            other => bail!("unknown override op {other:?} for {path}"),
        };
        let content_id = match content_id {
            Some(value) => Some(ContentId::parse(&value)?),
            None => None,
        };
        map.insert(
            path,
            Override {
                op,
                content_id,
                media_type,
                size,
            },
        );
    }
    Ok(map)
}

fn load_journal(vfs: &Vfs, name: &str) -> Result<Vec<JournalRow>> {
    let mut statement = vfs.db.prepare(
        "SELECT path,op,apply,content_id,media_type,size,base_content_id
         FROM worktree_journal WHERE worktree=?1 ORDER BY path",
    )?;
    let rows = statement.query_map(rusqlite::params![name], |row| {
        let op: String = row.get(1)?;
        Ok(JournalRow {
            path: row.get(0)?,
            op: if op == "upsert" {
                Planned::Upsert
            } else if op == "forget" {
                Planned::Forget
            } else {
                Planned::Remove
            },
            apply: row.get::<_, i64>(2)? != 0,
            content_id: row.get::<_, Option<String>>(3)?.map(ContentId),
            media_type: row.get(4)?,
            size: row.get::<_, i64>(5)? as u64,
            base_content_id: row.get::<_, Option<String>>(6)?.map(ContentId),
        })
    })?;
    Ok(rows.collect::<Result<Vec<_>, _>>()?)
}

fn save_journal(vfs: &Vfs, name: &str, rows: &[JournalRow]) -> Result<()> {
    let tx = vfs.db.unchecked_transaction()?;
    tx.execute(
        "DELETE FROM worktree_journal WHERE worktree=?1",
        rusqlite::params![name],
    )?;
    for row in rows {
        tx.execute(
            "INSERT INTO worktree_journal
               (worktree,path,op,apply,content_id,media_type,size,base_content_id)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            rusqlite::params![
                name,
                row.path,
                match row.op {
                    Planned::Upsert => "upsert",
                    Planned::Remove => "remove",
                    Planned::Forget => "forget",
                },
                i64::from(row.apply),
                row.content_id.as_ref().map(ToString::to_string),
                row.media_type,
                row.size as i64,
                row.base_content_id.as_ref().map(ToString::to_string),
            ],
        )?;
    }
    tx.commit()?;
    Ok(())
}

/// Paths Git reports as differing from the base commit, plus untracked files it
/// does not ignore.
///
/// This is where the cheapness comes from: Git has already compared the working
/// tree against the commit, and it did it by stat first and hash second. Re-doing
/// that comparison ourselves means reading every file in the tree.
fn dirty_paths(root: &Path) -> Result<BTreeSet<String>> {
    let listing = git(
        root,
        &[
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=all",
            "--no-renames",
        ],
    )?;
    let mut paths = BTreeSet::new();
    for entry in listing.split('\0') {
        // "XY path": two status bytes, one separator, then the path. With `-z`
        // the path is raw — unquoted, and never containing a NUL.
        if entry.len() < 4 {
            continue;
        }
        paths.insert(entry[3..].to_string());
    }
    Ok(paths)
}

/// Stream one content id onto disk, atomically and — where the filesystem can —
/// without copying the bytes.
///
/// Returns whether the file was placed by a copy-on-write clone. The write goes to
/// a temporary sibling and is renamed into place, so an interrupted record leaves
/// a stray temporary file rather than a truncated one where an editor is looking.
fn place(vfs: &Vfs, id: &ContentId, target: &Path) -> Result<bool> {
    let parent = target
        .parent()
        .ok_or_else(|| anyhow!("{} has no parent directory", target.display()))?;
    fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    let temporary = temporary_sibling(target);

    let source = vfs.blobs.path_for(id);
    let cloned = if source.is_file() {
        if clone_file(&source, &temporary) {
            true
        } else {
            fs::copy(&source, &temporary)
                .with_context(|| format!("copying {}", source.display()))?;
            false
        }
    } else {
        // The root keeps an older per-root object directory, or the object arrived
        // through it; `content` is the one place that knows how to read either.
        let bytes = vfs.content(id)?;
        fs::write(&temporary, &bytes)?;
        false
    };

    match fs::rename(&temporary, target) {
        Ok(()) => Ok(cloned),
        Err(error) => {
            let _ = fs::remove_file(&temporary);
            Err(error).with_context(|| format!("placing {}", target.display()))
        }
    }
}

/// A sibling of `target` to write first and rename into place.
fn temporary_sibling(target: &Path) -> PathBuf {
    static NONCE: AtomicU64 = AtomicU64::new(0);
    let nonce = NONCE.fetch_add(1, Ordering::Relaxed);
    let name = target
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    target.with_file_name(format!(".{name}.fungos-{}-{nonce}", std::process::id()))
}

#[cfg(target_os = "macos")]
unsafe extern "C" {
    /// `clonefile(2)`: a copy-on-write clone within one APFS volume.
    fn clonefile(
        source: *const libc::c_char,
        target: *const libc::c_char,
        flags: libc::c_int,
    ) -> libc::c_int;
}

/// Clone `source` to `target`, or report that this filesystem would not.
///
/// A reflink is what makes materializing an already-recorded worktree nearly
/// free: the bytes are not moved, only a new name for them. It fails on a
/// different volume and on filesystems with no such thing, which is a reason to
/// copy rather than an error to report.
#[cfg(target_os = "macos")]
fn clone_file(source: &Path, target: &Path) -> bool {
    use std::ffi::CString;

    let (Ok(source), Ok(target)) = (
        CString::new(source.as_os_str().as_encoded_bytes()),
        CString::new(target.as_os_str().as_encoded_bytes()),
    ) else {
        return false;
    };
    // SAFETY: both pointers are NUL-terminated C strings that outlive the call,
    // and `clonefile` only reads them.
    unsafe { clonefile(source.as_ptr(), target.as_ptr(), 0) == 0 }
}

#[cfg(not(target_os = "macos"))]
fn clone_file(_source: &Path, _target: &Path) -> bool {
    // Linux has reflinks through ioctl(FICLONE), on the filesystems that support
    // them. A plain copy is correct everywhere, so this waits for a caller that
    // measures the difference rather than guessing at one.
    false
}

fn now_ms() -> i64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeRole;
    use std::process::Command;

    /// A directory of this test's own, holding the repository, the VFS root, and
    /// the content store.
    fn scratch(tag: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("fungos-vfs-worktree-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn open_vfs(base: &Path) -> Vfs {
        Vfs::open_with(base.join("vfs"), NodeRole::Full, Some(&base.join("cas"))).unwrap()
    }

    /// A root of its own on the shared content store: two machines, one machine.
    fn open_root(base: &Path, tag: &str) -> Vfs {
        Vfs::open_with(
            base.join(tag).join("vfs"),
            NodeRole::Full,
            Some(&base.join("cas")),
        )
        .unwrap()
    }

    fn write(path: &Path, body: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, body).unwrap();
    }

    fn run(dir: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    fn commit_all(dir: &Path, message: &str) {
        run(dir, &["add", "-A"]);
        run(
            dir,
            &[
                "-c",
                "user.email=vfs@example.test",
                "-c",
                "user.name=vfs",
                "commit",
                "-qm",
                message,
            ],
        );
    }

    /// A repository with `files` files, each named and sized so a test can say
    /// exactly which ones changed.
    fn repository(base: &Path, files: usize) -> (PathBuf, String) {
        let repo = base.join("repo");
        fs::create_dir_all(&repo).unwrap();
        run(&repo, &["init", "-q"]);
        for index in 0..files {
            write(
                &repo.join(format!("src/file{index:03}.txt")),
                &body(index, ""),
            );
        }
        commit_all(&repo, "init");
        let head = run(&repo, &["rev-parse", "HEAD"]).trim().to_string();
        (repo, head)
    }

    fn body(index: usize, suffix: &str) -> String {
        format!("contents of file {index}{suffix}\n")
    }

    #[test]
    fn an_existing_checkout_is_adopted_rather_than_refused() {
        let base = scratch("adopt");
        let repo = base.join("repo");
        fs::create_dir_all(&repo).unwrap();
        run(&repo, &["init", "-q"]);
        for index in 0..4 {
            write(&repo.join(format!("src/f{index}.txt")), &body(index, ""));
        }
        commit_all(&repo, "init");
        // Already somebody's working directory, and already dirty: the recording
        // arrives after the work, which is the normal order.
        write(&repo.join("src/f1.txt"), &body(1, " (edited)"));
        let mut vfs = open_vfs(&base);

        let report = materialize(&mut vfs, &repo, "main", &repo, None, "projects").unwrap();
        assert!(report.adopted, "nothing in the directory was written");
        assert_eq!(report.placed, 0);

        let recorded = record(&mut vfs, "main", &RecordOptions::new("agent")).unwrap();
        assert_eq!(recorded.planned, 1, "{recorded:#?}");
        assert_eq!(overrides(&vfs, "main").unwrap().len(), 1);

        // A directory that is not a worktree of this repository is refused, not
        // silently adopted.
        let stranger = base.join("stranger");
        fs::create_dir_all(&stranger).unwrap();
        write(&stranger.join("x.txt"), "x\n");
        let error = materialize(&mut vfs, &repo, "other", &stranger, None, "projects").unwrap_err();
        assert!(
            format!("{error}").contains("is not a worktree of"),
            "{error}"
        );
    }

    #[test]
    fn a_recording_is_written_into_the_namespace_as_the_in_between() {
        let base = scratch("project");
        let (repo, _) = repository(&base, 8);
        let mut vfs = open_vfs(&base);
        let dest = base.join("work/main");
        materialize(&mut vfs, &repo, "main", &dest, None, "projects").unwrap();

        for index in [2usize, 6] {
            write(
                &dest.join(format!("src/file{index:03}.txt")),
                &body(index, " (edited)"),
            );
        }
        record(&mut vfs, "main", &RecordOptions::new("agent")).unwrap();

        // Something else had filled this prefix — an earlier attach, or a peer.
        vfs.put(
            "projects/repo/worktrees/main/stale.txt",
            b"gone\n",
            "text/plain",
            Expect::Any,
        )
        .unwrap();

        let known = state(&vfs, "main").unwrap().unwrap();
        assert_eq!(overrides(&vfs, "main").unwrap().len(), 2);
        let report = project(&mut vfs, &known, &known.prefix, false).unwrap();
        // `record` already wrote the in-between into the namespace; what is left
        // for a projection is reconciling, and it found the two files correct and
        // one path the recording does not hold.
        assert_eq!(report.unchanged, 2);
        assert_eq!(report.removed, 1, "{report:#?}");
        let listed = vfs.list(&known.prefix).unwrap();
        assert_eq!(listed.len(), 2);
        assert!(
            listed
                .iter()
                .all(|entry| !entry.path.ends_with("stale.txt"))
        );

        // Projecting again changes nothing: it is a projection, not a merge.
        let again = project(&mut vfs, &known, &known.prefix, false).unwrap();
        assert_eq!(again.added, 0);
        assert_eq!(again.changed, 0);
        assert_eq!(again.removed, 0);
        assert_eq!(again.unchanged, 2);
    }

    #[test]
    fn prune_forgets_a_clean_worktree_and_says_nothing_was_lost() {
        let base = scratch("prune-clean");
        let (repo, _) = repository(&base, 4);
        let mut vfs = open_vfs(&base);
        let dest = base.join("work/main");
        materialize(&mut vfs, &repo, "main", &dest, None, "projects").unwrap();

        // Nothing recorded and nothing on disk beyond the commit: the row is
        // bookkeeping and the checkout is reproducible from `base_commit`.
        let dry = prune(&mut vfs, None, false).unwrap();
        assert_eq!(dry.entries.len(), 1);
        assert_eq!(dry.entries[0].state, "clean");
        assert!(!dry.entries[0].removed);
        assert!(dry.entries[0].path.exists(), "a dry run changes nothing");
        assert_eq!(worktrees(&vfs).unwrap().len(), 1);

        let applied = prune(&mut vfs, None, true).unwrap();
        assert!(applied.entries[0].removed, "{applied:#?}");
        assert!(worktrees(&vfs).unwrap().is_empty(), "the row is gone");
        assert!(!dest.exists(), "and so is the checkout");
    }

    #[test]
    fn prune_refuses_a_worktree_something_is_working_in() {
        let base = scratch("prune-leased");
        let (repo, _) = repository(&base, 4);
        let mut vfs = open_vfs(&base);
        let dest = base.join("work/main");
        materialize(&mut vfs, &repo, "main", &dest, None, "projects").unwrap();
        // A record is running: it will write ops against these rows next.
        let prefix = worktrees(&vfs).unwrap()[0].prefix.clone();
        vfs.lease(&prefix, "builder-7", 60_000).unwrap();

        let report = prune(&mut vfs, None, true).unwrap();
        assert_eq!(report.entries[0].state, "leased");
        assert!(!report.entries[0].removed);
        assert_eq!(
            report.entries[0].detail.as_deref(),
            Some(format!("builder-7 holds {prefix}").as_str()),
            "the refusal names who to wait for"
        );
        assert_eq!(worktrees(&vfs).unwrap().len(), 1);
        assert!(dest.exists());
    }

    #[test]
    fn prune_keeps_a_worktree_whose_recording_holds_work() {
        let base = scratch("prune-recorded");
        let (repo, _) = repository(&base, 4);
        let mut vfs = open_vfs(&base);
        let dest = base.join("work/main");
        materialize(&mut vfs, &repo, "main", &dest, None, "projects").unwrap();
        write(&dest.join("src/file001.txt"), &body(1, " (edited)"));
        record(&mut vfs, "main", &RecordOptions::new("agent")).unwrap();

        // The work exists only as ops. Dropping the row would drop the only
        // pointer to it, so the in-between outranks a clean git status.
        let report = prune(&mut vfs, None, true).unwrap();
        assert_eq!(report.entries[0].state, "recorded");
        assert!(!report.entries[0].removed);
        assert_eq!(worktrees(&vfs).unwrap().len(), 1);
        assert!(dest.exists());
    }

    #[test]
    fn prune_keeps_work_the_recording_has_not_seen_yet() {
        let base = scratch("prune-dirty");
        let (repo, _) = repository(&base, 4);
        let mut vfs = open_vfs(&base);
        let dest = base.join("work/main");
        materialize(&mut vfs, &repo, "main", &dest, None, "projects").unwrap();
        // Edited, never recorded: the disk holds work no commit and no op knows.
        write(&dest.join("src/file002.txt"), &body(2, " (unsaved)"));

        let report = prune(&mut vfs, None, true).unwrap();
        assert_eq!(report.entries[0].state, "dirty");
        assert!(!report.entries[0].removed);
        assert!(dest.exists());
        assert_eq!(
            fs::read(dest.join("src/file002.txt")).unwrap(),
            body(2, " (unsaved)").into_bytes()
        );
    }

    #[test]
    fn prune_cannot_remove_the_projects_main_worktree() {
        let base = scratch("prune-main");
        let (repo, _) = repository(&base, 3);
        let mut vfs = open_vfs(&base);
        materialize(&mut vfs, &repo, "main", &repo, None, "projects").unwrap();

        // Clean, so nothing to lose — but it is the repository itself, and git is
        // the authority on that, not a rule copied here.
        let report = prune(&mut vfs, None, true).unwrap();
        assert_eq!(report.entries[0].state, "kept-by-git", "{report:#?}");
        assert!(!report.entries[0].removed);
        assert!(repo.join("src/file000.txt").exists());
        assert_eq!(worktrees(&vfs).unwrap().len(), 1, "the row stays with it");
    }

    #[test]
    fn a_second_store_lays_the_recording_over_its_own_checkout() {
        let base = scratch("handoff");
        let (repo, _) = repository(&base, 10);
        let mut a = open_root(&base, "a");
        let mut b = open_root(&base, "b");
        b.set_actor("agent-b");

        // A has the work: three edits, recorded, and the project attached so the
        // address travels with the ops.
        for index in [2usize, 5, 8] {
            write(
                &repo.join(format!("src/file{index:03}.txt")),
                &body(index, " (edited)"),
            );
        }
        materialize(&mut a, &repo, "main", &repo, None, "projects").unwrap();
        assert_eq!(
            record(&mut a, "main", &RecordOptions::new("agent-a"))
                .unwrap()
                .upserted,
            3
        );
        crate::attach_project(&mut a, &repo, None, "projects", false).unwrap();

        // B learns everything A knows, over the sync protocol.
        let request = crate::SyncRequest::new(b.sync_cursor().unwrap(), 500);
        let response = a.serve_sync(&request).unwrap();
        assert_eq!(b.apply_sync(&response).unwrap().len(), response.ops.len());

        // B has its own pristine checkout of the same base, like a fresh clone on
        // another machine, and lays the recording over it.
        let clone = base.join("b-checkout");
        assert!(
            std::process::Command::new("git")
                .args(["clone", "-q"])
                .arg(&repo)
                .arg(&clone)
                .status()
                .unwrap()
                .success()
        );
        let report = checkout(
            &mut b,
            &CheckoutOptions {
                name: "main".to_string(),
                path: clone.clone(),
                project: Some("repo".to_string()),
                prefix: "projects".to_string(),
                commit: None,
                force: false,
                owner: "agent-b:1".to_string(),
                lease_ms: DEFAULT_LEASE_MS,
            },
        )
        .unwrap();
        assert_eq!(report.replaced, 3, "{report:#?}");
        assert_eq!(report.written, 0);
        assert_eq!(report.base_commit, report.base_commit.trim());
        assert_eq!(
            fs::read(clone.join("src/file005.txt")).unwrap(),
            body(5, " (edited)").into_bytes()
        );
        assert_eq!(
            fs::read(clone.join("src/file000.txt")).unwrap(),
            body(0, "").into_bytes(),
            "the other seven are still the commit's"
        );

        // B is a peer now, not a reader: recording here writes no ops it should
        // not, and the next change reaches A with B's name on it.
        let adopt = record(&mut b, "main", &RecordOptions::new("agent-b")).unwrap();
        assert_eq!(adopt.planned, 0, "{adopt:#?}");
        write(&clone.join("src/file003.txt"), &body(3, " (from b)"));
        assert_eq!(
            record(&mut b, "main", &RecordOptions::new("agent-b"))
                .unwrap()
                .upserted,
            1
        );

        let request = crate::SyncRequest::new(a.sync_cursor().unwrap(), 500);
        let response = b.serve_sync(&request).unwrap();
        a.apply_sync(&response).unwrap();
        let page = changes(&a, "main", 0, 100).unwrap();
        let last = page.changes.last().expect("B's edit reached A");
        assert_eq!(last.path, "src/file003.txt");
        assert_eq!(last.author, "agent-b");
        assert_eq!(
            a.get(&format!("{}/src/file003.txt", page.prefix)).unwrap(),
            body(3, " (from b)").into_bytes()
        );
    }

    #[test]
    fn a_checkout_refuses_to_lay_a_recording_over_someone_elses_edit() {
        let base = scratch("checkout-conflict");
        let (repo, _) = repository(&base, 4);
        let mut a = open_root(&base, "a");
        let mut b = open_root(&base, "b");

        write(&repo.join("src/file001.txt"), &body(1, " (edited)"));
        materialize(&mut a, &repo, "main", &repo, None, "projects").unwrap();
        record(&mut a, "main", &RecordOptions::new("agent-a")).unwrap();
        crate::attach_project(&mut a, &repo, None, "projects", false).unwrap();
        let request = crate::SyncRequest::new(b.sync_cursor().unwrap(), 500);
        let response = a.serve_sync(&request).unwrap();
        b.apply_sync(&response).unwrap();

        let clone = base.join("b-checkout");
        assert!(
            std::process::Command::new("git")
                .args(["clone", "-q"])
                .arg(&repo)
                .arg(&clone)
                .status()
                .unwrap()
                .success()
        );
        // Somebody else's uncommitted work, on the path the recording would write.
        write(
            &clone.join("src/file001.txt"),
            "neither the base nor the recording\n",
        );

        let error = checkout(
            &mut b,
            &CheckoutOptions {
                name: "main".to_string(),
                path: clone.clone(),
                project: Some("repo".to_string()),
                prefix: "projects".to_string(),
                commit: None,
                force: false,
                owner: "agent-b:1".to_string(),
                lease_ms: DEFAULT_LEASE_MS,
            },
        )
        .unwrap_err();
        let message = format!("{error}");
        assert!(message.contains("src/file001.txt"), "{message}");
        assert!(
            fs::read_to_string(clone.join("src/file001.txt"))
                .unwrap()
                .contains("neither the base"),
            "a refused checkout writes nothing"
        );
    }

    #[test]
    fn materialize_checks_out_the_base_commit_and_records_where_it_is() {
        let base = scratch("materialize");
        let (repo, head) = repository(&base, 4);
        let mut vfs = open_vfs(&base);
        let dest = base.join("work/main");

        let report = materialize(&mut vfs, &repo, "main", &dest, None, "projects").unwrap();
        assert_eq!(report.commit, head);
        assert_eq!(report.placed, 0, "a clean checkout has no in-between",);
        assert_eq!(
            fs::read(dest.join("src/file000.txt")).unwrap(),
            body(0, "").into_bytes()
        );

        let recorded = worktrees(&vfs).unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].name, "main");
        assert_eq!(recorded[0].prefix, "projects/repo/worktrees/main");
        assert_eq!(recorded[0].base_commit, head);
        assert_eq!(recorded[0].base_tree.len(), 40);

        // Running it again is a no-op rather than a second checkout.
        let again = materialize(&mut vfs, &repo, "main", &dest, None, "projects").unwrap();
        assert!(again.reused);
    }

    #[test]
    fn three_edits_in_a_wide_checkout_cost_three_ops_and_nothing_else() {
        let base = scratch("three-edits");
        let (repo, _head) = repository(&base, 120);
        let mut vfs = open_vfs(&base);
        let dest = base.join("work/main");
        let materialized = materialize(&mut vfs, &repo, "main", &dest, None, "projects").unwrap();

        let edits = [7usize, 60, 119];
        let mut changed = 0u64;
        for index in edits {
            let body = body(index, " (edited)");
            changed += body.len() as u64;
            write(&dest.join(format!("src/file{index:03}.txt")), &body);
        }

        let report = record(&mut vfs, "main", &RecordOptions::new("agent")).unwrap();
        assert_eq!(report.planned, 3, "{report:#?}");
        assert_eq!(report.upserted, 3);
        assert_eq!(report.removed, 0);
        // The proof that the diff, and not the tree, was what got read.
        assert_eq!(report.hashed_bytes, changed);
        assert_eq!(report.written_bytes, changed);
        assert_eq!(report.base_advanced_to, None, "nothing was committed");

        // 120 files in the checkout, 3 in the namespace.
        let namespace = vfs.list(&materialized.prefix).unwrap();
        assert_eq!(namespace.len(), 3);
        assert_eq!(
            vfs.get(&format!("{}/src/file007.txt", materialized.prefix))
                .unwrap(),
            body(7, " (edited)").into_bytes()
        );

        // And again, with nothing changed: no ops, no bytes written. The three
        // edited files are hashed again — Git says they differ from the commit,
        // and the only way to know whether they differ from what was last recorded
        // is to look — but nothing else is opened and nothing is stored.
        let quiet = record(&mut vfs, "main", &RecordOptions::new("agent")).unwrap();
        assert_eq!(quiet.planned, 0, "{quiet:#?}");
        assert_eq!(quiet.upserted, 0);
        assert_eq!(quiet.written_bytes, 0);
        assert_eq!(quiet.hashed_bytes, changed);
    }

    #[test]
    fn an_interrupted_record_rolls_forward_without_duplicating_an_op() {
        let base = scratch("interrupted");
        let (repo, _) = repository(&base, 12);
        let mut vfs = open_vfs(&base);
        let dest = base.join("work/main");
        let materialized = materialize(&mut vfs, &repo, "main", &dest, None, "projects").unwrap();

        for index in [1usize, 5, 9] {
            write(
                &dest.join(format!("src/file{index:03}.txt")),
                &body(index, " (edited)"),
            );
        }

        // Stand in for a killed run: the plan is on disk and one op landed.
        let known = state(&vfs, "main").unwrap().unwrap();
        let mut report = RecordReport::default();
        let rows = plan_record(&vfs, &known, &mut report).unwrap();
        assert_eq!(rows.len(), 3);
        save_journal(&vfs, "main", &rows).unwrap();
        let mut partial = RecordReport::default();
        let options = RecordOptions::new("agent");
        apply_journal(&mut vfs, &known, &rows[..1], &options, &mut partial).unwrap();
        assert_eq!(partial.upserted, 1);

        // The next run finishes the plan rather than starting a new one.
        let resumed = record(&mut vfs, "main", &options).unwrap();
        assert!(resumed.resumed);
        assert_eq!(resumed.planned, 3);
        assert_eq!(resumed.replayed, 1, "the landed op is not repeated");
        assert_eq!(resumed.upserted, 2);
        assert_eq!(resumed.written_bytes, 0, "the plan already filed the bytes");

        for index in [1usize, 5, 9] {
            let path = format!("{}/src/file{index:03}.txt", materialized.prefix);
            assert_eq!(
                vfs.history(&path).unwrap().len(),
                1,
                "{path} was recorded twice"
            );
        }
        assert!(
            state(&vfs, "main").unwrap().unwrap().journal == 0,
            "plan cleared"
        );
    }

    #[test]
    fn a_second_record_is_refused_and_names_the_lease_holder() {
        let base = scratch("leased");
        let (repo, _) = repository(&base, 2);
        let mut vfs = open_vfs(&base);
        let dest = base.join("work/main");
        let state = materialize(&mut vfs, &repo, "main", &dest, None, "projects").unwrap();

        vfs.lease(&state.prefix, "other-agent:4242", DEFAULT_LEASE_MS)
            .unwrap();
        let error = record(&mut vfs, "main", &RecordOptions::new("agent")).unwrap_err();
        let message = format!("{error}");
        assert!(message.contains("other-agent:4242"), "{message}");
    }

    #[test]
    fn reverting_a_file_removes_the_override_instead_of_keeping_it() {
        let base = scratch("revert");
        let (repo, _) = repository(&base, 3);
        let mut vfs = open_vfs(&base);
        let dest = base.join("work/main");
        let state = materialize(&mut vfs, &repo, "main", &dest, None, "projects").unwrap();
        let file = dest.join("src/file001.txt");

        write(&file, &body(1, " (edited)"));
        assert_eq!(
            record(&mut vfs, "main", &RecordOptions::new("a"))
                .unwrap()
                .upserted,
            1
        );
        assert_eq!(vfs.list(&state.prefix).unwrap().len(), 1);

        // Back to what the commit says: the recording must let go, or a later
        // materialize would restore content the worktree deliberately dropped.
        write(&file, &body(1, ""));
        let reverted = record(&mut vfs, "main", &RecordOptions::new("a")).unwrap();
        assert_eq!(reverted.removed, 1, "{reverted:#?}");
        assert!(vfs.list(&state.prefix).unwrap().is_empty());
        assert!(overrides(&vfs, "main").unwrap().is_empty());
    }

    #[test]
    fn a_deleted_file_reaches_the_change_feed_even_though_the_tree_covered_it() {
        let base = scratch("deleted");
        let (repo, _) = repository(&base, 4);
        let mut vfs = open_vfs(&base);
        let dest = base.join("work/main");
        materialize(&mut vfs, &repo, "main", &dest, None, "projects").unwrap();

        fs::remove_file(dest.join("src/file002.txt")).unwrap();
        vfs.set_actor("agent");
        let report = record(&mut vfs, "main", &RecordOptions::new("agent")).unwrap();
        assert_eq!(report.removed, 1, "{report:#?}");

        let page = changes(&vfs, "main", 0, 100).unwrap();
        assert_eq!(page.changes.len(), 1);
        assert_eq!(page.changes[0].op, SyncOpKind::Remove);
        assert_eq!(page.changes[0].path, "src/file002.txt");
        assert_eq!(page.changes[0].author, "agent");
    }

    #[test]
    fn the_change_feed_stops_where_the_cursor_says() {
        let base = scratch("cursor");
        let (repo, _) = repository(&base, 6);
        let mut vfs = open_vfs(&base);
        let dest = base.join("work/main");
        materialize(&mut vfs, &repo, "main", &dest, None, "projects").unwrap();

        write(&dest.join("src/file000.txt"), &body(0, " (edited)"));
        write(&dest.join("src/file003.txt"), &body(3, " (edited)"));
        record(&mut vfs, "main", &RecordOptions::new("agent")).unwrap();

        let first = changes(&vfs, "main", 0, 100).unwrap();
        assert_eq!(first.changes.len(), 2);
        assert_eq!(first.changes[0].path, "src/file000.txt");
        assert_eq!(first.changes[1].path, "src/file003.txt");
        assert!(first.cursor > 0);

        // Nothing new since: the harness's poll is cheap and empty.
        let second = changes(&vfs, "main", first.cursor, 100).unwrap();
        assert!(second.changes.is_empty());
        assert_eq!(second.cursor, first.cursor);
    }

    #[test]
    fn ignored_output_and_symlinks_are_not_recorded() {
        let base = scratch("ignored");
        let (repo, _) = repository(&base, 2);
        write(&repo.join(".gitignore"), "target/\n");
        commit_all(&repo, "ignore build output");
        let mut vfs = open_vfs(&base);
        let dest = base.join("work/main");
        let state = materialize(&mut vfs, &repo, "main", &dest, None, "projects").unwrap();

        write(&dest.join("target/debug/thing.o"), "object code\n");
        #[cfg(unix)]
        std::os::unix::fs::symlink(dest.join("src/file000.txt"), dest.join("src/link.txt"))
            .unwrap();

        let report = record(&mut vfs, "main", &RecordOptions::new("agent")).unwrap();
        assert_eq!(report.planned, 0, "{report:#?}");
        assert!(report.skipped >= 1);
        assert!(vfs.list(&state.prefix).unwrap().is_empty());
    }

    #[test]
    fn committing_the_work_advances_the_base_instead_of_losing_it() {
        let base = scratch("commit");
        let (repo, head) = repository(&base, 3);
        let mut vfs = open_vfs(&base);
        let dest = base.join("work/main");
        materialize(&mut vfs, &repo, "main", &dest, None, "projects").unwrap();

        write(&dest.join("src/file001.txt"), &body(1, " (edited)"));
        record(&mut vfs, "main", &RecordOptions::new("agent")).unwrap();
        commit_all(&dest, "the work");
        let committed = run(&dest, &["rev-parse", "HEAD"]).trim().to_string();

        let after = record(&mut vfs, "main", &RecordOptions::new("agent")).unwrap();
        // Git sees nothing left to say; the recording lets go of its override and
        // moves the base onto the commit that now holds the content.
        assert_eq!(after.planned, 1, "{after:#?}");
        assert_eq!(after.base_advanced_to.as_deref(), Some(committed.as_str()));
        assert_ne!(committed, head);
        assert!(overrides(&vfs, "main").unwrap().is_empty());
        assert_eq!(state(&vfs, "main").unwrap().unwrap().base_commit, committed);
    }
}
