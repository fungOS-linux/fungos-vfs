//! Attaching a Git project to a desktop.
//!
//! A project is a repository plus the state Git does not keep for you: which
//! worktrees exist, what each one is built on, and the files somebody changed
//! and has not committed. Git's own objects stay Git's — the policy attached
//! here asks for a tree pointer and never copies a tracked file, so a clean
//! checkout costs one tree id rather than one blob per file.
//!
//! Every worktree is attached, because that is where the work is. A repository
//! with fourteen checkouts has fourteen places somebody might have left a
//! half-finished change, and a desktop that shows only the main one shows the
//! place least likely to be interesting. Each sibling lands under
//! `<prefix>/worktrees/<branch>`, so the namespace reads the way `git worktree
//! list` reads.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, anyhow, bail};
use serde::Serialize;

use crate::{
    Expect, GitPointer, IngestReport, PathPolicy, SourceMode, Vfs, ingest_dir_with_policy,
};

/// Where a desktop keeps what it knows about its projects.
///
/// Outside the project's own prefix on purpose: the marker is the desktop's
/// bookkeeping, not a file that belongs to the repository.
pub const PROJECTS_DIR: &str = ".fungos/projects";

/// The policy an attached project runs under.
///
/// `git: **` first, then the exclusions, because the last matching rule wins:
/// a repository that commits its build output (some do) must not have that
/// output treated as source.
///
/// Build output is **excluded here, not cached**. `cache: target/` would also be
/// legal — the class exists for exactly this shape of data — but attaching walks
/// `git ls-files`, which never lists an ignored `target/`, so the only case the
/// change would reach is a repository that tracks its own build output, and there
/// the bytes are already in the git tree. Storing them again is a cost with no
/// benefit. The cache is for the thing that produces build output, which is the
/// builder: it knows the ceiling and writes [crate::PathClass::Cache] entries.
///
/// Note what a dirty worktree costs: a tree id describes a commit, not the disk,
/// so a checkout with any change at all cannot use the pointer and its whole
/// tree is stored as blobs. The content store deduplicates them across
/// worktrees and desktops, but the cost is the checkout's size, not the size of
/// the diff. A clean checkout is the cheap case and should be the common one.
#[must_use]
pub fn attach_policy() -> &'static str {
    "\
# Everything the repository tracks is the repository's to store: one tree id
# instead of one blob per file.
git: **

# Neither Git's nor the desktop's: build output and caches.
exclude: target/
exclude: node_modules/
exclude: .venv/
exclude: dist/
exclude: build/
"
}

/// One worktree of a project, as Git describes it.
#[derive(Clone, Debug, Serialize)]
pub struct WorktreeRef {
    pub path: String,
    pub head: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    pub bare: bool,
}

/// What an ingest did, compactly: enough to answer "how much of this checkout
/// became blobs and how much stayed the tree's".
#[derive(Clone, Copy, Debug, Serialize)]
pub struct IngestSummary {
    pub added: usize,
    pub changed: usize,
    pub removed: usize,
    pub unchanged: usize,
    pub skipped: usize,
    pub excluded: usize,
    pub cached: usize,
    pub git_skipped: usize,
    pub bytes_written: u64,
    pub dirty: bool,
}

impl From<&IngestReport> for IngestSummary {
    fn from(report: &IngestReport) -> Self {
        Self {
            added: report.added,
            changed: report.changed,
            removed: report.removed,
            unchanged: report.unchanged,
            skipped: report.skipped,
            excluded: report.excluded,
            cached: report.cached,
            git_skipped: report.git_skipped,
            bytes_written: report.bytes_written,
            dirty: report.git.as_ref().is_some_and(|pointer| pointer.dirty),
        }
    }
}

/// One worktree and where in the namespace it landed.
#[derive(Clone, Debug, Serialize)]
pub struct AttachedWorktree {
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    pub head: String,
    pub bare: bool,
    /// Namespace prefix its files landed under; absent when it was skipped.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Attached from its recording rather than by walking the checkout: one
    /// pointer for the base commit and only the in-between as files.
    pub recorded: bool,
    /// Why it was skipped, when it was: `bare`, `missing`, or `outside`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skipped: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ingest: Option<IngestSummary>,
}

/// What `project attach` found and did.
#[derive(Debug, Serialize)]
pub struct ProjectReport {
    pub name: String,
    /// Namespace prefix the main worktree was ingested under.
    pub prefix: String,
    pub root: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remote: Option<String>,
    pub branch: String,
    pub head: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git: Option<GitPointer>,
    /// Every worktree, including the main one, and where each landed.
    pub worktrees: Vec<AttachedWorktree>,
    /// Where the desktop recorded this project.
    pub marker: String,
    pub ingest: IngestReport,
    pub dry_run: bool,
}

pub(crate) fn git(root: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()
        .with_context(|| format!("run git {} in {}", args.join(" "), root.display()))?;
    if !output.status.success() {
        bail!(
            "git {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Attach one worktree: from its recording when it has one, from the checkout
/// otherwise.
///
/// The recording is preferred because it is both cheaper and truer: the base
/// commit already describes every file that has not changed, and only the
/// in-between costs anything. Walking the checkout instead is what makes a dirty
/// worktree cost its whole tree — see [`attach_policy`] — so a worktree that has
/// been recorded never pays that again.
///
/// Returns the namespace it landed in, the report, and whether the recording
/// answered. A recording owns its address: it was computed from
/// [`crate::namespace_for`] when the recording was made, and recomputing it here
/// would be a second opinion about where the same worktree lives.
fn attach_one(
    vfs: &mut Vfs,
    path: &Path,
    namespace: &str,
    policy: &PathPolicy,
    dry_run: bool,
) -> Result<(String, IngestReport, bool)> {
    if let Some(known) = crate::worktree::state_for_path(vfs, path)? {
        let report = crate::worktree::project(vfs, &known, &known.prefix, dry_run)?;
        return Ok((known.prefix, report, true));
    }
    let report = ingest_dir_with_policy(vfs, path, namespace, SourceMode::Git, dry_run, policy)?;
    Ok((namespace.to_string(), report, false))
}

/// Every worktree of the repository at `root`, in Git's own order.
pub fn worktrees(root: &Path) -> Result<Vec<WorktreeRef>> {
    let listing = git(root, &["worktree", "list", "--porcelain"])?;
    Ok(parse_worktrees(&listing))
}

#[must_use]
pub fn parse_worktrees(listing: &str) -> Vec<WorktreeRef> {
    let mut worktrees = Vec::new();
    let mut current: Option<WorktreeRef> = None;
    for line in listing.lines() {
        let (key, value) = match line.split_once(' ') {
            Some(pair) => pair,
            None => (line, ""),
        };
        match key {
            "worktree" => {
                if let Some(previous) = current.take() {
                    worktrees.push(previous);
                }
                current = Some(WorktreeRef {
                    path: value.to_owned(),
                    head: String::new(),
                    branch: None,
                    bare: false,
                });
            }
            "HEAD" => {
                if let Some(entry) = current.as_mut() {
                    entry.head = value.to_owned();
                }
            }
            "branch" => {
                if let Some(entry) = current.as_mut() {
                    // `refs/heads/x` is noise in a listing a person reads.
                    entry.branch = Some(value.trim_start_matches("refs/heads/").to_owned());
                }
            }
            "bare" => {
                if let Some(entry) = current.as_mut() {
                    entry.bare = true;
                }
            }
            _ => {}
        }
    }
    if let Some(last) = current {
        worktrees.push(last);
    }
    worktrees
}

/// A branch name as one path segment.
///
/// `feat/thing` has to become `feat-thing`: a branch name is allowed to contain
/// the namespace separator, and a worktree must not be able to create
/// directories nobody asked for.
#[must_use]
pub fn label(branch: Option<&str>, path: &Path) -> String {
    let raw = branch
        .map(ToOwned::to_owned)
        .or_else(|| {
            path.file_name()
                .map(|name| name.to_string_lossy().into_owned())
        })
        .unwrap_or_else(|| "worktree".to_owned());
    let cleaned: String = raw
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '-'
            }
        })
        .collect();
    let cleaned = cleaned.trim_matches('-').to_owned();
    if cleaned.is_empty() {
        "worktree".to_owned()
    } else {
        cleaned
    }
}

/// Attach the repository containing `path` to this desktop.
///
/// `name` defaults to the repository's directory name; the namespace prefix is
/// `<prefix>/<name>`, with siblings at `<prefix>/<name>/worktrees/<label>`.
pub fn attach(
    vfs: &mut Vfs,
    path: &Path,
    name: Option<&str>,
    prefix: &str,
    dry_run: bool,
) -> Result<ProjectReport> {
    if !path.exists() {
        return Err(anyhow!("{} does not exist", path.display()));
    }
    let inside = git(path, &["rev-parse", "--is-inside-work-tree"])
        .map_err(|_| anyhow!("{} is not a Git worktree", path.display()))?;
    if inside.trim() != "true" {
        return Err(anyhow!("{} is not a Git worktree", path.display()));
    }
    let root = PathBuf::from(git(path, &["rev-parse", "--show-toplevel"])?.trim());
    let name = name.map_or_else(
        || {
            root.file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_else(|| "project".to_owned())
        },
        ToOwned::to_owned,
    );
    let prefix = format!("{}/{}", prefix.trim_matches('/'), name.trim_matches('/'));

    let head = git(&root, &["rev-parse", "HEAD"])
        .map(|value| value.trim().to_owned())
        .unwrap_or_default();
    let branch = git(&root, &["rev-parse", "--abbrev-ref", "HEAD"])
        .map(|value| value.trim().to_owned())
        .unwrap_or_else(|_| "HEAD".to_owned());
    let remote = git(&root, &["remote", "get-url", "origin"])
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    let worktrees = worktrees(&root).unwrap_or_default();

    let policy = PathPolicy::parse(attach_policy());
    let (root_namespace, ingest, recorded) = attach_one(vfs, &root, &prefix, &policy, dry_run)?;

    // The main worktree was just ingested; the siblings follow it, one namespace
    // each, so "where is my half-finished branch" has an answer.
    let canonical_root = std::fs::canonicalize(&root).unwrap_or_else(|_| root.clone());
    let mut attached = Vec::with_capacity(worktrees.len());
    let mut used: BTreeSet<String> = BTreeSet::new();
    for worktree in &worktrees {
        let path = PathBuf::from(&worktree.path);
        let canonical = std::fs::canonicalize(&path).unwrap_or_else(|_| path.clone());
        let mut entry = AttachedWorktree {
            path: worktree.path.clone(),
            branch: worktree.branch.clone(),
            head: worktree.head.clone(),
            bare: worktree.bare,
            namespace: None,
            recorded: false,
            skipped: None,
            ingest: None,
        };
        if canonical == canonical_root {
            entry.namespace = Some(root_namespace.clone());
            entry.recorded = recorded;
            entry.ingest = Some(IngestSummary::from(&ingest));
            attached.push(entry);
            continue;
        }
        if worktree.bare {
            entry.skipped = Some("bare".to_owned());
            attached.push(entry);
            continue;
        }
        // A recorded worktree is attached whether or not its directory is still
        // there: what it recorded is the work, and a handoff should not lose it
        // because somebody deleted the checkout.
        let has_recording = crate::worktree::state_for_path(vfs, &path)?.is_some();
        if !path.is_dir() && !has_recording {
            // Git still lists a worktree whose directory was removed by hand
            // until `git worktree prune` runs. Naming it beats ingesting into a
            // namespace that no longer has a source.
            entry.skipped = Some("missing".to_owned());
            attached.push(entry);
            continue;
        }
        let base = label(worktree.branch.as_deref(), &path);
        let mut candidate = base.clone();
        let mut suffix = 2;
        while !used.insert(candidate.clone()) {
            candidate = format!("{base}-{suffix}");
            suffix += 1;
        }
        let namespace = format!("{prefix}/worktrees/{candidate}");
        let (namespace, report, recorded) = attach_one(vfs, &path, &namespace, &policy, dry_run)?;
        entry.namespace = Some(namespace);
        entry.recorded = recorded;
        entry.ingest = Some(IngestSummary::from(&report));
        attached.push(entry);
    }

    // Git knows the worktrees of *this* clone. A recording can be of a checkout of
    // another clone — a Delta thread's worktree is exactly that — and it is a
    // worktree of the project all the same. Leaving it out of the marker strands
    // the recording on the far side, where `worktree checkout` can only restore
    // what the marker names.
    let known_paths: BTreeSet<PathBuf> = attached
        .iter()
        .map(|entry| {
            let path = PathBuf::from(&entry.path);
            std::fs::canonicalize(&path).unwrap_or(path)
        })
        .collect();
    for known in crate::worktree::worktrees(vfs)? {
        if !known.prefix.starts_with(&format!("{prefix}/")) {
            continue;
        }
        let canonical = std::fs::canonicalize(&known.path).unwrap_or_else(|_| known.path.clone());
        if known_paths.contains(&canonical) {
            continue;
        }
        let present = known.path.is_dir();
        let mut entry = AttachedWorktree {
            path: known.path.display().to_string(),
            branch: None,
            head: known.base_commit.clone(),
            bare: false,
            namespace: Some(known.prefix.clone()),
            recorded: true,
            skipped: if present {
                None
            } else {
                Some("missing".to_owned())
            },
            ingest: None,
        };
        if present {
            let (namespace, report, _) =
                attach_one(vfs, &known.path, &known.prefix, &policy, dry_run)?;
            entry.namespace = Some(namespace);
            entry.ingest = Some(IngestSummary::from(&report));
        }
        attached.push(entry);
    }

    let marker = format!("{PROJECTS_DIR}/{name}.json");
    if !dry_run {
        let record = serde_json::json!({
            "name": name,
            "root": root,
            "remote": remote,
            "branch": branch,
            "head": head,
            "dirty": ingest.git.as_ref().is_some_and(|pointer| pointer.dirty),
            "worktrees": attached,
        });
        let bytes = serde_json::to_vec_pretty(&record)?;
        vfs.put(&marker, &bytes, "application/json", Expect::Any)?;
    }

    Ok(ProjectReport {
        name,
        prefix,
        root,
        remote,
        branch,
        head,
        git: ingest.git.clone(),
        worktrees: attached,
        marker,
        ingest,
        dry_run,
    })
}

/// Every project this desktop has attached, oldest name first.
pub fn list(vfs: &mut Vfs) -> Result<Vec<serde_json::Value>> {
    let mut projects = Vec::new();
    for entry in vfs.list(PROJECTS_DIR)? {
        if !entry.path.ends_with(".json") {
            continue;
        }
        let bytes = vfs.get(&entry.path)?;
        let mut value: serde_json::Value = serde_json::from_slice(&bytes)?;
        value["marker"] = serde_json::Value::String(entry.path.clone());
        projects.push(value);
    }
    projects.sort_by(|left, right| {
        left.get("name")
            .and_then(serde_json::Value::as_str)
            .cmp(&right.get("name").and_then(serde_json::Value::as_str))
    });
    Ok(projects)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NodeRole, RecordOptions, Vfs, record};

    fn scratch(tag: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("fungos-vfs-project-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn open_vfs(base: &Path) -> Vfs {
        Vfs::open_with(base.join("vfs"), NodeRole::Full, Some(&base.join("cas"))).unwrap()
    }

    fn write(path: &Path, body: &str) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, body).unwrap();
    }

    fn commit_all(dir: &Path) {
        git(dir, &["add", "-A"]).unwrap();
        git(
            dir,
            &[
                "-c",
                "user.email=vfs@example.test",
                "-c",
                "user.name=vfs",
                "commit",
                "-qm",
                "init",
            ],
        )
        .unwrap();
    }

    /// A repository with `files` committed files and `dirty` of them edited.
    fn dirty_repository(base: &Path, files: usize, dirty: &[usize]) -> PathBuf {
        let repo = base.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        git(&repo, &["init", "-q"]).unwrap();
        for index in 0..files {
            write(&repo.join(format!("src/f{index:02}.txt")), &body(index, ""));
        }
        commit_all(&repo);
        for index in dirty {
            write(
                &repo.join(format!("src/f{index:02}.txt")),
                &body(*index, " (edited)"),
            );
        }
        repo
    }

    fn body(index: usize, suffix: &str) -> String {
        format!("file {index}{suffix}\n")
    }

    #[test]
    fn a_recorded_worktree_is_attached_from_its_in_between() {
        let base = scratch("recorded");
        let repo = dirty_repository(&base, 12, &[1, 5, 9]);
        let mut vfs = open_vfs(&base);

        crate::worktree::materialize(&mut vfs, &repo, "main", &repo, None, "projects").unwrap();
        let recorded = record(&mut vfs, "main", &RecordOptions::new("agent")).unwrap();
        assert_eq!(recorded.planned, 3);

        let report = attach(&mut vfs, &repo, None, "projects", false).unwrap();
        let main = &report.worktrees[0];
        assert!(main.recorded, "the recording is the cheaper answer");
        let summary = main.ingest.expect("the main worktree is attached");
        // `record` already put the in-between in the namespace at the address the
        // attach arrives at — one function names a worktree, so the two cannot
        // disagree about where it lives.
        assert_eq!(summary.unchanged, 3);
        assert_eq!(summary.added, 0);
        assert_eq!(summary.git_skipped, 0, "nothing was walked");

        // Three files in the namespace, not twelve: the other nine are still the
        // commit's tree.
        assert_eq!(vfs.list(&report.prefix).unwrap().len(), 3);
        assert_eq!(
            vfs.get(&format!("{}/src/f01.txt", report.prefix)).unwrap(),
            body(1, " (edited)").into_bytes()
        );
        // The pointer says which commit covers the rest, and that it is dirty —
        // dirty because the in-between exists, not because it was estimated.
        let pointer = report.git.expect("a recorded worktree still has a pointer");
        assert!(pointer.dirty);
        assert_eq!(pointer.commit.len(), 40);
    }

    #[test]
    fn a_recording_repairs_a_namespace_a_dirty_checkout_filled() {
        let base = scratch("repair");
        let repo = dirty_repository(&base, 12, &[1, 5, 9]);
        let mut vfs = open_vfs(&base);

        // Nothing recorded yet: attaching a dirty checkout costs its whole tree.
        let before = attach(&mut vfs, &repo, None, "projects", false).unwrap();
        assert!(!before.worktrees[0].recorded);
        assert_eq!(vfs.list(&before.prefix).unwrap().len(), 12);

        // Record it, then attach again: the namespace becomes the in-between and
        // the nine files the commit already covers are let go.
        crate::worktree::materialize(&mut vfs, &repo, "main", &repo, None, "projects").unwrap();
        record(&mut vfs, "main", &RecordOptions::new("agent")).unwrap();
        let after = attach(&mut vfs, &repo, None, "projects", false).unwrap();
        assert!(after.worktrees[0].recorded);
        assert_eq!(
            after.worktrees[0].ingest.expect("attached").removed,
            9,
            "{after:#?}"
        );
        assert_eq!(vfs.list(&after.prefix).unwrap().len(), 3);
    }

    #[test]
    fn a_recording_of_another_clone_is_in_the_marker() {
        let base = scratch("marker-clone");
        let repo = dirty_repository(&base, 6, &[2]);
        let mut vfs = open_vfs(&base);

        // A checkout of a clone the project has never seen: `git worktree list`
        // there knows nothing about it, so the marker has to learn it from the
        // recording or the far side cannot name it.
        let clone = base.join("thread-clone");
        crate::project::git(
            &base,
            &[
                "clone",
                "-q",
                &repo.to_string_lossy(),
                &clone.to_string_lossy(),
            ],
        )
        .unwrap();
        crate::worktree::materialize(&mut vfs, &repo, "thread", &clone, None, "projects").unwrap();
        write(&clone.join("src/f04.txt"), &body(4, " (thread)"));
        record(&mut vfs, "thread", &RecordOptions::new("agent")).unwrap();

        let report = attach(&mut vfs, &repo, None, "projects", false).unwrap();
        let thread = report
            .worktrees
            .iter()
            .find(|entry| entry.namespace.as_deref() == Some("projects/repo/worktrees/thread"))
            .unwrap_or_else(|| panic!("the recording is in the marker: {report:#?}"));
        assert!(thread.recorded);
        assert_eq!(thread.path, clone.display().to_string());
        assert_eq!(
            vfs.get("projects/repo/worktrees/thread/src/f04.txt")
                .unwrap(),
            body(4, " (thread)").into_bytes()
        );
    }

    #[test]
    fn a_worktree_listing_splits_into_one_entry_per_worktree() {
        let listing = "worktree /src/soil\nHEAD 1111\nbranch refs/heads/main\n\nworktree /src/soil/.worktrees/fix\nHEAD 2222\ndetached\n\nworktree /srv/soil.git\nHEAD 3333\nbare\n";
        let worktrees = parse_worktrees(listing);
        assert_eq!(worktrees.len(), 3);
        assert_eq!(worktrees[0].branch.as_deref(), Some("main"));
        assert_eq!(worktrees[0].head, "1111");
        assert_eq!(
            worktrees[1].branch, None,
            "a detached worktree has no branch"
        );
        assert!(worktrees[2].bare);
    }

    #[test]
    fn the_attach_policy_asks_git_for_the_tree_and_excludes_build_output() {
        let policy = PathPolicy::parse(attach_policy());
        // Tracked source: the repository's, not ours.
        assert_eq!(
            policy.classify("crates/soil-spore/src/lib.rs"),
            crate::PathClass::Git
        );
        // Build output: nobody's. Deliberately `Exclude` rather than `Cache`:
        // attaching walks `git ls-files`, which never lists an ignored `target/`,
        // so caching it here would only ever store a repository's *tracked* build
        // output — bytes the git tree already names. The builder fills the cache
        // instead. Changing this to `Cache` should mean deciding that attaching a
        // checkout of build output is worth the bytes.
        assert_eq!(
            policy.classify("target/debug/fungos-vfs"),
            crate::PathClass::Exclude
        );
        assert_eq!(
            policy.classify("ui/node_modules/x/index.js"),
            crate::PathClass::Exclude
        );
        // An artifact inside the repository belongs to the tree while the
        // checkout is clean; a dirty one falls back to blobs, which is where
        // the in-between lands.
        assert_eq!(policy.classify("docs/report.pdf"), crate::PathClass::Git);
    }

    #[test]
    fn a_branch_name_becomes_one_path_segment() {
        let nowhere = Path::new("/tmp/x");
        assert_eq!(label(Some("main"), nowhere), "main");
        assert_eq!(
            label(Some("feat/terminal-core"), nowhere),
            "feat-terminal-core"
        );
        assert_eq!(label(Some("fix#12"), nowhere), "fix-12");
        // A detached worktree has no branch, so the directory names it.
        assert_eq!(
            label(None, Path::new("/src/soil/.worktrees/weird")),
            "weird"
        );
        // Something unusable still has to produce a segment.
        assert_eq!(label(None, Path::new("/")), "worktree");
    }
}
