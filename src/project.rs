//! Attaching a Git project to a desktop.
//!
//! A project is a repository plus the state Git does not keep for you: which
//! worktrees exist, what each one is built on, and the files somebody changed
//! and has not committed. Git's own objects stay Git's — the policy attached
//! here asks for a tree pointer and never copies a tracked file, so a clean
//! checkout costs one tree id rather than one blob per file.
//!
//! That split is the whole point. `.git` stays minimal and one per repository,
//! worktrees share its object store so the load spreads without the copies, and
//! the desktop holds the in-between: dirty trees, agent output, handoffs,
//! artifacts. A path inside a Git tree that is also asked for as a blob is an
//! error elsewhere in the pipeline, which is the enforcement behind this rule.

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

/// What `project attach` found and did.
#[derive(Debug, Serialize)]
pub struct ProjectReport {
    pub name: String,
    /// Namespace prefix the worktree was ingested under.
    pub prefix: String,
    pub root: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remote: Option<String>,
    pub branch: String,
    pub head: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git: Option<GitPointer>,
    pub worktrees: Vec<WorktreeRef>,
    /// Where the desktop recorded this project.
    pub marker: String,
    pub ingest: IngestReport,
    pub dry_run: bool,
}

fn git(root: &Path, args: &[&str]) -> Result<String> {
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

/// Attach the repository containing `path` to this desktop.
///
/// `name` defaults to the repository's directory name; the namespace prefix is
/// `<prefix>/<name>`.
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
    let ingest = ingest_dir_with_policy(vfs, &root, &prefix, SourceMode::Git, dry_run, &policy)?;

    let marker = format!("{PROJECTS_DIR}/{name}.json");
    if !dry_run {
        let record = serde_json::json!({
            "name": name,
            "root": root,
            "remote": remote,
            "branch": branch,
            "head": head,
            "dirty": ingest.git.as_ref().is_some_and(|pointer| pointer.dirty),
            "worktrees": worktrees,
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
        worktrees,
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
        // Build output: nobody's.
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
}
