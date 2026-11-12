//! Carrying commits: the half of a worktree a diff cannot describe.
//!
//! A recording is addressed by a commit and carries the difference from it, which
//! works because the far side is expected to already have the commit. That
//! expectation is false exactly when it matters: a Delta thread works in its own
//! *shallow* clone, so a commit made there may exist on no remote and in no other
//! clone on earth. Swap machines and the work is gone with it.
//!
//! So a recording also carries the commit itself, as a Git **bundle**: one opaque
//! blob, named by its digest like any other content, that the far side hands to
//! `git fetch` and immediately stops being special. Nothing here re-implements
//! Git's object store, and nothing writes into `.git` except through `git`.
//!
//! Three rules are load-bearing, all of them learned the hard way:
//!
//! - **Bundle a ref, never a range.** `git bundle create x <head> --not <base>`
//!   inside a shallow clone produces "Refusing to create empty bundle", because
//!   the shallow cut is already a boundary and the computed set is empty. Bundling
//!   `refs/fungos/carry` works in a shallow clone and in a full one; Git then
//!   negotiates what the far side already has.
//! - **Only carry what a remote cannot supply.** If the commit is reachable from
//!   any remote ref, the far side can fetch it from there and a bundle is waste.
//!   The test is `git rev-list --count <commit> --not --remotes`, which is the
//!   question "does anybody else have this?" rather than a guess about workflow.
//! - **Never trust that it arrived.** After fetching, the commit is looked up
//!   again; a bundle that did not deliver it fails loudly, naming the commit.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result, bail};

use crate::{ContentId, Expect, Vfs, project::git};

/// The ref a bundle is created from, so the whole thing has a stable name that
/// does not depend on the worktree being on a branch.
pub const CARRY_REF: &str = "refs/fungos/carry";

/// Where a carried bundle lives in the namespace.
///
/// Addressed by the worktree's **namespace**, not by its name: the namespace is
/// the address that travels in the project marker, while a recording's name is
/// device-local and can differ between the two machines — which is exactly how the
/// first version of this looked in the right place on one machine and nowhere on
/// the other.
///
/// Deliberately *outside* the worktree's own prefix: everything under a worktree
/// prefix is treated as files to lay down and to reconcile away, and a bundle is
/// neither. `git/` is a namespace nothing else walks.
#[must_use]
pub fn key_for(namespace: &str, commit: &str) -> String {
    format!("git/{}/{commit}.bundle", namespace.trim_matches('/'))
}

/// What carrying a commit cost, for a report to say out loud.
#[derive(Clone, Debug)]
pub struct Carried {
    pub commit: String,
    pub content_id: ContentId,
    pub bytes: u64,
}

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn scratch_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "fungos-vfs-carry-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ))
}

/// Whether any remote already has this commit, so the far side could fetch it.
///
/// A repository with no remotes answers "no": nobody else can have it, which is
/// the case a thread clone with an unpushed commit is in.
fn upstream_has(repo: &Path, commit: &str) -> Result<bool> {
    let count = git(repo, &["rev-list", "--count", commit, "--not", "--remotes"])?;
    Ok(count.trim() == "0")
}

/// Ask Git for a bundle of exactly the carried ref, and clean up after itself.
///
/// The temporary ref exists so the bundle has a name; it is removed whether the
/// bundle succeeded or not, because a tool that leaves refs behind in somebody's
/// clone is a tool they stop running.
fn bundle(repo: &Path, commit: &str) -> Result<Vec<u8>> {
    let path = scratch_path();
    let outcome = (|| -> Result<Vec<u8>> {
        git(repo, &["update-ref", CARRY_REF, commit])
            .with_context(|| format!("naming {commit} as {CARRY_REF}"))?;
        git(
            repo,
            &[
                "bundle",
                "create",
                path.to_string_lossy().as_ref(),
                CARRY_REF,
            ],
        )
        .with_context(|| format!("bundling {commit}"))?;
        let bytes = fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
        if bytes.is_empty() {
            bail!("git made an empty bundle for {commit}");
        }
        Ok(bytes)
    })();
    let _ = git(repo, &["update-ref", "-d", CARRY_REF]);
    let _ = fs::remove_file(&path);
    outcome
}

/// Carry `commit` if no remote has it, filing the bundle as content.
pub fn carry(vfs: &mut Vfs, namespace: &str, repo: &Path, commit: &str) -> Result<Option<Carried>> {
    if upstream_has(repo, commit)? {
        return Ok(None);
    }
    let bytes = bundle(repo, commit)?;
    let key = key_for(namespace, commit);
    let media = "application/vnd.git.bundle";
    // Written outside the worktree's scope: the bundle is a sidecar of the
    // recording, not a file of the worktree, and a change feed that reported it as
    // one would make every record look like it touched a file called `git/…`.
    let previous = vfs.worktree().to_string();
    vfs.set_worktree("");
    let written = vfs.put(&key, &bytes, media, Expect::Any);
    vfs.set_worktree(previous);
    let content_id = written?.content_id;
    Ok(Some(Carried {
        commit: commit.to_owned(),
        content_id,
        bytes: bytes.len() as u64,
    }))
}

/// Make `commit` exist in the repository at `repo`, fetching the carried bundle
/// when the repository has never seen it.
///
/// Returns the number of bytes fetched, or `None` when the commit was already
/// there — the ordinary case for work that has been pushed.
pub fn ensure_commit(vfs: &Vfs, namespace: &str, repo: &Path, commit: &str) -> Result<Option<u64>> {
    if git(repo, &["cat-file", "-e", commit]).is_ok() {
        return Ok(None);
    }
    // One place to look: the namespace entry, keyed by the address that travels
    // with the recording. There is no local table to consult, so the answer is the
    // same on the machine that made the recording and on the one receiving it.
    let Some(entry) = vfs.file(&key_for(namespace, commit))? else {
        bail!(
            "{commit} is not in {} and no commit was carried with this recording; \
             fetch it from the machine that made it",
            repo.display()
        );
    };
    let content_id = entry.content_id;
    let carried_commit = commit.to_owned();
    let bytes = vfs
        .content(&content_id)
        .with_context(|| format!("reading the carried bundle for {carried_commit}"))?;
    let path = scratch_path();
    let fetched = (|| -> Result<()> {
        fs::write(&path, &bytes).with_context(|| format!("writing {}", path.display()))?;
        let target = format!("{CARRY_REF}:refs/fungos/{}", ref_component(namespace));
        git(
            repo,
            &["fetch", path.to_string_lossy().as_ref(), target.as_str()],
        )
        .with_context(|| format!("fetching the carried bundle for {carried_commit}"))?;
        Ok(())
    })();
    let _ = fs::remove_file(&path);
    fetched?;
    // The bundle said it carried the commit; the repository is the thing that can
    // confirm it, so ask rather than assume.
    if git(repo, &["cat-file", "-e", commit]).is_err() {
        bail!(
            "the carried bundle for {carried_commit} did not deliver {commit}; \
             it is stale for this recording"
        );
    }
    Ok(Some(bytes.len() as u64))
}

/// A worktree name as one ref path component.
fn ref_component(worktree: &str) -> String {
    let cleaned: String = worktree
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '-'
            }
        })
        .collect();
    if cleaned.trim_matches('-').is_empty() {
        "worktree".to_owned()
    } else {
        cleaned
    }
}
