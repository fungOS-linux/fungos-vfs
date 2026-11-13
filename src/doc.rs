//! A document: the merge layer bound to a file on disk and to its snapshots.
//!
//! [`crate::crdt`] decides what two replicas agree on. This module decides how a
//! document reaches a disk and comes back: a markdown file is the thing a person
//! and an agent actually edit, and it is a *view* — a save is a batch of ops from
//! whoever touched it.
//!
//! Three choices make the loop work:
//!
//! - **Each replica owns its own ops file.** `docs/<id>/ops/<actor>.json` is
//!   written by one actor and read by everybody. A single shared state blob would
//!   be a last-writer-wins record, and two worktrees reconciling at once would
//!   lose one side's edits at the storage layer — exactly what the merge layer
//!   exists to prevent.
//! - **Reconcile absorbs before it imports.** Every pass re-reads the other
//!   actors' ops first, so a worktree that reconciles after its neighbour gets the
//!   neighbour's edits and writes the merged text to disk.
//! - **The merged text is written back only when it differs.** An unchanged file
//!   produces no ops, no write, and no churn in `git status`.
//!
//! Git is the durable record: the file is committed normally, and
//! `docs/<id>/snapshot.md` in the root is the same text at the same moment. Git
//! holds the history; the op log holds the merge.

use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};

use crate::{BlockId, ContentId, Expect, Op, Vfs, crdt};

/// Where one replica's ops live.
#[must_use]
pub fn ops_key(id: &str, actor: &str) -> String {
    format!("docs/{id}/ops/{actor}.json")
}

/// What this worktree last saw of the document. Kept per actor so a worktree can
/// tell its own unsaved edits from a view that predates a pull.
#[must_use]
pub fn seen_key(id: &str, actor: &str) -> String {
    format!("docs/{id}/seen/{actor}.md")
}

/// Where the last merged rendering lives.
#[must_use]
pub fn snapshot_key(id: &str) -> String {
    format!("docs/{id}/snapshot.md")
}

/// The listing prefix. A namespace path has no trailing separator, so a stale
/// `docs/notes/opsX` would match it and is filtered by the caller.
fn ops_prefix(id: &str) -> String {
    format!("docs/{id}/ops")
}

fn under_ops(id: &str, path: &str) -> bool {
    path.starts_with(&format!("{}/", ops_prefix(id)))
}

/// What a reconcile did.
#[derive(Debug, Default)]
pub struct Reconciled {
    /// Ops this pass produced from the file.
    pub ops: usize,
    /// Whether the merged text differed from what was on disk and was written.
    pub wrote_file: bool,
    /// Ops still waiting for a block that never arrived. Should be zero; a caller
    /// that sees one is looking at a record it does not fully have.
    pub deferred: usize,
    /// The file held text this worktree had never seen — a view from before a
    /// pull — and the merged document replaced it. Loud on purpose: an uncommitted
    /// edit is the one thing a reconciler must not lose quietly.
    pub replaced_unseen_text: bool,
}

/// A document bound to a file.
pub struct DocFile {
    id: String,
    actor: String,
    path: PathBuf,
    doc: crdt::Doc,
    /// This replica's own op stream, as last written.
    mine: Vec<Op>,
    /// The text this worktree last showed for the document, if it has shown one.
    seen: Option<String>,
    /// The document moved under this worktree without the file being shown it —
    /// a pull landed before the file did. The next reconcile writes the merged
    /// text rather than reading the stale file as an edit.
    unshown: bool,
}

impl DocFile {
    /// Open a document bound to `path`, restoring state from the root.
    ///
    /// A root that has never seen this id starts empty, and the first
    /// [`reconcile`](Self::reconcile) reads the file: the text already on disk is
    /// the document, not a conflict to resolve.
    pub fn open(vfs: &Vfs, id: &str, actor: &str, path: &Path) -> Result<Self> {
        let mut doc = crdt::Doc::new(actor);
        let mut mine = Vec::new();
        for (stream, ops) in read_ops(vfs, id)? {
            if stream == ops_key(id, actor) {
                mine.clone_from(&ops);
            }
            doc.apply_all(&ops);
        }
        let (seen, unshown) = match vfs.file(&seen_key(id, actor))? {
            Some(entry) => (
                Some(String::from_utf8_lossy(&vfs.content(&entry.content_id)?).into_owned()),
                false,
            ),
            None if !doc.is_empty() => (Some(doc.render()), true),
            None => (None, false),
        };
        let open = Self {
            id: id.to_owned(),
            actor: actor.to_owned(),
            path: path.to_path_buf(),
            doc,
            mine,
            seen,
            unshown,
        };
        Ok(open)
    }

    /// An identity for a write this replica is making on this document's behalf.
    pub fn stamp(&mut self) -> BlockId {
        self.doc.stamp()
    }

    /// Take in what the other replicas have written since the last pass.
    fn absorb(&mut self, vfs: &Vfs) -> Result<()> {
        for (stream, ops) in read_ops(vfs, &self.id)? {
            if stream == ops_key(&self.id, &self.actor) {
                continue;
            }
            self.doc.apply_all(&ops);
        }
        Ok(())
    }

    /// Read the file, turn the difference into ops, merge, and write the result.
    ///
    /// The order matters: edits are read *before* the other replicas are taken in,
    /// while this replica's document still matches the text the file was rendered
    /// from. Absorbing first would make the file look like it were undoing
    /// whatever arrived.
    ///
    /// One honest limit: the file is only trusted as an edit when this worktree
    /// has seen it before. A file from before a pull is replaced by the merged
    /// document and reported — not merged, because there is no base to merge it
    /// against, and guessing would be how an uncommitted edit disappears.
    pub fn reconcile(&mut self, vfs: &mut Vfs) -> Result<Reconciled> {
        let on_disk = read_or_none(&self.path)?;
        let current = on_disk.clone().unwrap_or_default();
        let mut replaced_unseen_text = false;
        let ops = if on_disk.is_none() {
            // A document does not disappear because its file is gone: the file is
            // a view, and a missing one is written back rather than read as a
            // deletion of everything.
            Vec::new()
        } else if self.unshown {
            // The file is from before a pull. There is no base to merge it
            // against, so it loses to the document and the loss is reported.
            replaced_unseen_text =
                normalize(self.seen.as_deref().unwrap_or_default()) != normalize(&current);
            self.unshown = false;
            Vec::new()
        } else {
            match &self.seen {
                Some(seen) if *seen == current => Vec::new(),
                _ => self.doc.import(&current),
            }
        };
        let mut reconciled = self.commit(vfs, ops, &current)?;
        reconciled.replaced_unseen_text = replaced_unseen_text;
        Ok(reconciled)
    }

    /// Add ops this replica authored, without reading the file as an edit.
    ///
    /// This is the seam a projector uses: a transcript that grew by one turn is
    /// one insert, not a 20 KB file that has to be re-read, re-split and
    /// re-diffed. The file is still written and snapshotted, so git sees the same
    /// text either way.
    ///
    /// A file this worktree has never seen is replaced rather than merged, exactly
    /// as [`reconcile`](Self::reconcile) does — an op is not a base to merge a
    /// stale view against either.
    pub fn extend(&mut self, vfs: &mut Vfs, ops: Vec<Op>) -> Result<Reconciled> {
        let current = read_or_none(&self.path)?.unwrap_or_default();
        self.unshown = false;
        self.commit(vfs, ops, &current)
    }

    /// Apply `ops`, publish this replica's stream, and write the merged text.
    fn commit(&mut self, vfs: &mut Vfs, ops: Vec<Op>, current: &str) -> Result<Reconciled> {
        let report_ops = ops.len();
        let deferred = self.doc.apply_all(&ops).len();
        self.absorb(vfs)?;

        if !ops.is_empty() {
            self.mine.extend(ops);
            vfs.put(
                &ops_key(&self.id, &self.actor),
                &serde_json::to_vec(&self.mine)?,
                "application/json",
                Expect::Any,
            )?;
        }

        let merged = self.doc.render();
        let wrote_file = normalize(&merged) != normalize(current);
        if wrote_file {
            if let Some(parent) = self.path.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("creating {}", parent.display()))?;
            }
            fs::write(&self.path, with_trailing_newline(&merged))
                .with_context(|| format!("writing {}", self.path.display()))?;
        }
        vfs.put(
            &snapshot_key(&self.id),
            with_trailing_newline(&merged).as_bytes(),
            "text/markdown",
            Expect::Any,
        )?;
        vfs.put(
            &seen_key(&self.id, &self.actor),
            with_trailing_newline(&merged).as_bytes(),
            "text/markdown",
            Expect::Any,
        )?;
        self.seen = Some(with_trailing_newline(&merged));

        Ok(Reconciled {
            ops: report_ops,
            wrote_file,
            deferred,
            replaced_unseen_text: false,
        })
    }

    /// The document as it stands in memory.
    ///
    /// Read-only on purpose: the merge is this module's business, and a caller
    /// that could mutate the document could mutate it without publishing ops.
    #[must_use]
    pub fn doc(&self) -> &crdt::Doc {
        &self.doc
    }

    /// The document as it stands in memory.
    #[must_use]
    pub fn render(&self) -> String {
        self.doc.render()
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The snapshot this document last wrote.
    pub fn snapshot(&self, vfs: &Vfs) -> Result<Option<ContentId>> {
        Ok(vfs
            .file(&snapshot_key(&self.id))?
            .map(|entry| entry.content_id))
    }
}

/// Every actor's op file under `docs/<id>/ops`, with the path it came from.
fn read_ops(vfs: &Vfs, id: &str) -> Result<Vec<(String, Vec<Op>)>> {
    let mut streams = Vec::new();
    for entry in vfs.list(&ops_prefix(id))? {
        if !under_ops(id, &entry.path) {
            continue;
        }
        let bytes = vfs.content(&entry.content_id)?;
        let ops: Vec<Op> =
            serde_json::from_slice(&bytes).with_context(|| format!("reading {}", entry.path))?;
        streams.push((entry.path, ops));
    }
    Ok(streams)
}

/// A document as the root holds it, without binding it to a file.
///
/// Reading what a document *is* must not require the machine to hold the file
/// that displays it: listing the annotations on somebody else's transcript has
/// no file to open, and inventing a path for one would only be a lie about where
/// the document lives.
pub fn load(vfs: &Vfs, id: &str, actor: &str) -> Result<crdt::Doc> {
    let mut doc = crdt::Doc::new(actor);
    for (_, ops) in read_ops(vfs, id)? {
        doc.apply_all(&ops);
    }
    Ok(doc)
}

fn read_or_none(path: &Path) -> Result<Option<String>> {
    match fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("reading {}", path.display())),
    }
}

/// Compare ignoring the trailing blank lines a writer may or may not have left.
fn normalize(text: &str) -> &str {
    text.trim_end()
}

/// Files end with a newline; a renderer does not.
fn with_trailing_newline(text: &str) -> String {
    let mut out = text.trim_end().to_owned();
    out.push('\n');
    out
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use super::*;
    use crate::NodeRole;

    const BASE: &str = "# Notes\n\nfirst paragraph\n\nsecond paragraph";

    fn store() -> (tempfile::TempDir, Vfs) {
        let temp = tempfile::tempdir().unwrap();
        let cas = temp.path().join("cas");
        let vfs = Vfs::open_with(temp.path().join("root"), NodeRole::Full, Some(&cas)).unwrap();
        (temp, vfs)
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

    #[test]
    fn a_file_becomes_a_document_and_a_second_pass_changes_nothing() {
        let (temp, mut vfs) = store();
        let file = temp.path().join("NOTES.md");
        fs::write(&file, format!("{BASE}\n")).unwrap();

        let mut doc = DocFile::open(&vfs, "notes", "neo", &file).unwrap();
        let first = doc.reconcile(&mut vfs).unwrap();
        assert!(first.ops > 0, "the file is where the document comes from");
        assert!(!first.wrote_file, "there was nothing to correct");
        assert!(!first.replaced_unseen_text);
        assert_eq!(fs::read_to_string(&file).unwrap(), format!("{BASE}\n"));

        let second = doc.reconcile(&mut vfs).unwrap();
        assert_eq!(second.ops, 0);
        assert!(!second.wrote_file);
        assert!(doc.snapshot(&vfs).unwrap().is_some());
    }

    #[test]
    fn an_edit_to_the_file_becomes_ops() {
        let (temp, mut vfs) = store();
        let file = temp.path().join("NOTES.md");
        fs::write(&file, format!("{BASE}\n")).unwrap();
        let mut doc = DocFile::open(&vfs, "notes", "neo", &file).unwrap();
        doc.reconcile(&mut vfs).unwrap();

        fs::write(&file, format!("{}\n", BASE.replace("first", "the first"))).unwrap();
        let report = doc.reconcile(&mut vfs).unwrap();
        assert_eq!(report.ops, 1, "one block rewritten");
        assert!(!report.wrote_file, "the file already said it");
        assert!(doc.render().contains("the first paragraph"));
    }

    #[test]
    fn a_deleted_file_is_written_back_from_the_document() {
        let (temp, mut vfs) = store();
        let file = temp.path().join("NOTES.md");
        fs::write(&file, format!("{BASE}\n")).unwrap();
        let mut doc = DocFile::open(&vfs, "notes", "neo", &file).unwrap();
        doc.reconcile(&mut vfs).unwrap();

        fs::remove_file(&file).unwrap();
        let report = doc.reconcile(&mut vfs).unwrap();
        assert!(report.wrote_file);
        assert_eq!(fs::read_to_string(&file).unwrap(), format!("{BASE}\n"));
    }

    #[test]
    fn the_snapshot_is_the_text_in_the_file() {
        let (temp, mut vfs) = store();
        let file = temp.path().join("NOTES.md");
        fs::write(&file, format!("{BASE}\n")).unwrap();
        let mut doc = DocFile::open(&vfs, "notes", "neo", &file).unwrap();
        doc.reconcile(&mut vfs).unwrap();

        let id = doc.snapshot(&vfs).unwrap().unwrap();
        assert_eq!(vfs.content(&id).unwrap(), format!("{BASE}\n").as_bytes());
    }

    #[test]
    fn two_worktrees_editing_one_file_both_keep_their_edits() {
        // The end-to-end claim: two clones of one repository, one document, no
        // coordination, and afterwards both files hold both edits and both
        // commits are in history.
        let (temp, mut vfs) = store();
        let origin = temp.path().join("origin");
        fs::create_dir_all(&origin).unwrap();
        run(&origin, &["init", "-q", "-b", "main"]);
        fs::write(origin.join("NOTES.md"), format!("{BASE}\n")).unwrap();
        commit_all(&origin, "base");

        let a = temp.path().join("a");
        let b = temp.path().join("b");
        for dest in [&a, &b] {
            let output = Command::new("git")
                .args(["clone", "-q"])
                .arg(&origin)
                .arg(dest)
                .output()
                .unwrap();
            assert!(output.status.success());
        }

        let mut from_a = DocFile::open(&vfs, "notes", "neo", &a.join("NOTES.md")).unwrap();
        from_a.reconcile(&mut vfs).unwrap();

        // A rewrites the middle paragraph and commits.
        fs::write(
            a.join("NOTES.md"),
            format!(
                "{}\n",
                BASE.replace("first paragraph", "first paragraph, edited")
            ),
        )
        .unwrap();
        assert_eq!(from_a.reconcile(&mut vfs).unwrap().ops, 1);
        commit_all(&a, "edit the middle paragraph");

        // B is still on the base commit. It pulls nothing — it reconciles, which
        // brings A's text into its file.
        let mut from_b = DocFile::open(&vfs, "notes", "studio", &b.join("NOTES.md")).unwrap();
        let pulled = from_b.reconcile(&mut vfs).unwrap();
        assert!(
            pulled.replaced_unseen_text,
            "B held a view from before A's edit"
        );
        assert!(
            fs::read_to_string(b.join("NOTES.md"))
                .unwrap()
                .contains("first paragraph, edited")
        );

        // B appends a paragraph and commits.
        let text = fs::read_to_string(b.join("NOTES.md")).unwrap();
        fs::write(
            b.join("NOTES.md"),
            format!("{}\n\nthird paragraph\n", text.trim_end()),
        )
        .unwrap();
        assert_eq!(from_b.reconcile(&mut vfs).unwrap().ops, 1);
        commit_all(&b, "append a paragraph");

        // A reconciles and gets B's paragraph without losing its own edit.
        from_a.reconcile(&mut vfs).unwrap();

        let a_text = fs::read_to_string(a.join("NOTES.md")).unwrap();
        let b_text = fs::read_to_string(b.join("NOTES.md")).unwrap();
        assert_eq!(a_text, b_text, "both worktrees hold the same document");
        assert!(a_text.contains("first paragraph, edited"));
        assert!(a_text.contains("third paragraph"));
        assert!(
            a_text.find("edited").unwrap() < a_text.find("third").unwrap(),
            "the edit stayed in the middle: {a_text}"
        );

        // Both sides committed, and each commit is a readable snapshot.
        assert!(run(&a, &["log", "--oneline"]).lines().count() >= 2);
        assert!(run(&b, &["log", "--oneline"]).lines().count() >= 2);
    }

    #[test]
    fn a_second_worktree_that_pulls_first_reads_the_pulled_text_as_its_base() {
        // The reason the `seen` record exists: a worktree must not read the text it
        // was just given as an edit that reverts somebody.
        let (temp, mut vfs) = store();
        let file_a = temp.path().join("a.md");
        let file_b = temp.path().join("b.md");
        fs::write(&file_a, format!("{BASE}\n")).unwrap();
        fs::write(&file_b, format!("{BASE}\n")).unwrap();

        let mut from_a = DocFile::open(&vfs, "notes", "neo", &file_a).unwrap();
        from_a.reconcile(&mut vfs).unwrap();
        fs::write(&file_a, format!("{}\n", BASE.replace("first", "the first"))).unwrap();
        from_a.reconcile(&mut vfs).unwrap();

        // Open B twice: the first open has never seen the document, the second
        // knows what it was shown.
        let mut from_b = DocFile::open(&vfs, "notes", "studio", &file_b).unwrap();
        from_b.reconcile(&mut vfs).unwrap();
        assert!(fs::read_to_string(&file_b).unwrap().contains("the first"));

        let again = DocFile::open(&vfs, "notes", "studio", &file_b).unwrap();
        assert!(!again.unshown, "the second open has seen this text");
        let mut again = again;
        let report = again.reconcile(&mut vfs).unwrap();
        assert_eq!(report.ops, 0, "the pulled text is not an edit");
        assert!(!report.replaced_unseen_text);
    }
}
