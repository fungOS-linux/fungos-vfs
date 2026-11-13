//! Where a comment thread lives.
//!
//! [`crate::anchor`] decides what an annotation *means*; this module decides
//! where the record of it is kept, and it keeps it the same way
//! [`crate::doc`] keeps edits:
//!
//! - **Each commenter owns one file.** `docs/<id>/comments/<actor>.json` is
//!   written by one actor and read by everybody. A single shared book would be
//!   last-writer-wins at the storage layer, so two people commenting at once
//!   would lose one side's thread before the merge layer ever saw it.
//! - **The book is the merge of those files, and nothing else.** Opening reads
//!   every actor's ops; recording appends to this actor's file. There is no
//!   separate index to keep in step.
//! - **A comment is not on the document.** It is on a block id, and it is
//!   resolved against the document when somebody looks. A thread therefore
//!   needs no rewrite when the paragraph under it changes.

use anyhow::{Context, Result};

use crate::{Applied, BlockId, CommentBook, CommentOp, Doc, Resolution, Stamp, Thread, Vfs};

/// Where one commenter's ops live.
#[must_use]
pub fn comments_key(id: &str, actor: &str) -> String {
    format!("docs/{id}/comments/{actor}.json")
}

/// The listing prefix. Like a doc's ops prefix it has no trailing separator, so
/// a stale `docs/notes/commentsX` matches it and is filtered below.
fn comments_prefix(id: &str) -> String {
    format!("docs/{id}/comments")
}

fn under_comments(id: &str, path: &str) -> bool {
    path.starts_with(&format!("{}/", comments_prefix(id)))
}

/// One document's threads, as this replica sees them.
pub struct CommentFile {
    id: String,
    actor: String,
    book: CommentBook,
    /// This commenter's own ops, as last written. Kept so a new op appends to
    /// the stream rather than replacing everybody else's.
    mine: Vec<CommentOp>,
}

impl CommentFile {
    /// Open a document's threads, absorbing every commenter's file.
    pub fn open(vfs: &Vfs, id: &str, actor: &str) -> Result<Self> {
        let mut open = Self {
            id: id.to_owned(),
            actor: actor.to_owned(),
            book: CommentBook::default(),
            mine: Vec::new(),
        };
        open.absorb(vfs)?;
        Ok(open)
    }

    /// Take in what the other commenters have written.
    ///
    /// Every file is collected before anything is applied: a reply can land in
    /// one commenter's stream before the thread it answers is read from another,
    /// and applying file by file would drop it instead of retrying it. The
    /// deferred list is dropped on the floor here, and that is deliberate — a
    /// reply to a thread nobody has opened is a fact about the record, and
    /// [`record`](Self::record) is where it gets reported.
    pub fn absorb(&mut self, vfs: &Vfs) -> Result<()> {
        let mut ops = Vec::new();
        for entry in vfs.list(&comments_prefix(&self.id))? {
            if !under_comments(&self.id, &entry.path) {
                continue;
            }
            let bytes = vfs.content(&entry.content_id)?;
            let stream: Vec<CommentOp> = serde_json::from_slice(&bytes)
                .with_context(|| format!("reading {}", entry.path))?;
            if entry.path == comments_key(&self.id, &self.actor) {
                self.mine.clone_from(&stream);
            }
            ops.extend(stream);
        }
        self.book.apply_all(&ops);
        Ok(())
    }

    /// Start a thread on a block, and say the first thing in it.
    ///
    /// Opening and speaking are two ops because a thread with no comment is not
    /// a thing anyone means to create; the pair is what a caller wants.
    pub fn open_thread(
        &mut self,
        vfs: &mut Vfs,
        anchor: crate::Anchor,
        body: &str,
        at_ms: i64,
    ) -> Result<BlockId> {
        self.absorb(vfs)?;
        let thread = self.tick();
        self.record(
            vfs,
            CommentOp::Open {
                id: thread.clone(),
                anchor,
            },
        )?;
        self.say(vfs, &thread, body, at_ms)?;
        Ok(thread)
    }

    /// Add a reply to an existing thread.
    ///
    /// `at_ms` is this commenter's clock reading, and it is what orders replies:
    /// counters start at one in every commenter's own stream, so without a time
    /// an answer would sort before the question it answers.
    pub fn say(
        &mut self,
        vfs: &mut Vfs,
        thread: &BlockId,
        body: &str,
        at_ms: i64,
    ) -> Result<BlockId> {
        let id = self.tick();
        self.record(
            vfs,
            CommentOp::Say {
                thread: thread.clone(),
                id: id.clone(),
                author: self.actor.clone(),
                body: body.to_owned(),
                at_ms,
            },
        )?;
        Ok(id)
    }

    /// Mark a thread resolved, or reopen it.
    pub fn settle(
        &mut self,
        vfs: &mut Vfs,
        thread: &BlockId,
        resolved: bool,
        at_ms: i64,
    ) -> Result<Stamp> {
        let stamp = Stamp {
            at_ms,
            by: self.tick(),
        };
        self.record(
            vfs,
            CommentOp::Settle {
                thread: thread.clone(),
                stamp: stamp.clone(),
                resolved,
            },
        )?;
        Ok(stamp)
    }

    /// Apply one op and, if it is news, add it to this commenter's stream.
    ///
    /// A duplicate is not written twice: a caller that retries a comment after a
    /// failed write must not end up with two of them.
    fn record(&mut self, vfs: &mut Vfs, op: CommentOp) -> Result<Applied> {
        match self.book.apply(&op) {
            Applied::Duplicate => Ok(Applied::Duplicate),
            // A reply that names a thread nobody here has opened is a caller
            // error, not something to hold silently: the op names an identity
            // that does not exist in this document.
            Applied::Deferred => anyhow::bail!(
                "no thread {} in document {}: open it first, or absorb the \
                 commenter who did",
                named_thread(&op).map_or_else(|| "(none)".to_owned(), ToString::to_string),
                self.id
            ),
            Applied::Applied => {
                self.mine.push(op);
                vfs.put(
                    &comments_key(&self.id, &self.actor),
                    &serde_json::to_vec(&self.mine)?,
                    "application/json",
                    crate::Expect::Any,
                )?;
                Ok(Applied::Applied)
            }
        }
    }

    /// The next identity this commenter will use.
    ///
    /// Derived from the stream it has already written rather than kept in a
    /// counter file: the file is the record, so a stream that was restored from
    /// another machine still numbers its next comment after itself.
    fn tick(&mut self) -> BlockId {
        let next = self
            .mine
            .iter()
            .flat_map(stamps)
            .filter(|stamp| stamp.actor == self.actor)
            .map(|stamp| stamp.counter)
            .max()
            .unwrap_or(0)
            + 1;
        BlockId::new(next, &self.actor)
    }

    /// Every thread, with where it points in `doc` now.
    #[must_use]
    pub fn threads(&self, doc: &Doc) -> Vec<(&Thread, Resolution)> {
        self.book.resolved(doc)
    }

    /// One thread by identity.
    #[must_use]
    pub fn thread(&self, id: &BlockId) -> Option<&Thread> {
        self.book.thread(id)
    }

    #[must_use]
    pub fn book(&self) -> &CommentBook {
        &self.book
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.book.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.book.is_empty()
    }
}

/// Every identity one op mints.
fn stamps(op: &CommentOp) -> Vec<&BlockId> {
    match op {
        CommentOp::Open { id, .. } => vec![id],
        CommentOp::Say { id, .. } => vec![id],
        CommentOp::Settle { stamp, .. } => vec![&stamp.by],
    }
}

/// The thread an op names, for an error message.
fn named_thread(op: &CommentOp) -> Option<&BlockId> {
    match op {
        CommentOp::Open { .. } => None,
        CommentOp::Say { thread, .. } | CommentOp::Settle { thread, .. } => Some(thread),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Anchor, NodeRole};

    fn store() -> (tempfile::TempDir, Vfs) {
        let temp = tempfile::tempdir().unwrap();
        let cas = temp.path().join("cas");
        let vfs = Vfs::open_with(temp.path().join("root"), NodeRole::Full, Some(&cas)).unwrap();
        (temp, vfs)
    }

    /// A document with three paragraphs, plus the ids to hang threads on.
    fn doc() -> (Doc, Vec<BlockId>) {
        let mut doc = Doc::new("neo");
        let ops = doc.import("first paragraph\n\nsecond paragraph\n\nthird paragraph");
        doc.apply_all(&ops);
        let ids = doc.live().iter().map(|block| block.id.clone()).collect();
        (doc, ids)
    }

    #[test]
    fn two_commenters_see_one_thread_in_stamp_order() {
        // The whole reason each commenter owns a file: two people replying at
        // once must both survive, and both must read the same sequence.
        let (temp, mut vfs) = store();
        let (doc, ids) = doc();
        let anchor = Anchor::Block {
            block: ids[1].clone(),
        };

        let mut studio = CommentFile::open(&vfs, "notes", "studio").unwrap();
        let thread = studio
            .open_thread(&mut vfs, anchor, "why is this here?", 1_000)
            .unwrap();
        assert_eq!(thread.actor, "studio");

        let mut neo = CommentFile::open(&vfs, "notes", "neo").unwrap();
        assert_eq!(neo.len(), 1, "neo reads what studio wrote");
        neo.say(&mut vfs, &thread, "because the parser needs it", 2_000)
            .unwrap();

        // Reopening studio sees neo's reply, because the book is the merge of the
        // files rather than a copy of one.
        let studio = CommentFile::open(&vfs, "notes", "studio").unwrap();
        let threads = studio.threads(&doc);
        assert_eq!(threads.len(), 1);
        let (open, resolution) = &threads[0];
        assert_eq!(resolution, &Resolution::Exact(ids[1].clone()));
        assert_eq!(open.comments.len(), 2, "the question and the answer");
        assert!(
            open.comments[0].at_ms < open.comments[1].at_ms,
            "the question was said first, whatever the counters say"
        );
        assert_eq!(open.comments[1].author, "neo");

        // One file per commenter, both under the document.
        assert!(temp.path().join("root").exists());
        assert!(
            vfs.file(&comments_key("notes", "studio"))
                .unwrap()
                .is_some()
        );
        assert!(vfs.file(&comments_key("notes", "neo")).unwrap().is_some());
    }

    #[test]
    fn a_settle_and_a_reopen_survive_a_round_trip() {
        let (_temp, mut vfs) = store();
        let (doc, ids) = doc();
        let mut book = CommentFile::open(&vfs, "notes", "studio").unwrap();
        let thread = book
            .open_thread(
                &mut vfs,
                Anchor::Block {
                    block: ids[0].clone(),
                },
                "done?",
                1_000,
            )
            .unwrap();

        book.settle(&mut vfs, &thread, true, 1_000).unwrap();
        let settled = CommentFile::open(&vfs, "notes", "studio").unwrap();
        assert!(settled.thread(&thread).unwrap().resolved());

        let mut book = CommentFile::open(&vfs, "notes", "neo").unwrap();
        book.settle(&mut vfs, &thread, false, 2_000).unwrap();
        let reopened = CommentFile::open(&vfs, "notes", "studio").unwrap();
        assert!(
            !reopened.thread(&thread).unwrap().resolved(),
            "a reopen by somebody else reopens it for everybody"
        );
        assert_eq!(reopened.threads(&doc).len(), 1);
    }

    #[test]
    fn where_a_thread_points_is_computed_at_read_time() {
        // Nothing about the block's position is stored: deleting the paragraph
        // moves the thread to its neighbour without touching the comment file.
        let (_temp, mut vfs) = store();
        let (mut doc, ids) = doc();
        let mut book = CommentFile::open(&vfs, "notes", "studio").unwrap();
        let thread = book
            .open_thread(
                &mut vfs,
                Anchor::Block {
                    block: ids[1].clone(),
                },
                "middle paragraph",
                1_000,
            )
            .unwrap();

        let ops = doc.import("first paragraph\n\nthird paragraph");
        doc.apply_all(&ops);

        let book = CommentFile::open(&vfs, "notes", "studio").unwrap();
        let threads = book.threads(&doc);
        assert_eq!(threads[0].0.id, thread);
        assert_eq!(
            threads[0].1,
            Resolution::Neighbour(ids[2].clone()),
            "the paragraph is gone; the thread points where the reader would look"
        );
    }

    #[test]
    fn a_reply_to_a_thread_nobody_opened_is_an_error() {
        // Fail loud: silently holding the op would be a comment that never
        // appears and never explains itself.
        let (_temp, mut vfs) = store();
        let mut book = CommentFile::open(&vfs, "notes", "studio").unwrap();
        let error = book
            .say(&mut vfs, &BlockId::new(7, "nobody"), "hello?", 1_000)
            .unwrap_err();
        assert!(error.to_string().contains("no thread 7@nobody"), "{error}");
    }
}
