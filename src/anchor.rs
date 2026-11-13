//! Annotations: the thing comments are made of.
//!
//! A comment is not attached to a position in a file. It is attached to a
//! [`BlockId`], and this module is what answers "where is that now?" after
//! somebody else edited the document.
//!
//! The two properties that make that answer trustworthy are not built here —
//! they are built by the merge layer, and this module exists to *pin them as a
//! contract*:
//!
//! - **A rewrite is an update, not a delete and an insert** ([`crate::crdt`]), so
//!   an anchor on a paragraph survives that paragraph being rewritten.
//! - **A deletion leaves a tombstone**, so an anchor on a deleted paragraph
//!   resolves to the neighbour that took its place instead of dangling.
//!
//! A range inside a block is the weakest part on purpose. Until a text CRDT gives
//! every character an identity, a range is byte offsets into the block's current
//! text: it clamps when the text shortens and it cannot follow a reordering
//! inside the block. That is reported as [`Resolution::Clamped`] rather than
//! silently presented as exact, because a comment that quietly points at the
//! wrong words is worse than one that says it moved.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{Applied, BlockId, Doc};

/// What an annotation is attached to.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Anchor {
    /// The whole block.
    Block { block: BlockId },
    /// A byte range in the block's text.
    Range {
        block: BlockId,
        start: usize,
        end: usize,
    },
}

impl Anchor {
    #[must_use]
    pub fn block(&self) -> &BlockId {
        match self {
            Self::Block { block } | Self::Range { block, .. } => block,
        }
    }
}

/// Where an annotation points now.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Resolution {
    /// The block is here, unchanged.
    Exact(BlockId),
    /// The block is here, but the text changed under the range.
    Clamped(BlockId),
    /// The block is gone; this is the live block that took its place.
    Neighbour(BlockId),
    /// There is nothing left to point at.
    Orphan,
}

impl Resolution {
    #[must_use]
    pub fn block(&self) -> Option<&BlockId> {
        match self {
            Self::Exact(id) | Self::Clamped(id) | Self::Neighbour(id) => Some(id),
            Self::Orphan => None,
        }
    }
}

impl Anchor {
    /// Resolve against the document as it stands.
    ///
    /// A block that exists but whose text is shorter than the range clamps —
    /// reported, never hidden. A block that was deleted resolves to the next live
    /// block, else the previous one: the place the reader would look.
    #[must_use]
    pub fn resolve(&self, doc: &Doc) -> Resolution {
        let id = self.block();
        match doc.block(id) {
            Some(block) if !block.deleted => {
                if let Self::Range { start, end, .. } = self {
                    let len = block.text.len();
                    if *end > len || *start > len {
                        return Resolution::Clamped(id.clone());
                    }
                }
                Resolution::Exact(id.clone())
            }
            _ => match neighbour(doc, id) {
                Some(id) => Resolution::Neighbour(id),
                None => Resolution::Orphan,
            },
        }
    }

    /// The range, clamped into the block's current text.
    #[must_use]
    pub fn range(&self, doc: &Doc) -> Option<(usize, usize)> {
        let Self::Range { start, end, .. } = self else {
            return None;
        };
        let len = doc.block(self.block()).map(|b| b.text.len()).unwrap_or(0);
        let start = (*start).min(len);
        Some((start, (*end).clamp(start, len)))
    }
}

/// The live block a tombstone stood in front of, else behind.
fn neighbour(doc: &Doc, deleted: &BlockId) -> Option<BlockId> {
    let order = doc.order();
    let index = order.iter().position(|block| &block.id == deleted);
    for block in order.iter().skip(index.map_or(0, |i| i + 1)) {
        if !block.deleted {
            return Some(block.id.clone());
        }
    }
    for block in order.iter().take(index.unwrap_or(0)).rev() {
        if !block.deleted {
            return Some(block.id.clone());
        }
    }
    None
}

/// One thing somebody said.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Comment {
    pub id: BlockId,
    pub author: String,
    pub body: String,
}

/// A comment thread, anchored to one place.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Thread {
    pub id: BlockId,
    pub anchor: Anchor,
    pub comments: Vec<Comment>,
    /// Set while the thread is resolved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_at: Option<BlockId>,
    /// The stamp of the last settle or reopen, so a stale one cannot win later.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settled: Option<BlockId>,
}

impl Thread {
    #[must_use]
    pub fn resolved(&self) -> bool {
        self.resolved_at.is_some()
    }
}

/// What a comment is.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum CommentOp {
    Open {
        id: BlockId,
        anchor: Anchor,
    },
    Say {
        thread: BlockId,
        id: BlockId,
        author: String,
        body: String,
    },
    /// Mark resolved, or reopen with `resolved: false`.
    Settle {
        thread: BlockId,
        at: BlockId,
        resolved: bool,
    },
}

/// Every thread on one document.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CommentBook {
    threads: BTreeMap<BlockId, Thread>,
}

impl CommentBook {
    /// Apply one op. Idempotent, and order-independent for anything that does not
    /// name a thread this book has not seen — those are deferred like an edit to
    /// an unseen block.
    pub fn apply(&mut self, op: &CommentOp) -> Applied {
        match op {
            CommentOp::Open { id, anchor } => {
                if self.threads.contains_key(id) {
                    return Applied::Duplicate;
                }
                self.threads.insert(
                    id.clone(),
                    Thread {
                        id: id.clone(),
                        anchor: anchor.clone(),
                        comments: Vec::new(),
                        resolved_at: None,
                        settled: None,
                    },
                );
                Applied::Applied
            }
            CommentOp::Say {
                thread,
                id,
                author,
                body,
            } => {
                let Some(open) = self.threads.get_mut(thread) else {
                    return Applied::Deferred;
                };
                if open.comments.iter().any(|comment| &comment.id == id) {
                    return Applied::Duplicate;
                }
                open.comments.push(Comment {
                    id: id.clone(),
                    author: author.clone(),
                    body: body.clone(),
                });
                // Two replies that arrive out of order are the same set; sort by
                // stamp so every replica shows the same sequence.
                open.comments.sort_by(|a, b| a.id.cmp(&b.id));
                Applied::Applied
            }
            CommentOp::Settle {
                thread,
                at,
                resolved,
            } => {
                let Some(open) = self.threads.get_mut(thread) else {
                    return Applied::Deferred;
                };
                if open.settled.as_ref().is_some_and(|current| current >= at) {
                    return Applied::Duplicate;
                }
                open.settled = Some(at.clone());
                // A reopen has to remember that it won, not merely forget the
                // settle: otherwise a stale settle arriving afterwards wins.
                open.resolved_at = resolved.then(|| at.clone());
                Applied::Applied
            }
        }
    }

    /// Apply a batch in any order, retrying what referred to a thread that had not
    /// arrived yet.
    pub fn apply_all(&mut self, ops: &[CommentOp]) -> Vec<CommentOp> {
        let mut pending: Vec<&CommentOp> = ops.iter().collect();
        loop {
            let mut deferred = Vec::new();
            for op in pending {
                if self.apply(op) == Applied::Deferred {
                    deferred.push(op);
                }
            }
            if deferred.is_empty() || deferred.len() == ops.len() {
                return deferred.into_iter().cloned().collect();
            }
            pending = deferred;
        }
    }

    #[must_use]
    pub fn thread(&self, id: &BlockId) -> Option<&Thread> {
        self.threads.get(id)
    }

    /// Every thread, oldest first, with where it points now.
    #[must_use]
    pub fn resolved(&self, doc: &Doc) -> Vec<(&Thread, Resolution)> {
        self.threads
            .values()
            .map(|thread| (thread, thread.anchor.resolve(doc)))
            .collect()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.threads.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.threads.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Doc;

    /// A document with the blocks in one column, and the ids to hang threads on.
    fn doc() -> (Doc, Vec<BlockId>) {
        let mut doc = Doc::new("neo");
        let ops = doc.import("first paragraph\n\nsecond paragraph\n\nthird paragraph");
        doc.apply_all(&ops);
        let ids: Vec<BlockId> = doc.live().iter().map(|b| b.id.clone()).collect();
        (doc, ids)
    }

    #[test]
    fn a_comment_survives_the_paragraph_being_rewritten() {
        // The reason anchors hang off a block id rather than a position: a rewrite
        // is an update, so the thread is still on the same block afterwards.
        let (mut doc, ids) = doc();
        let mut book = CommentBook::default();
        book.apply(&CommentOp::Open {
            id: BlockId::new(1, "studio"),
            anchor: Anchor::Block {
                block: ids[1].clone(),
            },
        });

        let ops = doc.import("first paragraph\n\nsecond paragraph, rewritten\n\nthird paragraph");
        doc.apply_all(&ops);

        let threads = book.resolved(&doc);
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0].1, Resolution::Exact(ids[1].clone()));
    }

    #[test]
    fn a_comment_on_a_deleted_paragraph_resolves_to_its_neighbour() {
        // Deletion leaves a tombstone, so the thread is not lost — it moves to the
        // paragraph that took the place the reader was looking at.
        let (mut doc, ids) = doc();
        let mut book = CommentBook::default();
        book.apply(&CommentOp::Open {
            id: BlockId::new(1, "studio"),
            anchor: Anchor::Block {
                block: ids[1].clone(),
            },
        });

        let ops = doc.import("first paragraph\n\nthird paragraph");
        doc.apply_all(&ops);

        let threads = book.resolved(&doc);
        assert_eq!(threads[0].1, Resolution::Neighbour(ids[2].clone()));
    }

    #[test]
    fn a_range_that_no_longer_fits_is_reported_as_clamped() {
        let (mut doc, ids) = doc();
        let anchor = Anchor::Range {
            block: ids[0].clone(),
            start: 0,
            end: 15,
        };
        assert_eq!(anchor.resolve(&doc), Resolution::Exact(ids[0].clone()));
        assert_eq!(anchor.range(&doc), Some((0, 15)));

        let ops = doc.import("first\n\nsecond paragraph\n\nthird paragraph");
        doc.apply_all(&ops);

        assert_eq!(anchor.resolve(&doc), Resolution::Clamped(ids[0].clone()));
        assert_eq!(anchor.range(&doc), Some((0, 5)), "clamped to the new text");
    }

    #[test]
    fn a_thread_with_no_block_left_is_an_orphan() {
        let (mut doc, ids) = doc();
        let anchor = Anchor::Block {
            block: ids[0].clone(),
        };
        let ops = doc.import("");
        doc.apply_all(&ops);
        assert_eq!(anchor.resolve(&doc), Resolution::Orphan);
    }

    #[test]
    fn replies_and_settles_merge_the_same_way_from_either_side() {
        let (doc, ids) = doc();
        let open = CommentOp::Open {
            id: BlockId::new(1, "studio"),
            anchor: Anchor::Block {
                block: ids[1].clone(),
            },
        };
        let reply_a = CommentOp::Say {
            thread: BlockId::new(1, "studio"),
            id: BlockId::new(2, "neo"),
            author: "neo".to_owned(),
            body: "agreed".to_owned(),
        };
        let reply_b = CommentOp::Say {
            thread: BlockId::new(1, "studio"),
            id: BlockId::new(2, "studio"),
            author: "studio".to_owned(),
            body: "and one more thing".to_owned(),
        };
        let settle = CommentOp::Settle {
            thread: BlockId::new(1, "studio"),
            at: BlockId::new(3, "neo"),
            resolved: true,
        };
        let reopen = CommentOp::Settle {
            thread: BlockId::new(1, "studio"),
            at: BlockId::new(4, "studio"),
            resolved: false,
        };

        let forward = [
            open.clone(),
            reply_a.clone(),
            reply_b.clone(),
            settle.clone(),
            reopen.clone(),
        ];
        let mut one = CommentBook::default();
        assert!(one.apply_all(&forward).is_empty());

        let mut backward_ops = forward;
        backward_ops.reverse();
        let mut two = CommentBook::default();
        assert!(two.apply_all(&backward_ops).is_empty());

        let thread = BlockId::new(1, "studio");
        let one = one.thread(&thread).unwrap();
        let two = two.thread(&thread).unwrap();
        assert_eq!(one, two, "the same ops in either order give one thread");
        assert_eq!(one.comments.len(), 2, "both replies, in stamp order");
        assert!(!one.resolved(), "the later reopen wins");
        let _ = doc;
    }

    #[test]
    fn a_late_settle_does_not_beat_a_reopen() {
        let (_, ids) = doc();
        let thread = BlockId::new(1, "studio");
        let mut book = CommentBook::default();
        book.apply(&CommentOp::Open {
            id: thread.clone(),
            anchor: Anchor::Block {
                block: ids[0].clone(),
            },
        });
        book.apply(&CommentOp::Settle {
            thread: thread.clone(),
            at: BlockId::new(9, "studio"),
            resolved: false,
        });
        // A settle stamped earlier, arriving later, must not win.
        book.apply(&CommentOp::Settle {
            thread: thread.clone(),
            at: BlockId::new(3, "neo"),
            resolved: true,
        });
        assert!(!book.thread(&thread).unwrap().resolved());
    }
}
