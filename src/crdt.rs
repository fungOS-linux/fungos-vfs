//! The merge layer: blocks with identities, and what happens when two replicas
//! disagree.
//!
//! This module owns convergence and nothing else. It does not know what a
//! document is for, where its bytes live, or how it gets to a machine — a
//! markdown file, a transcript, a deck and (later) a sheet are all clients that
//! hand it an ordered set of blocks and ask what the result is.
//!
//! Two decisions carry it:
//!
//! - **Position is identity.** A block is a [`BlockId`] that both replicas agree
//!   on before they can disagree about content, so an insert from one side and an
//!   edit from the other compose instead of clobbering. Ordering is derived from
//!   `(after, id)` alone — never from arrival order — which is what makes two
//!   replicas that saw the same ops in different orders agree.
//! - **Deletion leaves a tombstone.** A removed block keeps its place in the tree
//!   so the blocks after it stay anchored, and so an annotation pointing at it can
//!   resolve to a neighbour instead of dangling. Coverage is a rendering
//!   decision; the record keeps the fact.
//!
//! What is deliberately not here: character-level merging inside a block. Two
//! edits to one block resolve last-writer-wins by `(counter, actor)`, visibly and
//! deterministically, until a text CRDT replaces that rule. The interface is
//! built so it can: a block's content is replaced, never patched, by this module.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// Who an edit is attributed to. The actor is part of every identity, so two
/// machines generate the same counter without colliding.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BlockId {
    pub counter: u64,
    pub actor: String,
}

impl BlockId {
    #[must_use]
    pub fn new(counter: u64, actor: &str) -> Self {
        Self {
            counter,
            actor: actor.to_owned(),
        }
    }
}

/// One thing a replica learned.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Op {
    /// A new block, placed after another. `after: None` puts it first.
    Insert {
        id: BlockId,
        after: Option<BlockId>,
        text: String,
    },
    /// A block's content replaced wholesale.
    Update {
        id: BlockId,
        text: String,
        at: BlockId,
    },
    /// A block removed. It stays in the tree as a tombstone.
    Delete { id: BlockId, at: BlockId },
}

/// One block as a replica holds it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Block {
    pub id: BlockId,
    /// The block this one follows. `None` means the head of the document.
    pub after: Option<BlockId>,
    pub text: String,
    /// The stamp of the last write to `text`.
    pub revision: BlockId,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub deleted: bool,
}

/// What applying an op did, so a caller can tell a duplicate from news.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Applied {
    /// This replica already knew it.
    Duplicate,
    Applied,
    /// The op names a block this replica has not seen. Retried after the insert
    /// lands; a caller that gives up on these has dropped a write.
    Deferred,
}

/// A document as one replica sees it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Doc {
    actor: String,
    /// The next counter this replica will hand out.
    clock: u64,
    /// Every block this replica knows, tombstones included, keyed by identity.
    blocks: BTreeMap<BlockId, Block>,
}

impl Doc {
    #[must_use]
    pub fn new(actor: &str) -> Self {
        Self {
            actor: actor.to_owned(),
            clock: 1,
            blocks: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn actor(&self) -> &str {
        &self.actor
    }

    /// The next identity this replica will use. Counters only move forward, and
    /// they move past anything it has seen, so an old counter never repeats.
    fn tick(&mut self) -> BlockId {
        let id = BlockId::new(self.clock, &self.actor);
        self.clock += 1;
        id
    }

    /// Apply one op. Idempotent: applying the same op twice is a duplicate, not a
    /// second block.
    pub fn apply(&mut self, op: &Op) -> Applied {
        match op {
            Op::Insert { id, after, text } => {
                if self.blocks.contains_key(id) {
                    return Applied::Duplicate;
                }
                self.observe(id.counter);
                self.blocks.insert(
                    id.clone(),
                    Block {
                        id: id.clone(),
                        after: after.clone(),
                        text: text.clone(),
                        revision: id.clone(),
                        deleted: false,
                    },
                );
                Applied::Applied
            }
            Op::Update { id, text, at } => {
                self.observe(at.counter);
                let Some(block) = self.blocks.get_mut(id) else {
                    return Applied::Deferred;
                };
                if block.revision >= *at {
                    return Applied::Duplicate;
                }
                block.text.clone_from(text);
                // A write that wins is a write: it also brings a block back if the
                // side it is racing deleted it. Last writer wins, including deletes.
                block.deleted = false;
                block.revision = at.clone();
                Applied::Applied
            }
            Op::Delete { id, at } => {
                self.observe(at.counter);
                let Some(block) = self.blocks.get_mut(id) else {
                    return Applied::Deferred;
                };
                if block.revision >= *at || block.deleted {
                    return Applied::Duplicate;
                }
                block.deleted = true;
                block.revision = at.clone();
                Applied::Applied
            }
        }
    }

    /// Keep this replica's clock ahead of everything it has seen, so its next
    /// identity sorts after an edit it is answering.
    fn observe(&mut self, counter: u64) {
        self.clock = self.clock.max(counter + 1);
    }

    /// Apply a batch, in any order. The result does not depend on which order.
    ///
    /// An op that names a block this replica has not seen is held and retried
    /// after the rest of the batch, because a batch that arrives newest-first is
    /// normal — compaction reorders, a pull resumes mid-stream. Whatever is still
    /// waiting is returned rather than dropped: a caller that ignores it is
    /// discarding writes.
    pub fn apply_all(&mut self, ops: &[Op]) -> Vec<Op> {
        let mut pending: Vec<&Op> = ops.iter().collect();
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

    /// Every block in document order, tombstones included.
    ///
    /// Order is a function of `(after, id)` and nothing else: siblings under the
    /// same anchor are visited in id order, so two replicas holding the same ops
    /// traverse the same tree. A block whose anchor never arrived is treated as
    /// anchored at the head rather than dropped — a replica that is missing an
    /// insert still shows the text.
    #[must_use]
    pub fn order(&self) -> Vec<&Block> {
        let mut children: BTreeMap<Option<&BlockId>, BTreeSet<&BlockId>> = BTreeMap::new();
        for block in self.blocks.values() {
            let anchor = block
                .after
                .as_ref()
                .filter(|after| self.blocks.contains_key(*after));
            children.entry(anchor).or_default().insert(&block.id);
        }
        let mut out = Vec::with_capacity(self.blocks.len());
        let mut seen = BTreeSet::new();
        // One frame per level: the children still to visit, deepest level last.
        // Siblings are visited in *descending* id order, which is what makes
        // "insert after A" land between A and the block that already followed it:
        // the newest insert at an anchor is the closest to it.
        let mut stack: Vec<Vec<&BlockId>> = Vec::new();
        let roots = children.get(&None);
        if let Some(roots) = roots {
            stack.push(roots.iter().copied().collect());
        }
        while let Some(frame) = stack.last_mut() {
            let Some(id) = frame.pop() else {
                stack.pop();
                continue;
            };
            if !seen.insert(id) {
                continue;
            }
            let block = &self.blocks[id];
            out.push(block);
            let next = children.get(&Some(&block.id));
            if let Some(next) = next {
                stack.push(next.iter().copied().collect());
            }
        }
        // A cycle in `after` links cannot come from honest ops, but a corrupt
        // record could produce one; anything unreachable is appended rather than
        // silently vanishing.
        for block in self.blocks.values() {
            if !seen.contains(&block.id) {
                out.push(block);
            }
        }
        out
    }

    /// The blocks that carry text, in order.
    #[must_use]
    pub fn live(&self) -> Vec<&Block> {
        self.order().into_iter().filter(|b| !b.deleted).collect()
    }

    /// The document as markdown: blocks separated by a blank line.
    #[must_use]
    pub fn render(&self) -> String {
        let mut out = String::new();
        for block in self.live() {
            if !out.is_empty() {
                out.push_str("\n\n");
            }
            out.push_str(block.text.trim_end_matches('\n'));
        }
        out
    }

    #[must_use]
    pub fn block(&self, id: &BlockId) -> Option<&Block> {
        self.blocks.get(id)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.blocks.values().filter(|b| !b.deleted).count()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Turn a file's contents into ops, against what this replica already holds.
    ///
    /// This is the seam that lets a plain editor — or an agent's `write` tool — be
    /// a first-class author: the file is one view of the document, and a save is a
    /// batch of ops from whoever touched it.
    ///
    /// Blocks that read the same in both versions are left alone. A run that
    /// changed is replaced: delete the old blocks, insert the new text anchored to
    /// the last block that survived. An unchanged file produces no ops at all.
    pub fn import(&mut self, text: &str) -> Vec<Op> {
        let incoming = split_blocks(text);
        let current: Vec<(BlockId, String)> = self
            .live()
            .into_iter()
            .map(|block| (block.id.clone(), block.text.clone()))
            .collect();
        let old: Vec<&str> = current.iter().map(|(_, text)| text.as_str()).collect();
        let new: Vec<&str> = incoming.iter().map(String::as_str).collect();
        // One slot per old block: which new block it survived as, if any.
        let survived = match_blocks(&old, &new);
        let mut ops = Vec::new();
        // A run of changed blocks between two survivors pairs up by position: the
        // first `min(old, new)` are the same block rewritten, which keeps its
        // identity — and therefore every annotation anchored to it — while an edit
        // that only grows a run inserts the surplus instead of rewriting a
        // neighbour.
        let mut pairs: Vec<(usize, usize)> = survived
            .iter()
            .enumerate()
            .filter_map(|(old_index, new)| new.map(|new| (old_index, new)))
            .collect();
        pairs.push((current.len(), incoming.len()));

        let mut last_placed: Option<BlockId> = None;
        let (mut old_cursor, mut new_cursor) = (0usize, 0usize);
        for (old_match, new_match) in pairs {
            let mut old_index = old_cursor;
            let mut new_index = new_cursor;
            while old_index < old_match && new_index < new_match {
                let at = self.tick();
                ops.push(Op::Update {
                    id: current[old_index].0.clone(),
                    text: incoming[new_index].clone(),
                    at,
                });
                last_placed = Some(current[old_index].0.clone());
                old_index += 1;
                new_index += 1;
            }
            for (id, _) in &current[old_index..old_match] {
                let at = self.tick();
                ops.push(Op::Delete { id: id.clone(), at });
            }
            for text in &incoming[new_index..new_match] {
                let id = self.tick();
                ops.push(Op::Insert {
                    id: id.clone(),
                    after: last_placed.clone(),
                    text: text.clone(),
                });
                last_placed = Some(id);
            }
            if old_match < current.len() {
                last_placed = Some(current[old_match].0.clone());
                old_cursor = old_match + 1;
                new_cursor = new_match + 1;
            }
        }
        ops
    }
}

/// Split markdown into blocks: a blank line ends one, and a fenced code block is
/// one block no matter how many blank lines it contains.
#[must_use]
pub fn split_blocks(text: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut current: Vec<&str> = Vec::new();
    let mut fence: Option<String> = None;
    for line in text.lines() {
        let trimmed = line.trim_start();
        let opens = trimmed.starts_with("```") || trimmed.starts_with("~~~");
        match &fence {
            Some(marker) => {
                current.push(line);
                if opens && trimmed.starts_with(marker.as_str()) {
                    fence = None;
                }
                continue;
            }
            None if opens => {
                fence = Some(trimmed[..3].to_owned());
                current.push(line);
                continue;
            }
            None => {}
        }
        if line.trim().is_empty() {
            if !current.is_empty() {
                blocks.push(current.join("\n"));
                current.clear();
            }
            continue;
        }
        current.push(line);
    }
    if !current.is_empty() {
        blocks.push(current.join("\n"));
    }
    blocks
}

/// Which old block each new block corresponds to, by position.
///
/// Returns one slot per *old* block holding the index of the new block it read the
/// same as — the same shape a diff has, because the merge is a diff. Equal runs
/// are matched by longest common subsequence; when a document is large enough that
/// the table would be silly, it falls back to matching only the prefix and suffix,
/// which still keeps untouched regions untouched.
#[must_use]
pub fn match_blocks(old: &[&str], new: &[&str]) -> Vec<Option<usize>> {
    let mut slots = vec![None; old.len()];
    if old.is_empty() || new.is_empty() {
        return slots;
    }
    if old.len().saturating_mul(new.len()) > 4_000_000 {
        let mut head = 0;
        while head < old.len() && head < new.len() && old[head] == new[head] {
            slots[head] = Some(head);
            head += 1;
        }
        let mut tail = 0;
        while tail < old.len() - head
            && tail < new.len() - head
            && old[old.len() - 1 - tail] == new[new.len() - 1 - tail]
        {
            slots[old.len() - 1 - tail] = Some(new.len() - 1 - tail);
            tail += 1;
        }
        return slots;
    }

    // LCS table, bottom-up, then walk it forward to mark matches.
    let mut table = vec![vec![0u32; new.len() + 1]; old.len() + 1];
    for i in (0..old.len()).rev() {
        for j in (0..new.len()).rev() {
            table[i][j] = if old[i] == new[j] {
                table[i + 1][j + 1] + 1
            } else {
                table[i + 1][j].max(table[i][j + 1])
            };
        }
    }
    let (mut i, mut j) = (0usize, 0usize);
    while i < old.len() && j < new.len() {
        if old[i] == new[j] {
            slots[i] = Some(j);
            i += 1;
            j += 1;
        } else if table[i + 1][j] >= table[i][j + 1] {
            i += 1;
        } else {
            j += 1;
        }
    }
    slots
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = "# Notes\n\nfirst paragraph\n\nsecond paragraph";

    /// A replica that has seen `ops`, as a second machine would after pulling.
    fn replica(actor: &str, ops: &[Op]) -> Doc {
        let mut doc = Doc::new(actor);
        doc.apply_all(ops);
        doc
    }

    #[test]
    fn two_replicas_editing_different_paragraphs_converge() {
        // The claim the whole hybrid rests on: two machines change the same file
        // without a coordinator, and afterwards both hold both changes.
        let mut neo = Doc::new("neo");
        let base = neo.import(BASE);
        neo.apply_all(&base);
        let mut studio = replica("studio", &base);

        // neo rewrites the middle paragraph; studio appends one.
        let edited = BASE.replace("first paragraph", "first paragraph, edited");
        let neo_ops = neo.import(&edited);
        neo.apply_all(&neo_ops);

        let appended = format!("{BASE}\n\nthird paragraph");
        let studio_ops = studio.import(&appended);
        studio.apply_all(&studio_ops);

        // Each learns what the other did, in the opposite order.
        neo.apply_all(&studio_ops);
        studio.apply_all(&neo_ops);

        assert_eq!(neo.render(), studio.render());
        let text = neo.render();
        assert!(text.contains("first paragraph, edited"), "{text}");
        assert!(text.contains("third paragraph"), "{text}");
        assert!(
            text.find("edited").unwrap() < text.find("third").unwrap(),
            "the edit stays where it was and the addition stays at the end: {text}"
        );
    }

    #[test]
    fn an_unchanged_file_produces_no_ops() {
        let mut doc = Doc::new("neo");
        let ops = doc.import(BASE);
        doc.apply_all(&ops);
        assert_eq!(doc.render(), BASE);
        assert!(doc.import(BASE).is_empty());
        assert!(doc.import(&format!("{BASE}\n\n")).is_empty());
    }

    #[test]
    fn editing_a_paragraph_keeps_its_identity() {
        // An annotation hangs off the block, so a rewrite has to be the same block
        // with new text — not a delete and an insert.
        let mut doc = Doc::new("neo");
        let ops = doc.import(BASE);
        doc.apply_all(&ops);
        let before = doc.live()[1].id.clone();

        let ops = doc.import(&BASE.replace("first", "the first"));
        assert_eq!(ops.len(), 1, "one rewrite, not a delete plus an insert");
        assert!(matches!(ops[0], Op::Update { .. }));
        doc.apply_all(&ops);

        assert_eq!(doc.live()[1].id, before);
        assert!(doc.render().contains("the first paragraph"));
        assert_eq!(doc.len(), 3);
    }

    #[test]
    fn a_paragraph_added_in_the_middle_stays_there() {
        let mut doc = Doc::new("neo");
        let ops = doc.import("a\n\nc");
        doc.apply_all(&ops);

        let ops = doc.import("a\n\nb\n\nc");
        doc.apply_all(&ops);
        assert_eq!(doc.render(), "a\n\nb\n\nc");
    }

    #[test]
    fn two_edits_to_one_block_resolve_by_stamp() {
        // Inside a block the rule is last-writer-wins until a text CRDT replaces
        // it. What matters now is that both replicas pick the same winner.
        let mut neo = Doc::new("neo");
        let base = neo.import(BASE);
        neo.apply_all(&base);
        let mut studio = replica("studio", &base);

        let neo_ops = neo.import(&BASE.replace("first", "NEO"));
        neo.apply_all(&neo_ops);
        let studio_ops = studio.import(&BASE.replace("first", "STUDIO"));
        studio.apply_all(&studio_ops);

        neo.apply_all(&studio_ops);
        studio.apply_all(&neo_ops);

        assert_eq!(neo.render(), studio.render());
        let text = neo.render();
        assert!(
            text.contains("NEO") ^ text.contains("STUDIO"),
            "exactly one side wins the block: {text}"
        );
        assert!(!text.contains("first paragraph"));
    }

    #[test]
    fn ops_apply_in_any_order() {
        let mut writer = Doc::new("neo");
        let mut ops = writer.import(BASE);
        writer.apply_all(&ops);
        ops.extend(writer.import(&BASE.replace("second", "last")));
        ops.push(Op::Insert {
            id: BlockId::new(99, "studio"),
            after: None,
            text: "opening line".to_owned(),
        });

        let forward = replica("a", &ops);
        let mut backward_ops = ops.clone();
        backward_ops.reverse();
        let backward = replica("b", &backward_ops);
        assert_eq!(forward.render(), backward.render());
        assert_eq!(
            forward.render(),
            "opening line\n\n# Notes\n\nfirst paragraph\n\nlast paragraph"
        );
    }

    #[test]
    fn a_deleted_block_keeps_its_place_for_the_blocks_after_it() {
        let mut doc = Doc::new("neo");
        let ops = doc.import("a\n\nb\n\nc");
        doc.apply_all(&ops);

        let ops = doc.import("a\n\nc");
        doc.apply_all(&ops);
        assert_eq!(doc.render(), "a\n\nc");
        // The tombstone is still in the tree, so an annotation on it can resolve
        // and the block after it has not moved.
        assert_eq!(doc.order().len(), 3);
        assert_eq!(doc.len(), 2);
    }

    #[test]
    fn a_fenced_code_block_is_one_block_however_many_blank_lines() {
        let blocks = split_blocks("intro\n\n```sh\none\n\ntwo\n```\n\nafter");
        assert_eq!(blocks.len(), 3);
        assert_eq!(blocks[1], "```sh\none\n\ntwo\n```");

        // And it survives a round trip through the document.
        let mut doc = Doc::new("neo");
        let ops = doc.import("intro\n\n```sh\none\n\ntwo\n```\n\nafter");
        doc.apply_all(&ops);
        assert_eq!(doc.render(), "intro\n\n```sh\none\n\ntwo\n```\n\nafter");
    }

    #[test]
    fn an_edit_to_a_deleted_block_brings_it_back() {
        // A delete that loses the race must not win by attrition.
        let id = BlockId::new(1, "neo");
        let mut deleted = Doc::new("neo");
        deleted.apply(&Op::Insert {
            id: id.clone(),
            after: None,
            text: "gone".to_owned(),
        });
        deleted.apply(&Op::Delete {
            id: id.clone(),
            at: BlockId::new(2, "neo"),
        });
        assert_eq!(deleted.render(), "");

        deleted.apply(&Op::Update {
            id,
            text: "back".to_owned(),
            at: BlockId::new(3, "studio"),
        });
        assert_eq!(deleted.render(), "back");
    }
}
