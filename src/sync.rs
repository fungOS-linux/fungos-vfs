//! Anti-entropy between two stores.
//!
//! The op log already records who wrote, when, in what order, against which
//! head, and for which worktree. This module is what uses that to move work
//! between stores, and it is deliberately transport-free: a peer session hands
//! ops and bytes back and forth in memory so the protocol can be tested without
//! a network, and Unibus becomes a carrier rather than the design.
//!
//! Three failures matter, and each has one answer here:
//!
//! - **Duplicates.** A sender that retries must not apply an op twice. Every
//!   applied remote op is recorded by `(origin, hlc)`, which is what the cursor
//!   is built from, so a second delivery is recognised before any work is done.
//! - **Divergence.** An op carries the head it was written against. If the
//!   receiver's head has moved, applying it would discard the intervening
//!   write, so the op is reported as diverged instead — never merged silently.
//! - **Missing content.** An op names content the receiver may not hold. The
//!   receiver asks for it by content id, and what arrives is verified against
//!   that id before it is stored, so a corrupted transfer cannot poison the
//!   store.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{ContentId, Hlc, SyncOpKind, Vfs, VfsError};

/// The highest clock reading this store has seen from each peer store.
///
/// Sending this to a peer is the whole request: "tell me what happened after
/// these points". It is per store rather than a single number because two
/// devices advancing independently produce interleaved clocks that no single
/// high-water mark describes.
///
/// Keyed by store identity, not by origin: origin names the machine, so two
/// stores on one host share it and would mask each other's newer ops.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncCursor {
    seen: BTreeMap<String, Hlc>,
}

impl SyncCursor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, origin: &str) -> Option<Hlc> {
        self.seen.get(origin).copied()
    }

    pub fn observe(&mut self, origin: &str, hlc: Hlc) {
        let entry = self.seen.entry(origin.to_string()).or_default();
        if hlc > *entry {
            *entry = hlc;
        }
    }

    pub fn origins(&self) -> impl Iterator<Item = (&str, Hlc)> {
        self.seen
            .iter()
            .map(|(origin, hlc)| (origin.as_str(), *hlc))
    }

    /// Whether `op` is already covered by this cursor.
    pub fn covers(&self, op: &crate::SyncOp) -> bool {
        self.get(&op.store).is_some_and(|hlc| hlc >= op.hlc)
    }
}

/// A sync request, as it travels.
///
/// The wire is deliberately plain: a carrier only has to move these bytes, so
/// ssh today and a peer session later are the same protocol over a different
/// pipe.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncRequest {
    pub cursor: SyncCursor,
    pub limit: usize,
}

/// A responder's answer, with the content the requester cannot supply itself.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncResponse {
    pub ops: Vec<crate::SyncOp>,
    pub content: Vec<ContentBlob>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContentBlob {
    pub id: String,
    /// Hex, not base64: this crate has no encoder and hex needs no dependency.
    pub data: String,
}

impl ContentBlob {
    pub fn from_bytes(id: &ContentId, bytes: &[u8]) -> Self {
        Self {
            id: id.to_string(),
            data: hex_encode(bytes),
        }
    }

    pub fn decode(&self) -> Result<Vec<u8>, VfsError> {
        hex_decode(&self.data)
    }
}

/// What happened to one remote op.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// The write landed here.
    Applied,
    /// Already applied; nothing to do.
    Duplicate,
    /// The head moved under the op, so applying it would discard that write.
    Diverged,
}

/// One store's answer to a sync request.
pub struct SyncBatch {
    pub ops: Vec<crate::SyncOp>,
    /// Content the sender must be asked for before these ops can be applied.
    pub missing_content: Vec<ContentId>,
}

impl Vfs {
    /// Build the answer a carrier sends back: the ops the requester lacks, and
    /// the bytes they carry.
    pub fn serve_sync(&mut self, request: &SyncRequest) -> Result<SyncResponse, VfsError> {
        let batch = self.answer_sync(&request.cursor, request.limit)?;
        let mut content = Vec::new();
        for op in &batch.ops {
            let Some(content_id) = &op.content_id else {
                continue;
            };
            // Read by content id, not by path: the path may have moved on since
            // the op was written, but the blob it names has not.
            match self.content(content_id) {
                Ok(bytes) => content.push(ContentBlob::from_bytes(content_id, &bytes)),
                Err(_) => continue,
            }
        }
        Ok(SyncResponse {
            ops: batch.ops,
            content,
        })
    }

    /// Apply a response, returning one outcome per op in order.
    pub fn apply_sync(&mut self, response: &SyncResponse) -> Result<Vec<ApplyOutcome>, VfsError> {
        let mut blobs = BTreeMap::new();
        for blob in &response.content {
            blobs.insert(blob.id.clone(), blob.decode()?);
        }
        let mut outcomes = Vec::with_capacity(response.ops.len());
        for op in &response.ops {
            let bytes = op
                .content_id
                .as_ref()
                .and_then(|id| blobs.get(&id.to_string()))
                .map(Vec::as_slice);
            outcomes.push(self.apply_remote(op, bytes)?);
        }
        Ok(outcomes)
    }

    /// Everything this store has seen, expressed as a cursor.
    pub fn sync_cursor(&self) -> Result<SyncCursor, VfsError> {
        let mut cursor = SyncCursor::new();
        let mut statement = self
            .db
            .prepare("SELECT store, hlc_ms, hlc_counter FROM sync_queue")?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                Hlc {
                    ms: row.get(1)?,
                    counter: row.get::<_, i64>(2)? as u32,
                },
            ))
        })?;
        for row in rows {
            let (origin, hlc) = row?;
            cursor.observe(&origin, hlc);
        }
        for (store, hlc) in self
            .applied_cursor()?
            .origins()
            .map(|(s, h)| (s.to_string(), h))
        {
            cursor.observe(&store, hlc);
        }
        Ok(cursor)
    }

    /// Ops this store holds that `cursor` has not seen, in replay order.
    ///
    /// Filtered in the store rather than in SQL because "after this clock, for
    /// this origin" is a two-column comparison that a cursor map expresses more
    /// clearly than a growing `OR` clause.
    pub fn ops_after(
        &self,
        cursor: &SyncCursor,
        limit: usize,
    ) -> Result<Vec<crate::SyncOp>, VfsError> {
        let mut ops = self.pending(limit.saturating_mul(4).max(limit))?;
        ops.retain(|op| !cursor.covers(op));
        ops.truncate(limit);
        Ok(ops)
    }

    /// Build the answer to a peer's request: the ops it lacks, and which of
    /// their content ids this store cannot supply either.
    pub fn answer_sync(
        &mut self,
        cursor: &SyncCursor,
        limit: usize,
    ) -> Result<SyncBatch, VfsError> {
        let ops = self.ops_after(cursor, limit)?;
        let mut missing_content = Vec::new();
        for op in &ops {
            let Some(content_id) = &op.content_id else {
                continue;
            };
            if !self.has_content(content_id)? {
                missing_content.push(content_id.clone());
            }
        }
        Ok(SyncBatch {
            ops,
            missing_content,
        })
    }

    /// Apply one remote op, with its bytes if the op carries content.
    ///
    /// Idempotent: a duplicate is recognised from `(origin, hlc)` before any
    /// write, so a retrying sender costs nothing after the first delivery.
    pub fn apply_remote(
        &mut self,
        op: &crate::SyncOp,
        bytes: Option<&[u8]>,
    ) -> Result<ApplyOutcome, VfsError> {
        if self.mark_applied(&op.store, op.hlc)? == 0 {
            return Ok(ApplyOutcome::Duplicate);
        }

        match op.kind {
            SyncOpKind::Upsert => {
                let content_id = op
                    .content_id
                    .as_ref()
                    .ok_or_else(|| VfsError::InvalidContentId(op.path.clone()))?;
                let bytes = bytes.ok_or_else(|| VfsError::NotHydrated(content_id.to_string()))?;
                if &ContentId::from_bytes(bytes) != content_id {
                    return Err(VfsError::InvalidContentId(content_id.to_string()));
                }
                // The op says what it was built on. If this store has moved on,
                // the write is refused rather than reordered into a silent
                // overwrite — the caller decides what to do about divergence.
                let current = self.file(&op.path)?;
                let current_id = current.as_ref().map(|entry| &entry.content_id);
                if current_id != op.base_content_id.as_ref() {
                    self.unmark_applied(&op.store, op.hlc)?;
                    return Ok(ApplyOutcome::Diverged);
                }
                self.apply_put(&op.path, bytes, current.as_ref(), &op.actor, &op.origin)?;
            }
            SyncOpKind::Remove => {
                if self.file(&op.path)?.is_some() {
                    self.apply_remove(&op.path)?;
                }
            }
            SyncOpKind::Mkdir => {
                if matches!(self.is_directory(&op.path), Ok(false)) {
                    self.apply_mkdir(&op.path)?;
                }
            }
            SyncOpKind::Rmdir => {
                if matches!(self.is_directory(&op.path), Ok(true)) {
                    self.apply_rmdir(&op.path)?;
                }
            }
        }
        Ok(ApplyOutcome::Applied)
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from_digit(u32::from(byte >> 4), 16).unwrap_or('0'));
        out.push(char::from_digit(u32::from(byte & 0x0f), 16).unwrap_or('0'));
    }
    out
}

fn hex_decode(text: &str) -> Result<Vec<u8>, VfsError> {
    if !text.len().is_multiple_of(2) {
        return Err(VfsError::InvalidContentId(text.to_string()));
    }
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.as_chunks::<2>().0 {
        let hi = (pair[0] as char)
            .to_digit(16)
            .ok_or_else(|| VfsError::InvalidContentId(text.to_string()))?;
        let lo = (pair[1] as char)
            .to_digit(16)
            .ok_or_else(|| VfsError::InvalidContentId(text.to_string()))?;
        out.push(((hi << 4) | lo) as u8);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Expect, Vfs};

    /// Two stores on one host, which is the harder case: they infer the same
    /// origin from the environment and only their store identity separates them.
    fn pair(dir: &std::path::Path) -> (Vfs, Vfs) {
        (
            Vfs::open(dir.join("a")).unwrap(),
            Vfs::open(dir.join("b")).unwrap(),
        )
    }

    /// The carrier only moves bytes: everything that matters has to survive
    /// being serialised, because that is what crosses the wire.
    #[test]
    fn a_request_and_response_survive_the_wire() {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, mut b) = pair(dir.path());
        a.put("notes/one.md", b"one", "text/markdown", Expect::Any)
            .unwrap();
        a.put("notes/two.md", b"two", "text/markdown", Expect::Any)
            .unwrap();

        let request = SyncRequest {
            cursor: b.sync_cursor().unwrap(),
            limit: 100,
        };
        let wire = serde_json::to_string(&request).unwrap();
        let decoded: SyncRequest = serde_json::from_str(&wire).unwrap();

        let response = a.serve_sync(&decoded).unwrap();
        assert_eq!(response.ops.len(), 2);
        assert_eq!(response.content.len(), 2);

        let wire = serde_json::to_string(&response).unwrap();
        let decoded: SyncResponse = serde_json::from_str(&wire).unwrap();
        let outcomes = b.apply_sync(&decoded).unwrap();
        assert_eq!(outcomes, vec![ApplyOutcome::Applied; 2]);
        assert_eq!(b.get("notes/one.md").unwrap(), b"one");
        assert_eq!(b.get("notes/two.md").unwrap(), b"two");

        // The second round trip has nothing left to send.
        let request = SyncRequest {
            cursor: b.sync_cursor().unwrap(),
            limit: 100,
        };
        let response = a.serve_sync(&request).unwrap();
        assert!(response.ops.is_empty());
    }

    #[test]
    fn two_stores_on_one_host_are_distinct_peers() {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, mut b) = pair(dir.path());

        // Same machine, so the same origin; the cursor must not rely on it.
        assert_eq!(a.writer().origin, b.writer().origin);
        assert_ne!(a.store(), b.store());

        // The store id survives reopening.
        let reopened = Vfs::open(dir.path().join("a")).unwrap();
        assert_eq!(reopened.store(), a.store());

        // Each store's op is in the other's future, which is what the cursor
        // keyed on origin could not express.
        a.put("a.md", b"a", "text/markdown", Expect::Any).unwrap();
        b.put("b.md", b"b", "text/markdown", Expect::Any).unwrap();

        let from_a = a.answer_sync(&b.sync_cursor().unwrap(), 100).unwrap();
        assert_eq!(from_a.ops.len(), 1, "b must still see a's write");
        let from_b = b.answer_sync(&a.sync_cursor().unwrap(), 100).unwrap();
        assert_eq!(from_b.ops.len(), 1, "a must still see b's write");
    }

    fn drain(reader: &mut Vfs, writer: &mut Vfs) -> Vec<ApplyOutcome> {
        let cursor = reader.sync_cursor().unwrap();
        let batch = writer.answer_sync(&cursor, 100).unwrap();
        assert!(
            batch.missing_content.is_empty(),
            "the writer holds its own content"
        );
        batch
            .ops
            .iter()
            .map(|op| {
                let bytes = writer.get(&op.path).unwrap();
                reader.apply_remote(op, Some(&bytes)).unwrap()
            })
            .collect()
    }

    #[test]
    fn a_peer_receives_each_op_once_and_does_not_rebroadcast_it() {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, mut b) = pair(dir.path());
        a.put("notes/one.md", b"one", "text/markdown", Expect::Any)
            .unwrap();
        a.put("notes/two.md", b"two", "text/markdown", Expect::Any)
            .unwrap();

        let outcomes = drain(&mut b, &mut a);
        assert_eq!(outcomes, vec![ApplyOutcome::Applied; 2]);
        assert_eq!(b.get("notes/one.md").unwrap(), b"one");
        assert_eq!(b.get("notes/two.md").unwrap(), b"two");

        // Applied ops are recorded, not re-queued: an echo would make every
        // store's log grow with other stores' history.
        assert!(b.pending(10).unwrap().is_empty());

        // The cursor now covers them, so the same request returns nothing.
        let again = a.answer_sync(&b.sync_cursor().unwrap(), 100).unwrap();
        assert!(again.ops.is_empty(), "{:?}", again.ops.len());
    }

    #[test]
    fn redelivery_of_an_applied_op_is_a_duplicate_not_a_second_write() {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, mut b) = pair(dir.path());
        a.put("f.md", b"v1", "text/markdown", Expect::Any).unwrap();

        let cursor = b.sync_cursor().unwrap();
        let batch = a.answer_sync(&cursor, 100).unwrap();
        let op = &batch.ops[0];
        assert_eq!(
            b.apply_remote(op, Some(b"v1")).unwrap(),
            ApplyOutcome::Applied
        );
        assert_eq!(
            b.apply_remote(op, Some(b"v1")).unwrap(),
            ApplyOutcome::Duplicate
        );
        assert_eq!(b.history("f.md").unwrap().len(), 1, "one write, not two");
    }

    #[test]
    fn an_op_built_on_a_head_that_moved_is_diverged_not_applied() {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, mut b) = pair(dir.path());

        // Both stores start from the same content.
        a.put("f.md", b"v1", "text/markdown", Expect::Any).unwrap();
        drain(&mut b, &mut a);

        // Each edits it independently.
        a.put("f.md", b"from-a", "text/markdown", Expect::Any)
            .unwrap();
        b.put("f.md", b"from-b", "text/markdown", Expect::Any)
            .unwrap();

        let cursor = b.sync_cursor().unwrap();
        let batch = a.answer_sync(&cursor, 100).unwrap();
        let op = &batch.ops[0];
        assert_eq!(
            b.apply_remote(op, Some(b"from-a")).unwrap(),
            ApplyOutcome::Diverged,
            "applying would have discarded b's own write"
        );
        assert_eq!(b.get("f.md").unwrap(), b"from-b", "nothing overwritten");

        // Divergence is not a receipt: the op can be retried once the
        // divergence is resolved rather than being silently dropped.
        assert!(!b.sync_cursor().unwrap().covers(op));
    }

    #[test]
    fn content_that_does_not_match_its_name_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, mut b) = pair(dir.path());
        a.put("f.md", b"real", "text/markdown", Expect::Any)
            .unwrap();

        let cursor = b.sync_cursor().unwrap();
        let batch = a.answer_sync(&cursor, 100).unwrap();
        let op = &batch.ops[0];
        let error = b.apply_remote(op, Some(b"forged")).unwrap_err();
        assert!(matches!(error, VfsError::InvalidContentId(_)), "{error:?}");
        assert!(b.file("f.md").unwrap().is_none());
    }

    #[test]
    fn ops_are_ordered_by_clock_across_two_writers() {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, _) = pair(dir.path());
        a.set_writer(crate::Writer::new("alice", "studio"));
        a.put("a.md", b"a", "text/markdown", Expect::Any).unwrap();
        a.set_writer(crate::Writer::new("carol", "studio"));
        a.put("b.md", b"b", "text/markdown", Expect::Any).unwrap();

        let cursor = SyncCursor::new();
        let batch = a.answer_sync(&cursor, 100).unwrap();
        assert_eq!(batch.ops.len(), 2);
        assert!(batch.ops[0].hlc < batch.ops[1].hlc);
        assert_eq!(batch.ops[0].actor, "alice");
        assert_eq!(batch.ops[1].actor, "carol");
    }
}
