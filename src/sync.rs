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
//! - **Truncation.** A log that only ever grows is a log that eventually takes
//!   longer to scan than the work it describes, so rows older than a cutoff are
//!   dropped. Dropping them is the easy half. The honest half is that a peer
//!   whose cursor now sits behind what was dropped can no longer be caught up
//!   from the log at all: the ops that would have filled its gap are exactly
//!   the ones that are gone. Such a peer is told so and handed a snapshot of
//!   the live namespace at a watermark instead of a short delta it would have
//!   no way to recognise as short.
//!
//! The horizon is a watermark, not a set of operations. It says "nothing at or
//! below this clock is still here", which is enough to recognise a redelivery
//! and enough to know when to snapshot; it is not a claim about every op that
//! ever passed below it. This store does not persist its last clock reading — it
//! restarts from the wall clock — so a clock that steps back across a restart
//! can put a later op underneath a horizon it has already passed. Persisting the
//! reading is the fix, and it belongs to the clock rather than to compaction.
//!
//! It is also capped by any divergence this store is still holding. A diverged
//! op records no receipt, so the horizon is the only thing that could still
//! turn away its next offer — which means the horizon must never reach it, or a
//! retried write would come back as a duplicate of a write this store does not
//! have. Compaction therefore stops short of the earliest unresolved divergence
//! for a store and leaves the rows above it in place. That is the cost, and it
//! is the right one to pay: an unresolved divergence is unfinalised state, and
//! a log that stays too long is a smaller problem than a write that disappears.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

use crate::{ContentId, Hlc, SyncOpKind, Vfs, VfsError, normalize_path, vfs::now_ms};

/// Attribution for versions that arrived as wholesale state rather than as an
/// authored op. A snapshot does not say who wrote each path, and inventing an
/// author would be worse than saying plainly that nobody is named.
const SNAPSHOT_ACTOR: &str = "snapshot";

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

/// Whether `cursor` sits below `horizons` for any store it names.
///
/// One store behind is all it takes: the log has a gap the peer would fall
/// into, and no page of it can close that gap.
fn behind_horizon(cursor: &SyncCursor, horizons: &BTreeMap<String, Hlc>) -> bool {
    cursor
        .origins()
        .any(|(store, seen)| horizons.get(store).is_some_and(|hlc| *hlc > seen))
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
    /// Present instead of a delta when the requester's cursor is behind this
    /// store's horizon: the ops the requester is missing were dropped, so the
    /// live state is the only complete answer left.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<Snapshot>,
    pub content: Vec<ContentBlob>,
}

/// The live namespace, for a peer that cannot be caught up from the log.
///
/// Deliberately not stored anywhere. A stored snapshot would be a second copy
/// of the state that could drift from the first, and a watermark recorded
/// beside it would then describe a state that no longer exists. Read at shoot
/// time, the entries and the watermark are the same instant by construction.
///
/// Entries name content by id rather than carrying bytes: the blobs travel the
/// same verified path ops already use, so a snapshot cannot smuggle in content
/// that does not match its name.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    /// One entry per live path.
    pub entries: Vec<SnapshotEntry>,
    /// Everything this store had seen when the entries were read, per store.
    /// A receiver adopts this as its horizon: below it, the entries are the
    /// state; above it, the log still has the ops.
    pub watermark: SyncCursor,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotEntry {
    pub path: String,
    pub kind: SnapshotKind,
    /// The blob this path holds. Absent for a directory, which has no content
    /// of its own — it exists so an empty directory survives the trip.
    pub content_id: Option<ContentId>,
    /// Carried because a media type is namespace metadata, not something the
    /// bytes imply: a snapshot that dropped it would silently retype every
    /// file it restored.
    pub media_type: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotKind {
    File,
    Directory,
}

/// What one compaction pass dropped, and where the log now starts.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CompactionReport {
    /// Rows strictly older than this clock reading were eligible to be dropped.
    pub cutoff_ms: i64,
    /// Outbound ops dropped, per store.
    pub dropped_ops: BTreeMap<String, u64>,
    /// Applied-op receipts dropped, per store.
    pub dropped_receipts: BTreeMap<String, u64>,
    /// Stores where the pass stopped short of the cutoff because an unresolved
    /// divergence sits below it, with the reading that held it back. Empty when
    /// nothing is pinned, which is the answer to "why is the log still here?".
    pub pinned: BTreeMap<String, Hlc>,
    /// The horizon per store after this pass: the highest reading this store
    /// no longer keeps. Reported even when nothing was dropped, so a caller
    /// can see the point a peer would have to be behind to get a snapshot.
    pub horizons: BTreeMap<String, Hlc>,
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
    /// A whole state replaced this store's namespace instead of a delta.
    Snapshot,
}

/// An op this store refused because the head had moved, and has not resolved.
///
/// Kept rather than forgotten so that compaction knows where it must stop. The
/// record is what distinguishes a write this store deliberately deferred from
/// one it never saw, and only the second is safe to compact away.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Divergence {
    pub store: String,
    pub hlc: Hlc,
    /// The path the op wanted to write, so the record can be explained.
    pub path: String,
    pub detected_at_ms: i64,
}

/// One store's answer to a sync request.
pub struct SyncBatch {
    pub ops: Vec<crate::SyncOp>,
    /// Content the sender must be asked for before these ops can be applied.
    pub missing_content: Vec<ContentId>,
    /// Set when `ops` is empty because a delta would have had a hole in it.
    pub snapshot: Option<Snapshot>,
}

impl Vfs {
    /// Build the answer a carrier sends back: the ops the requester lacks, and
    /// the bytes they carry.
    pub fn serve_sync(&mut self, request: &SyncRequest) -> Result<SyncResponse, VfsError> {
        let batch = self.answer_sync(&request.cursor, request.limit)?;
        // One blob may serve several paths, and a snapshot names every path it
        // has: sending the same bytes twice would make the answer grow with
        // the namespace rather than with the content in it.
        let mut wanted: BTreeSet<&ContentId> = BTreeSet::new();
        wanted.extend(batch.ops.iter().filter_map(|op| op.content_id.as_ref()));
        if let Some(snapshot) = &batch.snapshot {
            wanted.extend(
                snapshot
                    .entries
                    .iter()
                    .filter_map(|entry| entry.content_id.as_ref()),
            );
        }
        let mut content = Vec::new();
        for content_id in wanted {
            // Read by content id, not by path: the path may have moved on since
            // the op was written, but the blob it names has not.
            match self.content(content_id) {
                Ok(bytes) => content.push(ContentBlob::from_bytes(content_id, &bytes)),
                Err(_) => continue,
            }
        }
        Ok(SyncResponse {
            ops: batch.ops,
            snapshot: batch.snapshot,
            content,
        })
    }

    /// Apply a response, returning one outcome per op in order.
    ///
    /// A response that carries a snapshot yields a single `Snapshot` outcome
    /// and no per-op outcomes: the state was replaced, not replayed.
    pub fn apply_sync(&mut self, response: &SyncResponse) -> Result<Vec<ApplyOutcome>, VfsError> {
        let mut blobs = BTreeMap::new();
        for blob in &response.content {
            blobs.insert(blob.id.clone(), blob.decode()?);
        }
        let mut outcomes = Vec::with_capacity(response.ops.len());
        if let Some(snapshot) = &response.snapshot {
            self.apply_snapshot_with(snapshot, &blobs)?;
            outcomes.push(ApplyOutcome::Snapshot);
        }
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
        // A truncated prefix still counts as seen. Reporting a lower reading
        // here would ask peers to resend ops this store has decided not to
        // keep, and — worse — would let a peer that is genuinely behind look
        // caught up for the stores it was compacted against.
        for (store, hlc) in self.horizons()? {
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
        // A peer behind the horizon is missing ops this store has dropped, and
        // no amount of paging will produce them. Handing it the rest of the log
        // would look like a complete delta and leave it silently short of the
        // truth, so it gets the state itself instead. `limit` does not apply:
        // a snapshot is not a page, it is the answer.
        if behind_horizon(cursor, &self.horizons()?) {
            let snapshot = self.snapshot()?;
            let mut missing_content = Vec::new();
            for entry in &snapshot.entries {
                let Some(content_id) = &entry.content_id else {
                    continue;
                };
                if !self.has_content(content_id)? {
                    missing_content.push(content_id.clone());
                }
            }
            return Ok(SyncBatch {
                ops: Vec::new(),
                missing_content,
                snapshot: Some(snapshot),
            });
        }
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
            snapshot: None,
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
        // Truncation removed the receipt, so the horizon is what stands in for
        // it: anything at or below what was dropped is already reflected in
        // this store's state, and re-applying it would either duplicate a write
        // or read as a divergence from a head this store never lost.
        if self
            .horizons()?
            .get(&op.store)
            .is_some_and(|horizon| *horizon >= op.hlc)
        {
            return Ok(ApplyOutcome::Duplicate);
        }
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
                    self.mark_diverged(&op.store, op.hlc, &op.path)?;
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
        // If this op was deferred before and fits now, the deferral is over: a
        // record that outlived it would keep pinning the log on a difference
        // nobody has to reconcile any more.
        self.resolve_divergence(&op.store, op.hlc)?;
        Ok(ApplyOutcome::Applied)
    }

    /// The highest clock this store has dropped from its log, per store.
    ///
    /// Keyed exactly like the cursor, because it answers the same kind of
    /// question: what do you already know about this store? The difference is
    /// what it costs to be wrong. A cursor that is too high asks a peer for ops
    /// it has already seen; a horizon that is too high tells a peer it is
    /// caught up when it is not, so a horizon only ever moves forward and only
    /// to something that was actually dropped.
    pub fn horizons(&self) -> Result<BTreeMap<String, Hlc>, VfsError> {
        let mut statement = self
            .db
            .prepare("SELECT store, hlc_ms, hlc_counter FROM horizon")?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                Hlc {
                    ms: row.get(1)?,
                    counter: row.get::<_, i64>(2)? as u32,
                },
            ))
        })?;
        let mut horizons = BTreeMap::new();
        for row in rows {
            let (store, hlc) = row?;
            horizons.insert(store, hlc);
        }
        Ok(horizons)
    }

    /// Drop log rows older than `older_than`, and record how far the log now
    /// starts.
    ///
    /// Both halves of the log go together. The outbound ops are what peers can
    /// still be caught up from; the receipts are what tells this store it has
    /// already seen an op. Dropping either without raising the horizon would be
    /// the quiet lie this whole module exists to avoid: the rows would be gone,
    /// and nothing would say so, so a peer would be handed the delta that
    /// follows the gap as if it were the whole story.
    ///
    /// Nothing at or above the cutoff is touched, so a writer that is running
    /// right now cannot lose the ops it has not yet told anyone about. Running
    /// it twice with the same cutoff drops nothing the second time, because the
    /// horizon it raised is what would have been dropped.
    ///
    /// One thing holds compaction back: an unresolved divergence. A diverged op
    /// has no receipt, so if the horizon passed its clock its next offer would
    /// read as a duplicate of a write this store does not hold — the write would
    /// be swallowed, which is the failure the divergence report exists to avoid.
    /// The horizon is therefore capped below the earliest unresolved divergence
    /// for each store, and the rows above that point stay where they are until
    /// the divergence is resolved. A divergence nobody ever resolves pins
    /// compaction for its store; that log growing again is cheaper than the
    /// write it would have bought.
    pub fn compact(&mut self, older_than: Duration) -> Result<CompactionReport, VfsError> {
        // A cutoff is a boundary, not a duration, once it is compared against a
        // reading: `(ms, counter) < (cutoff, 0)` is the tuple form of
        // "older than the cutoff in milliseconds".
        let cutoff = Hlc {
            ms: now_ms().saturating_sub(i64::try_from(older_than.as_millis()).unwrap_or(i64::MAX)),
            counter: 0,
        };
        let pins = self.divergence_pins()?;
        let ops = self.rows_older_than("sync_queue", cutoff, &pins)?;
        let receipts = self.rows_older_than("applied", cutoff, &pins)?;

        let tx = self.db.transaction()?;
        for (store, dropped) in &ops {
            tx.execute(
                "DELETE FROM sync_queue WHERE store=?1 AND (hlc_ms,hlc_counter)<(?2,?3)",
                params![
                    store,
                    dropped.boundary.ms,
                    i64::from(dropped.boundary.counter)
                ],
            )?;
            raise_horizon(&tx, store, dropped.newest)?;
        }
        for (store, dropped) in &receipts {
            tx.execute(
                "DELETE FROM applied WHERE store=?1 AND (hlc_ms,hlc_counter)<(?2,?3)",
                params![
                    store,
                    dropped.boundary.ms,
                    i64::from(dropped.boundary.counter)
                ],
            )?;
            raise_horizon(&tx, store, dropped.newest)?;
        }
        tx.commit()?;

        Ok(CompactionReport {
            cutoff_ms: cutoff.ms,
            dropped_ops: ops
                .iter()
                .map(|(store, dropped)| (store.clone(), dropped.count))
                .collect(),
            dropped_receipts: receipts
                .iter()
                .map(|(store, dropped)| (store.clone(), dropped.count))
                .collect(),
            // Only a divergence older than the cutoff is holding the pass back:
            // one above it was never going to be reached anyway.
            pinned: pins.into_iter().filter(|(_, pin)| *pin < cutoff).collect(),
            horizons: self.horizons()?,
        })
    }

    /// The divergences this store is still holding, oldest first.
    ///
    /// A caller needs to see these to resolve them, and needs to resolve them
    /// for compaction to get past their clocks.
    pub fn divergences(&self) -> Result<Vec<Divergence>, VfsError> {
        let mut statement = self.db.prepare(
            "SELECT store, hlc_ms, hlc_counter, path, detected_at_ms FROM diverged
             ORDER BY store, hlc_ms, hlc_counter",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(Divergence {
                store: row.get(0)?,
                hlc: Hlc {
                    ms: row.get(1)?,
                    counter: row.get::<_, i64>(2)? as u32,
                },
                path: row.get(3)?,
                detected_at_ms: row.get(4)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(VfsError::from)
    }

    /// Settle one divergence without the op ever being applied.
    ///
    /// For the case where the local head wins: once the difference is decided,
    /// the op is no longer unfinalised, and compaction may pass its clock. A
    /// peer that keeps re-offering it will diverge again and record a fresh
    /// record, which is correct — that divergence is still real until the two
    /// sides agree on content.
    pub fn resolve_divergence(&self, store: &str, hlc: Hlc) -> Result<bool, VfsError> {
        let removed = self.db.execute(
            "DELETE FROM diverged WHERE store=?1 AND hlc_ms=?2 AND hlc_counter=?3",
            params![store, hlc.ms, i64::from(hlc.counter)],
        )?;
        Ok(removed > 0)
    }

    /// The earliest unresolved divergence per store, which is where compaction
    /// has to stop for that store.
    fn divergence_pins(&self) -> Result<BTreeMap<String, Hlc>, VfsError> {
        let mut pins: BTreeMap<String, Hlc> = BTreeMap::new();
        for divergence in self.divergences()? {
            let entry = pins.entry(divergence.store).or_insert(divergence.hlc);
            if divergence.hlc < *entry {
                *entry = divergence.hlc;
            }
        }
        Ok(pins)
    }

    /// The live namespace, as a replacement for a log a peer can no longer use.
    ///
    /// Computed when it is asked for, never stored. Both reads happen in one
    /// read transaction, watermark first and state second, so no write can land
    /// between them even from another process: everything at or below the
    /// watermark is already in the entries, and anything newer is still in the
    /// log for the receiver to replay. A stored snapshot would have to prove the
    /// same thing about a past instant, and would by then describe state that
    /// has moved on.
    pub fn snapshot(&self) -> Result<Snapshot, VfsError> {
        let tx = self.db.unchecked_transaction()?;
        let watermark = self.sync_cursor()?;
        let mut entries = Vec::new();
        let mut statement = self
            .db
            .prepare("SELECT path, content_id, media_type FROM files ORDER BY path")?;
        let rows = statement.query_map([], |row| {
            Ok(SnapshotEntry {
                path: row.get(0)?,
                kind: SnapshotKind::File,
                content_id: Some(ContentId(row.get(1)?)),
                media_type: Some(row.get(2)?),
            })
        })?;
        for row in rows {
            entries.push(row?);
        }
        let mut statement = self
            .db
            .prepare("SELECT path FROM directories ORDER BY path")?;
        let rows = statement.query_map([], |row| {
            Ok(SnapshotEntry {
                path: row.get(0)?,
                kind: SnapshotKind::Directory,
                content_id: None,
                media_type: None,
            })
        })?;
        for row in rows {
            entries.push(row?);
        }
        tx.commit()?;
        Ok(Snapshot { entries, watermark })
    }

    /// Replace this store's namespace with a peer's, and adopt its watermark.
    ///
    /// Destructive by definition — the state is made to match, so anything not
    /// in the snapshot goes — which is why it refuses when the local log still
    /// holds ops no peer has acknowledged. Those writes exist nowhere else yet;
    /// dropping them here would be the one failure this module cannot report
    /// after the fact.
    pub fn apply_snapshot(
        &mut self,
        snapshot: &Snapshot,
        content: &[ContentBlob],
    ) -> Result<(), VfsError> {
        let mut blobs = BTreeMap::new();
        for blob in content {
            blobs.insert(blob.id.clone(), blob.decode()?);
        }
        self.apply_snapshot_with(snapshot, &blobs)
    }

    fn apply_snapshot_with(
        &mut self,
        snapshot: &Snapshot,
        blobs: &BTreeMap<String, Vec<u8>>,
    ) -> Result<(), VfsError> {
        // The refusal is checked before anything is written, which is the only
        // moment at which it is still a refusal rather than a repair.
        let pending: i64 = self
            .db
            .query_row("SELECT COUNT(*) FROM sync_queue", [], |row| row.get(0))?;
        if pending > 0 {
            return Err(VfsError::SnapshotRefused {
                pending: pending as usize,
            });
        }

        // Everything the snapshot names is verified before the namespace is
        // touched, so a blob that is missing or does not match its id leaves the
        // store exactly as it was rather than half replaced.
        let mut files = Vec::new();
        let mut directories = Vec::new();
        for entry in &snapshot.entries {
            let path = normalize_path(&entry.path)?;
            match entry.kind {
                SnapshotKind::Directory => directories.push(path),
                SnapshotKind::File => {
                    let content_id = entry
                        .content_id
                        .clone()
                        .ok_or_else(|| VfsError::InvalidContentId(path.clone()))?;
                    let bytes = blobs
                        .get(&content_id.to_string())
                        .ok_or_else(|| VfsError::NotHydrated(content_id.to_string()))?;
                    if ContentId::from_bytes(bytes) != content_id {
                        return Err(VfsError::InvalidContentId(content_id.to_string()));
                    }
                    // Content-addressed and already verified, so storing the
                    // blob now costs nothing a later failure would have to undo.
                    self.blobs.put(bytes)?;
                    files.push((
                        path,
                        content_id,
                        bytes.len() as i64,
                        entry
                            .media_type
                            .clone()
                            .unwrap_or_else(|| "application/octet-stream".to_string()),
                    ));
                }
            }
        }

        let now = now_ms();
        let tx = self.db.transaction()?;
        let keep_files: BTreeSet<&str> = files.iter().map(|(path, ..)| path.as_str()).collect();
        let keep_dirs: BTreeSet<&str> = directories.iter().map(String::as_str).collect();
        for path in table_paths(&tx, "files")? {
            if !keep_files.contains(path.as_str()) {
                tx.execute("DELETE FROM files WHERE path=?1", params![path])?;
            }
        }
        for path in table_paths(&tx, "directories")? {
            if !keep_dirs.contains(path.as_str()) {
                tx.execute("DELETE FROM directories WHERE path=?1", params![path])?;
            }
        }
        for path in &directories {
            tx.execute(
                "INSERT OR IGNORE INTO directories(path,created_at_ms) VALUES(?1,?2)",
                params![path, now],
            )?;
        }
        for (path, content_id, size, media_type) in &files {
            let existing: Option<(i64, String, String, String)> = tx
                .query_row(
                    "SELECT version,content_id,media_type,hydration FROM files WHERE path=?1",
                    params![path],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()?;
            // Already matching: rewriting it would bump a version for a change
            // that did not happen.
            if let Some((_, current_id, current_media, hydration)) = &existing
                && current_id == &content_id.0
                && current_media == media_type
                && hydration == "present"
            {
                continue;
            }
            let version = existing.as_ref().map_or(1, |(version, ..)| version + 1);
            tx.execute(
                "INSERT INTO files(path,content_id,size,media_type,version,hydration,pinned,updated_at_ms)
                 VALUES(?1,?2,?3,?4,?5,'present',0,?6)
                 ON CONFLICT(path) DO UPDATE SET content_id=excluded.content_id,size=excluded.size,
                   media_type=excluded.media_type,version=excluded.version,hydration='present',
                   updated_at_ms=excluded.updated_at_ms",
                params![path, content_id.0, size, media_type, version, now],
            )?;
            // The history has to say where the version came from, even when the
            // answer is "it arrived as state, with no author named".
            tx.execute(
                "INSERT OR REPLACE INTO versions(path,version,content_id,size,created_at_ms,actor,origin)
                 VALUES(?1,?2,?3,?4,?5,?6,?6)",
                params![path, version, content_id.0, size, now, SNAPSHOT_ACTOR],
            )?;
        }

        // The watermark becomes this store's horizon: every write at or below it
        // is in the state just installed, so a later redelivery of one of them
        // is a duplicate, and the receipts that said so individually are now
        // redundant — keeping them would regrow the record the snapshot exists
        // to make unnecessary.
        for (store, hlc) in snapshot.watermark.origins() {
            // A divergence at or below the watermark stops being a divergence
            // here: the write it deferred is the state that was just installed.
            // One above the watermark is still open, and stays above the horizon
            // exactly as it would after a compaction pass.
            tx.execute(
                "DELETE FROM diverged WHERE store=?1 AND (hlc_ms,hlc_counter)<=(?2,?3)",
                params![store, hlc.ms, i64::from(hlc.counter)],
            )?;
            raise_horizon(&tx, store, hlc)?;
            tx.execute(
                "DELETE FROM applied WHERE store=?1 AND (hlc_ms,hlc_counter)<=(?2,?3)",
                params![store, hlc.ms, i64::from(hlc.counter)],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Rows older than one cutoff in a log table, counted and dated per store.
    ///
    /// The table name is a compile-time constant, never user input; the cutoff
    /// travels as a parameter. Each store gets its own boundary — the cutoff,
    /// unless an unresolved divergence sits below it — and the boundary is
    /// returned with the count so the delete and the horizon agree about exactly
    /// what was removed.
    fn rows_older_than(
        &self,
        table: &str,
        cutoff: Hlc,
        pins: &BTreeMap<String, Hlc>,
    ) -> Result<BTreeMap<String, Dropped>, VfsError> {
        let mut statement = self.db.prepare(&format!(
            "SELECT store, hlc_ms, hlc_counter FROM {table} WHERE (hlc_ms,hlc_counter)<(?1,?2)"
        ))?;
        let rows = statement.query_map(params![cutoff.ms, i64::from(cutoff.counter)], |row| {
            Ok((
                row.get::<_, String>(0)?,
                Hlc {
                    ms: row.get(1)?,
                    counter: row.get::<_, i64>(2)? as u32,
                },
            ))
        })?;
        let mut dropped: BTreeMap<String, Dropped> = BTreeMap::new();
        for row in rows {
            let (store, hlc) = row?;
            let boundary = pins.get(&store).map_or(cutoff, |pin| cutoff.min(*pin));
            if hlc >= boundary {
                continue;
            }
            let entry = dropped.entry(store).or_insert(Dropped {
                count: 0,
                newest: hlc,
                boundary,
            });
            entry.count += 1;
            if hlc > entry.newest {
                entry.newest = hlc;
            }
        }
        Ok(dropped)
    }
}

/// What one table is about to lose for one store.
struct Dropped {
    count: u64,
    newest: Hlc,
    /// The reading the drop stops short of: the cutoff, or an unresolved
    /// divergence below it.
    boundary: Hlc,
}

/// Raise `store`'s horizon to `hlc`, never lowering it.
///
/// A horizon is a claim that nothing at or below this reading is still kept.
/// Letting it retreat would let a later redelivery be applied twice, which is
/// the one thing the receipt it replaced existed to prevent.
fn raise_horizon(tx: &Connection, store: &str, hlc: Hlc) -> Result<(), VfsError> {
    tx.execute(
        "INSERT INTO horizon(store,hlc_ms,hlc_counter) VALUES(?1,?2,?3)
         ON CONFLICT(store) DO UPDATE SET hlc_ms=excluded.hlc_ms, hlc_counter=excluded.hlc_counter
         WHERE (excluded.hlc_ms,excluded.hlc_counter)>(hlc_ms,hlc_counter)",
        params![store, hlc.ms, i64::from(hlc.counter)],
    )?;
    Ok(())
}

/// Every path in one namespace table. The table name is a compile-time constant.
fn table_paths(tx: &Connection, table: &str) -> Result<Vec<String>, VfsError> {
    let mut statement = tx.prepare(&format!("SELECT path FROM {table}"))?;
    let rows = statement.query_map([], |row| row.get(0))?;
    rows.collect::<Result<Vec<_>, _>>().map_err(VfsError::from)
}

/// Ask a peer over ssh and apply the answer.
///
/// A carrier, not a protocol: the request and response are the same
/// structures a peer session carries, so this is one of several wires that
/// can move them. It lives in the library rather than a binary because the
/// shell and the VFS are two callers of the same exchange.
pub fn pull_over_ssh(
    vfs: &mut Vfs,
    remote: &str,
    remote_root: &str,
    remote_bin: &str,
    limit: usize,
) -> Result<Vec<ApplyOutcome>, VfsError> {
    let request = SyncRequest {
        cursor: vfs.sync_cursor()?,
        limit,
    };
    let command = format!(
        "{} --root {} sync serve",
        remote_bin,
        shell_quote(remote_root)
    );
    let mut child = std::process::Command::new("ssh")
        .arg("-o")
        .arg("BatchMode=yes")
        .arg(remote)
        .arg(&command)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|error| VfsError::Transport(format!("ssh {remote}: {error}")))?;
    if let Some(stdin) = child.stdin.as_mut() {
        use std::io::Write;
        let body = serde_json::to_vec(&request)
            .map_err(|error| VfsError::Transport(format!("encode request: {error}")))?;
        stdin
            .write_all(&body)
            .map_err(|error| VfsError::Transport(format!("send request: {error}")))?;
    }
    let output = child
        .wait_with_output()
        .map_err(|error| VfsError::Transport(format!("ssh {remote}: {error}")))?;
    if !output.status.success() {
        return Err(VfsError::Transport(format!(
            "sync from {remote} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let response: SyncResponse = serde_json::from_slice(&output.stdout)
        .map_err(|error| VfsError::Transport(format!("decode response: {error}")))?;
    vfs.apply_sync(&response)
}

/// Quote one argument for the remote shell.
fn shell_quote(value: &str) -> String {
    format!("'''{}'''", value.replace('\'', "'\''"))
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

    /// Wait until the wall clock has moved past `hlc`.
    ///
    /// `older_than` is a grace window, so a zero-width one can only drop a row
    /// written in an earlier millisecond. Sleeping is what makes that true
    /// rather than likely.
    fn wait_past(hlc: Hlc) {
        while now_ms() <= hlc.ms {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }

    /// The newest op this store has queued, which is the one a drop has to
    /// wait for.
    fn newest(vfs: &Vfs) -> Hlc {
        vfs.pending(100).unwrap().last().unwrap().hlc
    }

    fn snapshot_content(vfs: &Vfs, snapshot: &Snapshot) -> Vec<ContentBlob> {
        snapshot
            .entries
            .iter()
            .filter_map(|entry| {
                let content_id = entry.content_id.as_ref()?;
                Some(ContentBlob::from_bytes(
                    content_id,
                    &vfs.content(content_id).unwrap(),
                ))
            })
            .collect()
    }

    #[test]
    fn a_peer_behind_the_horizon_is_given_a_snapshot_not_a_short_delta() {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, mut b) = pair(dir.path());

        // b sees one write, so its cursor is a real reading that can later be
        // left behind rather than an empty one that was never caught up.
        a.put("notes/one.md", b"one", "text/markdown", Expect::Any)
            .unwrap();
        drain(&mut b, &mut a);
        a.put("notes/two.md", b"two", "text/markdown", Expect::Any)
            .unwrap();

        wait_past(newest(&a));
        let report = a.compact(std::time::Duration::ZERO).unwrap();
        assert_eq!(report.dropped_ops.values().sum::<u64>(), 2);
        assert!(report.horizons.contains_key(a.store()));

        // The delta b would get starts after the hole and looks complete, which
        // is precisely the failure being replaced: it is told the log cannot
        // reach it and handed the state instead.
        let cursor = b.sync_cursor().unwrap();
        let batch = a.answer_sync(&cursor, 100).unwrap();
        assert!(batch.ops.is_empty(), "{:?}", batch.ops);
        assert!(
            batch.snapshot.is_some(),
            "a gap must be answered with state"
        );

        let request = SyncRequest {
            cursor: b.sync_cursor().unwrap(),
            limit: 100,
        };
        let response = a.serve_sync(&request).unwrap();
        assert!(response.ops.is_empty());
        assert_eq!(
            b.apply_sync(&response).unwrap(),
            vec![ApplyOutcome::Snapshot]
        );

        // The point of the trip: both writes are here, including the one whose
        // op was dropped before b could ever see it.
        assert_eq!(b.get("notes/one.md").unwrap(), b"one");
        assert_eq!(b.get("notes/two.md").unwrap(), b"two");
    }

    #[test]
    fn a_store_with_unsynced_work_refuses_a_snapshot_and_loses_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, mut b) = pair(dir.path());
        a.put("shared.md", b"from-a", "text/markdown", Expect::Any)
            .unwrap();
        b.put("local.md", b"from-b", "text/markdown", Expect::Any)
            .unwrap();

        let snapshot = a.snapshot().unwrap();
        let content = snapshot_content(&a, &snapshot);

        // b's write exists nowhere else yet. Replacing the namespace would
        // delete the only copy, so the snapshot is refused instead.
        let error = b.apply_snapshot(&snapshot, &content).unwrap_err();
        assert!(
            matches!(error, VfsError::SnapshotRefused { pending: 1 }),
            "{error:?}"
        );
        assert_eq!(b.get("local.md").unwrap(), b"from-b");
        assert!(
            b.file("shared.md").unwrap().is_none(),
            "refused means untouched, not half applied"
        );

        // Once the local op has been acknowledged there is nothing left to
        // lose, and the snapshot goes through.
        let ids: Vec<i64> = b.pending(10).unwrap().iter().map(|op| op.id).collect();
        b.ack(&ids).unwrap();
        b.apply_snapshot(&snapshot, &content).unwrap();
        assert_eq!(b.get("shared.md").unwrap(), b"from-a");
        assert!(
            b.file("local.md").unwrap().is_none(),
            "a snapshot is the state, not a merge into it"
        );
    }

    #[test]
    fn incremental_sync_resumes_after_a_snapshot_without_redelivering_it() {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, mut b) = pair(dir.path());
        a.put("notes/one.md", b"one", "text/markdown", Expect::Any)
            .unwrap();
        drain(&mut b, &mut a);
        a.put("notes/two.md", b"two", "text/markdown", Expect::Any)
            .unwrap();
        wait_past(newest(&a));
        a.compact(std::time::Duration::ZERO).unwrap();

        let request = SyncRequest {
            cursor: b.sync_cursor().unwrap(),
            limit: 100,
        };
        let response = a.serve_sync(&request).unwrap();
        assert!(response.snapshot.is_some());
        assert_eq!(
            b.apply_sync(&response).unwrap(),
            vec![ApplyOutcome::Snapshot]
        );
        assert!(
            b.horizons().unwrap().contains_key(a.store()),
            "adopting the watermark is what makes the next answer a delta"
        );

        // Everything up to the watermark is now b's own horizon, so the next
        // answer is the write that follows it and nothing else: no replay of
        // what the snapshot already carried, no duplicate writes.
        a.put("notes/three.md", b"three", "text/markdown", Expect::Any)
            .unwrap();
        let request = SyncRequest {
            cursor: b.sync_cursor().unwrap(),
            limit: 100,
        };
        let response = a.serve_sync(&request).unwrap();
        assert!(response.snapshot.is_none(), "b is past the horizon now");
        assert_eq!(response.ops.len(), 1);
        assert_eq!(response.ops[0].path, "notes/three.md");
        assert_eq!(
            b.apply_sync(&response).unwrap(),
            vec![ApplyOutcome::Applied]
        );
        assert_eq!(b.get("notes/three.md").unwrap(), b"three");
        assert_eq!(b.get("notes/one.md").unwrap(), b"one");
    }

    #[test]
    fn compaction_never_drops_an_op_newer_than_the_cutoff() {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, _) = pair(dir.path());
        a.put("recent.md", b"recent", "text/markdown", Expect::Any)
            .unwrap();

        // A grace window is a threshold, not a trigger: a write made now is
        // newer than a cutoff an hour in the past, so it survives untouched.
        let report = a.compact(std::time::Duration::from_secs(3600)).unwrap();
        assert!(report.dropped_ops.is_empty(), "{:?}", report.dropped_ops);
        assert!(
            report.horizons.is_empty(),
            "nothing dropped means no horizon to raise"
        );
        assert_eq!(a.pending(10).unwrap().len(), 1);

        // Only once the clock has moved past it does a zero-width window see it
        // as old, which is the boundary the first assertion was guarding.
        wait_past(newest(&a));
        let report = a.compact(std::time::Duration::ZERO).unwrap();
        assert_eq!(report.dropped_ops.values().sum::<u64>(), 1);
        assert!(a.pending(10).unwrap().is_empty());
    }

    #[test]
    fn an_op_redelivered_below_the_applied_horizon_is_a_duplicate() {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, mut b) = pair(dir.path());
        a.put("f.md", b"v1", "text/markdown", Expect::Any).unwrap();

        let cursor = b.sync_cursor().unwrap();
        let op = a.answer_sync(&cursor, 100).unwrap().ops[0].clone();
        assert_eq!(
            b.apply_remote(&op, Some(b"v1")).unwrap(),
            ApplyOutcome::Applied
        );

        // Truncation takes the per-op receipt with it, so the receipt can no
        // longer be what recognises the redelivery.
        wait_past(op.hlc);
        let report = b.compact(std::time::Duration::ZERO).unwrap();
        assert_eq!(report.dropped_receipts.get(&op.store), Some(&1));
        assert!(
            report.horizons.get(&op.store).is_some_and(|h| *h >= op.hlc),
            "the horizon is what is left of the receipt"
        );
        assert!(!b.applied_cursor().unwrap().covers(&op));

        // Horizon membership replaces it: already reflected in this store's
        // state, so not a second write — and not a divergence from a head this
        // store never lost.
        assert_eq!(
            b.apply_remote(&op, Some(b"v1")).unwrap(),
            ApplyOutcome::Duplicate
        );
        assert_eq!(b.history("f.md").unwrap().len(), 1, "one write, not two");

        // And the horizon cannot be walked back by a later snapshot whose
        // watermark is older: a claim about what was dropped is not something a
        // peer gets to lower, or the duplicate above would be applied twice.
        let mut older = SyncCursor::new();
        older.observe(
            &op.store,
            Hlc {
                ms: op.hlc.ms - 1,
                ..Hlc::default()
            },
        );
        b.apply_snapshot(
            &Snapshot {
                entries: Vec::new(),
                watermark: older,
            },
            &[],
        )
        .unwrap();
        assert_eq!(
            b.horizons().unwrap().get(&op.store),
            report.horizons.get(&op.store)
        );
    }

    #[test]
    fn an_unresolved_divergence_pins_compaction_so_the_op_stays_deferred() {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, mut b) = pair(dir.path());

        // Both stores start from the same content, then each edits it.
        a.put("f.md", b"v1", "text/markdown", Expect::Any).unwrap();
        drain(&mut b, &mut a);
        a.put("f.md", b"from-a", "text/markdown", Expect::Any)
            .unwrap();
        b.put("f.md", b"from-b", "text/markdown", Expect::Any)
            .unwrap();

        let cursor = b.sync_cursor().unwrap();
        let op = a.answer_sync(&cursor, 100).unwrap().ops[0].clone();
        assert_eq!(
            b.apply_remote(&op, Some(b"from-a")).unwrap(),
            ApplyOutcome::Diverged
        );

        // The refusal is recorded rather than forgotten. Without the record
        // compaction has nothing to stop at, and the retry below would be
        // swallowed as a duplicate of a write this store does not hold.
        let divergences = b.divergences().unwrap();
        assert_eq!(divergences.len(), 1);
        assert_eq!(divergences[0].store, op.store);
        assert_eq!(divergences[0].hlc, op.hlc);
        assert_eq!(divergences[0].path, "f.md");

        // Compaction is asked to take everything, and the clock has moved past
        // the op — but the horizon must not reach an open divergence.
        wait_past(op.hlc);
        let report = b.compact(std::time::Duration::ZERO).unwrap();
        assert!(
            report.horizons.get(&op.store).is_none_or(|h| *h < op.hlc),
            "{:?}",
            report.horizons
        );
        assert_eq!(
            report.pinned.get(&op.store),
            Some(&op.hlc),
            "the pass is held at the divergence, and the report says so"
        );

        // The retry is deferred again, not counted as already applied, and the
        // local head is still the one b wrote.
        assert_eq!(
            b.apply_remote(&op, Some(b"from-a")).unwrap(),
            ApplyOutcome::Diverged
        );
        assert_eq!(b.get("f.md").unwrap(), b"from-b");
    }

    #[test]
    fn resolving_a_divergence_lets_the_horizon_move_past_it() {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, mut b) = pair(dir.path());
        a.put("f.md", b"v1", "text/markdown", Expect::Any).unwrap();
        drain(&mut b, &mut a);
        a.put("f.md", b"from-a", "text/markdown", Expect::Any)
            .unwrap();
        b.put("f.md", b"from-b", "text/markdown", Expect::Any)
            .unwrap();

        let cursor = b.sync_cursor().unwrap();
        let op = a.answer_sync(&cursor, 100).unwrap().ops[0].clone();
        assert_eq!(
            b.apply_remote(&op, Some(b"from-a")).unwrap(),
            ApplyOutcome::Diverged
        );
        wait_past(op.hlc);
        let pinned = b.compact(std::time::Duration::ZERO).unwrap();
        assert_eq!(pinned.pinned.get(&op.store), Some(&op.hlc));

        // A human decides the local head wins, which is the only thing that can
        // settle a difference the store cannot settle itself.
        assert!(b.resolve_divergence(&op.store, op.hlc).unwrap());
        assert!(b.divergences().unwrap().is_empty());
        assert!(
            !b.resolve_divergence(&op.store, op.hlc).unwrap(),
            "resolving is not something that happens twice"
        );

        // With nothing open, a later write can be dropped on the next pass and
        // the horizon passes the old divergence clock.
        a.put("g.md", b"g", "text/markdown", Expect::Any).unwrap();
        let cursor = b.sync_cursor().unwrap();
        let batch = a.answer_sync(&cursor, 100).unwrap();
        let later = batch
            .ops
            .iter()
            .find(|candidate| candidate.path == "g.md")
            .cloned()
            .unwrap();
        assert_eq!(
            b.apply_remote(&later, Some(b"g")).unwrap(),
            ApplyOutcome::Applied
        );

        wait_past(later.hlc);
        let report = b.compact(std::time::Duration::ZERO).unwrap();
        assert!(report.pinned.is_empty());
        assert!(
            report.horizons.get(&op.store).is_some_and(|h| *h > op.hlc),
            "{:?}",
            report.horizons
        );

        // And the op that was deferred is now genuinely moot rather than
        // pending: the resolution is what makes that difference.
        assert_eq!(
            b.apply_remote(&op, Some(b"from-a")).unwrap(),
            ApplyOutcome::Duplicate
        );
    }

    #[test]
    fn a_snapshot_settles_the_divergences_its_watermark_covers() {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, mut b) = pair(dir.path());
        a.put("f.md", b"v1", "text/markdown", Expect::Any).unwrap();
        drain(&mut b, &mut a);
        a.put("f.md", b"from-a", "text/markdown", Expect::Any)
            .unwrap();
        b.put("f.md", b"from-b", "text/markdown", Expect::Any)
            .unwrap();

        let cursor = b.sync_cursor().unwrap();
        let op = a.answer_sync(&cursor, 100).unwrap().ops[0].clone();
        assert_eq!(
            b.apply_remote(&op, Some(b"from-a")).unwrap(),
            ApplyOutcome::Diverged
        );

        // A snapshot replaces the state the divergence was a difference about,
        // so it settles the record rather than leaving a stale pin behind.
        let ids: Vec<i64> = b.pending(10).unwrap().iter().map(|op| op.id).collect();
        b.ack(&ids).unwrap();
        let snapshot = a.snapshot().unwrap();
        let content = snapshot_content(&a, &snapshot);
        b.apply_snapshot(&snapshot, &content).unwrap();

        assert!(b.divergences().unwrap().is_empty());
        assert_eq!(b.get("f.md").unwrap(), b"from-a");
        assert!(
            b.horizons()
                .unwrap()
                .get(&op.store)
                .is_some_and(|horizon| *horizon >= op.hlc)
        );
        assert_eq!(
            b.apply_remote(&op, Some(b"from-a")).unwrap(),
            ApplyOutcome::Duplicate,
            "the state now holds the write the op wanted"
        );
    }

    #[test]
    fn a_divergence_that_finally_fits_is_settled_and_stops_pinning() {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, mut b) = pair(dir.path());
        a.put("f.md", b"v1", "text/markdown", Expect::Any).unwrap();
        drain(&mut b, &mut a);

        // b edits the path, so a's next write was based on a head b no longer
        // has: the write is deferred rather than overwriting b's work.
        b.put("f.md", b"from-b", "text/markdown", Expect::Any)
            .unwrap();
        a.put("f.md", b"from-a", "text/markdown", Expect::Any)
            .unwrap();
        let cursor = b.sync_cursor().unwrap();
        let op = a.answer_sync(&cursor, 100).unwrap().ops[0].clone();
        assert_eq!(
            b.apply_remote(&op, Some(b"from-a")).unwrap(),
            ApplyOutcome::Diverged
        );
        assert_eq!(b.divergences().unwrap().len(), 1);

        // b puts its head back to what a's write was built on. The retry now
        // fits, so the deferral is settled by the write itself and the record
        // must not linger.
        b.put("f.md", b"v1", "text/markdown", Expect::Any).unwrap();
        assert_eq!(
            b.apply_remote(&op, Some(b"from-a")).unwrap(),
            ApplyOutcome::Applied
        );
        assert!(
            b.divergences().unwrap().is_empty(),
            "a resolved divergence must not keep pinning compaction"
        );
        assert_eq!(b.get("f.md").unwrap(), b"from-a");
    }

    #[test]
    fn compacting_twice_reports_nothing_the_second_time() {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, mut b) = pair(dir.path());
        a.put("notes/one.md", b"one", "text/markdown", Expect::Any)
            .unwrap();
        a.put("notes/two.md", b"two", "text/markdown", Expect::Any)
            .unwrap();
        drain(&mut b, &mut a);

        wait_past(newest(&a));
        let first = a.compact(std::time::Duration::ZERO).unwrap();
        assert_eq!(first.dropped_ops.values().sum::<u64>(), 2);
        assert!(
            first.dropped_receipts.is_empty(),
            "a wrote these; it received nothing"
        );

        // Nothing is left to drop, and the horizon has not moved: the second
        // pass is the same answer, not a second round of truncation.
        let second = a.compact(std::time::Duration::ZERO).unwrap();
        assert!(second.dropped_ops.is_empty(), "{:?}", second.dropped_ops);
        assert!(second.dropped_receipts.is_empty());
        assert_eq!(second.horizons, first.horizons);
    }
}
