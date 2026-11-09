//! A framed, streaming transfer for snapshots.
//!
//! [`crate::sync`] describes what a peer needs; this module decides how it
//! travels when the carrier can do better than one message in and one out.
//!
//! Three things change once the answer may be a snapshot, and all three come
//! from the same place: a snapshot is the whole namespace, so as one value it is
//! as large as the store.
//!
//! - **Framing.** A header frame says what the transfer is, then one frame
//!   carries each entry: its metadata as JSON and its content as raw bytes. The
//!   length prefix that delimited the old whole-message frame now delimits each
//!   frame, so [`MAX_FRAME`] bounds one entry rather than one store, and content
//!   is no longer hex — two bytes of wire per byte of payload was a JSON
//!   encoding cost, not a protocol requirement.
//! - **Staging.** The receiver applies entries as they arrive and commits the
//!   namespace once, at the end. A transfer that dies part way through leaves a
//!   store whose state never changed and some unreferenced blobs, which `gc`
//!   reclaims. The refusal check runs on the header, before any content is
//!   read, so a store with unsynced local work turns the transfer away instead
//!   of receiving what it cannot use.
//! - **Resume.** A session id names the transfer: a digest of the snapshot's
//!   watermark and entries, recomputed by the sender rather than remembered. A
//!   receiver that was cut off reconnects with the session it was in and the
//!   index it reached; the sender honours that only if its own state still
//!   produces the same digest, and otherwise starts again from zero. Neither
//!   side keeps session state, so there is nothing to expire or leak.
//!
//! The whole-message format is still there for the carrier that needs it: ssh is
//! one stdin and one stdout, so it cannot interleave frames or resume, and
//! `sync::pull_over_ssh` keeps using it. [`STREAM_VERSION`] and
//! [`crate::MESSAGE_VERSION`] are separate numbers so the two never get confused
//! for one another.

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{
    ApplyOutcome, SessionId, SnapshotEntry, SnapshotKind, SnapshotStaging, SyncCursor,
    SyncResponse, TransferReport, Vfs, VfsError,
};

/// The version of the framed format, as it appears on the wire.
///
/// Named and carried in the request and the header rather than agreed in prose:
/// a peer speaking a different revision is refused where the error can say which
/// two versions disagreed.
pub const STREAM_VERSION: u16 = 2;

/// The largest single frame this format will send or accept.
///
/// The old carrier capped one whole message, which meant a snapshot was capped at
/// roughly half this once its content was hex encoded. Now the cap applies to one
/// entry, so the size that matters is the largest file in the store rather than
/// the size of the store. A frame larger than this is still refused before it is
/// allocated — the cap moved, it did not disappear.
pub const MAX_FRAME: usize = 64 * 1024 * 1024;

/// A client's request: what it has seen, and where to continue from.
const KIND_REQUEST: u8 = 1;
/// A complete answer, as one JSON `SyncResponse`. The delta path, unchanged.
const KIND_DELTA: u8 = 2;
/// A snapshot header: everything before the content.
const KIND_SNAPSHOT: u8 = 3;
/// One entry: metadata as JSON, content as raw bytes.
const KIND_ENTRY: u8 = 4;
/// The last frame of a snapshot transfer, and the commit point.
const KIND_END: u8 = 5;

fn transport(what: &str, error: impl std::fmt::Display) -> VfsError {
    VfsError::Transport(format!("{what}: {error}"))
}

/// What a client asks for, in the framed format.
///
/// The cursor and the limit mean exactly what they mean in [`crate::SyncRequest`]:
/// a peer inside this store's retention gets a delta, and everything else about
/// the delta path is untouched. The extra field is the resume point, which only
/// a framed carrier can use.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamRequest {
    pub version: u16,
    pub cursor: SyncCursor,
    pub limit: usize,
    /// Where to continue an interrupted snapshot, when there is one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume: Option<Resume>,
}

/// A point a snapshot transfer can continue from.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Resume {
    pub session: SessionId,
    /// How many entries the receiver has already staged, in snapshot order.
    pub from: u64,
}

/// The first frame of a streamed snapshot: what is coming, and under what
/// identity.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotHeader {
    pub version: u16,
    pub session: SessionId,
    /// The state the entries describe, adopted by the receiver as its horizon.
    pub watermark: SyncCursor,
    /// How many entries the whole snapshot has, not how many this stream carries.
    pub entry_count: u64,
    /// The index of the first entry this stream carries.
    pub first_entry: u64,
    /// Whether `first_entry` came from the receiver's resume point. Informational:
    /// the receiver decides for itself from the session and the index.
    pub resumed: bool,
}

/// The last frame of a snapshot transfer.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct StreamEnd {
    /// Entries this stream carried.
    pub entries: u64,
    /// Content bytes those entries carried.
    pub content_bytes: u64,
}

/// Write one frame, returning how many bytes it cost on the wire.
async fn write_frame<W>(
    send: &mut W,
    max_frame: usize,
    kind: u8,
    body: &[u8],
) -> Result<u64, VfsError>
where
    W: AsyncWrite + Unpin,
{
    // Refused here rather than sent: the peer applies the same ceiling, so an
    // oversized frame would only be refused at the far end after crossing the
    // network.
    let Ok(length) = u32::try_from(body.len() + 1) else {
        return Err(transport(
            "send frame",
            format!(
                "{} bytes is above any frame this format can describe",
                body.len() + 1
            ),
        ));
    };
    if length as usize > max_frame {
        return Err(transport(
            "send frame",
            format!(
                "{} bytes is above the {max_frame} byte frame limit",
                body.len() + 1
            ),
        ));
    }
    send.write_all(&length.to_be_bytes())
        .await
        .map_err(|error| transport("send frame length", error))?;
    send.write_all(&[kind])
        .await
        .map_err(|error| transport("send frame", error))?;
    send.write_all(body)
        .await
        .map_err(|error| transport("send frame", error))?;
    send.flush()
        .await
        .map_err(|error| transport("send frame", error))?;
    Ok(u64::from(length) + 4)
}

/// Read one frame, returning its kind, its body, and what it cost on the wire.
async fn read_frame<R>(recv: &mut R, max_frame: usize) -> Result<(u8, Vec<u8>, u64), VfsError>
where
    R: AsyncRead + Unpin,
{
    let mut prefix = [0u8; 4];
    recv.read_exact(&mut prefix)
        .await
        .map_err(|error| transport("read frame length", error))?;
    let length = u32::from_be_bytes(prefix) as usize;
    if length == 0 || length > max_frame {
        return Err(transport(
            "read frame",
            format!("peer announced {length} bytes, outside the 1..={max_frame} a frame may be"),
        ));
    }
    let mut kind = [0u8; 1];
    recv.read_exact(&mut kind)
        .await
        .map_err(|error| transport("read frame", error))?;
    let mut body = vec![0u8; length - 1];
    recv.read_exact(&mut body)
        .await
        .map_err(|error| transport("read frame", error))?;
    Ok((kind[0], body, u64::from(length as u32) + 4))
}

/// One entry frame: a length, the metadata as JSON, then the content raw.
///
/// The content is the rest of the frame, so its length is the frame's length
/// minus the metadata's: one prefix, not two.
fn encode_entry(entry: &SnapshotEntry, content: &[u8]) -> Result<Vec<u8>, VfsError> {
    let meta = serde_json::to_vec(entry).map_err(|error| transport("encode entry", error))?;
    let mut body = Vec::with_capacity(4 + meta.len() + content.len());
    body.extend_from_slice(&(meta.len() as u32).to_be_bytes());
    body.extend_from_slice(&meta);
    body.extend_from_slice(content);
    Ok(body)
}

fn decode_entry(body: &[u8]) -> Result<(SnapshotEntry, &[u8]), VfsError> {
    let Some((prefix, rest)) = body.split_at_checked(4) else {
        return Err(transport(
            "decode entry",
            "frame is shorter than its metadata length",
        ));
    };
    let meta_len = u32::from_be_bytes(prefix.try_into().unwrap_or([0; 4])) as usize;
    let Some((meta, content)) = rest.split_at_checked(meta_len) else {
        return Err(transport(
            "decode entry",
            format!(
                "metadata claims {meta_len} bytes of a {} byte frame",
                body.len()
            ),
        ));
    };
    let entry: SnapshotEntry =
        serde_json::from_slice(meta).map_err(|error| transport("decode entry", error))?;
    // A directory has no content of its own, so bytes here are a sender that
    // disagrees with the format rather than something to quietly drop.
    if entry.kind == SnapshotKind::Directory && !content.is_empty() {
        return Err(transport(
            "decode entry",
            format!(
                "directory {} carried {} bytes of content",
                entry.path,
                content.len()
            ),
        ));
    }
    Ok((entry, content))
}

/// The receiving half of a streamed exchange, able to continue after a tear.
///
/// Holds exactly what a reconnect needs and nothing else: the request (so the
/// cursor does not drift while entries are being staged), the session, and how
/// far into it the last attempt got. An attempt that ends in a carrier error
/// leaves this intact, which is what makes the next attempt a resume rather than
/// a restart.
pub struct PullClient {
    request: StreamRequest,
    session: Option<SessionId>,
    staging: Option<SnapshotStaging>,
    /// Entries staged, which is the resume cursor.
    applied: u64,
    /// Entries (or ops) applied by the whole exchange, for the report.
    entries: u64,
    content_bytes: u64,
    resumed_from: u64,
    bytes: u64,
    attempts: u32,
}

impl PullClient {
    pub fn new(cursor: SyncCursor, limit: usize) -> Self {
        Self {
            request: StreamRequest {
                version: STREAM_VERSION,
                cursor,
                limit,
                resume: None,
            },
            session: None,
            staging: None,
            applied: 0,
            entries: 0,
            content_bytes: 0,
            resumed_from: 0,
            bytes: 0,
            attempts: 0,
        }
    }

    /// Where the next attempt should continue from, if a snapshot is in flight.
    pub fn resume(&self) -> Option<Resume> {
        self.session.as_ref().map(|session| Resume {
            session: session.clone(),
            from: self.applied,
        })
    }

    /// What this exchange has cost so far, across attempts.
    pub fn report(&self) -> TransferReport {
        TransferReport {
            session: self.session.clone(),
            bytes: self.bytes,
            entries: self.entries,
            content_bytes: self.content_bytes,
            resumed_from: self.resumed_from,
            attempts: self.attempts,
        }
    }

    /// Ask for the answer and apply it, over one connection's worth of stream.
    ///
    /// On success the snapshot is committed. On failure nothing is committed and
    /// the client is left holding the point to resume from, so the caller can
    /// try again on a new connection. A local refusal — unsynced work on this
    /// store — is an error like any other, and one that happens before any
    /// content has been read.
    pub async fn attempt<R, W>(
        &mut self,
        vfs: &mut Vfs,
        recv: &mut R,
        send: &mut W,
        max_frame: usize,
    ) -> Result<Vec<ApplyOutcome>, VfsError>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        self.attempts += 1;
        let resume = self.resume();
        self.request.resume = resume;
        let body = serde_json::to_vec(&self.request)
            .map_err(|error| transport("encode request", error))?;
        self.bytes += write_frame(send, max_frame, KIND_REQUEST, &body).await?;
        let (kind, body, read) = read_frame(recv, max_frame).await?;
        self.bytes += read;
        match kind {
            KIND_DELTA => {
                let response: SyncResponse = serde_json::from_slice(&body)
                    .map_err(|error| transport("decode delta", error))?;
                let outcomes = vfs.apply_sync(&response)?;
                // A delta is not a transfer to resume: there is nothing staged
                // and nothing to continue.
                self.session = None;
                self.staging = None;
                self.entries = outcomes.len() as u64;
                self.content_bytes = response
                    .content
                    .iter()
                    .map(|blob| blob.data.len() as u64 / 2)
                    .sum();
                Ok(outcomes)
            }
            KIND_SNAPSHOT => self.snapshot(vfs, recv, &body, max_frame).await,
            other => Err(transport(
                "read frame",
                format!("expected a delta or a snapshot header, got frame kind {other}"),
            )),
        }
    }

    async fn snapshot<R>(
        &mut self,
        vfs: &mut Vfs,
        recv: &mut R,
        body: &[u8],
        max_frame: usize,
    ) -> Result<Vec<ApplyOutcome>, VfsError>
    where
        R: AsyncRead + Unpin,
    {
        let header: SnapshotHeader = serde_json::from_slice(body)
            .map_err(|error| transport("decode snapshot header", error))?;
        if header.version != STREAM_VERSION {
            return Err(transport(
                "read snapshot header",
                format!(
                    "peer speaks stream version {}, this store speaks {STREAM_VERSION}",
                    header.version
                ),
            ));
        }
        // Continue only what this client actually staged: the same transfer, at
        // the point it reached. Anything else starts over, because the indices
        // in the two transfers describe different states.
        let continues = self.staging.is_some()
            && self.session.as_ref() == Some(&header.session)
            && header.first_entry == self.applied;
        if continues {
            self.resumed_from = header.first_entry;
        } else {
            // The refusal is decided here, on the header, before a byte of
            // content has been read: the only moment at which it is still a
            // refusal rather than a repair.
            self.staging = Some(vfs.begin_snapshot(&header.watermark)?);
            self.session = Some(header.session.clone());
            self.applied = 0;
            self.resumed_from = 0;
        }

        // Only what this attempt carried, which is not the whole transfer when
        // it is resuming.
        let mut received = 0u64;
        loop {
            let (kind, body, read) = read_frame(recv, max_frame).await?;
            self.bytes += read;
            match kind {
                KIND_ENTRY => {
                    let (entry, content) = decode_entry(&body)?;
                    let Some(staging) = self.staging.as_mut() else {
                        return Err(transport("stage entry", "no snapshot is in flight"));
                    };
                    received += content.len() as u64;
                    staging.stage(vfs, &entry, Some(content))?;
                    self.applied += 1;
                }
                KIND_END => {
                    let end: StreamEnd = serde_json::from_slice(&body)
                        .map_err(|error| transport("decode end", error))?;
                    let Some(staging) = self.staging.take() else {
                        return Err(transport("commit snapshot", "no snapshot is in flight"));
                    };
                    // The sender's own count is checked against what arrived
                    // before anything is committed: a receiver that staged a
                    // prefix of the state must not install it as the state.
                    if header.first_entry + end.entries != header.entry_count {
                        return Err(transport(
                            "commit snapshot",
                            format!(
                                "stream carried {} of {} entries from {}",
                                end.entries, header.entry_count, header.first_entry
                            ),
                        ));
                    }
                    // Per attempt, not per transfer: an entry the last attempt
                    // already delivered was not sent again.
                    if end.content_bytes != received {
                        return Err(transport(
                            "commit snapshot",
                            format!(
                                "sender counted {} content bytes, {received} arrived",
                                end.content_bytes
                            ),
                        ));
                    }
                    self.content_bytes = staging.content_bytes();
                    self.entries = staging.entries();
                    staging.commit(vfs)?;
                    return Ok(vec![ApplyOutcome::Snapshot]);
                }
                other => {
                    return Err(transport(
                        "read frame",
                        format!("expected an entry or the end marker, got frame kind {other}"),
                    ));
                }
            }
        }
    }
}

/// Answer one framed exchange, streaming a snapshot if that is the answer.
///
/// Entries are read from the blob store one at a time and written one at a time,
/// so peak memory follows the largest entry rather than the store. A delta takes
/// the same path it always did: one frame carrying the same `SyncResponse` the
/// whole-message carrier would have sent.
pub async fn serve<R, W>(
    vfs: &mut Vfs,
    recv: &mut R,
    send: &mut W,
    max_frame: usize,
) -> Result<TransferReport, VfsError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let (kind, body, read) = read_frame(recv, max_frame).await?;
    if kind != KIND_REQUEST {
        return Err(transport(
            "read request",
            format!("expected a request frame, got frame kind {kind}"),
        ));
    }
    let request: StreamRequest =
        serde_json::from_slice(&body).map_err(|error| transport("decode request", error))?;
    if request.version != STREAM_VERSION {
        return Err(transport(
            "read request",
            format!(
                "peer speaks stream version {}, this store speaks {STREAM_VERSION}",
                request.version
            ),
        ));
    }
    let batch = vfs.answer_sync(&request.cursor, request.limit)?;
    let mut bytes = read;

    let Some(snapshot) = batch.snapshot else {
        // The delta path, unchanged: the same value the whole-message carrier
        // builds, over the same framing it always had, in one frame.
        let response = vfs.pack_content(batch)?;
        let entries = response.ops.len() as u64;
        let content_bytes: u64 = response
            .content
            .iter()
            .map(|blob| blob.data.len() as u64 / 2)
            .sum();
        let body =
            serde_json::to_vec(&response).map_err(|error| transport("encode delta", error))?;
        bytes += write_frame(send, max_frame, KIND_DELTA, &body).await?;
        return Ok(TransferReport {
            session: None,
            bytes,
            entries,
            content_bytes,
            resumed_from: 0,
            attempts: 1,
        });
    };

    let session = snapshot.session();
    let entry_count = snapshot.entries.len() as u64;
    // Honouring a resume means proving it is still the same transfer, which the
    // digest does: the same watermark over the same entries. Anything else — a
    // store that moved on since the first attempt — starts from zero, because
    // the receiver's entry indices no longer describe this snapshot.
    let first_entry = match &request.resume {
        Some(resume) if resume.session == session && resume.from <= entry_count => resume.from,
        _ => 0,
    };
    let header = SnapshotHeader {
        version: STREAM_VERSION,
        session: session.clone(),
        watermark: snapshot.watermark.clone(),
        entry_count,
        first_entry,
        resumed: first_entry > 0,
    };
    let body = serde_json::to_vec(&header).map_err(|error| transport("encode header", error))?;
    bytes += write_frame(send, max_frame, KIND_SNAPSHOT, &body).await?;

    let mut entries = 0u64;
    let mut content_bytes = 0u64;
    for entry in &snapshot.entries[first_entry as usize..] {
        let content = match &entry.content_id {
            Some(content_id) => vfs.content(content_id)?,
            None => Vec::new(),
        };
        content_bytes += content.len() as u64;
        let body = encode_entry(entry, &content)?;
        bytes += write_frame(send, max_frame, KIND_ENTRY, &body).await?;
        entries += 1;
    }
    let end = StreamEnd {
        entries,
        content_bytes,
    };
    let body = serde_json::to_vec(&end).map_err(|error| transport("encode end", error))?;
    bytes += write_frame(send, max_frame, KIND_END, &body).await?;

    Ok(TransferReport {
        session: Some(session),
        bytes,
        entries,
        content_bytes,
        resumed_from: first_entry,
        attempts: 1,
    })
}

/// A reader that stops after a budget, the way a stream cut mid-transfer does.
///
/// Reads are capped rather than counted after the fact, so the cut can land in
/// the middle of a frame — which is the case that has to be safe, and the one a
/// test that only stops between frames would not reach.
#[cfg(test)]
pub(crate) struct Cut<R> {
    inner: R,
    budget: usize,
}

#[cfg(test)]
impl<R> Cut<R> {
    pub(crate) fn new(inner: R, budget: usize) -> Self {
        Self { inner, budget }
    }
}

#[cfg(test)]
impl<R: AsyncRead + Unpin> AsyncRead for Cut<R> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::task::Poll;
        let this = self.get_mut();
        if this.budget == 0 {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "stream cut",
            )));
        }
        let cap = this.budget.min(buf.remaining());
        let mut limited = tokio::io::ReadBuf::new(buf.initialize_unfilled_to(cap));
        match std::pin::Pin::new(&mut this.inner).poll_read(cx, &mut limited) {
            Poll::Ready(Ok(())) => {
                let read = limited.filled().len();
                this.budget -= read;
                buf.advance(read);
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Expect, Hlc, NodeRole, SyncRequest, Vfs};
    use std::{path::Path, path::PathBuf, time::Duration, time::Instant};

    /// Two stores on one host: the same origin, distinct store identities, which
    /// is the harder case for the cursor and therefore the one worth carrying.
    ///
    /// They share the content store under `dir`, which is the test's own, so a
    /// test that would otherwise let the machine's default store answer writes
    /// somewhere it owns instead. That is the shape two roots on one machine
    /// actually have.
    fn pair(dir: &Path) -> (Vfs, Vfs) {
        let cas = dir.join("cas");
        (
            Vfs::open_with(dir.join("a"), NodeRole::Full, Some(&cas)).unwrap(),
            Vfs::open_with(dir.join("b"), NodeRole::Full, Some(&cas)).unwrap(),
        )
    }

    /// Two stores with their own content, which is what two machines look like.
    ///
    /// A transfer between roots that share a store stages blobs that are already
    /// there — content is addressed by hash, so the write is a no-op — and that
    /// makes "what the transfer staged" unobservable. Keeping the stores apart
    /// is what lets the interruption test see the bytes a torn transfer costs.
    fn separate_pair(dir: &Path) -> (Vfs, Vfs) {
        let a = dir.join("cas-a");
        let b = dir.join("cas-b");
        (
            Vfs::open_with(dir.join("a"), NodeRole::Full, Some(&a)).unwrap(),
            Vfs::open_with(dir.join("b"), NodeRole::Full, Some(&b)).unwrap(),
        )
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    /// Compaction compares against the wall clock, so a row written in this
    /// millisecond is not old enough to drop yet.
    fn wait_past(hlc: Hlc) {
        while crate::vfs::now_ms() <= hlc.ms {
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn newest(vfs: &Vfs) -> Hlc {
        vfs.pending(100).unwrap().last().unwrap().hlc
    }

    /// A sender with `count` files, and a receiver whose cursor the sender has
    /// compacted past — the state in which the only complete answer is the
    /// namespace itself.
    ///
    /// The receiver is made to learn one write first: an empty cursor is not
    /// behind anything, it simply never caught up, so it has to hold a real
    /// reading for the compaction to leave behind.
    fn behind_horizon(
        mut a: Vfs,
        mut b: Vfs,
        count: usize,
        size: usize,
    ) -> (Vfs, Vfs, Vec<(String, Vec<u8>)>) {
        // The seed is part of the state that arrives in the snapshot, so it is
        // part of what the test counts as content.
        let mut files = vec![("seed.md".to_string(), b"seed".to_vec())];
        a.put("seed.md", b"seed", "text/markdown", Expect::Any)
            .unwrap();
        let batch = a.answer_sync(&b.sync_cursor().unwrap(), 100).unwrap();
        let response = a.pack_content(batch).unwrap();
        b.apply_sync(&response).unwrap();

        for index in 0..count {
            // Distinct bytes per file, so each is its own content id and the
            // answer cannot be smaller than the content in it.
            let mut bytes = vec![b'a'; size];
            bytes[0] = index as u8;
            let path = format!("bulk/{index}.bin");
            a.put(&path, &bytes, "application/octet-stream", Expect::Any)
                .unwrap();
            files.push((path, bytes));
        }
        wait_past(newest(&a));
        a.compact(Duration::ZERO).unwrap();
        (a, b, files)
    }

    /// One exchange over an in-memory pipe, with both ends driven at once.
    ///
    /// The pipe is small on purpose: a transfer that buffered the store would
    /// still finish, but one that streams is what the frame boundaries describe,
    /// and the cut budget is what turns a finished exchange into a torn one.
    async fn exchange(
        sender: &mut Vfs,
        client: &mut PullClient,
        receiver: &mut Vfs,
        max_frame: usize,
        cut: Option<usize>,
    ) -> (
        Result<TransferReport, VfsError>,
        Result<Vec<ApplyOutcome>, VfsError>,
    ) {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let (client_recv, client_send) = tokio::io::split(client_io);
        let (server_recv, server_send) = tokio::io::split(server_io);
        let served = async move {
            let mut recv = server_recv;
            let mut send = server_send;
            let report = serve(sender, &mut recv, &mut send, max_frame).await;
            // Closing the writing half is how the peer learns the transfer is
            // over, whether it ended with the last frame or with an error.
            let _ = send.shutdown().await;
            report
        };
        let pulled = async move {
            let mut recv = client_recv;
            let mut send = client_send;
            match cut {
                Some(budget) => {
                    let mut recv = Cut::new(recv, budget);
                    client
                        .attempt(receiver, &mut recv, &mut send, max_frame)
                        .await
                }
                None => {
                    client
                        .attempt(receiver, &mut recv, &mut send, max_frame)
                        .await
                }
            }
        };
        tokio::join!(served, pulled)
    }

    /// Everything about a store that a user could observe, as one string.
    fn describe(vfs: &Vfs) -> String {
        serde_json::json!({
            "files": vfs.list("").unwrap(),
            "cursor": vfs.sync_cursor().unwrap(),
            "pending": vfs.pending(1000).unwrap().iter().map(|op| op.id).collect::<Vec<_>>(),
            "divergences": vfs.divergences().unwrap(),
        })
        .to_string()
    }

    /// A digest of everything one content store holds, which is what "the store
    /// is unchanged" means beyond the namespace.
    ///
    /// It reads the store's own directory rather than the root's, because the
    /// bytes no longer live under the root: the store is shared per machine and a
    /// root keeps only its namespace.
    fn cas_digest(cas: &Path) -> String {
        let mut files = Vec::new();
        collect(cas.join("objects"), &mut files);
        files.sort();
        let mut hasher = blake3::Hasher::new();
        for path in files {
            hasher.update(path.to_string_lossy().as_bytes());
            hasher.update(&std::fs::read(&path).unwrap());
        }
        hasher.finalize().to_hex().to_string()
    }

    fn collect(dir: PathBuf, into: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect(path, into);
            } else {
                into.push(path);
            }
        }
    }

    #[test]
    fn a_peer_within_retention_gets_a_delta_over_the_stream() {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, mut b) = pair(dir.path());
        a.put("notes/one.md", b"one", "text/markdown", Expect::Any)
            .unwrap();

        let mut client = PullClient::new(b.sync_cursor().unwrap(), 100);
        let (served, pulled) =
            runtime().block_on(exchange(&mut a, &mut client, &mut b, MAX_FRAME, None));
        assert_eq!(pulled.unwrap(), vec![ApplyOutcome::Applied]);
        let served = served.unwrap();
        assert_eq!(served.entries, 1);
        assert!(
            client.report().session.is_none(),
            "a delta inside retention is not a session"
        );
        assert_eq!(b.get("notes/one.md").unwrap(), b"one");
    }

    #[test]
    fn a_snapshot_far_larger_than_one_frame_crosses_the_stream() {
        // The frame limit here stands in for `MAX_FRAME`: the cap moved from the
        // whole message to one frame, so a transfer several times the limit
        // succeeds and only a single entry is bounded by it.
        const FRAME: usize = 16 * 1024;
        const FILES: usize = 8;
        const SIZE: usize = 8 * 1024;
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = pair(dir.path());
        let (mut a, mut b, files) = behind_horizon(a, b, FILES, SIZE);

        let mut client = PullClient::new(b.sync_cursor().unwrap(), 100);
        let (served, pulled) =
            runtime().block_on(exchange(&mut a, &mut client, &mut b, FRAME, None));
        assert_eq!(pulled.unwrap(), vec![ApplyOutcome::Snapshot]);
        let served = served.unwrap();
        let payload: u64 = files.iter().map(|(_, bytes)| bytes.len() as u64).sum();
        assert!(
            served.bytes > 4 * FRAME as u64,
            "{} bytes proves nothing about a {FRAME} byte frame limit",
            served.bytes
        );
        assert_eq!(served.content_bytes, payload);
        for (path, bytes) in &files {
            assert_eq!(&b.get(path).unwrap(), bytes, "{path} did not arrive intact");
        }
    }

    #[test]
    fn an_entry_larger_than_a_frame_is_refused() {
        const SIZE: usize = 32 * 1024;
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = pair(dir.path());
        let (mut a, mut b, _) = behind_horizon(a, b, 1, SIZE);

        // A frame limit smaller than the one entry: the cap still refuses, it
        // just refuses per entry now instead of per store.
        let mut client = PullClient::new(b.sync_cursor().unwrap(), 100);
        let (served, pulled) =
            runtime().block_on(exchange(&mut a, &mut client, &mut b, SIZE / 2, None));
        let error = served.unwrap_err();
        assert!(
            error.to_string().contains("frame limit"),
            "the sender refused the frame: {error}"
        );
        assert!(pulled.is_err());
        assert!(b.file("bulk/0.bin").unwrap().is_none());
    }

    #[test]
    fn streamed_content_costs_one_copy_of_the_payload_not_two() {
        const FILES: usize = 8;
        const SIZE: usize = 32 * 1024;
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = pair(dir.path());
        let (mut a, mut b, files) = behind_horizon(a, b, FILES, SIZE);
        let payload: u64 = files.iter().map(|(_, bytes)| bytes.len() as u64).sum();

        // What the same content costs through the whole-message path: hex inside
        // JSON, materialised as one value.
        let request = SyncRequest::new(b.sync_cursor().unwrap(), 100);
        let whole = serde_json::to_vec(&a.serve_sync(&request).unwrap())
            .unwrap()
            .len() as u64;
        let whole_ratio = whole as f64 / payload as f64;
        assert!(
            whole_ratio > 2.0,
            "the whole-message path encodes hex inside JSON: {whole_ratio:.2}×"
        );

        let mut client = PullClient::new(b.sync_cursor().unwrap(), 100);
        let (_, pulled) =
            runtime().block_on(exchange(&mut a, &mut client, &mut b, MAX_FRAME, None));
        assert_eq!(pulled.unwrap(), vec![ApplyOutcome::Snapshot]);
        let report = client.report();
        assert_eq!(report.content_bytes, payload);
        let streamed_ratio = report.bytes as f64 / payload as f64;
        println!(
            "payload {payload} bytes: whole-message {whole} bytes ({whole_ratio:.3}×), \
             streamed {} bytes ({streamed_ratio:.3}×)",
            report.bytes
        );
        assert!(
            streamed_ratio < 1.2,
            "raw content costs about one copy: {streamed_ratio:.2}× of {payload} bytes"
        );
    }

    #[test]
    fn an_interrupted_transfer_leaves_the_store_exactly_as_it_was() {
        let dir = tempfile::tempdir().unwrap();
        // Their own content, as two machines have: a shared store would already
        // hold every blob the snapshot names, and a transfer that writes nothing
        // proves nothing about where an abandoned transfer's bytes go.
        let (a, b) = separate_pair(dir.path());
        let (mut a, mut b, _) = behind_horizon(a, b, 8, 8 * 1024);
        let b_cas = dir.path().join("cas-b");
        let before = describe(&b);
        let content = cas_digest(&b_cas);

        let mut client = PullClient::new(b.sync_cursor().unwrap(), 100);
        let started = Instant::now();
        let (served, pulled) = runtime().block_on(exchange(
            &mut a,
            &mut client,
            &mut b,
            MAX_FRAME,
            Some(20 * 1024),
        ));
        assert!(pulled.is_err(), "a cut stream is not a successful transfer");
        assert!(served.is_err(), "the sender noticed the stream go away");
        assert!(started.elapsed() < Duration::from_secs(5));

        // Nothing visible changed: not the namespace, not the cursor, not the
        // log. That is the commit-once rule doing its job.
        assert_eq!(describe(&b), before, "a torn transfer changed the store");
        assert_ne!(cas_digest(&b_cas), content, "content was staged");

        // What was staged is unreferenced by construction, which is exactly what
        // `gc` reclaims: the abandoned transfer costs bytes on disk, not state.
        // `gc` collects across the roots registered with this store, and the only
        // root registered with `cas-b` is `b`.
        assert!(b.gc().unwrap() > 0, "staged blobs are unreferenced");
        assert_eq!(
            cas_digest(&b_cas),
            content,
            "gc reclaimed the abandoned transfer, leaving the content store byte-identical"
        );
        assert_eq!(describe(&b), before);
    }

    #[test]
    fn an_interrupted_transfer_resumes_and_completes() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = pair(dir.path());
        let (mut a, mut b, files) = behind_horizon(a, b, 8, 8 * 1024);
        let before = describe(&b);

        let mut client = PullClient::new(b.sync_cursor().unwrap(), 100);
        let (_, first) = runtime().block_on(exchange(
            &mut a,
            &mut client,
            &mut b,
            MAX_FRAME,
            Some(20 * 1024),
        ));
        assert!(first.is_err());
        assert_eq!(describe(&b), before);
        let staged = client
            .resume()
            .expect("a snapshot in flight has a resume point")
            .from;
        assert!(staged > 0, "the cut landed after the first entry");

        // The next attempt continues from the entry it reached. Nothing was
        // collected in between, so the blobs it already staged are still there.
        let (served, second) =
            runtime().block_on(exchange(&mut a, &mut client, &mut b, MAX_FRAME, None));
        assert_eq!(second.unwrap(), vec![ApplyOutcome::Snapshot]);
        let served = served.unwrap();
        assert_eq!(served.resumed_from, staged);
        let report = client.report();
        assert_eq!(report.attempts, 2);
        assert_eq!(report.resumed_from, staged);
        for (path, bytes) in &files {
            assert_eq!(&b.get(path).unwrap(), bytes, "{path} did not arrive intact");
        }
        assert_eq!(b.get("seed.md").unwrap(), b"seed");
    }

    #[test]
    fn resuming_an_interrupted_transfer_costs_less_than_starting_over() {
        const CUT: usize = 20 * 1024;
        const FILES: usize = 8;
        const SIZE: usize = 8 * 1024;

        // The same interruption twice, on two stores with the same content. The
        // only difference is whether the second attempt keeps the client, which
        // is the difference between resuming and starting over.
        fn total(dir: &Path, resume: bool) -> (u64, u64) {
            let (a, b) = pair(dir);
            let (mut a, mut b, _) = behind_horizon(a, b, FILES, SIZE);
            let cursor = b.sync_cursor().unwrap();
            let runtime = runtime();
            let mut client = PullClient::new(cursor.clone(), 100);
            let (_, first) =
                runtime.block_on(exchange(&mut a, &mut client, &mut b, MAX_FRAME, Some(CUT)));
            assert!(first.is_err());
            let first_bytes = client.report().bytes;

            let mut second = if resume {
                client
            } else {
                PullClient::new(cursor, 100)
            };
            let (_, pulled) =
                runtime.block_on(exchange(&mut a, &mut second, &mut b, MAX_FRAME, None));
            assert_eq!(pulled.unwrap(), vec![ApplyOutcome::Snapshot]);
            // A resumed client keeps counting across attempts, so its second
            // report is the whole exchange; a fresh one starts from zero.
            let second_bytes = if resume {
                second.report().bytes - first_bytes
            } else {
                second.report().bytes
            };
            (first_bytes + second_bytes, second.report().resumed_from)
        }

        let dir = tempfile::tempdir().unwrap();
        let (resumed, resumed_from) = total(&dir.path().join("resumed"), true);
        let (restarted, restarted_from) = total(&dir.path().join("restarted"), false);
        assert!(resumed_from > 0, "the resumed attempt continued");
        assert_eq!(restarted_from, 0, "the discarded client started at zero");
        println!(
            "interrupted at {CUT} bytes: resuming cost {resumed} bytes, restarting cost {restarted}"
        );
        assert!(
            resumed < restarted,
            "resuming cost {resumed} bytes, restarting cost {restarted}"
        );
    }

    #[test]
    fn a_snapshot_is_refused_on_the_header_before_any_content_is_read() {
        let dir = tempfile::tempdir().unwrap();
        // Their own content, so a byte that leaked past the header would show up.
        let (a, b) = separate_pair(dir.path());
        let (mut a, mut b, _) = behind_horizon(a, b, 4, 8 * 1024);
        // A write that exists nowhere else yet. A snapshot would discard it, so
        // the receiver has to say no before it accepts anything.
        b.put("local.md", b"from-b", "text/markdown", Expect::Any)
            .unwrap();
        let b_cas = dir.path().join("cas-b");
        let before = describe(&b);
        let content = cas_digest(&b_cas);

        let mut client = PullClient::new(b.sync_cursor().unwrap(), 100);
        let (_served, pulled) =
            runtime().block_on(exchange(&mut a, &mut client, &mut b, MAX_FRAME, None));
        let error = pulled.unwrap_err();
        assert!(
            matches!(error, VfsError::SnapshotRefused { pending: 1 }),
            "{error:?}"
        );
        assert!(
            client.report().bytes < 4 * 1024,
            "the receiver read {} bytes: it stopped at the header rather than after the content",
            client.report().bytes
        );
        assert_eq!(
            cas_digest(&b_cas),
            content,
            "not one byte of content was staged"
        );
        assert_eq!(describe(&b), before);
        assert_eq!(b.get("local.md").unwrap(), b"from-b");
    }

    #[test]
    fn a_framed_request_in_the_whole_message_version_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, _b) = pair(dir.path());
        let result = runtime().block_on(async {
            let (mut client_io, server_io) = tokio::io::duplex(4 * 1024);
            let request = StreamRequest {
                version: crate::MESSAGE_VERSION,
                cursor: SyncCursor::new(),
                limit: 10,
                resume: None,
            };
            let body = serde_json::to_vec(&request).unwrap();
            write_frame(&mut client_io, MAX_FRAME, KIND_REQUEST, &body)
                .await
                .unwrap();
            let (mut server_recv, mut server_send) = tokio::io::split(server_io);
            serve(&mut a, &mut server_recv, &mut server_send, MAX_FRAME).await
        });
        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("stream version"),
            "the two formats are told apart by number: {error}"
        );
    }

    #[test]
    fn a_session_is_identified_by_the_state_it_carries() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = pair(dir.path());
        let (mut a, b, _) = behind_horizon(a, b, 2, 1024);
        let first = a.snapshot().unwrap().session();
        assert_eq!(
            first,
            a.snapshot().unwrap().session(),
            "the same state, the same session"
        );

        // Any change to the state that a resume would mis-splice is a change to
        // the session, which is what lets the sender refuse to continue one.
        a.put("late.md", b"late", "text/markdown", Expect::Any)
            .unwrap();
        assert_ne!(first, a.snapshot().unwrap().session());
        assert!(b.file("late.md").unwrap().is_none());
    }
}
