//! A content provider: the other end of a [`Replica`](crate::Replica).
//!
//! `Replica` is a trait, and a trait needs someone on the other side of a wire when
//! the content lives where the credentials are — an R2 bucket, a Kubo node. The
//! alternative is an HTTP client and a bucket signer compiled into every consumer
//! of this crate, which is how a library that owns no network stack ends up owning
//! one.
//!
//! So the wire is deliberately dull: a length-prefixed JSON request, a one-byte
//! status, a length-prefixed JSON reply, and the payload beside it. The bodies are
//! [`ContentFetch`] and [`ContentData`] from [`crate::bus`] — the same contract
//! Unibus carries, over a different carrier, the way `sync` has both a
//! whole-message and a framed form. Ranges are bounded by the same 64 KiB and the
//! same 64 MiB budget on both sides.
//!
//! What a provider may claim is part of the protocol rather than assumed: a
//! provider says whether it is writable, so `put` to a read-only shelf is refused
//! at the far end by the thing that knows, not discovered halfway through an
//! upload.

use std::{
    io::{ErrorKind, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::{
    ContentId, MAX_CONTENT_BYTES, MAX_RANGE_BYTES, Replica, Stored, VfsError, bus::ContentFetch,
};

/// The framed provider protocol's version, on the wire so two revisions refuse
/// each other where the error can name both.
pub const PROVIDER_VERSION: u16 = 1;

/// The largest single frame this protocol will send or accept.
///
/// A range is already bounded at [`MAX_RANGE_BYTES`]; this is the outer guard on
/// the length prefix, so a wrong number is refused before anything is allocated.
pub const MAX_PROVIDER_FRAME: usize = 64 * 1024 * 1024 + 4096;

/// Status byte on every reply. A number, not a JSON field, so a provider that
/// cannot even produce a reply body still has a way to say no.
const OK: u8 = 0;
const NOT_FOUND: u8 = 1;
const FAILED: u8 = 2;
const REFUSED: u8 = 3;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderRequest {
    pub version: u16,
    /// `hello`, `fetch`, or `put`.
    pub op: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_id: Option<ContentId>,
    #[serde(default)]
    pub offset: u64,
    #[serde(default)]
    pub length: u32,
}

impl ProviderRequest {
    fn hello() -> Self {
        Self {
            version: PROVIDER_VERSION,
            op: "hello".to_owned(),
            content_id: None,
            offset: 0,
            length: 0,
        }
    }

    fn range(id: &ContentId, offset: u64, length: u32) -> Self {
        Self {
            version: PROVIDER_VERSION,
            op: "fetch".to_owned(),
            content_id: Some(id.clone()),
            offset,
            length,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderReply {
    pub version: u16,
    /// What the provider calls itself, so a report names the shelf and not just
    /// "the network".
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub writable: bool,
    /// Whether this range ended the content.
    #[serde(default)]
    pub eof: bool,
    /// Where the content starts, spelled the way this provider spells it. Empty
    /// for a provider that stores our names verbatim.
    #[serde(default)]
    pub locator: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

fn transport(what: &str, error: impl std::fmt::Display) -> VfsError {
    VfsError::Transport(format!("{what}: {error}"))
}

fn write_frame(stream: &mut impl Write, bytes: &[u8]) -> Result<(), VfsError> {
    if bytes.len() > MAX_PROVIDER_FRAME {
        return Err(VfsError::Transport(format!(
            "frame of {} bytes exceeds the {} byte limit",
            bytes.len(),
            MAX_PROVIDER_FRAME
        )));
    }
    let length = u32::try_from(bytes.len())
        .map_err(|_| VfsError::Transport("frame too large to describe".to_owned()))?;
    stream.write_all(&length.to_le_bytes())?;
    stream.write_all(bytes)?;
    stream.flush()?;
    Ok(())
}

fn read_frame(stream: &mut impl Read) -> Result<Option<Vec<u8>>, VfsError> {
    let mut header = [0u8; 4];
    match stream.read_exact(&mut header) {
        Ok(()) => {}
        // A provider that has gone away is not an error at the frame layer; it is
        // the end of the conversation.
        Err(error) if error.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(VfsError::Io(error)),
    }
    let length = u32::from_le_bytes(header) as usize;
    if length > MAX_PROVIDER_FRAME {
        return Err(VfsError::Transport(format!(
            "frame of {length} bytes exceeds the {MAX_PROVIDER_FRAME} byte limit"
        )));
    }
    let mut bytes = vec![0u8; length];
    stream.read_exact(&mut bytes)?;
    Ok(Some(bytes))
}

fn write_reply(
    stream: &mut impl Write,
    status: u8,
    reply: &ProviderReply,
    payload: &[u8],
) -> Result<(), VfsError> {
    stream.write_all(&[status])?;
    let body = serde_json::to_vec(reply).map_err(|error| transport("encode reply", error))?;
    write_frame(stream, &body)?;
    write_frame(stream, payload)
}

fn read_reply(stream: &mut impl Read) -> Result<(u8, ProviderReply, Vec<u8>), VfsError> {
    let mut status = [0u8; 1];
    stream.read_exact(&mut status)?;
    let body = read_frame(stream)?.ok_or_else(|| transport("read reply", "connection closed"))?;
    let reply: ProviderReply =
        serde_json::from_slice(&body).map_err(|error| transport("decode reply", error))?;
    if reply.version != PROVIDER_VERSION {
        return Err(VfsError::Transport(format!(
            "provider speaks version {}, this build speaks {PROVIDER_VERSION}",
            reply.version
        )));
    }
    let payload = read_frame(stream)?.unwrap_or_default();
    Ok((status[0], reply, payload))
}

/// Serve `replica` to anyone who can reach this listener.
///
/// The replica decides everything that matters — whether it is writable, what it
/// is called, what it has. This layer only carries the answer, which is what lets
/// an adapter crate be five lines: implement [`Replica`], bind, serve.
pub struct ProviderServer {
    listener: TcpListener,
}

impl ProviderServer {
    pub fn bind(addr: impl ToSocketAddrs) -> Result<Self, VfsError> {
        let listener = TcpListener::bind(addr)?;
        Ok(Self { listener })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, VfsError> {
        Ok(self.listener.local_addr()?)
    }

    /// Serve until the process is killed.
    pub fn serve(&self, replica: &dyn Replica) -> Result<(), VfsError> {
        for stream in self.listener.incoming() {
            let stream = stream?;
            serve_connection(stream, replica)?;
        }
        Ok(())
    }

    /// Serve exactly `count` connections and return how many were answered.
    ///
    /// Exists for tests, and for a provider started from a script that should not
    /// outlive its client.
    pub fn serve_n(&self, replica: &dyn Replica, count: usize) -> Result<usize, VfsError> {
        let mut served = 0;
        for stream in self.listener.incoming().take(count) {
            serve_connection(stream?, replica)?;
            served += 1;
        }
        Ok(served)
    }
}

fn serve_connection(mut stream: TcpStream, replica: &dyn Replica) -> Result<(), VfsError> {
    let _ = stream.set_nodelay(true);
    let peer = stream.peer_addr().ok();
    loop {
        let Some(body) = read_frame(&mut stream)? else {
            return Ok(());
        };
        let request: ProviderRequest = match serde_json::from_slice(&body) {
            Ok(request) => request,
            Err(error) => {
                let reply = ProviderReply {
                    version: PROVIDER_VERSION,
                    detail: Some(match &peer {
                        Some(peer) => format!("unreadable request from {peer}: {error}"),
                        None => format!("unreadable request: {error}"),
                    }),
                    ..ProviderReply::default()
                };
                let _ = write_reply(&mut stream, FAILED, &reply, &[]);
                return Ok(());
            }
        };
        if request.version != PROVIDER_VERSION {
            let reply = ProviderReply {
                version: PROVIDER_VERSION,
                detail: Some(format!(
                    "client speaks version {}, this provider speaks {PROVIDER_VERSION}",
                    request.version
                )),
                ..ProviderReply::default()
            };
            write_reply(&mut stream, REFUSED, &reply, &[])?;
            return Ok(());
        }
        let hello = ProviderReply {
            version: PROVIDER_VERSION,
            name: replica.name().to_owned(),
            writable: replica.writable(),
            ..ProviderReply::default()
        };
        match request.op.as_str() {
            "hello" => write_reply(&mut stream, OK, &hello, &[])?,
            "fetch" => {
                let Some(id) = request.content_id.as_ref() else {
                    let reply = ProviderReply {
                        version: PROVIDER_VERSION,
                        name: replica.name().to_owned(),
                        detail: Some("fetch needs a content id".to_owned()),
                        ..ProviderReply::default()
                    };
                    write_reply(&mut stream, FAILED, &reply, &[])?;
                    continue;
                };
                let mut fetch = ContentFetch {
                    request_id: String::new(),
                    content_id: id.clone(),
                    offset: request.offset,
                    length: request.length,
                    locator: None,
                };
                fetch.request_id = "provider".to_owned();
                if let Err(error) = fetch.validate() {
                    let reply = ProviderReply {
                        version: PROVIDER_VERSION,
                        name: replica.name().to_owned(),
                        detail: Some(format!("{error}")),
                        ..ProviderReply::default()
                    };
                    write_reply(&mut stream, REFUSED, &reply, &[])?;
                    continue;
                }
                match replica.fetch_range(id, request.offset, request.length) {
                    Ok(range) => {
                        let reply = ProviderReply {
                            version: PROVIDER_VERSION,
                            name: replica.name().to_owned(),
                            eof: range.eof,
                            ..ProviderReply::default()
                        };
                        write_reply(&mut stream, OK, &reply, &range.bytes)?;
                    }
                    Err(VfsError::NotHydrated(_)) => {
                        let reply = ProviderReply {
                            version: PROVIDER_VERSION,
                            name: replica.name().to_owned(),
                            detail: Some(format!("{id} is not held here")),
                            ..ProviderReply::default()
                        };
                        write_reply(&mut stream, NOT_FOUND, &reply, &[])?;
                    }
                    Err(error) => {
                        let reply = ProviderReply {
                            version: PROVIDER_VERSION,
                            name: replica.name().to_owned(),
                            detail: Some(format!("{error}")),
                            ..ProviderReply::default()
                        };
                        write_reply(&mut stream, FAILED, &reply, &[])?;
                    }
                }
            }
            "put" => {
                let Some(id) = request.content_id.as_ref() else {
                    let reply = ProviderReply {
                        version: PROVIDER_VERSION,
                        name: replica.name().to_owned(),
                        detail: Some("put needs a content id".to_owned()),
                        ..ProviderReply::default()
                    };
                    write_reply(&mut stream, FAILED, &reply, &[])?;
                    continue;
                };
                let payload = read_frame(&mut stream)?.unwrap_or_default();
                if !replica.writable() {
                    let reply = ProviderReply {
                        version: PROVIDER_VERSION,
                        name: replica.name().to_owned(),
                        detail: Some(format!("{} is read-only", replica.name())),
                        ..ProviderReply::default()
                    };
                    write_reply(&mut stream, REFUSED, &reply, &[])?;
                    continue;
                }
                // The bytes are checked against the name before they are kept: a
                // provider that trusted a client would store a lie.
                if ContentId::from_bytes(&payload) != *id {
                    let reply = ProviderReply {
                        version: PROVIDER_VERSION,
                        name: replica.name().to_owned(),
                        detail: Some(format!("{id} does not match the bytes sent")),
                        ..ProviderReply::default()
                    };
                    write_reply(&mut stream, REFUSED, &reply, &[])?;
                    continue;
                }
                match replica.put(id, &payload) {
                    Ok(stored) => {
                        let reply = ProviderReply {
                            version: PROVIDER_VERSION,
                            name: replica.name().to_owned(),
                            writable: true,
                            detail: Some(match stored {
                                Stored::Added => "added".to_owned(),
                                Stored::AlreadyPresent => "already".to_owned(),
                            }),
                            ..ProviderReply::default()
                        };
                        write_reply(&mut stream, OK, &reply, &[])?;
                    }
                    Err(error) => {
                        let reply = ProviderReply {
                            version: PROVIDER_VERSION,
                            name: replica.name().to_owned(),
                            detail: Some(format!("{error}")),
                            ..ProviderReply::default()
                        };
                        write_reply(&mut stream, FAILED, &reply, &[])?;
                    }
                }
            }
            other => {
                let reply = ProviderReply {
                    version: PROVIDER_VERSION,
                    name: replica.name().to_owned(),
                    detail: Some(format!("unknown op {other:?}")),
                    ..ProviderReply::default()
                };
                write_reply(&mut stream, FAILED, &reply, &[])?;
            }
        }
    }
}

/// A [`Replica`] on the other end of a provider.
///
/// Fetching walks the content in bounded ranges and assembles it locally, then
/// checks the whole digest once — the same rule as every other source, applied at
/// the only place the whole thing exists.
pub struct ProviderReplica {
    name: String,
    endpoint: String,
    writable: bool,
    timeout: Duration,
}

impl ProviderReplica {
    /// Introduce ourselves before claiming the provider's name.
    pub fn connect(endpoint: &str) -> Result<Self, VfsError> {
        Self::connect_with_timeout(endpoint, Duration::from_secs(20))
    }

    pub fn connect_with_timeout(endpoint: &str, timeout: Duration) -> Result<Self, VfsError> {
        let mut stream = dial(endpoint, timeout)?;
        let body = serde_json::to_vec(&ProviderRequest::hello())
            .map_err(|error| transport("encode hello", error))?;
        write_frame(&mut stream, &body)?;
        let (status, reply, _) = read_reply(&mut stream)?;
        let _ = stream.shutdown(Shutdown::Both);
        if status != OK {
            return Err(VfsError::Transport(format!(
                "{endpoint} refused the introduction: {}",
                reply.detail.unwrap_or_default()
            )));
        }
        Ok(Self {
            name: if reply.name.is_empty() {
                endpoint.to_owned()
            } else {
                reply.name
            },
            endpoint: endpoint.to_owned(),
            writable: reply.writable,
            timeout,
        })
    }
}

fn dial(endpoint: &str, timeout: Duration) -> Result<TcpStream, VfsError> {
    let stream = TcpStream::connect_timeout(
        &endpoint
            .to_socket_addrs()
            .map_err(|error| transport(&format!("resolve {endpoint}"), error))?
            .next()
            .ok_or_else(|| VfsError::Transport(format!("{endpoint} resolved to nothing")))?,
        timeout,
    )
    .map_err(|error| transport(&format!("connect {endpoint}"), error))?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    let _ = stream.set_nodelay(true);
    Ok(stream)
}

impl Replica for ProviderReplica {
    fn name(&self) -> &str {
        &self.name
    }

    fn fetch(&self, id: &ContentId, size: u64) -> Result<Vec<u8>, VfsError> {
        if size > MAX_CONTENT_BYTES {
            return Err(VfsError::Transport(format!(
                "{id} is {size} bytes; a provider fetch is bounded at {MAX_CONTENT_BYTES}"
            )));
        }
        let mut stream = dial(&self.endpoint, self.timeout)?;
        let mut bytes = Vec::new();
        loop {
            let request = ProviderRequest::range(id, bytes.len() as u64, MAX_RANGE_BYTES);
            let body =
                serde_json::to_vec(&request).map_err(|error| transport("encode fetch", error))?;
            write_frame(&mut stream, &body)?;
            let (status, reply, payload) = read_reply(&mut stream)?;
            match status {
                OK => {}
                NOT_FOUND => {
                    return Err(VfsError::NotHydrated(format!(
                        "{}: {}",
                        self.name,
                        reply.detail.unwrap_or_default()
                    )));
                }
                _ => {
                    return Err(VfsError::Transport(format!(
                        "{}: {}",
                        self.name,
                        reply
                            .detail
                            .unwrap_or_else(|| "provider refused the request".into())
                    )));
                }
            }
            if payload.is_empty() && reply.eof {
                break;
            }
            bytes.extend_from_slice(&payload);
            if reply.eof {
                break;
            }
        }
        let _ = stream.shutdown(Shutdown::Both);
        if ContentId::from_bytes(&bytes) != *id {
            return Err(VfsError::InvalidContentId(format!(
                "{} returned bytes that hash to something else",
                self.name
            )));
        }
        Ok(bytes)
    }

    fn put(&self, id: &ContentId, bytes: &[u8]) -> Result<Stored, VfsError> {
        if bytes.len() as u64 > MAX_CONTENT_BYTES {
            return Err(VfsError::Transport(format!(
                "{} bytes exceeds the {MAX_CONTENT_BYTES} byte provider limit",
                bytes.len()
            )));
        }
        let mut stream = dial(&self.endpoint, self.timeout)?;
        let request = ProviderRequest {
            version: PROVIDER_VERSION,
            op: "put".to_owned(),
            content_id: Some(id.clone()),
            offset: 0,
            length: u32::try_from(bytes.len()).unwrap_or(u32::MAX),
        };
        let body = serde_json::to_vec(&request).map_err(|error| transport("encode put", error))?;
        write_frame(&mut stream, &body)?;
        write_frame(&mut stream, bytes)?;
        let (status, reply, _) = read_reply(&mut stream)?;
        let _ = stream.shutdown(Shutdown::Both);
        match (status, reply.detail.as_deref()) {
            (OK, Some("already")) => Ok(Stored::AlreadyPresent),
            (OK, _) => Ok(Stored::Added),
            (_, detail) => Err(VfsError::Transport(format!(
                "{}: {}",
                self.name,
                detail.unwrap_or("provider failed the write")
            ))),
        }
    }

    fn writable(&self) -> bool {
        self.writable
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;
    use crate::DiskReplica;

    /// A source that answers with the wrong bytes. The digest is the only thing
    /// standing between a provider's mistake and a file.
    struct Lying;

    impl Replica for Lying {
        fn name(&self) -> &str {
            "liar"
        }
        fn fetch(&self, _id: &ContentId, _size: u64) -> Result<Vec<u8>, VfsError> {
            Ok(b"not what you asked for".to_vec())
        }
        fn put(&self, _id: &ContentId, _bytes: &[u8]) -> Result<Stored, VfsError> {
            Err(VfsError::ReadOnly)
        }
    }

    fn mirror(dir: &std::path::Path) -> DiskReplica {
        DiskReplica::new("shelf", dir.join("shelf"))
    }

    #[test]
    fn a_provider_serves_ranges_and_takes_writes() {
        let temp = tempfile::tempdir().unwrap();
        let shelf = mirror(temp.path());
        // Larger than one range on purpose: assembling them is the client's job.
        let big: Vec<u8> = (0..(MAX_RANGE_BYTES as usize * 3 + 17))
            .map(|index| (index % 251) as u8)
            .collect();
        let big_id = ContentId::from_bytes(&big);
        shelf.put(&big_id, &big).unwrap();
        let fresh = b"written through the provider";
        let fresh_id = ContentId::from_bytes(fresh);

        let server = ProviderServer::bind("127.0.0.1:0").unwrap();
        let endpoint = server.local_addr().unwrap().to_string();
        thread::scope(|scope| {
            let answering = scope.spawn(|| server.serve_n(&shelf, 4).unwrap());
            let client = ProviderReplica::connect(&endpoint).unwrap();
            assert_eq!(client.name(), "shelf");
            assert!(client.writable());

            let fetched = client.fetch(&big_id, big.len() as u64).unwrap();
            assert_eq!(fetched, big, "ranges reassemble byte for byte");

            assert_eq!(client.put(&fresh_id, fresh).unwrap(), Stored::Added);
            assert_eq!(
                client.put(&fresh_id, fresh).unwrap(),
                Stored::AlreadyPresent
            );
            assert_eq!(answering.join().unwrap(), 4);
        });
        // And the bytes are really on the shelf, not just acknowledged.
        assert_eq!(shelf.fetch(&fresh_id, 0).unwrap(), fresh);
    }

    #[test]
    fn a_provider_names_what_it_does_not_have() {
        let temp = tempfile::tempdir().unwrap();
        let shelf = mirror(temp.path());
        let missing = ContentId::from_bytes(b"nobody put this here");
        let server = ProviderServer::bind("127.0.0.1:0").unwrap();
        let endpoint = server.local_addr().unwrap().to_string();
        thread::scope(|scope| {
            scope.spawn(|| server.serve_n(&shelf, 2).unwrap());
            let client = ProviderReplica::connect(&endpoint).unwrap();
            let error = client.fetch(&missing, 20).unwrap_err();
            let message = format!("{error}");
            assert!(message.contains("not held here"), "{message}");
            assert!(message.contains("shelf"), "{message}");
        });
    }

    #[test]
    fn a_provider_refuses_bytes_that_do_not_match_their_name() {
        let temp = tempfile::tempdir().unwrap();
        let shelf = mirror(temp.path());
        let server = ProviderServer::bind("127.0.0.1:0").unwrap();
        let endpoint = server.local_addr().unwrap().to_string();
        thread::scope(|scope| {
            scope.spawn(|| server.serve_n(&shelf, 2).unwrap());
            let client = ProviderReplica::connect(&endpoint).unwrap();
            // The id names other bytes than the ones sent: a provider that trusted
            // its client would file a lie under a digest.
            let wrong = ContentId::from_bytes(b"the name of something else");
            let error = client
                .put(&wrong, b"these are not those bytes")
                .unwrap_err();
            let message = format!("{error}");
            assert!(message.contains("does not match"), "{message}");
        });
    }

    #[test]
    fn a_lying_provider_is_caught_by_the_digest() {
        let server = ProviderServer::bind("127.0.0.1:0").unwrap();
        let endpoint = server.local_addr().unwrap().to_string();
        thread::scope(|scope| {
            scope.spawn(|| server.serve_n(&Lying, 2).unwrap());
            let client = ProviderReplica::connect(&endpoint).unwrap();
            assert_eq!(client.name(), "liar");
            let id = ContentId::from_bytes(b"the real content");
            let error = client.fetch(&id, 10).unwrap_err();
            let message = format!("{error}");
            assert!(message.contains("hash to something else"), "{message}");
        });
    }

    #[test]
    fn a_read_only_provider_refuses_writes() {
        let temp = tempfile::tempdir().unwrap();
        let shelf = mirror(temp.path()).read_only();
        let server = ProviderServer::bind("127.0.0.1:0").unwrap();
        let endpoint = server.local_addr().unwrap().to_string();
        thread::scope(|scope| {
            scope.spawn(|| server.serve_n(&shelf, 2).unwrap());
            let client = ProviderReplica::connect(&endpoint).unwrap();
            assert!(!client.writable(), "the provider said so at hello");
            let bytes = b"their copy";
            let id = ContentId::from_bytes(bytes);
            let error = client.put(&id, bytes).unwrap_err();
            assert!(format!("{error}").contains("read-only"), "{error}");
        });
    }

    #[test]
    fn a_provider_speaking_another_version_is_refused() {
        let temp = tempfile::tempdir().unwrap();
        let shelf = mirror(temp.path());
        let server = ProviderServer::bind("127.0.0.1:0").unwrap();
        let endpoint = server.local_addr().unwrap().to_string();
        thread::scope(|scope| {
            scope.spawn(|| server.serve_n(&shelf, 1).unwrap());
            let mut stream = TcpStream::connect(&endpoint).unwrap();
            let request = ProviderRequest {
                version: PROVIDER_VERSION + 1,
                op: "hello".to_owned(),
                content_id: None,
                offset: 0,
                length: 0,
            };
            let body = serde_json::to_vec(&request).unwrap();
            write_frame(&mut stream, &body).unwrap();
            let (status, reply, _) = read_reply(&mut stream).unwrap();
            assert_eq!(status, REFUSED);
            assert!(
                reply.detail.unwrap().contains("version"),
                "the refusal names the disagreement"
            );
        });
    }
}
