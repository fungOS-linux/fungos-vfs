//! A QUIC carrier for the sync protocol.
//!
//! [`crate::sync`] is transport-free on purpose: what a peer needs is a value,
//! and `pull_over_ssh` is a short function that moves it. This module is the
//! carrier that can do better than one message in and one out — it runs [`crate::stream`]
//! over a QUIC stream, which is the one wire here that can carry a snapshot a
//! frame at a time and continue one that was interrupted.
//!
//! Three things are specific to this wire:
//!
//! - **Framing.** Each frame travels as a 4-byte big-endian length followed by
//!   that many bytes, and the frame kinds and their bodies are
//!   [`crate::stream`]'s. Because the unit is a frame rather than a message, a
//!   snapshot is bounded by its largest entry instead of by its total size, and
//!   the receiver applies entries as they arrive instead of after the last one.
//!   A peer that announces a frame above [`crate::MAX_FRAME`] is refused rather
//!   than trusted.
//! - **Identity.** QUIC is TLS 1.3, so the server has a certificate. There is no
//!   CA here: the server self-signs, and the client refuses any certificate
//!   whose fingerprint does not match one it was given out of band. The binding
//!   is the point — when an L3 overlay arrives it supplies "this address is this
//!   peer", and the fingerprint stays the mechanism that makes it true.
//! - **No names.** An endpoint is an address string, parsed as `host:port` with
//!   `host` numeric. There is no lookup, no discovery and no NAT traversal:
//!   reaching a peer across hosts is the overlay's job, and this carrier only
//!   has to be correct once the address is known.
//!
//! One stream carries one exchange. A snapshot that dies with its stream can be
//! asked for again on a new connection, continuing from the entry the receiver
//! had reached; the session identity that makes that safe lives in
//! [`crate::stream`], so this module only has to reconnect and try.

use std::{fs, io::Write, net::SocketAddr, path::Path, sync::Arc, time::Duration};

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::rustls::{
    self, DigitallySignedStruct, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime},
};

use crate::{ApplyOutcome, PullClient, TransferReport, Vfs, VfsError, stream};

/// The protocol a client and server must agree on before anything is exchanged.
///
/// Named rather than empty so a future carrier change cannot be negotiated by
/// accident: a peer speaking a different revision fails the handshake instead of
/// deserialising something unexpected. Bumped to 2 with the framed format: a
/// peer that only knows the whole-message exchange would read a frame kind byte
/// where it expected JSON.
const ALPN: &[u8] = b"fungos-vfs-sync/2";

/// How many connections one exchange may take before it gives up.
///
/// Enough to ride out a peer that dropped a stream, few enough that a peer that
/// is simply gone is reported instead of retried forever.
const MAX_ATTEMPTS: u32 = 3;

/// The name the client presents and the server ignores.
///
/// Pinning is what decides whether the peer is who the caller meant, and a
/// pinned certificate is not checked against a name, so this label carries no
/// trust. It exists because the TLS API requires one.
const SERVER_NAME: &str = "fungos-vfs.local";

/// How long one session may take before it is abandoned.
///
/// A peer that vanishes mid-handshake or mid-transfer must not pin a CLI or a
/// server forever; a store sync that has not finished in this long has a problem
/// a longer wait will not fix.
const SESSION_TIMEOUT: Duration = Duration::from_secs(30);

/// The fingerprint of one certificate: BLAKE3 of its DER, hex encoded.
///
/// The same hash the store already uses for content ids, and the same hex the
/// wire already uses for blobs, so a fingerprint needs no new dependency and no
/// second encoding to be written down or compared.
fn fingerprint_of(der: &CertificateDer<'_>) -> String {
    blake3::hash(der.as_ref()).to_hex().to_string()
}

/// Map any carrier failure onto the one transport error the crate reports.
fn transport(what: impl std::fmt::Display, error: impl std::fmt::Display) -> VfsError {
    VfsError::Transport(format!("{what}: {error}"))
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn runtime() -> Result<tokio::runtime::Runtime, VfsError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| transport("start async runtime", error))
}

/// A server's TLS identity: a certificate nobody signed, and the key for it.
///
/// Self-signed is not a compromise here, it is the model. Nothing in this system
/// can vouch for a peer except the operator who handed over a fingerprint, so the
/// certificate's only job is to be a stable public half that a fingerprint can
/// name.
struct Identity {
    cert: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
}

impl Identity {
    fn generate() -> Result<Self, VfsError> {
        let generated = rcgen::generate_simple_self_signed(vec![SERVER_NAME.to_string()])
            .map_err(|error| transport("generate certificate", error))?;
        Ok(Self {
            cert: generated.cert.der().clone(),
            key: PrivateKeyDer::Pkcs8(generated.key_pair.serialize_der().into()),
        })
    }

    /// Read the identity in `path`, or generate one and write it there.
    ///
    /// Reuse rather than regenerate: the certificate is the server's name, so a
    /// restart that minted a new one would silently invalidate every fingerprint
    /// a peer was configured with.
    fn load_or_create(path: &Path) -> Result<Self, VfsError> {
        match fs::read(path) {
            Ok(pem_bytes) => Self::from_pem(&pem_bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let identity = Self::generate()?;
                identity.write(path)?;
                Ok(identity)
            }
            Err(error) => Err(VfsError::Io(error)),
        }
    }

    fn from_pem(bytes: &[u8]) -> Result<Self, VfsError> {
        let blocks =
            pem::parse_many(bytes).map_err(|error| transport("parse identity file", error))?;
        let cert = blocks
            .iter()
            .find(|block| block.tag() == "CERTIFICATE")
            .ok_or_else(|| transport("parse identity file", "no CERTIFICATE block"))?;
        let key = blocks
            .iter()
            .find(|block| block.tag() == "PRIVATE KEY")
            .ok_or_else(|| transport("parse identity file", "no PRIVATE KEY block"))?;
        Ok(Self {
            cert: CertificateDer::from(cert.contents().to_vec()),
            key: PrivateKeyDer::try_from(key.contents().to_vec())
                .map_err(|error| transport("parse identity key", error))?,
        })
    }

    fn to_pem(&self) -> String {
        let mut text = pem::encode(&pem::Pem::new("CERTIFICATE", self.cert.as_ref().to_vec()));
        text.push_str(&pem::encode(&pem::Pem::new(
            "PRIVATE KEY",
            self.key.secret_der().to_vec(),
        )));
        text
    }

    fn write(&self, path: &Path) -> Result<(), VfsError> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent)?;
        }
        // The file holds a private key, so it is written owner-only from the
        // start rather than narrowed afterwards.
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(path)?;
        file.write_all(self.to_pem().as_bytes())?;
        file.sync_all()?;
        Ok(())
    }
}

fn server_config(identity: &Identity) -> Result<quinn::ServerConfig, VfsError> {
    let mut config = rustls::ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|error| transport("tls config", error))?
        .with_no_client_auth()
        .with_single_cert(vec![identity.cert.clone()], identity.key.clone_key())
        .map_err(|error| transport("tls config", error))?;
    config.alpn_protocols = vec![ALPN.to_vec()];
    let crypto =
        QuicServerConfig::try_from(config).map_err(|error| transport("quic config", error))?;
    let mut server = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    server.transport_config(transport_config());
    Ok(server)
}

/// Flow control wide enough that a multi-megabyte snapshot is limited by the
/// network rather than by window-sized round trips.
fn transport_config() -> Arc<quinn::TransportConfig> {
    let mut transport = quinn::TransportConfig::default();
    transport.stream_receive_window(quinn::VarInt::from_u32(8 * 1024 * 1024));
    transport.receive_window(quinn::VarInt::from_u32(16 * 1024 * 1024));
    Arc::new(transport)
}

/// A listener that answers sync requests over QUIC.
///
/// It owns its async runtime, because the endpoint is registered with the reactor
/// that created it: sessions can be driven one call at a time from blocking code
/// without the socket outliving what it is registered on.
pub struct SyncServer {
    // Declared before the runtime so it is dropped first: an endpoint that
    // outlives its reactor is a dangling registration.
    endpoint: quinn::Endpoint,
    runtime: tokio::runtime::Runtime,
    cert: CertificateDer<'static>,
}

impl SyncServer {
    /// Listen on `addr` with a freshly generated identity.
    ///
    /// For a long-lived peer use [`SyncServer::bind_with_identity`]: an identity
    /// that is regenerated on every start is a new peer every time.
    pub fn bind(addr: &str) -> Result<Self, VfsError> {
        let identity = Identity::generate()?;
        Self::start(addr, identity)
    }

    /// Listen on `addr`, reading the identity from `path` or creating it there.
    pub fn bind_with_identity(addr: &str, path: &Path) -> Result<Self, VfsError> {
        let identity = Identity::load_or_create(path)?;
        Self::start(addr, identity)
    }

    fn start(addr: &str, identity: Identity) -> Result<Self, VfsError> {
        let addr: SocketAddr = addr
            .parse()
            .map_err(|error| transport("bind", format!("{addr} is not host:port: {error}")))?;
        let runtime = runtime()?;
        let config = server_config(&identity)?;
        let endpoint = runtime
            .block_on(async { quinn::Endpoint::server(config, addr) })
            .map_err(|error| transport("bind", error))?;
        Ok(Self {
            endpoint,
            runtime,
            cert: identity.cert,
        })
    }

    /// The fingerprint a client must be given out of band to reach this server.
    pub fn fingerprint(&self) -> String {
        fingerprint_of(&self.cert)
    }

    /// The address actually bound, which differs from the requested one when
    /// port zero was asked for.
    pub fn local_addr(&self) -> Result<SocketAddr, VfsError> {
        self.endpoint
            .local_addr()
            .map_err(|error| transport("local address", error))
    }

    /// Answer one peer connection and report how many requests it made.
    ///
    /// A connection ends when the peer closes it, which is what a client does
    /// after reading its answer. Sessions are served one at a time: the store has
    /// a single writer, so two peers in flight could only be interleaved, not
    /// helped. A caller that means to stay up calls this in a loop — and decides
    /// there whether a failed session is worth reporting, and to whom.
    pub fn accept_one(&self, vfs: &mut Vfs) -> Result<usize, VfsError> {
        let endpoint = &self.endpoint;
        self.runtime.block_on(async {
            match tokio::time::timeout(SESSION_TIMEOUT, serve_session(endpoint, vfs)).await {
                Ok(result) => result,
                Err(_) => Err(transport(
                    "serve session",
                    format!("no progress for {}s", SESSION_TIMEOUT.as_secs()),
                )),
            }
        })
    }
}

/// Answer every stream one peer opens, until that peer goes away.
async fn serve_session(endpoint: &quinn::Endpoint, vfs: &mut Vfs) -> Result<usize, VfsError> {
    let Some(incoming) = endpoint.accept().await else {
        return Err(transport("accept", "endpoint closed"));
    };
    let connection = incoming
        .await
        .map_err(|error| transport("handshake", error))?;
    let mut answered = 0;
    loop {
        let (mut send, mut recv) = match connection.accept_bi().await {
            Ok(stream) => stream,
            // A peer that closes the connection has finished, which is how a
            // client ends a session it got an answer out of.
            Err(quinn::ConnectionError::ApplicationClosed(_)) => break,
            Err(error) => return Err(transport("accept stream", error)),
        };
        // One stream is one exchange, and the exchange decides its own frames:
        // a delta is one frame, a snapshot is a header, an entry per path and an
        // end marker. A stream that dies mid-snapshot leaves the receiver's
        // store untouched — nothing is committed until the end marker — so the
        // answer to a torn stream is a new one, not a repair.
        stream::serve(vfs, &mut recv, &mut send, stream::MAX_FRAME).await?;
        let _ = send.finish();
        answered += 1;
    }
    connection.close(0u32.into(), b"done");
    Ok(answered)
}

/// A client that trusts exactly one certificate.
///
/// Every question rustls would normally answer with a chain is answered here by
/// comparing the end entity's fingerprint with the one the caller supplied. The
/// handshake signatures are still verified with the real algorithms, so the
/// certificate must also hold the key it claims to.
struct Pinned {
    expected: String,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl std::fmt::Debug for Pinned {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Pinned")
            .field("expected", &self.expected)
            .finish_non_exhaustive()
    }
}

impl ServerCertVerifier for Pinned {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let seen = fingerprint_of(end_entity);
        if seen == self.expected {
            Ok(ServerCertVerified::assertion())
        } else {
            // Naming both is what makes the failure fixable: a typo, a stale
            // identity file and a hostile peer all look the same otherwise.
            Err(rustls::Error::General(format!(
                "server certificate fingerprint {seen} does not match the pinned {}",
                self.expected
            )))
        }
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        // Only TLS 1.3 is configured, so a TLS 1.2 signature is a peer that is
        // not speaking this protocol at all.
        Err(rustls::Error::PeerIncompatible(
            rustls::PeerIncompatible::Tls12NotOffered,
        ))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn client_endpoint(addr: SocketAddr, pin: &str) -> Result<quinn::Endpoint, VfsError> {
    let provider = provider();
    let mut config = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|error| transport("tls config", error))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(Pinned {
            expected: pin.to_string(),
            provider,
        }))
        .with_no_client_auth();
    config.alpn_protocols = vec![ALPN.to_vec()];
    let crypto =
        QuicClientConfig::try_from(config).map_err(|error| transport("quic config", error))?;

    // One local socket family, chosen by the peer's, so an IPv6 endpoint is
    // reachable without a second configuration knob.
    let bind = if addr.is_ipv4() {
        SocketAddr::from(([0, 0, 0, 0], 0))
    } else {
        SocketAddr::from(([0u16; 8], 0))
    };
    let mut endpoint =
        quinn::Endpoint::client(bind).map_err(|error| transport("open socket", error))?;
    let mut client = quinn::ClientConfig::new(Arc::new(crypto));
    client.transport_config(transport_config());
    endpoint.set_default_client_config(client);
    Ok(endpoint)
}

/// Ask a peer over QUIC and apply the answer.
///
/// The second wire for the exchange [`crate::pull_over_ssh`] carries: the same
/// cursor, the same delta rules, the same snapshot applied by the same staging
/// code. The differences are the address (numeric `host:port`), that the peer
/// must present the certificate whose fingerprint the caller pins, and that a
/// snapshot arrives a frame at a time and can be continued after a tear.
pub fn pull_over_quic(
    vfs: &mut Vfs,
    addr: &str,
    fingerprint: &str,
    limit: usize,
) -> Result<(Vec<ApplyOutcome>, TransferReport), VfsError> {
    pull(
        vfs,
        addr,
        fingerprint,
        limit,
        stream::MAX_FRAME,
        MAX_ATTEMPTS,
    )
}

/// The whole exchange, also reporting what it cost.
///
/// An attempt that fails on the carrier while a snapshot is in flight is retried
/// on a fresh connection from the point the receiver reached. Anything else — a
/// refusal, a store that has nothing to give — is reported once, because trying
/// again would only fail the same way.
fn pull(
    vfs: &mut Vfs,
    addr: &str,
    expected: &str,
    limit: usize,
    max_frame: usize,
    max_attempts: u32,
) -> Result<(Vec<ApplyOutcome>, TransferReport), VfsError> {
    let target: SocketAddr = addr
        .parse()
        .map_err(|error| transport("connect", format!("{addr} is not host:port: {error}")))?;
    let runtime = runtime()?;
    let mut client = PullClient::new(vfs.sync_cursor()?, limit);
    let mut failure = None;
    for _ in 0..max_attempts {
        let result = runtime.block_on(async {
            let session = async {
                let endpoint = client_endpoint(target, expected)?;
                let connection = endpoint
                    .connect(target, SERVER_NAME)
                    .map_err(|error| transport("connect", error))?
                    .await
                    .map_err(|error| transport("handshake", error))?;
                let (mut send, mut recv) = connection
                    .open_bi()
                    .await
                    .map_err(|error| transport("open stream", error))?;
                let outcomes = client.attempt(vfs, &mut recv, &mut send, max_frame).await?;
                let _ = send.finish();
                connection.close(0u32.into(), b"done");
                endpoint.wait_idle().await;
                Ok::<_, VfsError>(outcomes)
            };
            match tokio::time::timeout(SESSION_TIMEOUT, session).await {
                Ok(result) => result,
                Err(_) => Err(transport(
                    "sync",
                    format!("no answer within {}s", SESSION_TIMEOUT.as_secs()),
                )),
            }
        });
        match result {
            Ok(outcomes) => return Ok((outcomes, client.report())),
            Err(error @ VfsError::Transport(_)) if client.resume().is_some() => {
                // The receiver still holds what it staged and the entry it
                // reached, so the next connection asks to continue rather than
                // asking for the store again.
                failure = Some(error);
            }
            Err(error) => return Err(error),
        }
    }
    Err(failure.unwrap_or_else(|| transport("sync", "no attempt was made")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::Cut;
    use crate::{Expect, Hlc, NodeRole, Vfs};
    use std::{path::Path, thread};

    /// Port zero: the kernel picks one, so tests never collide.
    const LOOPBACK: &str = "127.0.0.1:0";

    /// Two stores on one host: the same origin, distinct store identities, which
    /// is the harder case for the cursor and therefore the one worth carrying.
    ///
    /// The content store under `dir` is the test's own, so nothing here reads or
    /// writes the store the machine's real roots share.
    fn pair(dir: &Path) -> (Vfs, Vfs) {
        let cas = dir.join("cas");
        (
            Vfs::open_with(dir.join("a"), NodeRole::Full, Some(&cas)).unwrap(),
            Vfs::open_with(dir.join("b"), NodeRole::Full, Some(&cas)).unwrap(),
        )
    }

    /// A server on its own thread with its own store, plus the two things a
    /// client needs: where to connect, and what to pin.
    struct Peer {
        addr: SocketAddr,
        fingerprint: String,
        handle: thread::JoinHandle<Vec<String>>,
    }

    impl Peer {
        fn start(server: SyncServer, vfs: Vfs, sessions: usize) -> Self {
            Self::start_with(server, vfs, sessions, |_, _| {})
        }

        /// Serve `sessions` connections, calling `between` with the number
        /// finished so far. That is how a test changes the store between
        /// sessions, which is the only way a peer can fall behind a compaction.
        fn start_with(
            server: SyncServer,
            mut vfs: Vfs,
            sessions: usize,
            mut between: impl FnMut(usize, &mut Vfs) + Send + 'static,
        ) -> Self {
            let addr = server.local_addr().unwrap();
            let fingerprint = server.fingerprint();
            let handle = thread::spawn(move || {
                let mut failures = Vec::new();
                for session in 0..sessions {
                    if let Err(error) = server.accept_one(&mut vfs) {
                        failures.push(error.to_string());
                    }
                    between(session + 1, &mut vfs);
                }
                failures
            });
            Self {
                addr,
                fingerprint,
                handle,
            }
        }

        fn endpoint(&self) -> String {
            self.addr.to_string()
        }

        /// Sessions the server refused, which is empty for a well-behaved peer.
        fn join(self) -> Vec<String> {
            self.handle.join().expect("server thread panicked")
        }
    }

    /// Compaction compares against the wall clock, so a row written in this
    /// millisecond is not old enough to drop yet.
    fn wait_past(hlc: Hlc) {
        while crate::vfs::now_ms() <= hlc.ms {
            thread::sleep(Duration::from_millis(5));
        }
    }

    /// The newest op a store has queued, which is the one a drop has to wait for.
    fn newest(vfs: &Vfs) -> Hlc {
        vfs.pending(100).unwrap().last().unwrap().hlc
    }

    #[test]
    fn a_round_trip_over_quic_applies_ops_and_content() {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, mut b) = pair(dir.path());
        a.put("notes/one.md", b"one", "text/markdown", Expect::Any)
            .unwrap();
        a.put("notes/two.md", b"two", "text/markdown", Expect::Any)
            .unwrap();

        // Two sessions: one to catch up, one to show there is nothing left.
        let peer = Peer::start(SyncServer::bind(LOOPBACK).unwrap(), a, 2);

        let (outcomes, report) =
            pull_over_quic(&mut b, &peer.endpoint(), &peer.fingerprint, 100).unwrap();
        assert_eq!(outcomes, vec![ApplyOutcome::Applied; 2]);
        assert_eq!(report.entries, 2);
        assert!(report.session.is_none(), "a delta is not a session");
        assert_eq!(b.get("notes/one.md").unwrap(), b"one");
        assert_eq!(b.get("notes/two.md").unwrap(), b"two");

        // The second round trip has nothing left to send.
        let (outcomes, _) =
            pull_over_quic(&mut b, &peer.endpoint(), &peer.fingerprint, 100).unwrap();
        assert!(outcomes.is_empty(), "{outcomes:?}");

        assert!(peer.join().is_empty());
    }

    #[test]
    fn a_peer_behind_the_horizon_gets_a_snapshot_over_quic() {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, mut b) = pair(dir.path());
        a.put("notes/one.md", b"one", "text/markdown", Expect::Any)
            .unwrap();

        let peer = Peer::start_with(
            SyncServer::bind(LOOPBACK).unwrap(),
            a,
            2,
            |completed, store| {
                if completed == 1 {
                    store
                        .put("notes/two.md", b"two", "text/markdown", Expect::Any)
                        .unwrap();
                    wait_past(newest(store));
                    store.compact(Duration::ZERO).unwrap();
                }
            },
        );

        // b sees one write, so its cursor is a real reading that the compaction
        // can leave behind — an empty cursor was simply never caught up.
        let (outcomes, _) =
            pull_over_quic(&mut b, &peer.endpoint(), &peer.fingerprint, 100).unwrap();
        assert_eq!(outcomes, vec![ApplyOutcome::Applied]);

        // The delta b would get starts after the hole and looks complete, so it
        // is told the log cannot reach it and handed the state instead.
        let (outcomes, report) =
            pull_over_quic(&mut b, &peer.endpoint(), &peer.fingerprint, 100).unwrap();
        assert_eq!(outcomes, vec![ApplyOutcome::Snapshot]);
        assert!(report.session.is_some(), "a snapshot names its session");
        assert_eq!(report.entries, 2);
        assert_eq!(
            b.get("notes/one.md").unwrap(),
            b"one",
            "the write whose op was dropped still arrives"
        );
        assert_eq!(b.get("notes/two.md").unwrap(), b"two");

        assert!(peer.join().is_empty());
    }

    #[test]
    fn a_wrong_fingerprint_is_refused_before_anything_is_applied() {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, mut b) = pair(dir.path());
        let a_store = a.store().to_string();
        a.put("notes/one.md", b"one", "text/markdown", Expect::Any)
            .unwrap();

        // One session: the handshake fails, so no request is ever answered.
        let peer = Peer::start(SyncServer::bind(LOOPBACK).unwrap(), a, 1);
        // Another server's identity is a certificate this client has no reason to
        // trust, so the pin cannot match by anything but accident.
        let other = SyncServer::bind(LOOPBACK).unwrap().fingerprint();
        assert_ne!(other, peer.fingerprint);

        let error = pull_over_quic(&mut b, &peer.endpoint(), &other, 100).unwrap_err();
        assert!(matches!(error, VfsError::Transport(_)), "{error:?}");

        // Refused before any ops: the store still knows nothing about the peer.
        assert!(b.file("notes/one.md").unwrap().is_none());
        assert!(
            b.sync_cursor().unwrap().get(&a_store).is_none(),
            "nothing from the peer was recorded"
        );
        assert_eq!(
            peer.join().len(),
            1,
            "the attempt reached the server and was refused there too"
        );
    }

    #[test]
    fn a_snapshot_far_bigger_than_one_frame_crosses_the_stream_intact() {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, mut b) = pair(dir.path());
        a.put("seed.md", b"seed", "text/markdown", Expect::Any)
            .unwrap();

        const FILES: usize = 24;
        const SIZE: usize = 256 * 1024;
        // One entry has to fit in a frame; the store does not. The constant is
        // reduced here rather than sending 65 MiB, and it is the frame limit
        // being respected per frame that the test is about.
        const FRAME: usize = 512 * 1024;

        let peer = Peer::start_with(
            SyncServer::bind(LOOPBACK).unwrap(),
            a,
            2,
            |completed, store| {
                if completed != 1 {
                    return;
                }
                for index in 0..FILES {
                    // Distinct bytes per file, so each is its own content id and
                    // the answer cannot be smaller than the content in it.
                    let mut bytes = vec![b'a'; SIZE];
                    bytes[0] = index as u8;
                    store
                        .put(
                            &format!("bulk/{index}.bin"),
                            &bytes,
                            "application/octet-stream",
                            Expect::Any,
                        )
                        .unwrap();
                }
                wait_past(newest(store));
                store.compact(Duration::ZERO).unwrap();
            },
        );

        // b learns the seed first, so the compaction can leave it behind.
        let (outcomes, _) =
            pull_over_quic(&mut b, &peer.endpoint(), &peer.fingerprint, 100).unwrap();
        assert_eq!(outcomes, vec![ApplyOutcome::Applied]);

        let (outcomes, report) = pull(
            &mut b,
            &peer.endpoint(),
            &peer.fingerprint,
            100,
            FRAME,
            MAX_ATTEMPTS,
        )
        .unwrap();
        assert_eq!(outcomes, vec![ApplyOutcome::Snapshot]);
        assert!(
            report.bytes > 8 * FRAME as u64,
            "a {} byte answer does not prove the cap is per frame",
            report.bytes
        );
        // The seed is in the snapshot too, so it is part of the payload.
        assert_eq!(report.content_bytes, (FILES * SIZE + b"seed".len()) as u64);

        for index in 0..FILES {
            let mut expected = vec![b'a'; SIZE];
            expected[0] = index as u8;
            assert_eq!(
                b.get(&format!("bulk/{index}.bin")).unwrap(),
                expected,
                "file {index} did not arrive intact"
            );
        }
        assert_eq!(b.get("seed.md").unwrap(), b"seed");

        assert!(peer.join().is_empty());
    }

    #[test]
    fn an_interrupted_snapshot_resumes_on_a_second_connection() {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, mut b) = pair(dir.path());
        a.put("seed.md", b"seed", "text/markdown", Expect::Any)
            .unwrap();

        const FILES: usize = 12;
        const SIZE: usize = 64 * 1024;
        const FRAME: usize = 128 * 1024;
        // Three connections: the seed delta, the one that is cut, and the one
        // that resumes.
        let peer = Peer::start_with(
            SyncServer::bind(LOOPBACK).unwrap(),
            a,
            3,
            |completed, store| {
                if completed != 1 {
                    return;
                }
                for index in 0..FILES {
                    let mut bytes = vec![b'a'; SIZE];
                    bytes[0] = index as u8;
                    store
                        .put(
                            &format!("bulk/{index}.bin"),
                            &bytes,
                            "application/octet-stream",
                            Expect::Any,
                        )
                        .unwrap();
                }
                wait_past(newest(store));
                store.compact(Duration::ZERO).unwrap();
            },
        );

        let (outcomes, _) =
            pull_over_quic(&mut b, &peer.endpoint(), &peer.fingerprint, 100).unwrap();
        assert_eq!(outcomes, vec![ApplyOutcome::Applied]);
        let before = serde_json::to_string(&b.list("").unwrap()).unwrap();

        let target: SocketAddr = peer.endpoint().parse().unwrap();
        let runtime = runtime().unwrap();
        let mut client = PullClient::new(b.sync_cursor().unwrap(), 100);

        // The first attempt is cut three and a half entries in.
        let error = runtime
            .block_on(async {
                let endpoint = client_endpoint(target, &peer.fingerprint)?;
                let connection = endpoint
                    .connect(target, SERVER_NAME)
                    .map_err(|error| transport("connect", error))?
                    .await
                    .map_err(|error| transport("handshake", error))?;
                let (mut send, recv) = connection
                    .open_bi()
                    .await
                    .map_err(|error| transport("open stream", error))?;
                let mut cut = Cut::new(recv, FRAME + 3 * FRAME + FRAME / 2);
                let result = client.attempt(&mut b, &mut cut, &mut send, FRAME).await;
                // A cut is not a polite close: dropping the connection is what
                // makes the sender see a stream that ended without a request to
                // end it.
                connection.close(0u32.into(), b"cut");
                result
            })
            .unwrap_err();
        assert!(matches!(error, VfsError::Transport(_)), "{error:?}");
        assert_eq!(
            serde_json::to_string(&b.list("").unwrap()).unwrap(),
            before,
            "a torn transfer changed the namespace"
        );
        let staged = client
            .resume()
            .expect("a snapshot in flight has a resume point")
            .from;
        assert!(staged > 0 && staged < FILES as u64 + 1, "{staged}");

        let outcomes = runtime
            .block_on(async {
                let endpoint = client_endpoint(target, &peer.fingerprint)?;
                let connection = endpoint
                    .connect(target, SERVER_NAME)
                    .map_err(|error| transport("connect", error))?
                    .await
                    .map_err(|error| transport("handshake", error))?;
                let (mut send, mut recv) = connection
                    .open_bi()
                    .await
                    .map_err(|error| transport("open stream", error))?;
                let outcomes = client.attempt(&mut b, &mut recv, &mut send, FRAME).await?;
                let _ = send.finish();
                connection.close(0u32.into(), b"done");
                endpoint.wait_idle().await;
                Ok::<_, VfsError>(outcomes)
            })
            .unwrap();
        assert_eq!(outcomes, vec![ApplyOutcome::Snapshot]);
        let report = client.report();
        assert_eq!(report.attempts, 2);
        assert_eq!(report.resumed_from, staged);
        for index in 0..FILES {
            let mut expected = vec![b'a'; SIZE];
            expected[0] = index as u8;
            assert_eq!(
                b.get(&format!("bulk/{index}.bin")).unwrap(),
                expected,
                "file {index} did not arrive intact"
            );
        }

        // A cut may surface as a failed session, or be absorbed by the send
        // window before the sender notices; either way the server survived it,
        // and the next connection was served.
        let failures = peer.join();
        assert!(failures.len() <= 1, "{failures:?}");
    }
}
