//! End-to-end content fetch over an authenticated `unibus/peer/1` Iroh session.

use std::{
    collections::BTreeSet,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use fungos_vfs::{CONTENT_DATA_KIND, CONTENT_FETCH_KIND, ContentData, ContentFetch, Expect, Vfs};
use fungos_vfs_unibus::ContentService;
use iroh::{Endpoint, endpoint::presets};
use unibus::{
    DirectSessionClaims, DirectSessionError, DirectSessionVerifier, Envelope, IrohPeerEndpoint,
    Message, PeerBinding, PeerCandidate, PeerSession, PeerTransport,
};
use unibus_iroh::transport::{IrohListener, IrohTransport, SessionConfig};

struct FixtureVerifier(DirectSessionClaims);

impl DirectSessionVerifier for FixtureVerifier {
    fn verify(
        &self,
        assertion: &str,
        _now: u64,
    ) -> Result<DirectSessionClaims, DirectSessionError> {
        let claims: DirectSessionClaims =
            serde_json::from_str(assertion).map_err(|e| DirectSessionError(e.to_string()))?;
        if claims == self.0 {
            Ok(claims)
        } else {
            Err(DirectSessionError("claims mismatch".into()))
        }
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

async fn endpoint() -> Endpoint {
    Endpoint::builder(presets::Minimal)
        .relay_mode(iroh::RelayMode::Disabled)
        .bind_addr("127.0.0.1:0")
        .unwrap()
        .alpns(vec![unibus_iroh::PEER_ALPN.as_bytes().to_vec()])
        .bind()
        .await
        .unwrap()
}

fn binding(producer: &str, endpoint: &Endpoint) -> PeerBinding {
    PeerBinding {
        producer_id: producer.into(),
        key_binding: format!("iroh:{}", endpoint.id()),
        candidates: vec![],
    }
}

fn session_config(producer: &str, claims: DirectSessionClaims) -> SessionConfig {
    SessionConfig {
        verifier: Arc::new(FixtureVerifier(claims)),
        audience: "test".into(),
        installation_id: "test".into(),
        local_producer: producer.into(),
        allowed_relays: BTreeSet::new(),
        access_policy: None,
        endpoint_identity: None,
    }
}

#[tokio::test]
async fn content_service_serves_a_range_over_iroh() {
    let temp = tempfile::tempdir().unwrap();
    let mut vfs = Vfs::open(temp.path()).unwrap();
    let bytes: Vec<u8> = (0..4096).map(|value| (value % 251) as u8).collect();
    let entry = vfs
        .put("asset.bin", &bytes, "application/octet-stream", Expect::Any)
        .unwrap();
    let service = ContentService::new(vfs);

    let cache = endpoint().await;
    let studio = endpoint().await;
    let now = now();
    let claims = DirectSessionClaims {
        schema_version: 1,
        iss: "fixture".into(),
        aud: "test".into(),
        jti: "content-session".into(),
        installation_id: "test".into(),
        iat: now,
        exp: now + 60,
        initiator: binding("cache", &cache),
        responder: binding("studio", &studio),
        kinds: vec![
            CONTENT_FETCH_KIND.into(),
            CONTENT_DATA_KIND.into(),
            fungos_vfs::CONTENT_ERROR_KIND.into(),
        ],
        max_payload_bytes: fungos_vfs::MAX_RANGE_BYTES,
    };
    let candidate = PeerCandidate {
        peer_id: "studio".into(),
        site: None,
        endpoints: vec![],
        expires_at_unix_s: now + 60,
        iroh_endpoints: vec![IrohPeerEndpoint {
            endpoint_id: studio.id().to_string(),
            direct_addresses: studio.addr().ip_addrs().copied().collect(),
            relay_urls: vec![],
        }],
    };
    let transport = IrohTransport::new(cache.clone(), session_config("cache", claims.clone()));
    let listener = IrohListener::new(studio.clone(), session_config("studio", claims.clone()));
    let assertion = serde_json::to_string(&claims).unwrap();
    let (client, server) =
        tokio::join!(transport.connect(&candidate, &assertion), listener.accept());
    let mut client = client.unwrap();
    let server = server.unwrap();

    let service_task = {
        let service = service.clone();
        let mut session: Box<dyn PeerSession> = Box::new(server);
        tokio::spawn(async move { service.serve_peer(session.as_mut(), "studio").await })
    };

    let fetch = ContentFetch {
        request_id: "iroh-range-1".into(),
        content_id: entry.content_id.clone(),
        offset: 1024,
        length: 64,
        locator: None,
    };
    let mut request = Envelope::new(
        "test",
        "cache",
        Some("studio"),
        "",
        Message::new(CONTENT_FETCH_KIND, serde_json::to_value(&fetch).unwrap()),
    );
    request.payload_bytes = 0;
    client.send(&request, &[]).await.unwrap();
    let (reply, payload) =
        match tokio::time::timeout(std::time::Duration::from_secs(2), client.receive()).await {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => {
                if service_task.is_finished() {
                    eprintln!("server task result: {:?}", service_task.await);
                }
                panic!("receive: {error}");
            }
            Err(_) => {
                service_task.abort();
                panic!("receive timeout");
            }
        };
    assert_eq!(reply.message.kind, CONTENT_DATA_KIND);
    let data: ContentData = serde_json::from_value(reply.message.body.into()).unwrap();
    assert_eq!(data.request_id, fetch.request_id);
    assert_eq!(payload, bytes[1024..1088]);
    assert_eq!(data.length, 64);

    service_task.abort();
    drop(client);
    cache.close().await;
    studio.close().await;
}
