//! Unibus content provider. It answers bounded `unibus.content.fetch.v1`
//! ranges from a full-node VFS and can serve either the TCP hub client or an
//! authenticated Unibus peer session (for example `unibus/peer/1` over Iroh).

use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use fungos_vfs::{
    CONTENT_DATA_KIND, CONTENT_ERROR_KIND, CONTENT_FETCH_KIND, ContentData, ContentError,
    ContentFetch, Vfs, VfsError,
};
use unibus::{Message, PeerSession, client::Client};

#[derive(Clone)]
pub struct ContentService {
    vfs: Arc<Mutex<Vfs>>,
}

impl ContentService {
    pub fn new(vfs: Vfs) -> Self {
        Self {
            vfs: Arc::new(Mutex::new(vfs)),
        }
    }

    pub fn handle(&self, fetch: &ContentFetch) -> Result<(ContentData, Vec<u8>), ContentError> {
        if let Err(error) = fetch.validate() {
            return Err(ContentError {
                request_id: fetch.request_id.clone(),
                code: "invalid_request".into(),
                detail: error.to_string(),
            });
        }
        let mut vfs = self.vfs.lock().expect("content vfs poisoned");
        let Some(entry) = vfs
            .find_by_content(&fetch.content_id)
            .map_err(service_error(fetch))?
        else {
            return Err(ContentError {
                request_id: fetch.request_id.clone(),
                code: "not_found".into(),
                detail: fetch.content_id.to_string(),
            });
        };
        let bytes = match vfs.get(&entry.path) {
            Ok(bytes) => bytes,
            Err(VfsError::NotHydrated(_)) => {
                return Err(ContentError {
                    request_id: fetch.request_id.clone(),
                    code: "not_hydrated".into(),
                    detail: fetch.content_id.to_string(),
                });
            }
            Err(error) => return Err(service_error(fetch)(error)),
        };
        let start = usize::try_from(fetch.offset).unwrap_or(usize::MAX);
        if start > bytes.len() {
            return Err(ContentError {
                request_id: fetch.request_id.clone(),
                code: "range_error".into(),
                detail: "offset beyond content".into(),
            });
        }
        let end = start.saturating_add(fetch.length as usize).min(bytes.len());
        let payload = bytes[start..end].to_vec();
        let data = ContentData {
            request_id: fetch.request_id.clone(),
            content_id: fetch.content_id.clone(),
            offset: fetch.offset,
            length: payload.len() as u32,
            eof: end == bytes.len(),
        };
        Ok((data, payload))
    }

    /// Synchronous TCP/hub loop. Runs until the client disconnects.
    pub fn serve_client(&self, client: &Client) -> Result<(), String> {
        loop {
            let Some((envelope, payload)) = client
                .try_read()
                .map_err(|error| format!("read content request: {error:#}"))?
            else {
                thread::sleep(Duration::from_millis(1));
                continue;
            };
            if !payload.is_empty() || envelope.message.kind != CONTENT_FETCH_KIND {
                continue;
            }
            let fetch: ContentFetch = match serde_json::from_value(envelope.message.body.into()) {
                Ok(fetch) => fetch,
                Err(error) => {
                    eprintln!("unibus content: bad fetch body: {error}");
                    continue;
                }
            };
            let (kind, body, bytes) = match self.handle(&fetch) {
                Ok((data, bytes)) => (
                    CONTENT_DATA_KIND,
                    serde_json::to_value(data).map_err(|error| error.to_string())?,
                    bytes,
                ),
                Err(error) => (
                    CONTENT_ERROR_KIND,
                    serde_json::to_value(error).map_err(|error| error.to_string())?,
                    Vec::new(),
                ),
            };
            let reply = client
                .envelope(Some(&envelope.producer_id), Message::new(kind, body))
                .map_err(|error| format!("build content reply: {error:#}"))?;
            client
                .send(&reply, &bytes)
                .map_err(|error| format!("send content reply: {error:#}"))?;
            // Keep the loop responsive while avoiding a hard spin if a
            // caller's client has no frame ready.
            thread::yield_now();
            thread::sleep(Duration::from_millis(1));
        }
    }

    /// Async peer loop for `unibus/peer/1` sessions, including Iroh.
    pub async fn serve_peer(
        &self,
        session: &mut dyn PeerSession,
        local_producer: &str,
    ) -> Result<(), String> {
        loop {
            let (envelope, payload) = session
                .receive()
                .await
                .map_err(|error| format!("receive content request: {error}"))?;
            if !payload.is_empty() || envelope.message.kind != CONTENT_FETCH_KIND {
                continue;
            }
            let fetch: ContentFetch = match serde_json::from_value(envelope.message.body.into()) {
                Ok(fetch) => fetch,
                Err(error) => {
                    eprintln!("unibus content: bad peer fetch body: {error}");
                    continue;
                }
            };
            let (kind, body, bytes) = match self.handle(&fetch) {
                Ok((data, bytes)) => (
                    CONTENT_DATA_KIND,
                    serde_json::to_value(data).map_err(|error| error.to_string())?,
                    bytes,
                ),
                Err(error) => (
                    CONTENT_ERROR_KIND,
                    serde_json::to_value(error).map_err(|error| error.to_string())?,
                    Vec::new(),
                ),
            };
            let mut reply = unibus::Envelope::new(
                &envelope.installation_id,
                local_producer,
                Some(&envelope.producer_id),
                "",
                Message::new(kind, body),
            );
            reply.payload_bytes = bytes.len();
            session
                .send(&reply, &bytes)
                .await
                .map_err(|error| format!("send content reply: {error}"))?;
        }
    }
}

fn service_error(fetch: &ContentFetch) -> impl FnOnce(VfsError) -> ContentError + '_ {
    move |error| ContentError {
        request_id: fetch.request_id.clone(),
        code: match error {
            VfsError::NotHydrated(_) => "not_hydrated",
            VfsError::NotFound(_) => "not_found",
            _ => "internal",
        }
        .into(),
        detail: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fungos_vfs::{ContentId, NodeRole, Vfs};

    #[test]
    fn serves_bounded_ranges_from_a_full_node() {
        let temp = tempfile::tempdir().unwrap();
        let mut vfs = Vfs::open(temp.path()).unwrap();
        let bytes: Vec<u8> = (0..4096).map(|value| (value % 251) as u8).collect();
        let entry = vfs
            .put("asset.bin", &bytes, "application/octet-stream", None)
            .unwrap();
        let service = ContentService::new(vfs);
        let fetch = ContentFetch {
            request_id: "range-1".into(),
            content_id: entry.content_id,
            offset: 1024,
            length: 64,
            locator: None,
        };
        let (data, payload) = service.handle(&fetch).unwrap();
        assert_eq!(payload, bytes[1024..1088]);
        assert_eq!(data.length, 64);
        assert!(!data.eof);

        let tail = ContentFetch {
            request_id: "range-2".into(),
            content_id: ContentId::from_bytes(&bytes),
            offset: 4032,
            length: 64,
            locator: None,
        };
        let (data, payload) = service.handle(&tail).unwrap();
        assert_eq!(payload, bytes[4032..]);
        assert!(data.eof);
    }

    #[test]
    fn cache_node_cannot_serve_a_missing_blob() {
        let temp = tempfile::tempdir().unwrap();
        let mut vfs = Vfs::open_with_role(temp.path(), NodeRole::Cache).unwrap();
        let content_id = ContentId::from_bytes(b"missing");
        vfs.register_placeholder(
            "missing.bin",
            content_id.clone(),
            7,
            "application/octet-stream",
        )
        .unwrap();
        let service = ContentService::new(vfs);
        let error = service
            .handle(&ContentFetch {
                request_id: "missing-1".into(),
                content_id,
                offset: 0,
                length: 16,
                locator: None,
            })
            .unwrap_err();
        assert_eq!(error.code, "not_hydrated");
    }
}
