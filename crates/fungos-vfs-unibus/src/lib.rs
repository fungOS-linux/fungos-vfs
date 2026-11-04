//! Unibus `unibus.content` hydrator for pull-through VFS cache nodes.
//!
//! This is an L2 user of Unibus's L1 transport. It sends bounded content fetch
//! requests and reconstructs the file from correlated data frames.

use std::{
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use fungos_vfs::{
    CONTENT_DATA_KIND, CONTENT_ERROR_KIND, CONTENT_FETCH_KIND, ContentData, ContentError,
    ContentFetch, ContentId, Hydrator, MAX_RANGE_BYTES, VfsError,
};
use unibus::{Message, client::Client};

pub struct UnibusHydrator {
    client: Arc<Client>,
    destination: Option<String>,
    timeout: Duration,
}

impl UnibusHydrator {
    pub fn new(client: Arc<Client>, destination: Option<String>) -> Self {
        Self {
            client,
            destination,
            timeout: Duration::from_secs(20),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    fn request_range(
        &self,
        content_id: &ContentId,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, VfsError> {
        let request_id = unibus::envelope::next_request_id();
        let fetch = ContentFetch {
            request_id: request_id.clone(),
            content_id: content_id.clone(),
            offset,
            length,
            locator: None,
        };
        fetch.validate()?;
        let body = serde_json::to_value(&fetch)
            .map_err(|error| VfsError::Transport(format!("encode content request: {error}")))?;
        let envelope = self
            .client
            .envelope(
                self.destination.as_deref(),
                Message::new(CONTENT_FETCH_KIND, body),
            )
            .map_err(|error| VfsError::Transport(format!("build content request: {error:#}")))?;
        self.client
            .send(&envelope, &[])
            .map_err(|error| VfsError::Transport(format!("send content request: {error:#}")))?;

        let deadline = Instant::now() + self.timeout;
        loop {
            if Instant::now() >= deadline {
                return Err(VfsError::Transport(format!(
                    "timed out fetching {content_id} at {offset}"
                )));
            }
            let Some((envelope, payload)) = self.client.try_read().map_err(|error| {
                VfsError::Transport(format!("read content response: {error:#}"))
            })?
            else {
                thread::sleep(Duration::from_millis(5));
                continue;
            };
            if envelope.message.kind == CONTENT_ERROR_KIND {
                let error: ContentError = serde_json::from_value(envelope.message.body.into())
                    .map_err(|error| {
                        VfsError::Transport(format!("decode content error: {error}"))
                    })?;
                if error.request_id == request_id {
                    return Err(VfsError::Transport(format!(
                        "content {} failed: {} ({})",
                        error.request_id, error.code, error.detail
                    )));
                }
                continue;
            }
            if envelope.message.kind != CONTENT_DATA_KIND {
                continue;
            }
            let data: ContentData = serde_json::from_value(envelope.message.body.into())
                .map_err(|error| VfsError::Transport(format!("decode content data: {error}")))?;
            if data.request_id != request_id {
                continue;
            }
            if data.content_id != *content_id || data.offset != offset {
                return Err(VfsError::Transport(
                    "content response does not match request".into(),
                ));
            }
            if payload.len() > length as usize {
                return Err(VfsError::Transport(
                    "content response exceeds requested range".into(),
                ));
            }
            if data.length as usize != payload.len() {
                return Err(VfsError::Transport(
                    "content response length does not match payload".into(),
                ));
            }
            return Ok(payload);
        }
    }
}

impl Hydrator for UnibusHydrator {
    fn fetch(&self, _path: &str, content_id: &ContentId, size: u64) -> Result<Vec<u8>, VfsError> {
        let mut bytes = Vec::with_capacity(usize::try_from(size).unwrap_or(0));
        while (bytes.len() as u64) < size {
            let remaining = size - bytes.len() as u64;
            let length = remaining.min(u64::from(MAX_RANGE_BYTES)) as u32;
            let chunk = self.request_range(content_id, bytes.len() as u64, length)?;
            if chunk.is_empty() {
                return Err(VfsError::Transport(format!(
                    "empty content range for {content_id}"
                )));
            }
            bytes.extend_from_slice(&chunk);
        }
        if ContentId::from_bytes(&bytes) != *content_id {
            return Err(VfsError::InvalidContentId(content_id.to_string()));
        }
        Ok(bytes)
    }
}
