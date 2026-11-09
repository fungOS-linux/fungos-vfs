//! Kubo (IPFS) content provider for the VFS.
//!
//! Kubo is a *provider*, not the identity. A blob's name is always its blake3
//! `ContentId`; the CID below is only how that name is spelled to Kubo. Because
//! blake3 is multihash `0x1e` and the raw codec adds nothing, Kubo stores the
//! digest verbatim and can be asked for a block by the name we already have.
//! That is why there is no lookup index anywhere in here.
//!
//! Two operations matter:
//!
//! - [`KuboHydrator`] pulls one block out of a node (`block/get`), which is what
//!   turns a placeholder into a present file.
//! - [`CarBatch`] pushes many blocks in one request (`dag/import`). Adding files
//!   one at a time costs a round trip each and, worse, a pin decision each.

use std::time::Duration;

use fungos_vfs::{
    CODEC_DAG_CBOR, ContentId, Hydrator, VfsError, cid_bytes, cid_string, content_id_to_cid,
};

/// The environment variable naming the Kubo API endpoint to hydrate from.
///
/// `KuboHydrator::from_env` reads it. There is intentionally no compiled-in
/// fallback address; see that constructor for why.
pub const KUBO_API_ENV: &str = "KUBO_API";

/// Reads blocks from a Kubo node over its HTTP API.
pub struct KuboHydrator {
    api: String,
    client: reqwest::blocking::Client,
}

/// Resolve the endpoint from a `KUBO_API` lookup, refusing to invent one.
///
/// Split out from [`KuboHydrator::from_env`] so the refusal is testable without
/// mutating process-global environment state.
fn api_endpoint(lookup: Option<String>) -> Result<String, VfsError> {
    match lookup {
        Some(api) if !api.trim().is_empty() => Ok(api),
        _ => Err(VfsError::Config(format!(
            "{KUBO_API_ENV} is not set: name the Kubo API endpoint to hydrate from, for \
             example {KUBO_API_ENV}=http://127.0.0.1:5001. There is no safe default because \
             the correct node differs per deployment."
        ))),
    }
}

impl KuboHydrator {
    pub fn new(api: impl Into<String>) -> Self {
        Self::with_timeout(api, Duration::from_secs(30))
    }

    pub fn with_timeout(api: impl Into<String>, timeout: Duration) -> Self {
        Self {
            api: api.into().trim_end_matches('/').to_string(),
            client: reqwest::blocking::Client::builder()
                .timeout(timeout)
                .build()
                .unwrap_or_default(),
        }
    }

    /// A hydrator for the node named by [`KUBO_API_ENV`].
    ///
    /// Nothing is guessed when the variable is absent. Which node to hydrate
    /// from is deployment-specific: on a host running more than one Kubo, a
    /// fallback address reaches whichever node happens to listen there. The
    /// blocks land, the pin does not, the import still exits successfully, and
    /// the node's next GC reclaims the content. A missing endpoint is a
    /// configuration error worth failing on, not a detail to paper over.
    pub fn from_env() -> Result<Self, VfsError> {
        api_endpoint(std::env::var(KUBO_API_ENV).ok()).map(Self::new)
    }

    /// Raw block bytes for `cid`.
    ///
    /// `block/get` is used rather than `cat`: `cat` walks UnixFS, which a raw
    /// block is not, whereas `block/get` returns exactly the bytes whose blake3
    /// digest names them.
    fn block_get(&self, cid: &str) -> Result<Vec<u8>, VfsError> {
        let response = self
            .client
            .post(format!("{}/api/v0/block/get", self.api))
            .query(&[("arg", cid)])
            .send()
            .map_err(|error| VfsError::Transport(format!("kubo block/get {cid}: {error}")))?;

        let status = response.status();
        let bytes = response
            .bytes()
            .map_err(|error| VfsError::Transport(format!("kubo block/get {cid}: {error}")))?;
        if !status.is_success() {
            return Err(VfsError::Transport(format!(
                "kubo block/get {cid}: HTTP {status}: {}",
                String::from_utf8_lossy(&bytes)
            )));
        }
        Ok(bytes.to_vec())
    }
}

impl Hydrator for KuboHydrator {
    fn fetch(&self, _path: &str, content_id: &ContentId, _size: u64) -> Result<Vec<u8>, VfsError> {
        let cid = content_id_to_cid(content_id)?;
        let bytes = self.block_get(&cid)?;
        // The digest is the check. A node that returns the wrong block cannot be
        // trusted to have told us anything else correctly either.
        if ContentId::from_bytes(&bytes) != *content_id {
            return Err(VfsError::InvalidContentId(content_id.to_string()));
        }
        Ok(bytes)
    }
}

/// A batch of blocks destined for one `dag/import` request.
pub struct CarBatch {
    blocks: Vec<(ContentId, Vec<u8>)>,
}

impl CarBatch {
    pub fn new() -> Self {
        Self { blocks: Vec::new() }
    }

    /// Queue `bytes` under `content_id`, rejecting a name that does not match
    /// the bytes: a CAR is trusted input to the node.
    pub fn add(&mut self, content_id: &ContentId, bytes: Vec<u8>) -> Result<(), VfsError> {
        if ContentId::from_bytes(&bytes) != *content_id {
            return Err(VfsError::InvalidContentId(content_id.to_string()));
        }
        self.blocks.push((content_id.clone(), bytes));
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.blocks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    /// Encode as a CAR.
    ///
    /// The file gets a single dag-cbor manifest root that *links* every block.
    /// The root is what makes one `dag/import` enough: pinning the manifest
    /// recursively pins every block it names, so retention follows from the
    /// import instead of needing a pin call per file.
    pub fn encode(&self) -> Result<Vec<u8>, VfsError> {
        Ok(self.encode_with_root()?.0)
    }

    /// The CAR and the text form of its root CID.
    fn encode_with_root(&self) -> Result<(Vec<u8>, String), VfsError> {
        if self.blocks.is_empty() {
            return Err(VfsError::Transport("cannot encode an empty CAR".into()));
        }

        let mut cids = Vec::with_capacity(self.blocks.len());
        for (content_id, bytes) in &self.blocks {
            debug_assert_eq!(&ContentId::from_bytes(bytes), content_id);
            cids.push(cid_bytes(content_id, fungos_vfs::CODEC_RAW)?);
        }

        let manifest = manifest_bytes(&cids);
        let manifest_id = ContentId::from_bytes(&manifest);
        let root = cid_bytes(&manifest_id, CODEC_DAG_CBOR)?;
        let root_text = cid_string(&manifest_id, CODEC_DAG_CBOR)?;

        let header = car_header(&root);
        let mut out = Vec::with_capacity(manifest.len() + header.len() + 64);
        push_varint(header.len() as u64, &mut out);
        out.extend_from_slice(&header);

        // Every block, the manifest included, is framed the same way.
        push_varint((root.len() + manifest.len()) as u64, &mut out);
        out.extend_from_slice(&root);
        out.extend_from_slice(&manifest);
        for (cid, (_, bytes)) in cids.iter().zip(&self.blocks) {
            push_varint((cid.len() + bytes.len()) as u64, &mut out);
            out.extend_from_slice(cid);
            out.extend_from_slice(bytes);
        }
        Ok((out, root_text))
    }

    /// Send the batch to a node and pin its root: the usual case, because a
    /// block that is not reachable from a pin is reclaimed by GC.
    pub fn import(&self, api: &str) -> Result<String, VfsError> {
        self.send(api, true)
    }

    /// Send the batch to a node, optionally pinning the manifest root, and
    /// return the root CID.
    ///
    /// The root is computed here rather than read back: with `pin-roots=false`
    /// the node answers with an empty body, and the value is already known.
    pub fn send(&self, api: &str, pin: bool) -> Result<String, VfsError> {
        let (car, root) = self.encode_with_root()?;
        let part = reqwest::blocking::multipart::Part::bytes(car).file_name("batch.car");
        let form = reqwest::blocking::multipart::Form::new().part("file", part);
        let response = reqwest::blocking::Client::new()
            .post(format!("{}/api/v0/dag/import", api.trim_end_matches('/')))
            .query(&[("pin-roots", pin.to_string())])
            .multipart(form)
            .send()
            .map_err(|error| VfsError::Transport(format!("kubo dag/import: {error}")))?;

        let status = response.status();
        let body = response
            .text()
            .map_err(|error| VfsError::Transport(format!("kubo dag/import: {error}")))?;
        if !status.is_success() {
            return Err(VfsError::Transport(format!(
                "kubo dag/import: HTTP {status}: {body}"
            )));
        }
        // One JSON object per root. A node that stored the blocks but could not
        // record the pin still succeeds at import, so the pin error is read
        // rather than inferred from the status code.
        for line in body.lines().filter(|line| !line.trim().is_empty()) {
            let entry: serde_json::Value = serde_json::from_str(line).map_err(|error| {
                VfsError::Transport(format!("kubo dag/import: unparseable line: {error}"))
            })?;
            if let Some(message) = entry["Root"]["PinErrorMsg"].as_str()
                && !message.is_empty()
            {
                return Err(VfsError::Transport(format!(
                    "kubo dag/import pin: {message}"
                )));
            }
        }
        Ok(root)
    }
}

impl Default for CarBatch {
    fn default() -> Self {
        Self::new()
    }
}

/// `{"v": 1, "blocks": [<link>...]}` in canonical dag-cbor.
fn manifest_bytes(cids: &[Vec<u8>]) -> Vec<u8> {
    let mut out = vec![0xa2]; // map, 2 entries
    out.push(0x61); // text, 1 byte
    out.push(b'v');
    cbor_header(&mut out, 0, 1);
    out.push(0x66); // text, 6 bytes
    out.extend_from_slice(b"blocks");
    cbor_header(&mut out, 4, cids.len() as u64);
    for cid in cids {
        // Tag 42 is the dag-cbor link, with a 0x00 identity-multibase prefix.
        out.extend_from_slice(&[0xd8, 0x2a]);
        cbor_header(&mut out, 2, (cid.len() + 1) as u64);
        out.push(0x00);
        out.extend_from_slice(cid);
    }
    out
}

/// `{"roots": [<link>], "version": 1}` in canonical dag-cbor.
fn car_header(root: &[u8]) -> Vec<u8> {
    let mut out = vec![0xa2];
    out.push(0x65); // text, 5 bytes
    out.extend_from_slice(b"roots");
    out.push(0x81); // array, 1 entry
    out.extend_from_slice(&[0xd8, 0x2a]);
    cbor_header(&mut out, 2, (root.len() + 1) as u64);
    out.push(0x00);
    out.extend_from_slice(root);
    out.push(0x67); // text, 7 bytes
    out.extend_from_slice(b"version");
    cbor_header(&mut out, 0, 1);
    out
}

fn cbor_header(out: &mut Vec<u8>, major: u8, value: u64) {
    let base = major << 5;
    match value {
        0..=23 => out.push(base | value as u8),
        24..=0xff => {
            out.push(base | 24);
            out.push(value as u8);
        }
        0x100..=0xffff => {
            out.push(base | 25);
            out.extend_from_slice(&(value as u16).to_be_bytes());
        }
        0x1_0000..=0xffff_ffff => {
            out.push(base | 26);
            out.extend_from_slice(&(value as u32).to_be_bytes());
        }
        _ => {
            out.push(base | 27);
            out.extend_from_slice(&value.to_be_bytes());
        }
    }
}

fn push_varint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_varint(bytes: &[u8], at: &mut usize) -> u64 {
        let mut value = 0u64;
        let mut shift = 0;
        loop {
            let byte = bytes[*at];
            *at += 1;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return value;
            }
            shift += 7;
        }
    }

    /// Walk the CAR framing without decoding CBOR, and confirm the block count
    /// is what went in plus the manifest.
    #[test]
    fn car_frames_the_manifest_and_every_block() {
        let mut batch = CarBatch::new();
        batch
            .add(&ContentId::from_bytes(b"hello-vfs"), b"hello-vfs".to_vec())
            .unwrap();
        batch
            .add(&ContentId::from_bytes(b"second"), b"second".to_vec())
            .unwrap();

        let car = batch.encode().unwrap();
        let mut at = 0usize;
        let header_len = read_varint(&car, &mut at) as usize;
        let header = &car[at..at + header_len];
        assert_eq!(header[0], 0xa2, "header is a two-entry map");
        assert!(header.windows(5).any(|w| w == b"roots"));
        assert!(header.windows(7).any(|w| w == b"version"));

        at += header_len;
        let mut blocks = 0;
        while at < car.len() {
            let len = read_varint(&car, &mut at) as usize;
            let frame = &car[at..at + len];
            assert!(frame.len() > 4);
            at += len;
            blocks += 1;
        }
        assert_eq!(blocks, 3, "two blocks plus the manifest root");
    }

    #[test]
    fn a_batch_refuses_bytes_that_do_not_match_their_name() {
        let mut batch = CarBatch::new();
        let wrong = ContentId::from_bytes(b"something else");
        let error = batch.add(&wrong, b"payload".to_vec()).unwrap_err();
        assert!(matches!(error, VfsError::InvalidContentId(_)));
    }

    /// The node these live tests talk to. Deliberately not defaulted: a test
    /// that silently points at the wrong node proves nothing.
    fn live_api() -> String {
        std::env::var(KUBO_API_ENV).expect("set KUBO_API to a running node's API endpoint")
    }

    /// An absent endpoint must fail, not resolve to an address that happens to
    /// be listening. Guessing is how a hydrator lands blocks on the wrong node
    /// and loses the pin that made them durable.
    #[test]
    fn an_unconfigured_hydrator_refuses_to_guess_a_node() {
        let error = api_endpoint(None).unwrap_err();
        assert!(matches!(error, VfsError::Config(_)), "{error}");
        let message = error.to_string();
        assert!(message.contains(KUBO_API_ENV), "{message}");
        assert!(message.contains("no safe default"), "{message}");

        // Blank is absence, not an endpoint.
        assert!(matches!(
            api_endpoint(Some("   ".into())).unwrap_err(),
            VfsError::Config(_)
        ));

        // A supplied endpoint is used verbatim.
        assert_eq!(
            api_endpoint(Some("http://node.example:5002".into())).unwrap(),
            "http://node.example:5002"
        );
    }

    /// Same round trip without pinning, which also covers a node whose pin
    /// store is unavailable: storing and reading a block must still work.
    #[test]
    #[ignore = "requires a running Kubo node"]
    fn stores_and_fetches_without_pinning() {
        let api = live_api();
        let payload = b"fungos vfs unpinned block\n".to_vec();
        let content_id = ContentId::from_bytes(&payload);

        let mut batch = CarBatch::new();
        batch.add(&content_id, payload.clone()).unwrap();
        let root = batch.send(&api, false).unwrap();
        assert!(root.starts_with("bafy"), "root is a dag-cbor CID: {root}");

        let hydrator = KuboHydrator::new(&api);
        let fetched = hydrator
            .fetch("any", &content_id, payload.len() as u64)
            .unwrap();
        assert_eq!(fetched, payload);
    }

    /// Live check against a running node. `KUBO_API` selects it, e.g.
    /// `KUBO_API=http://127.0.0.1:5101 cargo test -p fungos-vfs-kubo -- --ignored`.
    #[test]
    #[ignore = "requires a running Kubo node"]
    fn round_trip_against_a_live_node() {
        let api = live_api();
        let payload = b"fungos vfs <-> kubo round trip\n".to_vec();
        let content_id = ContentId::from_bytes(&payload);

        let mut batch = CarBatch::new();
        batch.add(&content_id, payload.clone()).unwrap();
        let root = batch.import(&api).unwrap();
        assert!(root.starts_with("baf"));

        let hydrator = KuboHydrator::new(&api);
        let fetched = hydrator
            .fetch("any", &content_id, payload.len() as u64)
            .unwrap();
        assert_eq!(fetched, payload);
    }
}
