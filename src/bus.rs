//! Unibus content L2 wire contract.
//!
//! Unibus is the L1 transport; these are opaque L2 bodies. The binary payload
//! is carried beside the JSON body, never inside `bytes`.
use serde::{Deserialize, Serialize};

use crate::{ContentId, VfsError};

pub const CONTENT_FETCH_KIND: &str = "unibus.content.fetch.v1";
pub const CONTENT_DATA_KIND: &str = "unibus.content.data.v1";
pub const CONTENT_ERROR_KIND: &str = "unibus.content.error.v1";
pub const CONTENT_AVAILABLE_KIND: &str = "unibus.content.available.v1";

pub const MAX_RANGE_BYTES: u32 = 64 * 1024;
pub const MAX_CONTENT_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentFetch {
    pub request_id: String,
    pub content_id: ContentId,
    pub offset: u64,
    pub length: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locator: Option<String>,
}

impl ContentFetch {
    pub fn validate(&self) -> Result<(), VfsError> {
        if self.request_id.is_empty() || self.request_id.len() > 128 {
            return Err(VfsError::InvalidPath("invalid request_id".into()));
        }
        if self.length == 0 || self.length > MAX_RANGE_BYTES {
            return Err(VfsError::InvalidPath(
                "range length outside 1..=65536".into(),
            ));
        }
        if self.offset.saturating_add(u64::from(self.length)) > MAX_CONTENT_BYTES {
            return Err(VfsError::InvalidPath("range exceeds 64 MiB".into()));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentData {
    pub request_id: String,
    pub content_id: ContentId,
    pub offset: u64,
    pub length: u32,
    /// True when this range ends at EOF and may be shorter than `length`.
    pub eof: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentError {
    pub request_id: String,
    pub code: String,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentAvailable {
    pub content_id: ContentId,
    pub size: u64,
    pub media_type: String,
    pub locators: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fetch_ranges_are_bounded() {
        let id = ContentId::from_bytes(b"x");
        let mut fetch = ContentFetch {
            request_id: "r1".into(),
            content_id: id,
            offset: 0,
            length: 64 * 1024,
            locator: None,
        };
        fetch.validate().unwrap();
        fetch.length = 0;
        assert!(fetch.validate().is_err());
        fetch.length = 64 * 1024 + 1;
        assert!(fetch.validate().is_err());
    }
}
