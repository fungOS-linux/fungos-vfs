use serde::{Deserialize, Serialize};

use crate::VfsError;

/// A whole-content identity. Provider locators such as IPFS CIDs are not the
/// primary key; they live outside the VFS namespace.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ContentId(pub String);

impl ContentId {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self(format!("blake3:{}", blake3::hash(bytes).to_hex()))
    }

    pub fn parse(value: &str) -> Result<Self, VfsError> {
        let Some(hex) = value.strip_prefix("blake3:") else {
            return Err(VfsError::InvalidContentId(value.into()));
        };
        if hex.len() != 64 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(VfsError::InvalidContentId(value.into()));
        }
        Ok(Self(format!("blake3:{}", hex.to_ascii_lowercase())))
    }

    pub fn hex(&self) -> &str {
        self.0.strip_prefix("blake3:").unwrap_or(&self.0)
    }
}

impl std::fmt::Display for ContentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
