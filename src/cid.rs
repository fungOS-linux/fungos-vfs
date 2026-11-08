//! Kubo/IPFS addressing for a `ContentId`.
//!
//! Also see: <https://github.com/ipld/cid> and
//! <https://github.com/multiformats/multihash>.
//!
//! The point of this module is that a `ContentId` needs no lookup index to be
//! fetched from IPFS. `blake3` is multihash code `0x1e`, so a CIDv1 with the raw
//! codec carries our digest verbatim:
//!
//! ```text
//! CID = base32( varint(1) ‖ varint(0x55) ‖ varint(0x1e) ‖ varint(32) ‖ digest )
//! ```
//!
//! Verified against Kubo 0.43.1: `ipfs add --cid-version 1 --hash blake3` of the
//! bytes `hello-vfs` produces a CID whose digest is byte-identical to the
//! `ContentId` this crate computes for the same bytes.

use crate::{ContentId, VfsError};

const CID_VERSION_1: u64 = 1;
/// dag-raw: the block bytes are the content, with no envelope to parse.
pub const CODEC_RAW: u64 = 0x55;
/// dag-cbor. A CAR needs one node to name as a root; the manifest uses this.
pub const CODEC_DAG_CBOR: u64 = 0x71;
const MULTIHASH_BLAKE3: u64 = 0x1e;
const BLAKE3_DIGEST_LEN: u64 = 32;

/// Encode a `ContentId` as a CIDv1 string (base32, lowercase, no padding).
pub fn content_id_to_cid(id: &ContentId) -> Result<String, VfsError> {
    cid_string(id, CODEC_RAW)
}

/// The text form of a CID with an arbitrary codec.
pub fn cid_string(id: &ContentId, codec: u64) -> Result<String, VfsError> {
    Ok(format!("b{}", base32_lower(&cid_bytes(id, codec)?)))
}

/// The binary CID (version ‖ codec ‖ multihash) that a CAR frames its blocks
/// with. No multibase prefix: that belongs to the text form only.
pub fn cid_bytes(id: &ContentId, codec: u64) -> Result<Vec<u8>, VfsError> {
    let digest = decode_hex(id.hex())?;
    if digest.len() != BLAKE3_DIGEST_LEN as usize {
        return Err(VfsError::InvalidContentId(id.to_string()));
    }
    let mut bytes = Vec::with_capacity(digest.len() + 8);
    for value in [CID_VERSION_1, codec, MULTIHASH_BLAKE3, BLAKE3_DIGEST_LEN] {
        push_varint(value, &mut bytes);
    }
    bytes.extend_from_slice(&digest);
    Ok(bytes)
}

fn push_varint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn decode_hex(hex: &str) -> Result<Vec<u8>, VfsError> {
    if !hex.len().is_multiple_of(2) {
        return Err(VfsError::InvalidContentId(hex.to_string()));
    }
    let bytes = hex.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.as_chunks::<2>().0 {
        let hi = hex_value(pair[0])?;
        let lo = hex_value(pair[1])?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn hex_value(byte: u8) -> Result<u8, VfsError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(VfsError::InvalidContentId((byte as char).to_string())),
    }
}

/// RFC 4648 base32, lowercase, unpadded — the multibase `b` alphabet.
fn base32_lower(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut out = String::with_capacity(bytes.len().div_ceil(5) * 8);
    for chunk in bytes.chunks(5) {
        let mut buffer = [0u8; 5];
        buffer[..chunk.len()].copy_from_slice(chunk);
        let bits = u64::from(buffer[0]) << 32
            | u64::from(buffer[1]) << 24
            | u64::from(buffer[2]) << 16
            | u64::from(buffer[3]) << 8
            | u64::from(buffer[4]);
        // Each 5-byte group emits 8 characters; the tail emits only the
        // characters that carry real bits, matching unpadded base32.
        let characters = match chunk.len() {
            1 => 2,
            2 => 4,
            3 => 5,
            4 => 7,
            _ => 8,
        };
        for index in 0..characters {
            let shift = 35 - index * 5;
            let value = ((bits >> shift) & 0x1f) as usize;
            out.push(ALPHABET[value] as char);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cid_digest_matches_kubo_output_for_known_bytes() {
        // Golden pair captured from Kubo 0.43.1:
        //   printf 'hello-vfs' | ipfs add --only-hash --cid-version 1 --hash blake3 -
        let id = ContentId::from_bytes(b"hello-vfs");
        assert_eq!(
            id.hex(),
            "bbf816b5f01dec872e140b6323efbc18a277df064b9e49a58135828cb65265b4"
        );
        assert_eq!(
            content_id_to_cid(&id).unwrap(),
            "bafkr4if37alll4a55sds4falmmr67payuj356bsltze2lajvqkglmutfwq"
        );
    }

    #[test]
    fn cid_prefix_is_cidv1_raw_blake3() {
        // Version 1, codec 0x55 (raw), multihash 0x1e (blake3), length 32 encodes
        // to the human-readable prefix `bafkr4i`, which is what Kubo emits.
        let id = ContentId::from_bytes(b"any bytes at all");
        assert!(content_id_to_cid(&id).unwrap().starts_with("bafkr4i"));
    }
}
