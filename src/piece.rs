//! Pieces: one blob, many holders.
//!
//! This is the one idea from BitTorrent (and so from Resilio Sync) worth taking.
//! Their protocol splits a file into hashed pieces and fetches whichever pieces
//! it is missing from whichever peers have them; ours asked a single source for
//! a whole `Vec<u8>` and failed if that source did not answer. The difference is
//! not speed on a good day — it is whether a machine holding *half* of something
//! is useful to a machine holding the other half.
//!
//! The wire already carries ranges (`unibus.content.fetch.v1` bounded by
//! `MAX_RANGE_BYTES`); what was missing is a manifest that makes each range
//! independently verifiable, so pieces can come from anyone rather than being
//! concatenated from one provider.
//!
//! Piece size is 256 KiB, which is what UnixFS chunks by. That is deliberate:
//! the identity layer is already IPFS-compatible (a raw blake3 CIDv1's digest is
//! our `ContentId` byte for byte), and matching the chunk size means a UnixFS
//! producer could become one more [`PieceSource`] without changing this module.

use serde::{Deserialize, Serialize};

use crate::ContentId;

/// Bytes in one piece. 256 KiB, the UnixFS default.
pub const PIECE_SIZE: u32 = 256 * 1024;

/// One verifiable slice of a blob.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Piece {
    pub offset: u64,
    pub length: u32,
    /// blake3 of this slice alone, so it can be checked without its neighbours.
    pub content_id: ContentId,
}

/// What a blob is made of, and the id the whole thing answers to.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub content_id: ContentId,
    pub size: u64,
    pub piece_size: u32,
    pub pieces: Vec<Piece>,
}

/// Why a blob could not be assembled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PieceError {
    /// A source answered with bytes of the wrong length for the piece.
    WrongLength { expected: u32, got: usize },
    /// A source answered with bytes that do not hash to the piece id.
    WrongBytes { piece: ContentId },
    /// No source answered for this piece.
    NoSource { piece: ContentId },
    /// Every piece arrived but the whole does not match the blob id.
    WholeBlobMismatch { expected: ContentId },
}

impl std::fmt::Display for PieceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongLength { expected, got } => {
                write!(formatter, "piece was {got} bytes, not {expected}")
            }
            Self::WrongBytes { piece } => write!(formatter, "piece {piece} does not match its id"),
            Self::NoSource { piece } => write!(formatter, "no source has piece {piece}"),
            Self::WholeBlobMismatch { expected } => {
                write!(formatter, "assembled bytes are not {expected}")
            }
        }
    }
}

impl std::error::Error for PieceError {}

impl Manifest {
    /// The manifest for these bytes.
    ///
    /// Empty input is one empty piece rather than zero pieces, so a manifest
    /// always describes a blob that can be fetched rather than one that is
    /// trivially complete.
    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        let mut pieces = Vec::new();
        let mut offset = 0usize;
        loop {
            let end = (offset + PIECE_SIZE as usize).min(bytes.len());
            let slice = &bytes[offset..end.max(offset)];
            pieces.push(Piece {
                offset: offset as u64,
                length: (slice.len()) as u32,
                content_id: ContentId::from_bytes(slice),
            });
            if end >= bytes.len() {
                break;
            }
            offset = end;
        }
        Self {
            content_id: ContentId::from_bytes(bytes),
            size: bytes.len() as u64,
            piece_size: PIECE_SIZE,
            pieces,
        }
    }

    /// The piece covering `offset`, if any.
    #[must_use]
    pub fn piece_at(&self, offset: u64) -> Option<&Piece> {
        self.pieces
            .iter()
            .find(|piece| offset >= piece.offset && offset < piece.offset + u64::from(piece.length))
    }

    /// Assemble and check a complete set of pieces.
    pub fn verify(&self, bytes: &[u8]) -> Result<(), PieceError> {
        if ContentId::from_bytes(bytes) != self.content_id {
            return Err(PieceError::WholeBlobMismatch {
                expected: self.content_id.clone(),
            });
        }
        Ok(())
    }
}

/// Somewhere pieces can be asked for.
///
/// Deliberately per-piece rather than per-file: a source may hold only part of a
/// blob, which is the whole point.
pub trait PieceSource: Send + Sync {
    fn piece(&self, piece: &Piece) -> Result<Vec<u8>, String>;
}

/// Which pieces are already held, in manifest order.
pub type Have = Vec<Option<Vec<u8>>>;

/// A fresh `Have` for a manifest: nothing held yet.
#[must_use]
pub fn empty_have(manifest: &Manifest) -> Have {
    vec![None; manifest.pieces.len()]
}

/// Fill whatever is missing from the sources, cheapest-first and in order.
///
/// A source that answers with the wrong bytes is retried against the next one
/// rather than accepted — a piece is verifiable on its own, which is what makes
/// trusting no single peer acceptable here.
///
/// `have` is taken by `&mut` so an interrupted fill keeps what it already
/// verified: resumption is a consequence of the representation rather than a
/// separate feature.
pub fn fetch(
    manifest: &Manifest,
    sources: &[&dyn PieceSource],
    have: &mut Have,
) -> Result<Vec<u8>, PieceError> {
    if have.len() != manifest.pieces.len() {
        *have = empty_have(manifest);
    }
    let mut cursor = 0usize;
    for (index, piece) in manifest.pieces.iter().enumerate() {
        if have[index].is_some() {
            continue;
        }
        let mut found = None;
        for attempt in 0..sources.len() {
            let source = sources[(cursor + attempt) % sources.len()];
            let Ok(bytes) = source.piece(piece) else {
                continue;
            };
            if bytes.len() != piece.length as usize {
                continue;
            }
            if ContentId::from_bytes(&bytes) != piece.content_id {
                continue;
            }
            found = Some(bytes);
            // Start from the source that worked last time: it is the one most
            // likely to hold the next piece too.
            cursor = (cursor + attempt) % sources.len();
            break;
        }
        match found {
            Some(bytes) => have[index] = Some(bytes),
            None => {
                return Err(PieceError::NoSource {
                    piece: piece.content_id.clone(),
                });
            }
        }
    }
    let mut out = Vec::with_capacity(manifest.size as usize);
    for piece in have.iter().flatten() {
        out.extend_from_slice(piece);
    }
    manifest.verify(&out)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn blob(pieces: usize) -> Vec<u8> {
        // Deterministic and incompressible enough that pieces differ.
        (0..pieces * PIECE_SIZE as usize)
            .map(|index| (index.wrapping_mul(31) % 251) as u8)
            .collect()
    }

    struct Holder {
        pieces: Vec<(ContentId, Vec<u8>)>,
        answered: AtomicUsize,
        lie: bool,
    }

    impl Holder {
        fn with(pieces: &[Piece], bytes: &[u8], lie: bool) -> Self {
            Self {
                pieces: pieces
                    .iter()
                    .map(|piece| {
                        let start = piece.offset as usize;
                        let end = start + piece.length as usize;
                        (piece.content_id.clone(), bytes[start..end].to_vec())
                    })
                    .collect(),
                answered: AtomicUsize::new(0),
                lie,
            }
        }
    }

    impl PieceSource for Holder {
        fn piece(&self, piece: &Piece) -> Result<Vec<u8>, String> {
            self.answered.fetch_add(1, Ordering::SeqCst);
            let found = self
                .pieces
                .iter()
                .find(|(id, _)| id == &piece.content_id)
                .map(|(_, bytes)| bytes.clone())
                .ok_or_else(|| format!("missing {}", piece.content_id))?;
            Ok(if self.lie {
                vec![0u8; found.len()]
            } else {
                found
            })
        }
    }

    #[test]
    fn a_manifest_covers_the_whole_blob_and_the_last_piece_may_be_short() {
        let data = blob(3);
        let short = &data[..data.len() - 7];
        let manifest = Manifest::of(short);
        assert_eq!(manifest.pieces.len(), 3);
        assert_eq!(manifest.size, short.len() as u64);
        assert_eq!(manifest.pieces[0].offset, 0);
        assert_eq!(manifest.pieces[1].offset, u64::from(PIECE_SIZE));
        assert_eq!(
            manifest.pieces.last().unwrap().length as usize,
            PIECE_SIZE as usize - 7,
            "the tail is whatever is left"
        );
        assert_eq!(manifest.content_id, ContentId::from_bytes(short));
    }

    #[test]
    fn an_empty_blob_still_has_one_piece() {
        let manifest = Manifest::of(b"");
        assert_eq!(manifest.pieces.len(), 1);
        assert_eq!(manifest.pieces[0].length, 0);
        assert!(manifest.verify(b"").is_ok());
    }

    #[test]
    fn two_holders_split_the_work() {
        let data = blob(4);
        let manifest = Manifest::of(&data);
        let first = Holder::with(&manifest.pieces[..2], &data, false);
        let second = Holder::with(&manifest.pieces[2..], &data, false);
        let mut have = empty_have(&manifest);

        let assembled = fetch(&manifest, &[&first, &second], &mut have).unwrap();
        assert_eq!(assembled, data);
        assert!(first.answered.load(Ordering::SeqCst) >= 2);
        assert!(second.answered.load(Ordering::SeqCst) >= 2);
    }

    #[test]
    fn a_lying_source_is_skipped_for_one_that_tells_the_truth() {
        let data = blob(2);
        let manifest = Manifest::of(&data);
        let liar = Holder::with(&manifest.pieces, &data, true);
        let honest = Holder::with(&manifest.pieces, &data, false);
        let mut have = empty_have(&manifest);

        // Bytes that do not hash to the piece id are not accepted, so the liar
        // contributes nothing even though it answered.
        let assembled = fetch(&manifest, &[&liar, &honest], &mut have).unwrap();
        assert_eq!(assembled, data);
        assert!(
            liar.answered.load(Ordering::SeqCst) >= 1,
            "the liar is asked, refused, and not asked again once another source answered"
        );
    }

    #[test]
    fn a_piece_nobody_has_is_an_error_not_a_short_blob() {
        let data = blob(2);
        let manifest = Manifest::of(&data);
        let half = Holder::with(&manifest.pieces[..1], &data, false);
        let mut have = empty_have(&manifest);
        let error = fetch(&manifest, &[&half], &mut have).unwrap_err();
        assert!(matches!(error, PieceError::NoSource { .. }));
        // What did arrive is kept, so a later attempt resumes instead of
        // starting again.
        assert!(have[0].is_some());
        assert!(have[1].is_none());
    }

    #[test]
    fn a_resumed_fill_asks_only_for_what_is_missing() {
        let data = blob(3);
        let manifest = Manifest::of(&data);
        let everything = Holder::with(&manifest.pieces, &data, false);
        let mut have = empty_have(&manifest);
        have[0] = Some(data[..PIECE_SIZE as usize].to_vec());

        let assembled = fetch(&manifest, &[&everything], &mut have).unwrap();
        assert_eq!(assembled, data);
        assert_eq!(
            everything.answered.load(Ordering::SeqCst),
            2,
            "the piece already held is not asked for again"
        );
    }

    #[test]
    fn bytes_that_assemble_to_the_wrong_blob_are_refused() {
        let data = blob(2);
        let manifest = Manifest::of(&data);
        let mut have = empty_have(&manifest);
        // A complete, individually valid set in the wrong order.
        have[0] = Some(data[PIECE_SIZE as usize..].to_vec());
        have[1] = Some(data[..PIECE_SIZE as usize].to_vec());
        let error = fetch(&manifest, &[], &mut have).unwrap_err();
        assert!(matches!(error, PieceError::WholeBlobMismatch { .. }));
    }
}
