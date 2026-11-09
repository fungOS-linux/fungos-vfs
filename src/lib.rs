//! fungOS local-first VFS.
//!
//! A root is a SQLite namespace plus version history over a content-addressed
//! store that every root on the machine shares. A node can be a full replica or a
//! pull-through cache. Distributed transport is deliberately outside this crate.

mod bus;
mod cid;
mod content;
mod hydrate;
mod path;
mod policy;
mod quic;
mod store;
mod sync;
mod transfer;
mod vfs;

pub use bus::{
    CONTENT_AVAILABLE_KIND, CONTENT_DATA_KIND, CONTENT_ERROR_KIND, CONTENT_FETCH_KIND,
    ContentAvailable, ContentData, ContentError, ContentFetch, MAX_CONTENT_BYTES, MAX_RANGE_BYTES,
};
pub use cid::{CODEC_DAG_CBOR, CODEC_RAW, cid_bytes, cid_string, content_id_to_cid};
pub use content::ContentId;
pub use hydrate::Hydrator;
pub use path::normalize_path;
pub use policy::{PathClass, PathPolicy};
pub use quic::{SyncServer, pull_over_quic};
pub use sync::{
    ApplyOutcome, CompactionReport, ContentBlob, Divergence, Snapshot, SnapshotEntry, SnapshotKind,
    SyncBatch, SyncCursor, SyncRequest, SyncResponse, pull_over_ssh,
};
pub use transfer::{
    ExportReport, GitPointer, IngestReport, SourceMode, export_dir, ingest_dir,
    ingest_dir_with_policy,
};
pub use vfs::{
    Expect, FileEntry, Hlc, Hydration, LEGACY_STATE_DIR, Lease, LodPolicy, NodeRole, STATE_DIR,
    StorageMigration, SyncOp, SyncOpKind, Version, Vfs, VfsError, Writer, migrate_storage,
    migrate_storage_with_cas,
};
