//! fungOS local-first VFS.
//!
//! A root is a SQLite namespace plus version history over a content-addressed
//! store that every root on the machine shares. A node can be a full replica or a
//! pull-through cache. Distributed transport is deliberately outside this crate.

mod bus;
mod carry;
mod cid;
mod content;
mod crdt;
mod hydrate;
mod path;
mod pi;
mod piece;
mod policy;
mod project;
mod projector;
mod provider;
mod quic;
mod replica;
mod session;
mod store;
mod stream;
mod sync;
mod transfer;
mod vfs;
mod worktree;

pub use bus::{
    CONTENT_AVAILABLE_KIND, CONTENT_DATA_KIND, CONTENT_ERROR_KIND, CONTENT_FETCH_KIND,
    ContentAvailable, ContentData, ContentError, ContentFetch, MAX_CONTENT_BYTES, MAX_RANGE_BYTES,
};
pub use carry::{CARRY_REF, Carried, ensure_commit as ensure_carried_commit};
pub use cid::{CODEC_DAG_CBOR, CODEC_RAW, cid_bytes, cid_string, content_id_to_cid};
pub use content::ContentId;
pub use crdt::{Applied, Block, BlockId, Doc, Op, match_blocks, split_blocks};
pub use hydrate::Hydrator;
pub use path::normalize_path;
pub use pi::{ImportReport, import as import_pi, import_incremental as import_pi_incremental};
pub use piece::{Have, Manifest, PIECE_SIZE, Piece, PieceError, PieceSource, empty_have, fetch};
pub use policy::{PathClass, PathPolicy};
pub use project::{ProjectReport, WorktreeRef, attach as attach_project, list as list_projects};
pub use projector::{Projection, ProjectionReport, to_operator};
pub use provider::{
    MAX_PROVIDER_FRAME, PROVIDER_VERSION, ProviderReplica, ProviderReply, ProviderRequest,
    ProviderServer,
};
pub use quic::{SyncServer, pull_over_quic};
pub use replica::{
    DiskReplica, Fetched, FetchedRange, Hydrated, Replica, ReplicaConfig, ReplicaSpec, Replicas,
    Replicated, Stored, config_path as replica_config_path,
};
pub use session::{
    Role, Session, SessionSummary, ToolCall, Turn, Usage, blob_key as session_blob_key,
    record_key as session_record_key,
};
pub use stream::{MAX_FRAME, PullClient, Resume, STREAM_VERSION, SnapshotHeader, StreamRequest};
pub use sync::{
    ApplyOutcome, CompactionReport, ContentBlob, Divergence, MESSAGE_VERSION, SessionId, Snapshot,
    SnapshotEntry, SnapshotKind, SnapshotStaging, SyncBatch, SyncCursor, SyncRequest, SyncResponse,
    TransferReport, pull_over_ssh,
};
pub use transfer::{
    ExportReport, GitPointer, IngestReport, SourceMode, export_dir, ingest_dir,
    ingest_dir_with_policy,
};
pub use vfs::{
    CacheEntry, Change, Expect, FileEntry, Hlc, Hydration, LEGACY_STATE_DIR, Lease, LodPolicy,
    NodeRole, STATE_DIR, StorageMigration, SyncOp, SyncOpKind, Version, Vfs, VfsError, Writer,
    migrate_storage, migrate_storage_with_cas,
};
pub use worktree::{
    ChangePage, CheckoutOptions, CheckoutReport, DEFAULT_LEASE_MS, MaterializeReport, PruneEntry,
    PruneReport, RecordOptions, RecordReport, WorktreeChange, WorktreeState,
    changes as worktree_changes, checkout, materialize, namespace_for, prune, record, worktrees,
};
