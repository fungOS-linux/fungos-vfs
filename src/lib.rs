//! fungOS local-first VFS.
//!
//! A workspace is a content-addressed blob store plus a SQLite namespace and
//! version history. A node can be a full replica or a pull-through cache.
//! Distributed transport is deliberately outside this crate.

mod bus;
mod content;
mod hydrate;
mod path;
mod store;
mod vfs;

pub use bus::{
    CONTENT_AVAILABLE_KIND, CONTENT_DATA_KIND, CONTENT_ERROR_KIND, CONTENT_FETCH_KIND,
    ContentAvailable, ContentData, ContentError, ContentFetch, MAX_CONTENT_BYTES, MAX_RANGE_BYTES,
};
pub use content::ContentId;
pub use hydrate::Hydrator;
pub use path::normalize_path;
pub use vfs::{
    FileEntry, Hydration, Lease, LodPolicy, NodeRole, SyncOp, SyncOpKind, Vfs, VfsError,
};
