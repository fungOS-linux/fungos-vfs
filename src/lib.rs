//! fungOS local-first VFS.
//!
//! A workspace is a content-addressed blob store plus a SQLite namespace and
//! version history. A node can be a full replica or a pull-through cache.
//! Distributed transport is deliberately outside this crate.

mod content;
mod path;
mod store;
mod vfs;

pub use content::ContentId;
pub use path::normalize_path;
pub use vfs::{
    FileEntry, Hydration, Lease, LodPolicy, NodeRole, SyncOp, SyncOpKind, Vfs, VfsError,
};
