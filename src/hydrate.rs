use crate::{ContentId, FileEntry, Hydration, Vfs, VfsError};

/// Pull-through source for a placeholder.
///
/// A full node, Unibus adapter, Kubo adapter, or iroh-blobs adapter implements
/// this. The VFS verifies the returned bytes against the registered content id.
pub trait Hydrator {
    fn fetch(&self, path: &str, content_id: &ContentId, size: u64) -> Result<Vec<u8>, VfsError>;
}

impl Vfs {
    /// Hydrate a placeholder from an external source and return the now-present
    /// entry. This is the cache pull-through primitive.
    pub fn hydrate_from<H: Hydrator>(
        &mut self,
        path: &str,
        hydrator: &H,
    ) -> Result<FileEntry, VfsError> {
        let entry = self
            .file(path)?
            .ok_or_else(|| VfsError::NotFound(path.into()))?;
        if entry.hydration == Hydration::Present {
            return Ok(entry);
        }
        let bytes = hydrator.fetch(&entry.path, &entry.content_id, entry.size)?;
        self.hydrate(&entry.path, &bytes)
    }

    /// Get a file, hydrating through the supplied source if this node only has
    /// a placeholder. Present files never call the source.
    pub fn get_or_hydrate<H: Hydrator>(
        &mut self,
        path: &str,
        hydrator: &H,
    ) -> Result<Vec<u8>, VfsError> {
        match self.get(path) {
            Ok(bytes) => Ok(bytes),
            Err(VfsError::NotHydrated(_)) => {
                self.hydrate_from(path, hydrator)?;
                self.get(path)
            }
            Err(error) => Err(error),
        }
    }
}
