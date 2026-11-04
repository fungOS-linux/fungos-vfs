use std::{fs, io, path::PathBuf};

use crate::{ContentId, VfsError};

/// Content-addressed local blob store under `.fungos/objects`.
pub struct BlobStore {
    root: PathBuf,
}

impl BlobStore {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, VfsError> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    pub fn path_for(&self, id: &ContentId) -> PathBuf {
        let hex = id.hex();
        self.root.join(&hex[..2]).join(hex)
    }

    pub fn put(&self, bytes: &[u8]) -> Result<ContentId, VfsError> {
        let id = ContentId::from_bytes(bytes);
        let path = self.path_for(&id);
        if path.exists() {
            return Ok(id);
        }
        let Some(parent) = path.parent() else {
            return Err(VfsError::InvalidPath(path.display().to_string()));
        };
        fs::create_dir_all(parent)?;
        let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
        fs::write(&temporary, bytes)?;
        fs::rename(temporary, &path)?;
        Ok(id)
    }

    pub fn get(&self, id: &ContentId) -> Result<Vec<u8>, VfsError> {
        let path = self.path_for(id);
        fs::read(&path).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                VfsError::NotHydrated(id.to_string())
            } else {
                VfsError::Io(error)
            }
        })
    }

    pub fn remove(&self, id: &ContentId) -> Result<(), VfsError> {
        match fs::remove_file(self.path_for(id)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(VfsError::Io(error)),
        }
    }

    pub fn gc<'a>(
        &self,
        reachable: impl IntoIterator<Item = &'a ContentId>,
    ) -> Result<usize, VfsError> {
        let reachable: std::collections::HashSet<_> = reachable.into_iter().cloned().collect();
        let mut removed = 0;
        for shard in fs::read_dir(&self.root)? {
            let shard = shard?;
            if !shard.file_type()?.is_dir() {
                continue;
            }
            for entry in fs::read_dir(shard.path())? {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().to_string();
                let Ok(id) = ContentId::parse(&format!("blake3:{name}")) else {
                    continue;
                };
                if !reachable.contains(&id) {
                    fs::remove_file(entry.path())?;
                    removed += 1;
                }
            }
        }
        Ok(removed)
    }
}
