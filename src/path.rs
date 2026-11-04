use crate::VfsError;

/// Normalize a workspace-relative path. Absolute paths, empty segments, `.`,
/// and `..` are rejected so the namespace cannot escape its root.
pub fn normalize_path(input: &str) -> Result<String, VfsError> {
    if input.is_empty() || input.starts_with('/') || input.contains('\\') {
        return Err(VfsError::InvalidPath(input.into()));
    }
    let mut parts = Vec::new();
    for part in input.split('/') {
        if part.is_empty() || part == "." || part == ".." {
            return Err(VfsError::InvalidPath(input.into()));
        }
        parts.push(part);
    }
    Ok(parts.join("/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_paths_are_canonical() {
        assert_eq!(normalize_path("docs/a.md").unwrap(), "docs/a.md");
        assert_eq!(normalize_path("a/b/c").unwrap(), "a/b/c");
    }

    #[test]
    fn invalid_paths_are_rejected() {
        for path in ["", "/a", "a//b", "a/./b", "a/../b", "a\\b"] {
            assert!(normalize_path(path).is_err(), "{path}");
        }
    }
}
