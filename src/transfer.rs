//! Directory ingress and egress.
//!
//! Other tools materialize a tree on disk — a Delta checkout is the motivating
//! case — and need it in or out of a VFS namespace without one process per file.
//! Selection follows the same rule Delta applies when it imports a repository:
//! files Git tracks plus untracked files Git does not ignore.

use std::{
    fs,
    io,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Result};
use serde::Serialize;

use crate::{ContentId, Hydration, Vfs, normalize_path};

/// How to decide which files under a directory belong to the tree.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SourceMode {
    /// Files Git tracks plus untracked files Git does not ignore. Falls back to
    /// [`SourceMode::All`] when the directory is not inside a Git repository.
    #[default]
    Git,
    /// Every regular file, skipping `.git` and symbolic links.
    All,
}

impl SourceMode {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "git" => Ok(Self::Git),
            "all" => Ok(Self::All),
            other => anyhow::bail!("unknown source mode {other:?}; expected `git` or `all`"),
        }
    }
}

#[derive(Debug, Default, Serialize)]
pub struct IngestReport {
    pub prefix: String,
    pub added: usize,
    pub changed: usize,
    pub unchanged: usize,
    pub skipped: usize,
    pub bytes_written: u64,
}

#[derive(Debug, Default, Serialize)]
pub struct ExportReport {
    pub prefix: String,
    pub written: usize,
    pub skipped_placeholders: usize,
    pub bytes_written: u64,
}

/// Copy `source` into the namespace under `prefix`.
///
/// A file whose content id already matches is left alone, so re-running ingest
/// after a small edit costs one hash per file and one write per changed file.
/// With `dry_run` the namespace is never written; the report describes what
/// would happen.
pub fn ingest_dir(
    vfs: &mut Vfs,
    source: &Path,
    prefix: &str,
    mode: SourceMode,
    dry_run: bool,
) -> Result<IngestReport> {
    let prefix = prefix.trim_matches('/');
    let mut report = IngestReport {
        prefix: prefix.to_string(),
        ..IngestReport::default()
    };

    let mut files = match mode {
        SourceMode::Git => match git_files(source) {
            Ok(files) => files,
            // Not a repository, or git is unavailable: a plain walk is the
            // honest fallback rather than refusing to import anything.
            Err(_) => walk_files(source)
                .with_context(|| format!("walking {}", source.display()))?,
        },
        SourceMode::All => walk_files(source).with_context(|| format!("walking {}", source.display()))?,
    };
    files.sort();

    for rel in files {
        let virtual_path = if prefix.is_empty() {
            rel.clone()
        } else {
            format!("{prefix}/{rel}")
        };
        let virtual_path = normalize_path(&virtual_path)
            .with_context(|| format!("normalizing {virtual_path}"))?;

        let absolute = source.join(&rel);
        // A symlink is not content: following it would import bytes from
        // outside the tree and make the result depend on this machine.
        if fs::symlink_metadata(&absolute).map(|m| m.file_type().is_symlink()).unwrap_or(false) {
            report.skipped += 1;
            continue;
        }

        let bytes = match fs::read(&absolute) {
            Ok(bytes) => bytes,
            // Vanished between listing and reading; normal on a live checkout.
            Err(_) => {
                report.skipped += 1;
                continue;
            }
        };
        let content_id = ContentId::from_bytes(&bytes);

        match vfs.file(&virtual_path)? {
            Some(entry) if entry.content_id == content_id => report.unchanged += 1,
            Some(_) => {
                if !dry_run {
                    vfs.put(&virtual_path, &bytes, media_type_for(&rel), None)?;
                }
                report.changed += 1;
                report.bytes_written += bytes.len() as u64;
            }
            None => {
                if !dry_run {
                    vfs.put(&virtual_path, &bytes, media_type_for(&rel), None)?;
                }
                report.added += 1;
                report.bytes_written += bytes.len() as u64;
            }
        }
    }

    Ok(report)
}

/// Materialize everything under `prefix` into `dest`.
///
/// Placeholder entries have no local bytes, so they are counted and skipped
/// rather than written as empty files.
pub fn export_dir(vfs: &mut Vfs, prefix: &str, dest: &Path) -> Result<ExportReport> {
    let prefix = prefix.trim_matches('/');
    let entries = vfs.list(prefix)?;
    let mut report = ExportReport {
        prefix: prefix.to_string(),
        ..ExportReport::default()
    };
    fs::create_dir_all(dest).with_context(|| format!("creating {}", dest.display()))?;

    for entry in entries {
        if entry.hydration != Hydration::Present {
            report.skipped_placeholders += 1;
            continue;
        }
        let rel = strip_prefix(&entry.path, prefix);
        if rel.is_empty() {
            continue;
        }
        let bytes = vfs.get(&entry.path)?;
        let out = dest.join(&rel);
        if let Some(parent) = out.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        fs::write(&out, &bytes).with_context(|| format!("writing {}", out.display()))?;
        report.written += 1;
        report.bytes_written += bytes.len() as u64;
    }

    Ok(report)
}

fn strip_prefix(path: &str, prefix: &str) -> String {
    if prefix.is_empty() {
        return path.to_string();
    }
    path.strip_prefix(prefix)
        .map(|rest| rest.trim_start_matches('/').to_string())
        .unwrap_or_else(|| path.to_string())
}

/// `git ls-files` reports paths relative to `dir`, honours `.gitignore`, and
/// includes tracked files even when a later ignore rule matches them — the same
/// set Delta imports.
fn git_files(dir: &Path) -> io::Result<Vec<String>> {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["ls-files", "-z", "-c", "-o", "--exclude-standard"])
        .output()?;
    if !output.status.success() {
        return Err(io::Error::other("git ls-files failed"));
    }
    Ok(output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|chunk| !chunk.is_empty())
        .map(|chunk| String::from_utf8_lossy(chunk).into_owned())
        .collect())
}

fn walk_files(dir: &Path) -> Result<Vec<String>> {
    let mut files = Vec::new();
    let mut stack: Vec<PathBuf> = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        for entry in fs::read_dir(&current)
            .with_context(|| format!("reading {}", current.display()))?
        {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let name = entry.file_name();
            if file_type.is_dir() {
                if name == ".git" {
                    continue;
                }
                stack.push(entry.path());
            } else if file_type.is_file() || file_type.is_symlink() {
                // Symlinks are listed rather than dropped here so ingest can
                // report them as skipped; deciding they are not content belongs
                // in one place, not two.
                let rel = entry
                    .path()
                    .strip_prefix(dir)
                    .with_context(|| format!("relativizing {}", entry.path().display()))?
                    .to_string_lossy()
                    .replace('\\', "/");
                files.push(rel);
            }
        }
    }
    Ok(files)
}

/// Media types are a display hint here, not an authority: the VFS keys on the
/// content id. Unknown extensions stay opaque rather than guessing.
fn media_type_for(path: &str) -> &'static str {    let ext = path.rsplit_once('.').map(|(_, ext)| ext).unwrap_or("");
    match ext.to_ascii_lowercase().as_str() {
        "md" | "markdown" => "text/markdown",
        "txt" | "log" => "text/plain",
        "rs" => "text/x-rust",
        "toml" => "application/toml",
        "json" => "application/json",
        "yaml" | "yml" => "application/yaml",
        "sh" => "application/x-sh",
        "html" | "htm" => "text/html",
        "css" => "text/css",
        "js" | "mjs" => "text/javascript",
        "ts" => "text/typescript",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "svg" => "image/svg+xml",
        "pdf" => "application/pdf",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn scratch(tag: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("fungos-vfs-transfer-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn write(path: &Path, body: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, body).unwrap();
    }

    #[test]
    fn ingest_then_export_is_byte_identical_and_idempotent() {
        let base = scratch("roundtrip");
        let source = base.join("source");
        write(&source.join("docs/a.txt"), "hello cid\n");
        write(&source.join("docs/nested/b.md"), "# title\n");
        write(&source.join("top.txt"), "top\n");

        let vfs_root = base.join("vfs");
        let mut vfs = Vfs::open(&vfs_root).unwrap();

        let first = ingest_dir(&mut vfs, &source, "projects/x", SourceMode::All, false).unwrap();
        assert_eq!(first.added, 3);
        assert_eq!(first.unchanged, 0);

        // Re-running must not rewrite anything: unchanged is decided by content id.
        let second = ingest_dir(&mut vfs, &source, "projects/x", SourceMode::All, false).unwrap();
        assert_eq!(second.added, 0);
        assert_eq!(second.unchanged, 3);
        assert_eq!(second.bytes_written, 0);

        // A dry run on a changed file reports it without touching the namespace.
        write(&source.join("top.txt"), "top changed\n");
        let dry = ingest_dir(&mut vfs, &source, "projects/x", SourceMode::All, true).unwrap();
        assert_eq!(dry.changed, 1);
        assert_eq!(vfs.get("projects/x/top.txt").unwrap(), b"top\n");

        let applied = ingest_dir(&mut vfs, &source, "projects/x", SourceMode::All, false).unwrap();
        assert_eq!(applied.changed, 1);

        let dest = base.join("out");
        let report = export_dir(&mut vfs, "projects/x", &dest).unwrap();
        assert_eq!(report.written, 3);
        assert_eq!(
            fs::read(dest.join("docs/a.txt")).unwrap(),
            b"hello cid\n"
        );
        assert_eq!(fs::read(dest.join("top.txt")).unwrap(), b"top changed\n");
        assert!(dest.join("docs/nested/b.md").is_file());
    }

    #[test]
    fn export_skips_placeholders_instead_of_writing_empty_files() {
        let base = scratch("placeholder");
        let mut vfs = Vfs::open(base.join("vfs")).unwrap();
        vfs.register_placeholder(
            "projects/p/big.bin",
            ContentId::parse("blake3:0000000000000000000000000000000000000000000000000000000000000000")
                .unwrap(),
            4096,
            "application/octet-stream",
        )
        .unwrap();

        let dest = base.join("out");
        let report = export_dir(&mut vfs, "projects/p", &dest).unwrap();
        assert_eq!(report.written, 0);
        assert_eq!(report.skipped_placeholders, 1);
        assert!(!dest.join("big.bin").exists());
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_are_skipped_because_they_are_not_content() {
        let base = scratch("symlink");
        let source = base.join("source");
        write(&source.join("real.txt"), "real\n");
        std::os::unix::fs::symlink(source.join("real.txt"), source.join("link.txt")).unwrap();

        let mut vfs = Vfs::open(base.join("vfs")).unwrap();
        let report = ingest_dir(&mut vfs, &source, "projects/s", SourceMode::All, false).unwrap();
        assert_eq!(report.added, 1);
        assert_eq!(report.skipped, 1);
        assert!(vfs.file("projects/s/link.txt").unwrap().is_none());
    }
}
