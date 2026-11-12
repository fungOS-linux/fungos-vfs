//! Path classification for a namespace.
//!
//! Not every file deserves to become a blob. Source that Git already tracks is
//! content-addressed by Git's own tree object, so copying each file into the VFS
//! duplicates work Git has done and turns a 97k-file checkout into 97k
//! transactions. Opaque artifacts (PDF, PPTX, images) are the opposite: Git can
//! neither merge nor delta-compress them, so they belong here.
//!
//! A policy file names the difference. Format, one rule per line:
//!
//! ```text
//! # comment
//! node_modules/          bare pattern: never stored
//! exclude: *.iso         explicit form of the same thing
//! cid: **/*.pdf          stored as content-addressed blob
//! git: **/*.svg          belongs to the Git tree; do not duplicate as blobs
//! cache: target/         stored in the CAS, never versioned, evictable
//! ```
//!
//! Last matching rule wins. A path matching nothing is stored as a blob, which
//! is the behaviour that existed before policies.

use std::{fs, path::Path};

use crate::VfsError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PathClass {
    /// Never stored: build output, caches, local secrets.
    Exclude,
    /// Stored as a content-addressed blob.
    Blob,
    /// Belongs to a Git tree. Record the tree, do not copy the files.
    Git,
    /// Stored in the CAS and never versioned: a build's output.
    ///
    /// The difference from [`PathClass::Blob`] is not storage, it is what the
    /// collector may do. A blob is a fact with a history and a divergence record,
    /// and losing one loses information; a cache entry is a hint whose loss is
    /// always safe, so it can be evicted by size while every other class is
    /// collected only by reachability. Merging the two would leave the collector
    /// unable to tell "safe to forget" from "never forget", and that distinction
    /// is what lets it run aggressively at all.
    Cache,
}

#[derive(Clone, Debug, Default)]
pub struct PathPolicy {
    rules: Vec<(String, PathClass)>,
}

impl PathPolicy {
    /// A policy that classifies everything as a blob.
    pub fn store_everything() -> Self {
        Self::default()
    }

    pub fn parse(text: &str) -> Self {
        let mut rules = Vec::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (class, pattern) = split_directive(line);
            let pattern = pattern.trim();
            if pattern.is_empty() {
                continue;
            }
            rules.push((pattern.to_string(), class));
        }
        Self { rules }
    }

    /// Load `.vfsignore` from `root`, if present.
    pub fn load(root: impl AsRef<Path>) -> Result<Self, VfsError> {
        let path = root.as_ref().join(".vfsignore");
        match fs::read_to_string(&path) {
            Ok(text) => Ok(Self::parse(&text)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(Self::store_everything())
            }
            Err(error) => Err(VfsError::Io(error)),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Whether any rule asks for Git-tree representation.
    pub fn has_git_rules(&self) -> bool {
        self.rules.iter().any(|(_, rule)| *rule == PathClass::Git)
    }

    /// Whether any rule asks for blob storage explicitly. Only an explicit
    /// request conflicts with an already-tracked path; the default does not.
    pub fn has_blob_rules(&self) -> bool {
        self.rules.iter().any(|(_, rule)| *rule == PathClass::Blob)
    }

    /// The rule that decided this path, or `None` when nothing matched and the
    /// path fell through to the default.
    pub fn explicit_class(&self, path: &str) -> Option<PathClass> {
        let path = path.trim_matches('/');
        let mut class = None;
        for (pattern, rule) in &self.rules {
            if pattern_matches(pattern, path) {
                class = Some(*rule);
            }
        }
        class
    }

    /// Last matching rule wins, so a later `git:` can carve an exception out of
    /// a broader `cid:` rule.
    pub fn classify(&self, path: &str) -> PathClass {
        self.explicit_class(path).unwrap_or(PathClass::Blob)
    }
}

fn split_directive(line: &str) -> (PathClass, &str) {
    for (prefix, class) in [
        ("exclude:", PathClass::Exclude),
        ("cache:", PathClass::Cache),
        ("cid:", PathClass::Blob),
        ("store:", PathClass::Blob),
        ("git:", PathClass::Git),
    ] {
        if let Some(rest) = line
            .get(..prefix.len())
            .filter(|head| head.eq_ignore_ascii_case(prefix))
            .and_then(|_| line.get(prefix.len()..))
        {
            return (class, rest);
        }
    }
    (PathClass::Exclude, line)
}

/// Match a rule against a namespace path.
///
/// A trailing `/` marks a directory; it does not anchor the pattern. Otherwise a
/// pattern containing no `/` matches a single path segment at any depth, the way
/// `.gitignore` treats an unanchored name. An anchored pattern is matched
/// segment by segment, where `*` and `?` never cross a separator and `**`
/// matches zero or more segments.
fn pattern_matches(pattern: &str, path: &str) -> bool {
    let path_segments: Vec<&str> = path.split('/').collect();
    let is_dir = pattern.ends_with('/');
    let body = pattern.trim_matches('/');

    if !body.contains('/') {
        // A directory pattern matches the components a file lives under, never
        // the file name itself.
        let candidates = if is_dir {
            &path_segments[..path_segments.len().saturating_sub(1)]
        } else {
            &path_segments[..]
        };
        return candidates
            .iter()
            .any(|segment| segment_matches(body, segment));
    }

    let mut segments: Vec<&str> = body.split('/').collect();
    if is_dir {
        // `node_modules/` means the directory and everything inside it.
        segments.push("**");
    }
    segments_match(&segments, &path_segments)
}

fn segments_match(pattern: &[&str], path: &[&str]) -> bool {
    match pattern.split_first() {
        None => path.is_empty(),
        Some((&"**", rest)) => (0..=path.len()).any(|skip| segments_match(rest, &path[skip..])),
        Some((segment, rest)) => match path.split_first() {
            Some((candidate, path_rest)) => {
                segment_matches(segment, candidate) && segments_match(rest, path_rest)
            }
            None => false,
        },
    }
}

fn segment_matches(pattern: &str, text: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = text.chars().collect();
    let (mut p, mut t) = (0usize, 0usize);
    let (mut star, mut resume) = (None, 0usize);
    while t < text.len() {
        match pattern.get(p) {
            Some('*') => {
                star = Some(p);
                resume = t;
                p += 1;
            }
            Some('?') => {
                p += 1;
                t += 1;
            }
            Some(c) if *c == text[t] => {
                p += 1;
                t += 1;
            }
            _ => match star {
                Some(star_at) => {
                    p = star_at + 1;
                    resume += 1;
                    t = resume;
                }
                None => return false,
            },
        }
    }
    pattern[p..].iter().all(|c| *c == '*')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bare_pattern_excludes_that_name_at_any_depth() {
        let policy = PathPolicy::parse("node_modules/\n# a comment\n");
        assert_eq!(
            policy.classify("node_modules/left-pad/index.js"),
            PathClass::Exclude
        );
        assert_eq!(
            policy.classify("web/node_modules/left-pad/index.js"),
            PathClass::Exclude
        );
        assert_eq!(policy.classify("src/index.js"), PathClass::Blob);
    }

    #[test]
    fn a_cache_rule_is_its_own_class() {
        // Build output is stored, so it is not `exclude:`, and it is never
        // versioned, so it is not `cid:` either. The class is what tells the
        // collector it may evict this content by size.
        let policy = PathPolicy::parse("cache: target/\n");
        assert_eq!(
            policy.classify("target/debug/deps/libfoo.rmeta"),
            PathClass::Cache
        );
        assert_eq!(policy.classify("src/main.rs"), PathClass::Blob);
        // Last rule wins, so a later `exclude:` can still carve something out.
        let narrow = PathPolicy::parse("cache: target/\nexclude: target/final/\n");
        assert_eq!(narrow.classify("target/final/app"), PathClass::Exclude);
    }

    #[test]
    fn a_later_rule_overrides_an_earlier_one() {
        // Artifacts are blobs, except SVGs, which Git can merge and delta.
        let policy = PathPolicy::parse("cid: **/*.svg\ngit: **/*.svg\n");
        assert_eq!(policy.classify("img/logo.svg"), PathClass::Git);
        assert_eq!(policy.classify("logo.svg"), PathClass::Git);
        // Unmatched paths fall through to blob storage, not to a rule.
        assert_eq!(policy.classify("img/photo.png"), PathClass::Blob);
        assert_eq!(policy.explicit_class("img/photo.png"), None);
    }

    #[test]
    fn unanchored_names_match_a_single_segment_only() {
        let policy = PathPolicy::parse("exclude: *.iso\n");
        assert_eq!(policy.classify("debian.iso"), PathClass::Exclude);
        assert_eq!(policy.classify("images/debian.iso"), PathClass::Exclude);
        // `*` never crosses a separator, so a deeper path is untouched.
        assert_eq!(policy.classify("iso/keep.txt"), PathClass::Blob);
    }

    #[test]
    fn double_star_matches_zero_or_more_segments() {
        let policy = PathPolicy::parse("cid: docs/**/*.pdf\n");
        assert_eq!(policy.classify("docs/a.pdf"), PathClass::Blob);
        assert_eq!(policy.classify("docs/x/y/a.pdf"), PathClass::Blob);
        assert_eq!(policy.classify("docs/a.txt"), PathClass::Blob);
        assert_eq!(policy.explicit_class("docs/a.txt"), None);
    }
}
