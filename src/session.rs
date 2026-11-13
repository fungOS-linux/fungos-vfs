//! Sessions as records: one shape for every harness.
//!
//! A conversation is evidence. It says what was asked, what was run, what came
//! back, and which model was standing at the time — and every harness keeps that
//! in its own shape, in its own place, on one machine. The point of this module is
//! that the shape stops being the harness's business: an importer reads a native
//! store and writes one of these, a projector renders one of these into whatever
//! the reader wants.
//!
//! Two decisions carry it:
//!
//! - **Turns reference content, they do not contain it.** Every text, argument
//!   block and result is filed in the store under its own digest first, so the same
//!   prompt pasted into two sessions is stored once, and a session record stays a
//!   few kilobytes of pointers no matter how long the conversation was.
//! - **`extras` is opaque.** Whatever the harness has that this schema does not
//!   gets carried as JSON rather than dropped, so importing never loses a fact and
//!   never invents one. A projector that needs it can read it; one that does not
//!   can ignore it.
//!
//! What is deliberately *not* here: ranking, search, or a transcript format. This
//! is the record. Rendering is somebody else's function.

use serde::{Deserialize, Serialize};

use crate::{ContentId, Expect, Vfs, VfsError};

/// Where a shared payload lives: one entry per distinct byte string, so the store
/// dedupes across sessions and across harnesses.
#[must_use]
pub fn blob_key(content_id: &ContentId) -> String {
    let hex = content_id.hex();
    format!("blobs/{}/{}", &hex[..2], hex)
}

/// Where a session record lives.
#[must_use]
pub fn record_key(harness: &str, native_id: &str) -> String {
    format!("sessions/{harness}/{native_id}/session.json")
}

/// Who was speaking. Normalized to four words because every harness has these four
/// even when it spells them differently.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

/// A tool invocation, when a turn made one.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolCall {
    pub name: String,
    /// The arguments as JSON, filed in the store and named by digest.
    pub arguments: ContentId,
    /// The result, when the harness recorded one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<ContentId>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub failed: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// Token accounting, when the harness reported any.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct Usage {
    pub input: u64,
    pub output: u64,
    pub reasoning: u64,
}

impl Usage {
    fn is_empty(&self) -> bool {
        self.input == 0 && self.output == 0 && self.reasoning == 0
    }
}

/// One thing that happened, in order.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Turn {
    /// Position in the session, from zero.
    pub index: u32,
    pub at_ms: i64,
    pub role: Role,
    /// The harness's own id for this record, and the one it answered, so a
    /// conversation that forked can still be unfolded.
    pub native_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<ContentId>,
    /// Text that was the model thinking, kept apart from what was said.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thinking: Option<ContentId>,
    /// Every call this turn made. A list, not a slot: harnesses fan out —
    /// Pi answers one assistant message with several `toolCall` blocks — and a
    /// record that kept one of them would lose the others and orphan their
    /// results.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ToolCall>,
    #[serde(default, skip_serializing_if = "Usage::is_empty")]
    pub usage: Usage,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// What this turn carried that the fields above have no home for: an image
    /// the user pasted, the error a model returned. Payloads are filed in the
    /// store and named by digest, exactly like the text, so this stays small.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extras: Option<serde_json::Value>,
}

/// One conversation, whole.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Session {
    pub harness: String,
    /// The harness's own identity for the session: the key an import is idempotent
    /// on, and the only name a re-import can be matched by.
    pub native_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub started_at_ms: i64,
    pub ended_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub models: Vec<String>,
    pub turns: Vec<Turn>,
    /// Lines the importer could not normalize, counted rather than dropped
    /// silently — a number here is the difference between "supported" and "looks
    /// supported".
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub unnormalized: std::collections::BTreeMap<String, u64>,
    /// Anything the harness said that this schema has no field for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extras: Option<serde_json::Value>,
}

/// A session as listed, without its turns: enough to choose one.
#[derive(Clone, Debug, Serialize)]
pub struct SessionSummary {
    pub harness: String,
    pub native_id: String,
    pub name: Option<String>,
    pub started_at_ms: i64,
    pub ended_at_ms: i64,
    pub turns: usize,
    pub bytes: u64,
}

impl Vfs {
    /// File one payload and name it by digest. Idempotent: the second session with
    /// the same text costs a lookup, not a copy.
    pub fn put_blob(&mut self, bytes: &[u8]) -> Result<ContentId, VfsError> {
        let content_id = ContentId::from_bytes(bytes);
        self.put(
            &blob_key(&content_id),
            bytes,
            "application/octet-stream",
            Expect::Any,
        )?;
        Ok(content_id)
    }

    /// Write a session record, and say whether it changed.
    ///
    /// An unchanged record is not rewritten. Re-running an import is supposed to be
    /// free, and a write that leaves the same bytes still appends a version and
    /// moves the timestamp — which would make a no-op look like a change.
    pub fn put_session(&mut self, session: &Session) -> Result<bool, VfsError> {
        let key = record_key(&session.harness, &session.native_id);
        let bytes = serde_json::to_vec_pretty(session)
            .map_err(|error| VfsError::Config(format!("encoding session: {error}")))?;
        let after = ContentId::from_bytes(&bytes);
        if self.file(&key)?.map(|entry| entry.content_id).as_ref() == Some(&after) {
            return Ok(false);
        }
        self.put(&key, &bytes, "application/json", Expect::Any)?;
        Ok(true)
    }

    /// Every session record under `sessions/`, one summary each.
    pub fn sessions(&self, harness: Option<&str>) -> Result<Vec<SessionSummary>, VfsError> {
        let prefix = match harness {
            Some(harness) => format!("sessions/{harness}"),
            None => "sessions".to_owned(),
        };
        let mut summaries = Vec::new();
        for entry in self.list(&prefix)? {
            if !entry.path.ends_with("/session.json") {
                continue;
            }
            let bytes = self.content(&entry.content_id)?;
            let session: Session = match serde_json::from_slice(&bytes) {
                Ok(session) => session,
                // A record this build cannot read is named rather than skipped
                // silently: a list that quietly omits half the store is worse than
                // one that shows a broken entry.
                Err(_) => continue,
            };
            summaries.push(SessionSummary {
                harness: session.harness.clone(),
                native_id: session.native_id.clone(),
                name: session.name.clone(),
                started_at_ms: session.started_at_ms,
                ended_at_ms: session.ended_at_ms,
                turns: session.turns.len(),
                bytes: entry.size,
            });
        }
        summaries.sort_by_key(|summary| summary.started_at_ms);
        Ok(summaries)
    }

    /// One session, whole.
    pub fn session(&self, harness: &str, native_id: &str) -> Result<Option<Session>, VfsError> {
        let key = record_key(harness, native_id);
        let Some(entry) = self.file(&key)? else {
            return Ok(None);
        };
        let bytes = self.content(&entry.content_id)?;
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| VfsError::Config(format!("{key}: {error}")))
    }

    /// The text of a turn, when it had any.
    pub fn turn_text(&self, turn: &Turn) -> Result<Option<String>, VfsError> {
        match &turn.text {
            Some(id) => Ok(Some(
                String::from_utf8_lossy(&self.content(id)?).into_owned(),
            )),
            None => Ok(None),
        }
    }
}
