//! Projectors: one record, rendered into a target.
//!
//! A projector reads a [`Session`] and writes somebody else's format. It never
//! mutates the record and never reads a harness's native store, which is what
//! keeps the number of converters linear — N harnesses plus M targets — instead
//! of quadratic.
//!
//! The first target is `operator`'s run bundle: NDJSON under
//! `<workspace>/.operator/runs/`, a `meta` line and then one line per message,
//! which is what its resume path (`run::load_session`) reads back into a live
//! transcript. Three things about writing that format are worth saying out loud:
//!
//! - **`session_key` is not operator's head hash.** Operator derives that key from
//!   the first messages it can see, so a projected bundle has no such history to
//!   hash. The key here is the digest of the record instead. It only affects
//!   sticky cache routing, and operator recomputes its own hint on every resumed
//!   turn — a guess at a hash we cannot compute would be worse than a key that
//!   says plainly which record this is.
//! - **`stop_reason` is `end_turn` and `status` is `done`.** A projected bundle is
//!   a transcript import, not a terminal contract. Reporting `incomplete` for a
//!   conversation that ended on another machine months ago would let `--strict`
//!   exit codes describe an event this build never saw.
//! - **Tool results are matched to their calls by digest.** `operator` needs a
//!   result line to name the call it answers; the record stores the result on the
//!   call, so the call id is synthesized (`call-<turn>`) and the result line is
//!   whichever `Role::Tool` turn carries the same payload.

use std::{
    collections::{BTreeMap, VecDeque},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use base64::Engine as _;
use serde::Serialize;
use serde_json::{Map, Value, json};

use crate::{BlockId, ContentId, DocFile, Op, Role, Session, Vfs, split_blocks};

/// Named in the bundle's `version` field so a reader can tell a projected bundle
/// from one this machine's own run wrote.
const VERSION: &str = concat!("fungos-vfs ", env!("CARGO_PKG_VERSION"));

/// `operator-content` refuses an attachment larger than this, and a provider
/// would refuse to replay one.
const MAX_ATTACHMENT_BYTES: usize = 10 * 1024 * 1024;

/// A bundle on disk, and what projecting it did.
#[derive(Debug)]
pub struct Projection {
    pub path: PathBuf,
    pub report: ProjectionReport,
}

#[derive(Debug, Default, Serialize)]
pub struct ProjectionReport {
    /// Whether the file on disk had to change. A projection that produces the same
    /// bytes does not rewrite them, so a timer is not a stream of identical writes.
    pub written: bool,
    /// Message lines written.
    pub messages: usize,
    pub attachments: usize,
    /// What the target cannot represent, counted by reason rather than dropped.
    pub skipped: BTreeMap<String, u64>,
    pub bytes: u64,
}

/// Project one session into `operator`'s run bundle under `out_dir`.
///
/// Idempotent: the file is named after the session's start time, so projecting the
/// same record twice replaces one bundle rather than adding a second.
pub fn to_operator(
    vfs: &Vfs,
    session: &Session,
    out_dir: &Path,
    workspace: Option<&str>,
    dry_run: bool,
) -> Result<Projection> {
    let mut report = ProjectionReport::default();
    let mut lines: Vec<Value> = Vec::new();

    // Two ways a result finds its call. By digest when the record attached the
    // result to the call, which is the common case — a queue per digest, because
    // the same output twice is two results that happen to read alike. Otherwise
    // by position, in call order: an empty result files no payload to match
    // against, and Pi's commands that printed nothing are still results.
    let mut by_digest: BTreeMap<ContentId, VecDeque<(String, String)>> = BTreeMap::new();
    let mut unanswered: VecDeque<(String, String)> = VecDeque::new();
    for turn in &session.turns {
        for (slot, tool) in turn.tools.iter().enumerate() {
            match &tool.result {
                Some(result) => by_digest
                    .entry(result.clone())
                    .or_default()
                    .push_back((call_id(turn, slot), tool.name.clone())),
                None => unanswered.push_back((call_id(turn, slot), tool.name.clone())),
            }
        }
    }

    let mut prompt_tokens = 0u64;
    let mut completion_tokens = 0u64;
    let mut turns = 0u64;
    let mut last_model = String::new();

    for (seq, turn) in session.turns.iter().enumerate() {
        prompt_tokens += turn.usage.input;
        completion_tokens += turn.usage.output;
        if let Some(model) = &turn.model
            && !model.is_empty()
        {
            last_model.clone_from(model);
        }

        let mut line = Map::new();
        line.insert("type".into(), json!("message"));
        line.insert("seq".into(), json!(seq));
        line.insert("ts_ms".into(), json!(turn.at_ms.max(0)));

        match turn.role {
            Role::System => {
                line.insert("role".into(), json!("system"));
                line.insert("kind".into(), json!("message"));
                insert_content(vfs, &mut line, turn.text.as_ref(), "content")?;
            }
            Role::User => {
                line.insert("role".into(), json!("user"));
                line.insert("kind".into(), json!("message"));
                insert_content(vfs, &mut line, turn.text.as_ref(), "content")?;
                let attachments = attachments(vfs, turn, &mut report)?;
                if !attachments.is_empty() {
                    report.attachments += attachments.len();
                    line.insert("attachments".into(), Value::Array(attachments));
                }
            }
            Role::Assistant => {
                turns += 1;
                line.insert("role".into(), json!("assistant"));
                line.insert(
                    "kind".into(),
                    json!(if turn.tools.is_empty() {
                        "message"
                    } else {
                        "tool_call"
                    }),
                );
                insert_content(vfs, &mut line, turn.text.as_ref(), "content")?;
                insert_content(vfs, &mut line, turn.thinking.as_ref(), "reasoning")?;
                if !turn.tools.is_empty() {
                    // A turn may fan out. Operator's own bundles already carry a
                    // list here, and a resumed transcript needs every call in it:
                    // one dropped call means one result with nowhere to go.
                    let mut calls = Vec::new();
                    for (slot, tool) in turn.tools.iter().enumerate() {
                        let args = arguments(vfs, tool, &mut report)?;
                        if slot == 0 {
                            line.insert("tool_name".into(), json!(tool.name));
                            line.insert("tool_args".into(), args.clone());
                        }
                        calls.push(json!({
                            "id": call_id(turn, slot),
                            "name": tool.name,
                            "args": args,
                        }));
                    }
                    line.insert("tool_calls".into(), Value::Array(calls));
                }
            }
            Role::Tool => {
                line.insert("role".into(), json!("tool"));
                line.insert("kind".into(), json!("tool_result"));
                insert_content(vfs, &mut line, turn.text.as_ref(), "content")?;
                let found = turn
                    .text
                    .as_ref()
                    .and_then(|id| by_digest.get_mut(id))
                    .and_then(|calls| calls.pop_front())
                    .or_else(|| unanswered.pop_front());
                match found {
                    Some((id, name)) => {
                        line.insert("tool_call_id".into(), json!(id));
                        line.insert("tool_name".into(), json!(name));
                    }
                    // A result nothing called cannot be part of a valid
                    // transcript; it is named rather than invented a caller.
                    None => {
                        *report
                            .skipped
                            .entry("tool-result-without-call".to_owned())
                            .or_default() += 1
                    }
                }
            }
        }

        lines.push(Value::Object(line));
    }

    report.messages = lines.len();

    let model = if last_model.is_empty() {
        session.models.last().cloned().unwrap_or_default()
    } else {
        last_model
    };
    let mut meta = Map::new();
    meta.insert("type".into(), json!("meta"));
    meta.insert("run_id".into(), json!(run_id(session)));
    meta.insert(
        "session_id".into(),
        json!(format!("{}-{}", session.harness, session.native_id)),
    );
    meta.insert("session_key".into(), json!(session_key(session)?));
    if let Some(name) = session.name.as_ref().filter(|name| !name.trim().is_empty()) {
        meta.insert("name".into(), json!(name));
    }
    meta.insert("branch_at".into(), json!(0));
    meta.insert(
        "workspace".into(),
        json!(workspace.or(session.cwd.as_deref()).unwrap_or_default()),
    );
    meta.insert("model".into(), json!(model));
    meta.insert("version".into(), json!(VERSION));
    meta.insert("start_ms".into(), json!(session.started_at_ms.max(0)));
    meta.insert("end_ms".into(), json!(session.ended_at_ms.max(0)));
    meta.insert("stop_reason".into(), json!("end_turn"));
    meta.insert("status".into(), json!("done"));
    meta.insert("turns".into(), json!(turns));
    meta.insert("prompt_tokens".into(), json!(prompt_tokens));
    meta.insert("cached_tokens".into(), json!(0));
    meta.insert("completion_tokens".into(), json!(completion_tokens));
    meta.insert("cost_usd".into(), json!(0.0));
    meta.insert("dirty".into(), json!(false));
    meta.insert("unpushed".into(), json!(0));
    meta.insert("has_remote".into(), json!(false));

    let mut body = serde_json::to_string(&Value::Object(meta))?;
    body.push('\n');
    for line in &lines {
        body.push_str(&serde_json::to_string(line)?);
        body.push('\n');
    }
    report.bytes = body.len() as u64;

    let path = out_dir.join(format!("{}.ndjson", run_id(session)));
    if !dry_run {
        let same = fs::read(&path).is_ok_and(|existing| existing == body.as_bytes());
        if !same {
            fs::create_dir_all(out_dir)
                .with_context(|| format!("creating {}", out_dir.display()))?;
            fs::write(&path, &body).with_context(|| format!("writing {}", path.display()))?;
            report.written = true;
        }
    }
    Ok(Projection { path, report })
}

/// Render a session as a markdown transcript.
///
/// This is the target that needs no target: a transcript is a document, so the
/// merge layer, the snapshots and git all apply without a line of new machinery.
/// Each turn is one block — a heading and what happened under it — which is what
/// makes appending a turn a single insert and re-projecting an unchanged session
/// produce nothing at all.
pub fn to_markdown(vfs: &Vfs, session: &Session) -> Result<String> {
    let mut parts = vec![header_chunk(session)];
    for turn in &session.turns {
        parts.push(turn_chunk(vfs, turn)?);
    }
    let mut canonical = crate::split_blocks(&parts.join("")).join("\n\n");
    canonical.push('\n');
    Ok(canonical)
}

/// A transcript is a document, and this is where it is grown.
///
/// [`to_markdown`] renders a whole record, which is the right thing for an export
/// and the wrong thing for a session that is still running: it re-reads every
/// payload in the record to answer "what changed?" when the answer is "one turn
/// arrived". This walks the same layout and appends:
///
/// - **Identity is positional and owned by the record.** Turn *i*'s blocks are
///   numbered after the blocks already emitted, and their actor is the record
///   (`pi:1a2b3c4d`), not the machine. Two replicas that absorb each other's ops
///   before projecting therefore extend one transcript instead of each building
///   its own.
/// - **Only the tail can change.** A harness appends its record; it does not
///   rewrite a turn it already wrote. So a sync re-projects the last turn it
///   knows (which may still be growing) and then everything after it. A record
///   rewritten in the middle is a full [`to_markdown`] plus `doc sync`, and this
///   type does not pretend to notice it.
/// - **Turn boundaries are recovered from the layout, not stored.** Every turn
///   starts with a `## role` block, so which blocks belong to which turn is a
///   fact about the document, not a cursor somebody has to keep in step.
pub struct Transcript;

/// What one append did.
#[derive(Debug, Default, Serialize)]
pub struct Synced {
    /// Turns in the record, whether or not this pass wrote them.
    pub turns: usize,
    /// Turns this pass looked at again: the last one it knew, and the new ones.
    pub replayed: usize,
    pub inserted: usize,
    pub updated: usize,
    pub deleted: usize,
    /// Whether the text on disk changed.
    pub wrote_file: bool,
}

impl Transcript {
    /// The actor a record's blocks are owned by.
    ///
    /// Derived from the harness and the record's own id, so the same record
    /// projects to the same identities wherever it is projected. A harness id can
    /// hold anything a path can, so the digest is what keeps it short and legal.
    #[must_use]
    pub fn author(session: &Session) -> String {
        let digest = ContentId::from_bytes(session.native_id.as_bytes());
        format!("{}:{}", session.harness, &digest.hex()[..8])
    }

    /// Append what `session` has beyond what `doc` already holds, and write the
    /// merged text back to the document's file.
    pub fn sync(doc: &mut DocFile, vfs: &mut Vfs, session: &Session) -> Result<Synced> {
        let author = Self::author(session);
        let emitted: Vec<(BlockId, String)> = doc
            .doc()
            .order()
            .into_iter()
            .filter(|block| block.id.actor == author && !block.deleted)
            .map(|block| (block.id.clone(), block.text.clone()))
            .collect();

        // Turn *t* owns the blocks from the *t*-th `## role` heading to the next
        // one; everything before the first heading is the header.
        let mut runs: Vec<Vec<(BlockId, String)>> = Vec::new();
        let mut header: Vec<(BlockId, String)> = Vec::new();
        for (id, text) in emitted {
            if text.starts_with("## ") {
                runs.push(Vec::new());
            }
            match runs.last_mut() {
                Some(run) => run.push((id, text)),
                None => header.push((id, text)),
            }
        }

        let mut next = doc
            .doc()
            .order()
            .iter()
            .filter(|block| block.id.actor == author)
            .map(|block| block.id.counter)
            .max()
            .map_or(1, |counter| counter + 1);
        let mut fresh = || {
            let id = BlockId::new(next, &author);
            next += 1;
            id
        };

        let mut ops: Vec<Op> = Vec::new();
        let mut synced = Synced {
            turns: session.turns.len(),
            ..Synced::default()
        };
        // The header carries the name, the cwd and the models, and it is cheap to
        // render: it is checked on every pass so a renamed session is corrected.
        let want = split_blocks(&header_chunk(session));
        let mut previous: Option<BlockId> = None;
        for (index, text) in want.iter().enumerate() {
            match header.get(index) {
                Some((id, have)) if have == text => previous = Some(id.clone()),
                Some((id, _)) => {
                    ops.push(Op::Update {
                        id: id.clone(),
                        text: text.clone(),
                        at: doc.stamp(),
                    });
                    synced.updated += 1;
                    previous = Some(id.clone());
                }
                None => {
                    let id = fresh();
                    ops.push(Op::Insert {
                        id: id.clone(),
                        after: previous.clone(),
                        text: text.clone(),
                    });
                    synced.inserted += 1;
                    previous = Some(id);
                }
            }
        }
        for (id, _) in header.iter().skip(want.len()) {
            ops.push(Op::Delete {
                id: id.clone(),
                at: doc.stamp(),
            });
            synced.deleted += 1;
        }

        // Re-project the last turn this document knows: it may have grown since.
        let replay = runs.len().saturating_sub(1);
        synced.replayed = session.turns.len().saturating_sub(replay);
        for (turn, source) in session.turns.iter().enumerate().skip(replay) {
            let want = split_blocks(&turn_chunk(vfs, source)?);
            let have = runs.get(turn).map_or(&[][..], Vec::as_slice);
            for (index, text) in want.iter().enumerate() {
                match have.get(index) {
                    Some((id, existing)) if existing == text => previous = Some(id.clone()),
                    Some((id, _)) => {
                        ops.push(Op::Update {
                            id: id.clone(),
                            text: text.clone(),
                            at: doc.stamp(),
                        });
                        synced.updated += 1;
                        previous = Some(id.clone());
                    }
                    None => {
                        let id = fresh();
                        ops.push(Op::Insert {
                            id: id.clone(),
                            after: previous.clone(),
                            text: text.clone(),
                        });
                        synced.inserted += 1;
                        previous = Some(id);
                    }
                }
            }
            for (id, _) in have.iter().skip(want.len()) {
                ops.push(Op::Delete {
                    id: id.clone(),
                    at: doc.stamp(),
                });
                synced.deleted += 1;
            }
        }

        let report = doc.extend(vfs, ops)?;
        synced.wrote_file = report.wrote_file;
        Ok(synced)
    }
}

/// The header: what the record is, and where it ran.
fn header_chunk(session: &Session) -> String {
    let title = session
        .name
        .as_deref()
        .filter(|name| !name.trim().is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("{} {}", session.harness, session.native_id));
    let mut out = format!("# {title}\n\n");
    if let Some(cwd) = &session.cwd {
        out.push_str(&format!("`{cwd}`"));
        if !session.models.is_empty() {
            out.push_str(" · ");
        }
    }
    if !session.models.is_empty() {
        out.push_str(&session.models.join(", "));
    }
    out.push('\n');
    out
}

/// One turn: a heading, what was said, what was called, what was thought.
fn turn_chunk(vfs: &Vfs, turn: &crate::Turn) -> Result<String> {
    let role = match turn.role {
        Role::System => "system",
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::Tool => "tool",
    };
    let mut out = format!("\n## {role}\n\n");
    if let Some(text) = vfs.turn_text(turn)? {
        out.push_str(text.trim_end());
        out.push('\n');
    }
    for tool in &turn.tools {
        let bytes = vfs.content(&tool.arguments)?;
        let args = String::from_utf8_lossy(&bytes);
        let args = args.trim();
        let args = if args.len() > 120 {
            format!("{}…", &args[..120])
        } else {
            args.to_owned()
        };
        out.push_str(&format!(
            "\n- `{}` {}{}\n",
            tool.name,
            args,
            if tool.failed { " (failed)" } else { "" }
        ));
    }
    if let Some(thinking) = &turn.thinking {
        let bytes = vfs.content(thinking)?;
        let text = String::from_utf8_lossy(&bytes);
        // Folded into one line so a turn stays one block: the point of the
        // markdown is that a re-projection of an unchanged session is free.
        out.push_str(&format!(
            "\n<sub>thought: {}</sub>\n",
            text.replace('\n', " ").trim()
        ));
    }
    Ok(out)
}

/// A bundle is one file named after the session; the same record projects to the
/// same name, so a second projection replaces rather than duplicates.
///
/// The start time alone is not enough to name one: two conversations can begin in
/// the same millisecond, and the second would silently overwrite the first. The
/// digest of the harness's own id makes the name unique and still deterministic.
fn run_id(session: &Session) -> String {
    let digest = ContentId::from_bytes(session.native_id.as_bytes());
    format!(
        "{:016x}-{}-import",
        session.started_at_ms.max(0),
        &digest.hex()[..8]
    )
}

/// The digest of the record, as sixteen hex digits. See the module note for why
/// this is not operator's head hash.
fn session_key(session: &Session) -> Result<String> {
    let bytes = serde_json::to_vec(session)?;
    Ok(ContentId::from_bytes(&bytes).hex()[..16].to_owned())
}

/// The call id a projected assistant turn's tool invocation gets. Synthesized
/// because the record keys tools by turn, not by the harness's own call id.
fn call_id(turn: &crate::Turn, slot: usize) -> String {
    format!("call-{}-{slot}", turn.index)
}

fn insert_content(
    vfs: &Vfs,
    line: &mut Map<String, Value>,
    id: Option<&ContentId>,
    field: &str,
) -> Result<()> {
    if let Some(id) = id {
        let text = String::from_utf8_lossy(&vfs.content(id)?).into_owned();
        if !text.is_empty() {
            line.insert(field.to_owned(), Value::String(text));
        }
    }
    Ok(())
}

/// Tool arguments, as the JSON object operator parses out of a call.
fn arguments(vfs: &Vfs, tool: &crate::ToolCall, report: &mut ProjectionReport) -> Result<Value> {
    let bytes = vfs.content(&tool.arguments)?;
    match serde_json::from_slice::<Value>(&bytes) {
        Ok(value) => Ok(value),
        Err(_) => {
            *report
                .skipped
                .entry("tool-arguments-unparseable".to_owned())
                .or_default() += 1;
            Ok(Value::Null)
        }
    }
}

/// Images a turn carried, as `operator-content` attachments. A projector that
/// emitted an attachment operator will refuse to replay would be lying about
/// what it produced, so anything the target cannot take is counted instead.
fn attachments(vfs: &Vfs, turn: &crate::Turn, report: &mut ProjectionReport) -> Result<Vec<Value>> {
    let mut out = Vec::new();
    let images = turn
        .extras
        .as_ref()
        .and_then(|extras| extras.get("images"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten();
    for image in images {
        let mime = image.get("mimeType").and_then(Value::as_str).unwrap_or("");
        let Some(extension) = extension_for(mime) else {
            *report
                .skipped
                .entry(format!("attachment-mime:{mime}"))
                .or_default() += 1;
            continue;
        };
        let Some(id) = image
            .get("content")
            .and_then(Value::as_str)
            .and_then(|id| ContentId::parse(id).ok())
        else {
            *report
                .skipped
                .entry("attachment-without-payload".to_owned())
                .or_default() += 1;
            continue;
        };
        let bytes = vfs.content(&id)?;
        if bytes.is_empty() || bytes.len() > MAX_ATTACHMENT_BYTES {
            *report
                .skipped
                .entry(format!("attachment-bytes:{}", bytes.len()))
                .or_default() += 1;
            continue;
        }
        // A declared mime is a claim, not a fact. The target sniffs the bytes, so
        // a mismatched one would pass through this projector and only fail on
        // replay — which is exactly the failure this count exists to prevent.
        if sniff(&bytes) != Some(mime) {
            *report
                .skipped
                .entry(format!("attachment-mime-mismatch:{mime}"))
                .or_default() += 1;
            continue;
        }
        out.push(json!({
            "name": format!("image-{}-{}.{extension}", turn.index, out.len()),
            "mime_type": mime,
            "data": base64::engine::general_purpose::STANDARD.encode(&bytes),
        }));
    }
    Ok(out)
}

/// What the bytes say they are, by signature — the same set of types
/// `operator-content` accepts.
fn sniff(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        return Some("image/png");
    }
    if bytes.starts_with(b"\xff\xd8\xff") {
        return Some("image/jpeg");
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        return Some("image/gif");
    }
    if bytes.len() > 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        return Some("image/webp");
    }
    if bytes.starts_with(b"%PDF-") {
        return Some("application/pdf");
    }
    None
}

/// The image types `operator-content` accepts, and the extension each one earns.
fn extension_for(mime: &str) -> Option<&'static str> {
    Some(match mime {
        "image/png" => "png",
        "image/jpeg" => "jpg",
        "image/gif" => "gif",
        "image/webp" => "webp",
        "application/pdf" => "pdf",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;

    use super::*;
    use crate::{NodeRole, Session, ToolCall, Turn, Usage};

    /// A store this test owns. A projector reads payloads out of the store, so a
    /// test pointed at the machine's real one would be reading somebody's data.
    /// A PNG by its signature, which is what the target checks.
    const PNG: &[u8] = b"\x89PNG\r\n\x1a\n not really pixels";

    fn store() -> (tempfile::TempDir, Vfs) {
        let temp = tempfile::tempdir().unwrap();
        let cas = temp.path().join("cas");
        let vfs = Vfs::open_with(temp.path().join("root"), NodeRole::Full, Some(&cas)).unwrap();
        (temp, vfs)
    }

    /// A conversation with one of everything an operator transcript needs.
    fn session(vfs: &mut Vfs) -> Session {
        let system = vfs.put_blob(b"you are pi").unwrap();
        let question = vfs.put_blob(b"read the readme").unwrap();
        let answer = vfs.put_blob(b"reading it").unwrap();
        let thinking = vfs.put_blob(b"they want a look").unwrap();
        let arguments = vfs.put_blob(br#"{"path":"README.md"}"#).unwrap();
        let result = vfs.put_blob(b"boom").unwrap();
        let image = vfs.put_blob(PNG).unwrap();

        let turn = |index: u32, at_ms: i64, role: Role| Turn {
            index,
            at_ms,
            role,
            native_id: format!("e{index}"),
            parent: None,
            text: None,
            thinking: None,
            tools: Vec::new(),
            usage: Usage::default(),
            model: None,
            extras: None,
        };

        let mut turns = vec![
            Turn {
                text: Some(system),
                ..turn(0, 1_000, Role::System)
            },
            Turn {
                text: Some(question),
                extras: Some(json!({ "images": [
                    { "mimeType": "image/png", "content": image.to_string() }
                ]})),
                ..turn(1, 2_000, Role::User)
            },
            Turn {
                text: Some(answer),
                thinking: Some(thinking),
                usage: Usage {
                    input: 10,
                    output: 20,
                    reasoning: 5,
                },
                model: Some("fpl/coder".to_owned()),
                tools: vec![ToolCall {
                    name: "read".to_owned(),
                    arguments,
                    result: Some(result.clone()),
                    failed: true,
                }],
                ..turn(2, 3_000, Role::Assistant)
            },
            Turn {
                text: Some(result),
                ..turn(3, 4_000, Role::Tool)
            },
        ];
        turns[2].tools[0].result = Some(turns[3].text.clone().unwrap());

        Session {
            harness: "pi".to_owned(),
            native_id: "s-1".to_owned(),
            name: Some("prove-it".to_owned()),
            started_at_ms: 1_000,
            ended_at_ms: 4_000,
            cwd: Some("/work/repo".to_owned()),
            models: vec!["fpl/coder".to_owned()],
            turns,
            unnormalized: Default::default(),
            extras: None,
        }
    }

    fn lines(path: &Path) -> Vec<serde_json::Value> {
        fs::read_to_string(path)
            .unwrap()
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[test]
    fn a_record_becomes_an_operator_bundle() {
        let (temp, mut vfs) = store();
        let session = session(&mut vfs);
        let out = temp.path().join("runs");

        let projection = to_operator(&vfs, &session, &out, None, false).unwrap();
        assert!(projection.path.exists());
        assert_eq!(projection.report.messages, 4);
        assert_eq!(projection.report.attachments, 1);
        assert!(projection.report.skipped.is_empty());

        let lines = lines(&projection.path);
        let meta = &lines[0];
        // A bundle is resumable only if `meta` is the first non-empty line.
        assert_eq!(meta["type"], "meta");
        assert_eq!(meta["session_id"], "pi-s-1");
        assert_eq!(meta["name"], "prove-it");
        // The record's cwd is the only workspace fact an import has.
        assert_eq!(meta["workspace"], "/work/repo");
        assert_eq!(meta["model"], "fpl/coder");
        assert_eq!(meta["start_ms"], 1_000);
        assert_eq!(meta["end_ms"], 4_000);
        assert_eq!(
            meta["turns"], 1,
            "system, user and tool lines are not turns"
        );
        assert_eq!(meta["prompt_tokens"], 10);
        assert_eq!(meta["completion_tokens"], 20);
        // Deliberate: a projection is a transcript import, not a terminal event.
        assert_eq!(meta["stop_reason"], "end_turn");
        assert_eq!(meta["status"], "done");
        assert!(meta["version"].as_str().unwrap().starts_with("fungos-vfs "));
        assert_eq!(meta["session_key"].as_str().unwrap().len(), 16);
        assert!(meta["run_id"].as_str().unwrap().ends_with("-import"));

        assert_eq!(lines[1]["role"], "system");
        assert_eq!(lines[1]["content"], "you are pi");
        assert_eq!(lines[1]["seq"], 0);

        let user = &lines[2];
        assert_eq!(user["role"], "user");
        assert_eq!(user["content"], "read the readme");
        assert_eq!(user["attachments"][0]["mime_type"], "image/png");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(user["attachments"][0]["data"].as_str().unwrap())
            .unwrap();
        assert_eq!(decoded, PNG, "the bytes, not the harness's base64");
        assert!(
            user["attachments"][0]["name"]
                .as_str()
                .unwrap()
                .ends_with(".png")
        );

        let assistant = &lines[3];
        assert_eq!(assistant["role"], "assistant");
        assert_eq!(assistant["kind"], "tool_call");
        assert_eq!(assistant["content"], "reading it");
        assert_eq!(assistant["reasoning"], "they want a look");
        assert_eq!(assistant["tool_calls"][0]["id"], "call-2-0");
        assert_eq!(assistant["tool_calls"][0]["name"], "read");
        assert_eq!(assistant["tool_calls"][0]["args"]["path"], "README.md");

        // The result line has to name the call it answers, or the transcript a
        // resume rebuilds is not one a provider will accept.
        let tool = &lines[4];
        assert_eq!(tool["role"], "tool");
        assert_eq!(tool["kind"], "tool_result");
        assert_eq!(tool["tool_call_id"], "call-2-0");
        assert_eq!(tool["tool_name"], "read");
        assert_eq!(tool["content"], "boom");
        assert_eq!(tool["ts_ms"], 4_000, "the result keeps its own time");
    }

    #[test]
    fn projecting_the_same_record_twice_writes_one_bundle() {
        let (temp, mut vfs) = store();
        let session = session(&mut vfs);
        let out = temp.path().join("runs");

        let first = to_operator(&vfs, &session, &out, None, false).unwrap();
        let second = to_operator(&vfs, &session, &out, None, false).unwrap();
        assert!(first.report.written);
        assert!(!second.report.written, "the bytes are already there");
        assert_eq!(first.path, second.path);
        assert_eq!(
            fs::read(&first.path).unwrap(),
            fs::read(&second.path).unwrap()
        );
        let bundles: Vec<_> = fs::read_dir(&out)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .collect();
        assert_eq!(bundles.len(), 1, "one session, one file");
    }

    #[test]
    fn two_sessions_that_started_at_the_same_moment_are_two_bundles() {
        // A millisecond is not an identity. Two Pi sessions opened together would
        // otherwise project onto one filename, and the second would quietly win.
        let (temp, mut vfs) = store();
        let one = session(&mut vfs);
        let mut two = one.clone();
        two.native_id = "s-2".to_owned();
        let out = temp.path().join("runs");

        let first = to_operator(&vfs, &one, &out, None, false).unwrap();
        let second = to_operator(&vfs, &two, &out, None, false).unwrap();
        assert_ne!(first.path, second.path);
        let bundles = fs::read_dir(&out).unwrap().filter_map(|e| e.ok()).count();
        assert_eq!(bundles, 2);
    }

    #[test]
    fn a_payload_the_target_cannot_take_is_counted_not_written() {
        let (temp, mut vfs) = store();
        let mut session = session(&mut vfs);
        // A TIFF is not an image operator-content accepts, and a projector that
        // wrote it anyway would produce a bundle that fails on replay, not here.
        let text = session.turns[0].text.clone().unwrap();
        session.turns[1].extras = Some(json!({ "images": [
            { "mimeType": "image/tiff", "content": text.to_string() }
        ]}));

        let projection =
            to_operator(&vfs, &session, &temp.path().join("runs"), None, false).unwrap();
        assert_eq!(projection.report.attachments, 0);
        assert_eq!(
            projection.report.skipped.get("attachment-mime:image/tiff"),
            Some(&1)
        );
        let lines = lines(&projection.path);
        assert!(lines[2].get("attachments").is_none());
    }

    #[test]
    fn a_projected_transcript_is_already_the_document_layout() {
        // Projecting and then syncing must not count as a change: the file a
        // projector writes is the file the document layer would have written.
        let (temp, mut vfs) = store();
        let session = session(&mut vfs);
        let text = to_markdown(&vfs, &session).unwrap();
        let file = temp.path().join("transcript.md");
        fs::write(&file, &text).unwrap();

        let mut doc = crate::DocFile::open(&vfs, "transcript", "neo", &file).unwrap();
        let first = doc.reconcile(&mut vfs).unwrap();
        assert!(
            first.ops > 0,
            "a transcript is where the document comes from"
        );
        assert!(!first.wrote_file, "the layout was already canonical");
        assert_eq!(fs::read_to_string(&file).unwrap(), text);

        let second = doc.reconcile(&mut vfs).unwrap();
        assert_eq!(second.ops, 0);
        assert!(!second.wrote_file, "and a second pass is free");
    }

    #[test]
    fn an_attachment_whose_bytes_are_not_what_they_claim_is_counted() {
        let (temp, mut vfs) = store();
        let mut session = session(&mut vfs);
        let plain = vfs.put_blob(b"not a png at all").unwrap();
        session.turns[1].extras = Some(json!({ "images": [
            { "mimeType": "image/png", "content": plain.to_string() }
        ]}));

        let projection =
            to_operator(&vfs, &session, &temp.path().join("runs"), None, false).unwrap();
        assert_eq!(projection.report.attachments, 0);
        assert_eq!(
            projection
                .report
                .skipped
                .get("attachment-mime-mismatch:image/png"),
            Some(&1)
        );
    }

    #[test]
    fn a_tool_result_nobody_called_is_counted() {
        let (temp, mut vfs) = store();
        let mut session = session(&mut vfs);
        // No call at all, and the transcript still has an answer to it.
        session.turns[2].tools.clear();

        let projection =
            to_operator(&vfs, &session, &temp.path().join("runs"), None, false).unwrap();
        assert_eq!(
            projection.report.skipped.get("tool-result-without-call"),
            Some(&1)
        );
        let lines = lines(&projection.path);
        assert!(lines[4].get("tool_call_id").is_none());
        assert_eq!(lines[4]["content"], "boom", "the text is still there");
    }

    #[test]
    fn a_turn_that_fanned_out_projects_every_call() {
        // Pi answers one assistant message with several calls and then files one
        // result per call. A record that kept one call lost the rest, and every
        // extra result arrived with nothing to answer.
        let (temp, mut vfs) = store();
        let mut session = session(&mut vfs);
        let second_args = vfs.put_blob(br#"{"path":"SECURITY.md"}"#).unwrap();
        let second_result = vfs.put_blob(b"read the other one").unwrap();
        let first = session.turns[2].tools.remove(0);
        session.turns[2].tools = vec![
            first,
            ToolCall {
                name: "read".to_owned(),
                arguments: second_args,
                result: Some(second_result.clone()),
                failed: false,
            },
        ];
        let mut second = session.turns[3].clone();
        second.text = Some(second_result.clone());
        second.index = 4;
        second.tools.clear();
        session.turns.push(second);

        let projection =
            to_operator(&vfs, &session, &temp.path().join("runs"), None, false).unwrap();
        assert!(
            projection.report.skipped.is_empty(),
            "no result is orphaned"
        );

        let lines = lines(&projection.path);
        let calls = &lines[3]["tool_calls"];
        assert_eq!(calls.as_array().unwrap().len(), 2);
        assert_eq!(calls[0]["id"], "call-2-0");
        assert_eq!(calls[1]["id"], "call-2-1");
        assert_eq!(calls[1]["args"]["path"], "SECURITY.md");
        assert_eq!(lines[4]["tool_call_id"], "call-2-0");
        assert_eq!(lines[5]["tool_call_id"], "call-2-1");
        assert_eq!(lines[5]["tool_name"], "read");
    }

    #[test]
    fn a_result_with_nothing_to_say_still_names_its_call() {
        // Pi files no payload for a command that printed nothing, so the result
        // cannot be matched by digest. Dropping its linkage would leave a
        // transcript a provider rejects; guessing would be worse. Position is the
        // fact that remains.
        let (temp, mut vfs) = store();
        let mut session = session(&mut vfs);
        session.turns[2].tools[0].result = None;
        session.turns[3].text = None;

        let projection =
            to_operator(&vfs, &session, &temp.path().join("runs"), None, false).unwrap();
        assert!(projection.report.skipped.is_empty());
        let lines = lines(&projection.path);
        assert_eq!(lines[4]["tool_call_id"], "call-2-0");
        assert_eq!(lines[4]["tool_name"], "read");
        assert!(lines[4].get("content").is_none(), "there was no text");
    }

    #[test]
    fn a_dry_run_names_the_bundle_and_writes_nothing() {
        let (temp, mut vfs) = store();
        let session = session(&mut vfs);
        let out = temp.path().join("runs");

        let projection = to_operator(&vfs, &session, &out, None, true).unwrap();
        assert_eq!(projection.report.messages, 4);
        assert!(projection.report.bytes > 0);
        assert!(!projection.path.exists());
        assert!(!out.exists());
    }

    #[test]
    fn the_workspace_can_be_named_explicitly() {
        // Where a bundle is written and where it claims to have run are two
        // different facts; a resumed session captures git against the second.
        let (temp, mut vfs) = store();
        let session = session(&mut vfs);
        let projection = to_operator(
            &vfs,
            &session,
            &temp.path().join("runs"),
            Some("/elsewhere"),
            false,
        )
        .unwrap();
        assert_eq!(lines(&projection.path)[0]["workspace"], "/elsewhere");
    }

    /// A transcript document bound to a file, ready to be grown.
    fn transcript(vfs: &Vfs, temp: &tempfile::TempDir) -> (DocFile, std::path::PathBuf) {
        let path = temp.path().join("transcript.md");
        (
            DocFile::open(vfs, "transcript", "neo", &path).unwrap(),
            path,
        )
    }

    fn turn(index: u32, role: Role, text: Option<ContentId>) -> Turn {
        Turn {
            index,
            at_ms: 5_000 + i64::from(index),
            role,
            native_id: format!("e{index}"),
            parent: None,
            text,
            thinking: None,
            tools: Vec::new(),
            usage: Usage::default(),
            model: None,
            extras: None,
        }
    }

    #[test]
    fn the_appended_document_is_the_projected_file() {
        // The incremental path and the whole-record renderer must agree block for
        // block, or `session project` and `doc sync` would disagree about what the
        // transcript says.
        let (temp, mut vfs) = store();
        let session = session(&mut vfs);
        let (mut doc, path) = transcript(&vfs, &temp);

        let synced = Transcript::sync(&mut doc, &mut vfs, &session).unwrap();
        assert!(synced.inserted > 0, "a fresh document gets every block");
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            to_markdown(&vfs, &session).unwrap()
        );
    }

    #[test]
    fn a_second_sync_of_an_unchanged_session_writes_nothing() {
        let (temp, mut vfs) = store();
        let session = session(&mut vfs);
        let (mut doc, path) = transcript(&vfs, &temp);
        Transcript::sync(&mut doc, &mut vfs, &session).unwrap();
        let before = fs::read_to_string(&path).unwrap();

        let synced = Transcript::sync(&mut doc, &mut vfs, &session).unwrap();
        assert_eq!(synced.inserted, 0);
        assert_eq!(synced.updated, 0);
        assert_eq!(synced.deleted, 0);
        assert!(!synced.wrote_file);
        assert_eq!(fs::read_to_string(&path).unwrap(), before);
    }

    #[test]
    fn a_growing_session_appends_only_the_new_turn() {
        // The reason this exists: a live session must not cost a rewrite of every
        // turn it ever had. The blocks already there keep their identities, which
        // is also what keeps an annotation on an old turn attached to it.
        let (temp, mut vfs) = store();
        let mut session = session(&mut vfs);
        let (mut doc, path) = transcript(&vfs, &temp);
        Transcript::sync(&mut doc, &mut vfs, &session).unwrap();
        let before: Vec<BlockId> = doc.doc().order().iter().map(|b| b.id.clone()).collect();

        let said = vfs.put_blob(b"and now a summary").unwrap();
        session.turns.push(turn(4, Role::Assistant, Some(said)));
        session.ended_at_ms = 6_000;
        let synced = Transcript::sync(&mut doc, &mut vfs, &session).unwrap();

        assert_eq!(synced.updated, 0, "nothing earlier was rewritten");
        assert_eq!(
            synced.inserted, 2,
            "a turn is a heading block and a text block: two places, both new"
        );
        assert!(synced.wrote_file);

        let after: Vec<BlockId> = doc.doc().order().iter().map(|b| b.id.clone()).collect();
        assert_eq!(
            after[..before.len()],
            before[..],
            "every earlier block kept its identity"
        );
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            to_markdown(&vfs, &session).unwrap(),
            "the appended document is the rendered one"
        );
    }

    #[test]
    fn a_turn_that_is_still_being_written_is_updated_in_place() {
        let (temp, mut vfs) = store();
        let mut session = session(&mut vfs);
        let (mut doc, _path) = transcript(&vfs, &temp);
        Transcript::sync(&mut doc, &mut vfs, &session).unwrap();

        let later = vfs.put_blob(b"boom, and then it worked").unwrap();
        let last = session.turns.len() - 1;
        session.turns[last].text = Some(later);
        let synced = Transcript::sync(&mut doc, &mut vfs, &session).unwrap();

        assert_eq!(synced.inserted, 0, "the turn was already there");
        assert_eq!(synced.updated, 1, "its text was replaced");
        assert!(
            doc.render().contains("boom, and then it worked"),
            "{}",
            doc.render()
        );
    }
}
