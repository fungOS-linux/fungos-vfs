//! The Pi harness's sessions, read as records.
//!
//! Pi writes one JSON Lines file per session under
//! `~/.pi/agent/sessions/<encoded-project-path>/<timestamp>_<uuid>.jsonl`. Each
//! line is a typed record with its own id and a `parentId`, so a file is a
//! conversation *tree* rather than a list: the first line names the session, its cwd
//! and its id, and every later line is something that happened, in order.
//!
//! This module is an importer and nothing else: read that, write a
//! [`Session`](crate::Session). It does not know what a projector wants, and no
//! projector needs to know what a `custom` line is.
//!
//! Two habits worth keeping:
//!
//! - **Count what cannot be normalized.** `custom`, `custom_message`,
//!   `context_edit`, `compaction` and `thinking_level_change` records are real
//!   events this schema has no field for. They are counted per kind (and per
//!   `customType` where Pi gives one) and reported, so "imported everything" is a
//!   claim with a number behind it rather than an assumption.
//! - **A malformed line is data about the file, not a reason to stop.** The line is
//!   counted, the session continues, and the report says how many.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use base64::Engine as _;
use serde::Serialize;
use serde_json::{Value, json};

use crate::{ContentId, Expect, Role, Session, ToolCall, Turn, Usage, Vfs, session::blob_key};

/// What an import did.
#[derive(Debug, Default, Serialize)]
pub struct ImportReport {
    pub sessions: usize,
    /// Records written for the first time or changed.
    pub written: usize,
    pub unchanged: usize,
    pub turns: usize,
    /// Payloads this import added to the store. A second run over the same corpus
    /// reports none, because the store already holds them.
    pub blobs: usize,
    pub bytes: u64,
    /// Files an incremental run did not open, because they had not changed since
    /// the last one. The point of the number is that a timer is not a full re-read.
    pub files_skipped: usize,
    /// Record kinds this schema has no home for, counted by kind.
    pub unnormalized: BTreeMap<String, u64>,
}

/// How far an incremental import got, and which store it got there in. Keyed by
/// store because two machines can both import their own `~/.pi` into the same root
/// — a watermark shared between them would skip the other machine's history.
#[derive(serde::Deserialize, serde::Serialize)]
struct ImportState {
    store: String,
    up_to_ms: i64,
}

/// Where an importer keeps its watermark. Deliberately not a record: the session
/// listing looks for `session.json` and skips this.
const IMPORT_STATE_KEY: &str = "sessions/pi/import-state.json";

/// Import every session under a Pi sessions directory.
///
/// Idempotent: a session is keyed by the id Pi gave it, and its payloads are keyed
/// by their own digest, so a second run writes nothing and says so.
pub fn import(vfs: &mut Vfs, root: &Path, dry_run: bool) -> Result<ImportReport> {
    import_since(vfs, root, dry_run, None)
}

/// Import what changed since the last incremental run, and remember this one.
///
/// A Pi session file is append-only, so "changed" is its modification time against
/// the watermark taken when the previous run started. Files that have not moved are
/// not opened at all: importing a 686-session store in full costs minutes of I/O
/// that a timer should not spend every quarter hour.
pub fn import_incremental(vfs: &mut Vfs, root: &Path, dry_run: bool) -> Result<ImportReport> {
    let started = now_ms();
    let store = root
        .canonicalize()
        .unwrap_or_else(|_| root.to_path_buf())
        .display()
        .to_string();
    let since = vfs
        .file(IMPORT_STATE_KEY)?
        .and_then(|entry| vfs.content(&entry.content_id).ok())
        .and_then(|bytes| serde_json::from_slice::<ImportState>(&bytes).ok())
        .filter(|state| state.store == store)
        .map(|state| state.up_to_ms);

    let report = import_since(vfs, root, dry_run, since)?;
    if !dry_run {
        let state = ImportState {
            store,
            up_to_ms: started,
        };
        vfs.put(
            IMPORT_STATE_KEY,
            &serde_json::to_vec(&state)?,
            "application/json",
            Expect::Any,
        )?;
    }
    Ok(report)
}

fn import_since(
    vfs: &mut Vfs,
    root: &Path,
    dry_run: bool,
    since: Option<i64>,
) -> Result<ImportReport> {
    let mut report = ImportReport::default();
    let mut files = Vec::new();
    collect_session_files(root, &mut files)?;
    files.sort();
    // One filing set across the whole import: a system prompt repeated by every
    // session in the corpus is one payload, and the report should say so.
    let mut filing = Filing::default();

    for path in files {
        if let Some(since) = since
            && modified_ms(&path)? <= since
        {
            report.files_skipped += 1;
            continue;
        }
        match read_session(vfs, &path, dry_run, &mut filing)? {
            Some(session) => {
                report.sessions += 1;
                report.turns += session.turns.len();
                for (kind, count) in &session.unnormalized {
                    *report.unnormalized.entry(kind.clone()).or_default() += count;
                }
                if dry_run {
                    continue;
                }
                if vfs.put_session(&session)? {
                    report.written += 1;
                } else {
                    report.unchanged += 1;
                }
            }
            None => {
                *report
                    .unnormalized
                    .entry("no-session-header".to_owned())
                    .or_default() += 1;
            }
        }
    }
    report.blobs = filing.blobs;
    report.bytes = filing.bytes;
    Ok(report)
}

/// Wall clock, milliseconds.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

fn modified_ms(path: &Path) -> Result<i64> {
    let modified = fs::metadata(path)
        .with_context(|| format!("reading the time of {}", path.display()))?
        .modified()?;
    Ok(modified
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default())
}

fn collect_session_files(root: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(root).with_context(|| format!("reading {}", root.display()))? {
        let path = entry?.path();
        if path.is_dir() {
            collect_session_files(&path, out)?;
        } else if path.extension().is_some_and(|ext| ext == "jsonl") {
            out.push(path);
        }
    }
    Ok(())
}

/// One file, one session.
fn read_session(
    vfs: &mut Vfs,
    path: &Path,
    dry_run: bool,
    filing: &mut Filing,
) -> Result<Option<Session>> {
    let text = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let mut session: Option<Session> = None;
    let mut unnormalized: BTreeMap<String, u64> = BTreeMap::new();
    let mut models: Vec<String> = Vec::new();
    // A result arrives after the call it answers, so calls are remembered by Pi's
    // own call id and the answers are attached once the file has been read.
    let mut calls: BTreeMap<String, (usize, usize)> = BTreeMap::new();
    let mut results: Vec<(String, ContentId, bool)> = Vec::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(record) = serde_json::from_str::<Value>(line) else {
            *unnormalized.entry("malformed".to_owned()).or_default() += 1;
            continue;
        };
        let kind = record.get("type").and_then(Value::as_str).unwrap_or("");
        match kind {
            "session" => {
                let native_id = record
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                if native_id.is_empty() {
                    *unnormalized
                        .entry("session-without-id".to_owned())
                        .or_default() += 1;
                    continue;
                }
                let started = parse_time(record.get("timestamp")).unwrap_or_default();
                session = Some(Session {
                    harness: "pi".to_owned(),
                    native_id,
                    name: None,
                    started_at_ms: started,
                    ended_at_ms: started,
                    cwd: record
                        .get("cwd")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                    models: Vec::new(),
                    turns: Vec::new(),
                    unnormalized: BTreeMap::new(),
                    // Pi's own format version: the one fact about the file that
                    // says whether this import read it the way it was written.
                    extras: record
                        .get("version")
                        .map(|version| json!({ "piVersion": version })),
                });
            }
            "session_info" => {
                if let Some(current) = session.as_mut()
                    && let Some(name) = record.get("name").and_then(Value::as_str)
                {
                    current.name = Some(name.to_owned());
                }
            }
            "model_change" => {
                if let Some(model) = record.get("modelId").and_then(Value::as_str)
                    && !models.iter().any(|known| known == model)
                {
                    models.push(model.to_owned());
                }
            }
            "message" => {
                let Some(current) = session.as_mut() else {
                    *unnormalized
                        .entry("message-before-session".to_owned())
                        .or_default() += 1;
                    continue;
                };
                let index = current.turns.len();
                let Some(turn) = message_turn(
                    vfs,
                    &record,
                    index as u32,
                    dry_run,
                    filing,
                    &mut unnormalized,
                )?
                else {
                    *unnormalized.entry("empty-message".to_owned()).or_default() += 1;
                    continue;
                };
                if let Some((call_id, slot)) = &turn.call {
                    calls.insert(call_id.clone(), (*slot, index));
                }
                if let (Some(call_id), Some(result)) = (&turn.result_of, &turn.turn.text) {
                    results.push((call_id.clone(), result.clone(), turn.failed));
                }
                if turn.turn.at_ms > current.ended_at_ms {
                    current.ended_at_ms = turn.turn.at_ms;
                }
                if let Some(model) = &turn.turn.model
                    && !models.iter().any(|known| known == model)
                {
                    models.push(model.clone());
                }
                current.turns.push(turn.turn);
            }
            other => {
                // A `custom` or `custom_message` record carries its meaning in
                // `customType`; counting it by the bare type would merge an
                // autopilot state blob with a message shown to the user.
                let base = if other.is_empty() { "untyped" } else { other };
                let key = match record.get("customType").and_then(Value::as_str) {
                    Some(custom) => format!("{base}:{custom}"),
                    None => base.to_owned(),
                };
                *unnormalized.entry(key).or_default() += 1;
            }
        }
    }

    let Some(mut session) = session else {
        return Ok(None);
    };
    session.models = models;
    for (call_id, result, failed) in results {
        if let Some((_, index)) = calls.get(&call_id)
            && let Some(tool) = session.turns[*index].tool.as_mut()
        {
            tool.result = Some(result);
            tool.failed = failed || tool.failed;
        }
    }
    session.unnormalized = unnormalized;
    Ok(Some(session))
}

/// What a message line turned into, plus the two ids that only matter while the
/// file is being read: the tool call this turn made, and the call this result
/// answers.
struct Parsed {
    turn: Turn,
    call: Option<(String, usize)>,
    result_of: Option<String>,
    failed: bool,
}

fn message_turn(
    vfs: &mut Vfs,
    record: &Value,
    index: u32,
    dry_run: bool,
    filing: &mut Filing,
    unnormalized: &mut BTreeMap<String, u64>,
) -> Result<Option<Parsed>> {
    let message = record.get("message").cloned().unwrap_or(Value::Null);
    let role = match message.get("role").and_then(Value::as_str) {
        Some("system") => Role::System,
        Some("user") => Role::User,
        Some("assistant") => Role::Assistant,
        Some("toolResult") => Role::Tool,
        other => {
            let key = format!("role:{}", other.unwrap_or("none"));
            *unnormalized.entry(key).or_default() += 1;
            return Ok(None);
        }
    };
    let at_ms = parse_time(record.get("timestamp"))
        .or_else(|| parse_time(message.get("timestamp")))
        .unwrap_or_default();

    let mut text = String::new();
    let mut thinking = String::new();
    let mut tool: Option<ToolCall> = None;
    let mut call: Option<(String, usize)> = None;
    let mut images: Vec<Value> = Vec::new();

    // A system turn carries a plain string here and its real text in `sections`;
    // every other role carries a list of blocks.
    if let Some(plain) = message.get("content").and_then(Value::as_str) {
        push_part(&mut text, Some(plain));
    }

    for block in message
        .get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        match block.get("type").and_then(Value::as_str).unwrap_or("") {
            "text" => push_part(&mut text, block.get("text").and_then(Value::as_str)),
            "thinking" => push_part(
                &mut thinking,
                block
                    .get("thinking")
                    .or_else(|| block.get("text"))
                    .and_then(Value::as_str),
            ),
            "toolCall" => {
                let name = block.get("name").and_then(Value::as_str).unwrap_or("tool");
                let arguments = block.get("arguments").cloned().unwrap_or(Value::Null);
                let bytes = serde_json::to_vec(&arguments)?;
                let id = filing.file(vfs, &bytes, dry_run)?;
                call = block
                    .get("id")
                    .and_then(Value::as_str)
                    .map(|call_id| (call_id.to_owned(), index as usize));
                tool = Some(ToolCall {
                    name: name.to_owned(),
                    arguments: id,
                    result: None,
                    // Pi already knows the arguments never assembled. That is a
                    // failure of the call whether or not a result follows it.
                    failed: block.get("argumentsError").is_some(),
                });
            }
            "image" => match image_block(vfs, block, dry_run, filing)? {
                Some(image) => images.push(image),
                None => {
                    *unnormalized
                        .entry("image-undecodable".to_owned())
                        .or_default() += 1
                }
            },
            _ => {}
        }
    }

    // A system turn keeps its sections: for Pi that is the preamble, the tool list,
    // the rules and the docs, which together are most of what a system prompt *is*.
    if text.is_empty()
        && let Some(sections) = message.get("sections").and_then(Value::as_object)
    {
        text = join_sections(sections);
    }

    let text_id = filing.maybe(vfs, &text, dry_run)?;
    let thinking_id = filing.maybe(vfs, &thinking, dry_run)?;
    let usage = Usage {
        input: number(&message, &["usage", "input"]),
        output: number(&message, &["usage", "output"]),
        reasoning: number(&message, &["usage", "reasoning"]),
    };
    let failed = message
        .get("isError")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let result_of = message
        .get("toolCallId")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let model = message
        .get("model")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);

    // What Pi said that the turn has no field for. An error a model returned, a
    // reason a call was cut short, and an image the user pasted are all facts a
    // transcript needs and a bare text field cannot hold.
    let mut extras = serde_json::Map::new();
    if !images.is_empty() {
        extras.insert("images".to_owned(), Value::Array(images));
    }
    if let Some(error) = message.get("errorMessage").and_then(Value::as_str) {
        extras.insert("error".to_owned(), Value::from(error));
    }
    // `stop` and `toolUse` are how a turn is supposed to end. Anything else — the
    // model ran out of room, the call failed, the user interrupted it — is worth
    // keeping, and is often the only thing the turn has to say.
    if let Some(stop) = message.get("stopReason").and_then(Value::as_str)
        && !matches!(stop, "stop" | "toolUse")
    {
        extras.insert("stopReason".to_owned(), Value::from(stop));
    }
    if text.is_empty() && thinking.is_empty() && tool.is_none() && extras.is_empty() {
        return Ok(None);
    }
    let extras = if extras.is_empty() {
        None
    } else {
        Some(Value::Object(extras))
    };

    Ok(Some(Parsed {
        turn: Turn {
            index,
            at_ms,
            role,
            native_id: record
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            parent: record
                .get("parentId")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            text: text_id,
            thinking: thinking_id,
            tool,
            usage,
            model,
            extras,
        },
        call,
        result_of,
        failed,
    }))
}

/// Append one block of text, keeping blocks apart with a newline so two paragraphs
/// never run together.
fn push_part(into: &mut String, part: Option<&str>) {
    let Some(part) = part else { return };
    if part.is_empty() {
        return;
    }
    if !into.is_empty() {
        into.push('\n');
    }
    into.push_str(part);
}

/// Join a system message's sections in the order Pi declares them, with any section
/// this build has not seen before kept at the end rather than dropped.
fn join_sections(sections: &serde_json::Map<String, Value>) -> String {
    const DECLARED: [&str; 4] = ["preamble", "tools", "rules", "docs"];
    let rank = |key: &str| {
        DECLARED
            .iter()
            .position(|known| *known == key)
            .unwrap_or(DECLARED.len())
    };
    let mut keys: Vec<&str> = sections.keys().map(String::as_str).collect();
    keys.sort_by(|left, right| rank(left).cmp(&rank(right)).then_with(|| left.cmp(right)));
    keys.iter()
        .filter_map(|key| sections.get(*key).and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// File an image block's bytes and name them by digest. Base64 is Pi's encoding,
/// not the payload's, so the store keeps the bytes.
fn image_block(
    vfs: &mut Vfs,
    block: &Value,
    dry_run: bool,
    filing: &mut Filing,
) -> Result<Option<Value>> {
    let (Some(data), Some(mime)) = (
        block.get("data").and_then(Value::as_str),
        block.get("mimeType").and_then(Value::as_str),
    ) else {
        return Ok(None);
    };
    let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(data) else {
        return Ok(None);
    };
    let id = filing.file(vfs, &bytes, dry_run)?;
    Ok(Some(json!({ "mimeType": mime, "content": id.to_string() })))
}

fn number(message: &Value, path: &[&str]) -> u64 {
    let mut value = message;
    for key in path {
        match value.get(*key) {
            Some(next) => value = next,
            None => return 0,
        }
    }
    value.as_u64().unwrap_or(0)
}

/// Payloads filed by this import, counted rather than re-counted: a corpus that
/// repeats the same system prompt files it once.
#[derive(Default)]
struct Filing {
    seen: std::collections::BTreeSet<ContentId>,
    blobs: usize,
    bytes: u64,
}

impl Filing {
    fn file(&mut self, vfs: &mut Vfs, bytes: &[u8], dry_run: bool) -> Result<ContentId> {
        let id = ContentId::from_bytes(bytes);
        if !self.seen.insert(id.clone()) {
            return Ok(id);
        }
        // The store may already hold this payload from an earlier import, another
        // harness, or another session. Asking keeps the report's numbers honest:
        // a second run over the same corpus adds nothing and says so.
        if vfs.file(&blob_key(&id))?.is_none() {
            self.blobs += 1;
            self.bytes += bytes.len() as u64;
            if !dry_run {
                vfs.put_blob(bytes)?;
            }
        }
        Ok(id)
    }

    fn maybe(&mut self, vfs: &mut Vfs, text: &str, dry_run: bool) -> Result<Option<ContentId>> {
        if text.is_empty() {
            return Ok(None);
        }
        self.file(vfs, text.as_bytes(), dry_run).map(Some)
    }
}

/// A timestamp from either shape Pi uses: ISO-8601 in the header, epoch
/// milliseconds in a message.
fn parse_time(value: Option<&Value>) -> Option<i64> {
    match value? {
        Value::Number(number) => {
            let raw = number.as_i64()?;
            Some(if raw > 100_000_000_000 {
                raw
            } else {
                raw.saturating_mul(1000)
            })
        }
        Value::String(text) => iso_to_ms(text),
        _ => None,
    }
}

/// `2026-11-01T02:10:51.403Z` to milliseconds, without a date library.
fn iso_to_ms(text: &str) -> Option<i64> {
    let text = text.trim_end_matches('Z');
    let (date, time) = text.split_once('T')?;
    let mut parts = date.split('-');
    let year: i64 = parts.next()?.parse().ok()?;
    let month: i64 = parts.next()?.parse().ok()?;
    let day: i64 = parts.next()?.parse().ok()?;
    let mut clock = time.split(':');
    let hour: i64 = clock.next()?.parse().ok()?;
    let minute: i64 = clock.next()?.parse().ok()?;
    let second_text = clock.next()?;
    let (seconds, fraction) = match second_text.split_once('.') {
        Some((seconds, fraction)) => (seconds, fraction),
        None => (second_text, "0"),
    };
    let second: i64 = seconds.parse().ok()?;
    let millis: i64 = format!("{fraction:0<3}")[..3].parse().ok()?;
    let days = days_from_civil(year, month, day);
    Some(((days * 86_400 + hour * 3600 + minute * 60 + second) * 1000) + millis)
}

/// Howard Hinnant's civil-date to days-since-epoch, the inverse of the conversion
/// the R2 adapter uses.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month_position = if month > 2 { month - 3 } else { month + 9 };
    let day_of_year = (153 * month_position + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::{Expect, NodeRole};

    /// A store this test owns: the machine's real one is shared with real roots,
    /// so an import must never be pointed at it.
    fn store() -> (tempfile::TempDir, Vfs) {
        let temp = tempfile::tempdir().unwrap();
        let cas = temp.path().join("cas");
        let vfs = Vfs::open_with(temp.path().join("root"), NodeRole::Full, Some(&cas)).unwrap();
        (temp, vfs)
    }

    /// One Pi session file, in the shape Pi writes it: a project directory holding
    /// `<timestamp>_<uuid>.jsonl`.
    fn pi_file(store_root: &Path, name: &str, lines: &[&str]) -> PathBuf {
        let dir = store_root.join("--work-repo--");
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join(format!("2026-10-21T12-12-14_{name}.jsonl"));
        fs::write(&file, lines.join("\n") + "\n").unwrap();
        file
    }

    /// A session with one of every record kind this importer handles.
    fn session_lines() -> Vec<&'static str> {
        vec![
            r#"{"type":"session","version":3,"id":"s-1","timestamp":"2026-10-21T12:12:14.980Z","cwd":"/work/repo"}"#,
            r#"{"type":"session_info","id":"e1","parentId":null,"timestamp":"2026-10-21T12:12:14.981Z","name":"prove-it"}"#,
            r#"{"type":"model_change","id":"e2","parentId":"e1","timestamp":"2026-10-21T12:12:15.000Z","provider":"bifrost","modelId":"fpl/coder"}"#,
            r#"{"type":"message","id":"e3","parentId":"e2","timestamp":"2026-10-21T12:12:15.100Z","message":{"role":"system","content":"","sections":{"preamble":"You are pi.","rules":"<rules>be brief</rules>","tools":"<tools>(none)</tools>"}}}"#,
            r#"{"type":"message","id":"e4","parentId":"e3","timestamp":"2026-10-21T12:12:16.000Z","message":{"role":"user","content":[{"type":"text","text":"look at this"},{"type":"image","data":"aGVsbG8=","mimeType":"image/png"}]}}"#,
            r#"{"type":"message","id":"e5","parentId":"e4","timestamp":"2026-10-21T12:12:17.000Z","message":{"role":"assistant","content":[{"type":"thinking","thinking":"they want a look"},{"type":"toolCall","id":"call-1","name":"read","arguments":{"path":"README.md"}}],"model":"fpl/coder","usage":{"input":10,"output":20,"reasoning":5}}}"#,
            r#"{"type":"message","id":"e6","parentId":"e5","timestamp":"2026-10-21T12:12:18.000Z","message":{"role":"toolResult","toolCallId":"call-1","toolName":"read","content":[{"type":"text","text":"boom"}],"isError":true}}"#,
            r#"{"type":"custom","id":"e7","parentId":"e6","timestamp":"2026-10-21T12:12:19.000Z","customType":"pi.virtual-model-state","data":{}}"#,
        ]
    }

    #[test]
    fn imports_a_pi_session_as_one_record() {
        let (temp, mut vfs) = store();
        pi_file(temp.path(), "s-1", &session_lines());

        let report = import(&mut vfs, temp.path(), false).unwrap();
        assert_eq!(report.sessions, 1);
        assert_eq!(report.written, 1);
        assert_eq!(report.turns, 4, "system, user, assistant, tool result");
        assert_eq!(
            report.unnormalized.get("custom:pi.virtual-model-state"),
            Some(&1)
        );

        let session = vfs.session("pi", "s-1").unwrap().unwrap();
        assert_eq!(session.name.as_deref(), Some("prove-it"));
        assert_eq!(session.cwd.as_deref(), Some("/work/repo"));
        assert_eq!(session.models, vec!["fpl/coder".to_owned()]);
        assert!(session.ended_at_ms > session.started_at_ms);
        // Pi's own format version, carried because this schema has no field for it.
        assert_eq!(session.extras.unwrap()["piVersion"], 3);

        // A system prompt is split across sections; joining them keeps all of it,
        // in Pi's order rather than the map's.
        assert_eq!(
            vfs.turn_text(&session.turns[0]).unwrap().unwrap(),
            "You are pi.\n\n<tools>(none)</tools>\n\n<rules>be brief</rules>"
        );

        let user = &session.turns[1];
        assert_eq!(vfs.turn_text(user).unwrap().unwrap(), "look at this");
        let image = &user.extras.as_ref().unwrap()["images"][0];
        assert_eq!(image["mimeType"], "image/png");
        let image_id = ContentId::parse(image["content"].as_str().unwrap()).unwrap();
        assert_eq!(vfs.content(&image_id).unwrap(), b"hello");

        let assistant = &session.turns[2];
        assert_eq!(
            vfs.content(assistant.thinking.as_ref().unwrap()).unwrap(),
            b"they want a look"
        );
        assert_eq!(assistant.model.as_deref(), Some("fpl/coder"));
        assert_eq!(assistant.usage.input, 10);
        assert_eq!(assistant.usage.reasoning, 5);
        let tool = assistant.tool.as_ref().unwrap();
        assert_eq!(tool.name, "read");
        assert_eq!(
            vfs.content(&tool.arguments).unwrap(),
            br#"{"path":"README.md"}"#
        );
        // The result arrived on a later line and was attached to the call it
        // answers, along with the failure Pi recorded.
        assert_eq!(
            vfs.content(tool.result.as_ref().unwrap()).unwrap(),
            b"boom",
            "the result is filed once, by digest"
        );
        assert!(tool.failed);

        assert_eq!(session.turns[3].role, Role::Tool);
        assert!(session.turns[3].tool.is_none());
        assert_eq!(session.turns[3].parent.as_deref(), Some("e5"));
    }

    #[test]
    fn a_second_import_writes_nothing() {
        let (temp, mut vfs) = store();
        pi_file(temp.path(), "s-1", &session_lines());
        let first = import(&mut vfs, temp.path(), false).unwrap();
        assert!(first.blobs > 0, "the first pass has work to do");

        let second = import(&mut vfs, temp.path(), false).unwrap();
        assert_eq!(second.sessions, 1);
        assert_eq!(second.written, 0);
        assert_eq!(second.unchanged, 1);
        assert_eq!(second.blobs, 0, "the store already held every payload");
        assert_eq!(second.bytes, 0);
    }

    #[test]
    fn a_dry_run_writes_nothing_and_still_counts() {
        let (temp, mut vfs) = store();
        pi_file(temp.path(), "s-1", &session_lines());

        let report = import(&mut vfs, temp.path(), true).unwrap();
        assert_eq!(report.sessions, 1);
        assert_eq!(report.turns, 4);
        assert!(report.blobs > 0);
        assert!(
            vfs.file(&crate::session_record_key("pi", "s-1"))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn an_incremental_import_skips_a_file_that_did_not_change() {
        let (temp, mut vfs) = store();
        pi_file(temp.path(), "s-1", &session_lines());

        let first = import_incremental(&mut vfs, temp.path(), false).unwrap();
        assert_eq!(first.sessions, 1);
        assert_eq!(first.files_skipped, 0);

        let second = import_incremental(&mut vfs, temp.path(), false).unwrap();
        assert_eq!(second.sessions, 0, "nothing was opened");
        assert_eq!(second.files_skipped, 1);
        assert_eq!(vfs.sessions(Some("pi")).unwrap().len(), 1);
    }

    #[test]
    fn an_incremental_import_picks_up_a_session_that_grew() {
        let (temp, mut vfs) = store();
        let file = pi_file(temp.path(), "s-1", &session_lines());
        import_incremental(&mut vfs, temp.path(), false).unwrap();
        assert_eq!(vfs.session("pi", "s-1").unwrap().unwrap().turns.len(), 4);

        // Pi appends to a session file as the conversation continues.
        let mut text = fs::read_to_string(&file).unwrap();
        text.push_str(
            r#"{"type":"message","id":"e8","parentId":"e7","timestamp":"2026-10-21T12:12:20.000Z","message":{"role":"assistant","content":[{"type":"text","text":"done"}],"model":"fpl/coder"}}"#,
        );
        text.push('\n');
        fs::write(&file, text).unwrap();

        let next = import_incremental(&mut vfs, temp.path(), false).unwrap();
        assert_eq!(next.sessions, 1, "the file moved, so it was read again");
        assert_eq!(next.written, 1);
        assert_eq!(vfs.session("pi", "s-1").unwrap().unwrap().turns.len(), 5);
    }

    #[test]
    fn the_watermark_belongs_to_one_store() {
        // Two machines importing their own `~/.pi` into one root must not skip each
        // other's history because one of them got there first.
        let (temp, mut vfs) = store();
        let neo = temp.path().join("neo");
        let studio = temp.path().join("studio");
        fs::create_dir_all(&neo).unwrap();
        fs::create_dir_all(&studio).unwrap();
        pi_file(&neo, "a", &session_lines());
        pi_file(&studio, "b", &session_lines());

        assert_eq!(
            import_incremental(&mut vfs, &neo, false).unwrap().sessions,
            1
        );
        let other = import_incremental(&mut vfs, &studio, false).unwrap();
        assert_eq!(other.sessions, 1, "a different store reads in full");
        assert_eq!(other.files_skipped, 0);
    }

    #[test]
    fn a_dry_run_leaves_the_watermark_alone() {
        // A dry run must not claim to have imported anything, or the next real run
        // would skip the files the dry run only looked at.
        let (temp, mut vfs) = store();
        pi_file(temp.path(), "s-1", &session_lines());
        import_incremental(&mut vfs, temp.path(), true).unwrap();
        let real = import_incremental(&mut vfs, temp.path(), false).unwrap();
        assert_eq!(real.sessions, 1);
        assert_eq!(real.files_skipped, 0);
    }

    #[test]
    fn a_malformed_line_is_counted_and_the_session_survives() {
        let (temp, mut vfs) = store();
        let mut lines = session_lines();
        lines.insert(4, "this is not json");
        pi_file(temp.path(), "s-1", &lines);

        let report = import(&mut vfs, temp.path(), false).unwrap();
        assert_eq!(report.sessions, 1);
        assert_eq!(report.unnormalized.get("malformed"), Some(&1));
        assert_eq!(vfs.session("pi", "s-1").unwrap().unwrap().turns.len(), 4);
    }

    #[test]
    fn a_file_without_a_session_header_is_named_rather_than_skipped() {
        let (temp, mut vfs) = store();
        pi_file(
            temp.path(),
            "orphan",
            &[
                r#"{"type":"message","id":"e1","timestamp":"2026-10-21T12:12:16.000Z","message":{"role":"user","content":[{"type":"text","text":"hello"}]}}"#,
            ],
        );

        let report = import(&mut vfs, temp.path(), false).unwrap();
        assert_eq!(report.sessions, 0);
        assert_eq!(report.unnormalized.get("no-session-header"), Some(&1));
    }

    #[test]
    fn a_turn_with_an_unnormalized_role_is_counted() {
        let (temp, mut vfs) = store();
        let mut lines = vec![
            r#"{"type":"session","version":3,"id":"s-2","timestamp":"2026-10-21T12:12:14.980Z","cwd":"/work/repo"}"#,
        ];
        lines.push(
            r#"{"type":"message","id":"e1","timestamp":"2026-10-21T12:12:16.000Z","message":{"role":"narrator","content":"meanwhile"}}"#,
        );
        pi_file(temp.path(), "s-2", &lines);

        let report = import(&mut vfs, temp.path(), false).unwrap();
        assert_eq!(report.unnormalized.get("role:narrator"), Some(&1));
        assert_eq!(vfs.session("pi", "s-2").unwrap().unwrap().turns.len(), 0);
    }

    #[test]
    fn a_failed_model_call_is_a_turn_even_with_nothing_to_say() {
        let (temp, mut vfs) = store();
        let lines = vec![
            r#"{"type":"session","version":3,"id":"s-3","timestamp":"2026-10-21T12:12:14.980Z","cwd":"/work/repo"}"#,
            r#"{"type":"message","id":"e1","parentId":null,"timestamp":"2026-10-21T12:12:16.000Z","message":{"role":"assistant","content":[],"model":"fpl/coder","stopReason":"error","errorMessage":"429: Go usage limit exceeded","usage":{"input":0,"output":0}}}"#,
        ];
        pi_file(temp.path(), "s-3", &lines);

        let report = import(&mut vfs, temp.path(), false).unwrap();
        assert_eq!(report.turns, 1, "a call that failed is still an event");
        assert_eq!(report.unnormalized.get("empty-message"), None);

        let session = vfs.session("pi", "s-3").unwrap().unwrap();
        let turn = &session.turns[0];
        assert!(turn.text.is_none());
        let extras = turn.extras.as_ref().unwrap();
        assert_eq!(extras["stopReason"], "error");
        assert_eq!(extras["error"], "429: Go usage limit exceeded");
    }

    #[test]
    fn an_iso_timestamp_becomes_epoch_milliseconds() {
        // 2026-10-21T12:12:14.980Z, the header of the sample session.
        assert_eq!(
            iso_to_ms("2026-10-21T12:12:14.980Z"),
            Some(1_792_584_734_980)
        );
    }

    #[test]
    fn a_copy_of_a_session_is_one_record_not_two() {
        // The same conversation saved twice — Pi copying a file, a second machine's
        // store, a backup — is one session, because the key is the id Pi gave it.
        let (temp, mut vfs) = store();
        pi_file(temp.path(), "s-1", &session_lines());
        let other = temp.path().join("--elsewhere--");
        fs::create_dir_all(&other).unwrap();
        fs::write(
            other.join("2026-10-22T00-00-00_s-1.jsonl"),
            session_lines().join("\n") + "\n",
        )
        .unwrap();

        let report = import(&mut vfs, temp.path(), false).unwrap();
        assert_eq!(report.sessions, 2, "two files read");
        assert_eq!(report.written, 1, "one record written");
        assert_eq!(report.unchanged, 1);
        assert_eq!(vfs.sessions(Some("pi")).unwrap().len(), 1);
    }

    #[test]
    fn putting_a_session_twice_says_whether_it_changed() {
        let (temp, mut vfs) = store();
        pi_file(temp.path(), "s-1", &session_lines());
        import(&mut vfs, temp.path(), false).unwrap();
        let session = vfs.session("pi", "s-1").unwrap().unwrap();

        let key = crate::session_record_key("pi", "s-1");
        let version = vfs.file(&key).unwrap().unwrap().version;
        assert!(
            !vfs.put_session(&session).unwrap(),
            "identical, so unchanged"
        );
        assert_eq!(
            vfs.file(&key).unwrap().unwrap().version,
            version,
            "an unchanged record is not rewritten, so it gains no version"
        );
        let listed = vfs.sessions(None).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].turns, 4);
        assert!(listed[0].bytes > 0);
        // The namespace is addressed by harness, so a filtered list finds it and a
        // list of another harness does not.
        assert_eq!(vfs.sessions(Some("pi")).unwrap().len(), 1);
        assert!(vfs.sessions(Some("delta")).unwrap().is_empty());
    }

    #[test]
    fn a_blob_is_stored_once_however_many_turns_share_it() {
        let (_temp, mut vfs) = store();
        let repeated = "the same words in two places";
        let first = vfs.put_blob(repeated.as_bytes()).unwrap();
        let second = vfs.put_blob(repeated.as_bytes()).unwrap();
        assert_eq!(first, second);
        assert_eq!(vfs.content(&first).unwrap(), repeated.as_bytes());
        assert!(vfs.file(&blob_key(&first)).unwrap().is_some());
        // Filing is a plain put: a cache root cannot be written, so this is not a
        // cache entry and cannot be collected as one.
        let entry = vfs.file(&blob_key(&first)).unwrap().unwrap();
        assert_eq!(
            vfs.put(
                &blob_key(&first),
                b"other",
                "application/octet-stream",
                Expect::Any
            )
            .unwrap()
            .version,
            entry.version + 1
        );
    }
}
