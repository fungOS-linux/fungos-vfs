//! A `.docx` read as a document, not as a blob.
//!
//! A Word file is a zip of XML parts. The part that matters is
//! `word/document.xml`: a body of paragraphs (`w:p`), each holding runs (`w:r`) of
//! text (`w:t`), and each optionally carrying a style (`w:pStyle`) and an
//! identity (`w14:paraId`).
//!
//! Three things about that decide the design here:
//!
//! - **Word already numbers its paragraphs.** `w14:paraId` is a stable 32-bit id
//!   that survives editing, which is the same thing [`BlockId`] is for. Adopting
//!   it means re-reading an edited document is an identity match rather than a
//!   text diff, so a paragraph keeps its thread, its ordering and its stamp when
//!   everything around it changes. A paragraph without one falls back to its
//!   position, and the report says how many did.
//! - **Every save rewrites the zip.** Byte equality can never be the "did this
//!   change" signal for an office file, and the archive even stamps every entry
//!   with the same 1980 date. So the comparison is between two block lists.
//! - **Most of the file is not modelled, and that is the point.** Runs with bold
//!   and italic, tables, numbering, fields, drawings, footnotes, `customXml`, the
//!   theme: the reader counts what it saw and did not understand and leaves the
//!   part alone. Rendering this back to `.docx` would be a writer for a format we
//!   are not writing; the file on disk stays the authority for what we did not
//!   read.
//!
//! The seam is [`Docx::ops`], which turns a read into the same ops a markdown
//! edit produces, so a Word document and a text file are the same kind of thing
//! to the merge layer.

use std::{collections::BTreeMap, io::Cursor};

use anyhow::{Context, Result};
use quick_xml::{Reader, escape::unescape, events::Event};

use crate::{BlockId, Doc, Op};

/// The part the reader models. Everything else in the archive is left alone.
const BODY: &str = "word/document.xml";

/// A container that looks like an office file, by its first local file header.
#[must_use]
pub fn is_docx(bytes: &[u8]) -> bool {
    bytes.starts_with(b"PK\x03\x04")
}

/// One paragraph, as the document layer sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Paragraph {
    /// The identity this paragraph is known by, from `w14:paraId` when Word wrote
    /// one.
    pub id: BlockId,
    /// `w:pStyle`, when the paragraph names one.
    pub style: Option<String>,
    pub text: String,
}

/// What reading one file found and what it ignored.
#[derive(Debug, Default)]
pub struct ReadReport {
    pub paragraphs: usize,
    /// Paragraphs identified by `w14:paraId`.
    pub identified: usize,
    /// Paragraphs identified by position because the file gave no id.
    pub synthetic: usize,
    /// Elements seen and not modelled, counted by name.
    pub unmodeled: BTreeMap<String, usize>,
    /// Every part in the archive.
    pub parts: Vec<String>,
}

/// A `.docx` as a list of paragraphs.
#[derive(Debug)]
pub struct Docx {
    /// The document this was read as. Every identity this file mints is scoped to
    /// it, so two files never collide in one root.
    pub id: String,
    pub paragraphs: Vec<Paragraph>,
    pub report: ReadReport,
}

/// What turning a read into ops did.
#[derive(Debug, Default)]
pub struct ImportReport {
    pub inserted: usize,
    pub updated: usize,
    pub deleted: usize,
    /// Paragraphs that are present but whose place in the document moved. The op
    /// vocabulary has no "move", so this is reported rather than faked.
    pub reordered: usize,
}

impl Docx {
    /// Read a `.docx` as the document `id`.
    pub fn read(bytes: &[u8], id: &str) -> Result<Self> {
        let mut archive =
            zip::ZipArchive::new(Cursor::new(bytes)).context("reading the docx container")?;
        let mut report = ReadReport::default();
        for index in 0..archive.len() {
            if let Ok(entry) = archive.by_index(index) {
                report.parts.push(entry.name().to_owned());
            }
        }
        let mut body = String::new();
        {
            use std::io::Read as _;
            let mut part = archive
                .by_name(BODY)
                .with_context(|| format!("this container has no {BODY}"))?;
            part.read_to_string(&mut body)
                .with_context(|| format!("reading {BODY}"))?;
        }
        let mut paragraphs = parse(&body, id, &mut report)?;
        // The archive stores entries in whatever order it likes; the document's
        // order is the order they appear in the body.
        paragraphs.shrink_to_fit();
        report.paragraphs = paragraphs.len();
        Ok(Self {
            id: id.to_owned(),
            paragraphs,
            report,
        })
    }

    /// The paragraph list as the markdown view of this document.
    ///
    /// Enough to read and to hand to a person: a heading from the style Word
    /// named, one block per paragraph. It is not a fidelity renderer, and the
    /// parts this reader did not model are not in it.
    ///
    /// A block carries no style yet, so the heading level is written into the
    /// block's text. That is the one place this reader is lossy for a reason that
    /// will stop being true: when a block grows a style, the prefix comes out and
    /// the markdown view renders it instead.
    #[must_use]
    pub fn markdown(&self) -> String {
        let mut out = String::new();
        for paragraph in &self.paragraphs {
            let text = block_text(paragraph);
            if text.is_empty() {
                continue;
            }
            if !out.is_empty() {
                out.push_str("\n\n");
            }
            out.push_str(&text);
        }
        if !out.is_empty() {
            out.push('\n');
        }
        out
    }

    /// The ops that make `doc` say what this file says.
    ///
    /// Matched by identity, so an edit moves nothing and a paragraph keeps its
    /// thread. `replica` names whoever is importing, and is only used for the
    /// stamps on the writes — the identities belong to the file.
    #[must_use]
    pub fn ops(&self, doc: &Doc, replica: &str) -> (Vec<Op>, ImportReport) {
        let mut report = ImportReport::default();
        let mut ops = Vec::new();
        let mut next = doc
            .order()
            .iter()
            .map(|block| block.id.counter)
            .max()
            .unwrap_or(0)
            + 1;
        let mut stamp = || {
            let id = BlockId::new(next, replica);
            next += 1;
            id
        };

        let mut previous: Option<BlockId> = None;
        // A paragraph with no text is spacing, not content: it carries no block,
        // and one that went blank in this save is a block that must go.
        let mut present: std::collections::BTreeSet<&BlockId> = std::collections::BTreeSet::new();
        for paragraph in &self.paragraphs {
            let text = block_text(paragraph);
            if text.is_empty() {
                continue;
            }
            present.insert(&paragraph.id);
            match doc.block(&paragraph.id) {
                Some(block) if !block.deleted => {
                    if block.text != text {
                        ops.push(Op::Update {
                            id: paragraph.id.clone(),
                            text,
                            at: stamp(),
                        });
                        report.updated += 1;
                    }
                    if block.after != previous {
                        report.reordered += 1;
                    }
                }
                _ => {
                    ops.push(Op::Insert {
                        id: paragraph.id.clone(),
                        after: previous.clone(),
                        text,
                    });
                    report.inserted += 1;
                }
            }
            previous = Some(paragraph.id.clone());
        }

        // A paragraph this file used to have and no longer does is deleted, not
        // forgotten: the tombstone is what keeps an annotation on it resolvable.
        let ours = [
            format!("docx:{}", self.id),
            format!("docx:{}#position", self.id),
        ];
        for block in doc.order() {
            if block.deleted || !ours.contains(&block.id.actor) {
                continue;
            }
            if !present.contains(&block.id) {
                ops.push(Op::Delete {
                    id: block.id.clone(),
                    at: stamp(),
                });
                report.deleted += 1;
            }
        }

        (ops, report)
    }
}

/// The heading level a Word style name implies, if any.
fn heading_level(style: Option<&str>) -> Option<usize> {
    let style = style?;
    if style.eq_ignore_ascii_case("title") {
        return Some(1);
    }
    let level = style.strip_prefix("Heading")?;
    Some(level.parse::<usize>().ok()?.clamp(1, 6))
}

/// One paragraph as a block's text.
fn block_text(paragraph: &Paragraph) -> String {
    let text = paragraph.text.trim();
    if text.is_empty() {
        return String::new();
    }
    match heading_level(paragraph.style.as_deref()) {
        Some(level) => format!("{} {text}", "#".repeat(level)),
        None => text.to_owned(),
    }
}

/// The paragraphs of a body, in order.
fn parse(xml: &str, id: &str, report: &mut ReadReport) -> Result<Vec<Paragraph>> {
    let mut reader = Reader::from_str(xml);
    let mut paragraphs: Vec<Paragraph> = Vec::new();
    // The paragraph being read, how deep in `<w:p>` elements we are, and whether
    // what arrives next is the document's words. A text box can hold a paragraph
    // inside a paragraph, and whitespace between elements is not prose — the
    // `w:t` element is what decides both.
    let mut current: Option<Paragraph> = None;
    let mut depth = 0usize;
    let mut in_text = false;

    loop {
        let event = reader.read_event().context("reading word/document.xml")?;
        match event {
            Event::Eof => break,
            Event::Start(element) | Event::Empty(element) => {
                let name = element.name();
                let name = name.as_ref();
                let local = element.local_name();
                let local: &str = local.as_ref();
                let _ = local;
                if name == "w:p" {
                    if depth > 0 {
                        count(report, "nested paragraph");
                    } else {
                        let identified = attribute(&element, "paraId")?.is_some();
                        if identified {
                            report.identified += 1;
                        } else {
                            report.synthetic += 1;
                        }
                        current = Some(new_paragraph(&element, id, paragraphs.len())?);
                    }
                    depth += 1;
                    continue;
                }
                if name == "w:pStyle" {
                    if let Some(paragraph) = current.as_mut() {
                        paragraph.style = attribute(&element, "val")?;
                    }
                    continue;
                }
                if name == "w:t" {
                    in_text = depth > 0;
                    continue;
                }
                if name == "w:tab" && depth > 0 {
                    if let Some(paragraph) = current.as_mut() {
                        paragraph.text.push('\t');
                    }
                    continue;
                }
                if (name == "w:br" || name == "w:cr") && depth > 0 {
                    if let Some(paragraph) = current.as_mut() {
                        paragraph.text.push('\n');
                    }
                    continue;
                }
                if is_unmodeled(name) {
                    count(report, local_name(name));
                }
            }
            Event::Text(text) if in_text => {
                if let (Ok(decoded), Some(paragraph)) = (unescape(text.as_ref()), current.as_mut())
                {
                    paragraph.text.push_str(&decoded);
                }
            }
            Event::End(element) => match element.name().as_ref() {
                "w:t" => in_text = false,
                "w:p" => {
                    depth = depth.saturating_sub(1);
                    if depth == 0
                        && let Some(paragraph) = current.take()
                    {
                        paragraphs.push(paragraph);
                    }
                }
                _ => {}
            },
            _ => {}
        }
    }

    if paragraphs.is_empty() {
        anyhow::bail!(
            "no paragraphs in {BODY}: this reader only understands the `w:` prefix \
             that Word and LibreOffice write"
        );
    }
    Ok(paragraphs)
}

fn new_paragraph(
    element: &quick_xml::events::BytesStart<'_>,
    id: &str,
    index: usize,
) -> Result<Paragraph> {
    let identity = match attribute(element, "paraId")? {
        Some(hex) => {
            let counter = u64::from_str_radix(hex.trim(), 16)
                .with_context(|| format!("{hex} is not a paragraph id"))?;
            BlockId::new(counter, &format!("docx:{id}"))
        }
        // No id: position is all this file offers, and the report says so. An
        // edited paragraph in such a file looks like a different paragraph.
        None => BlockId::new(index as u64, &format!("docx:{id}#position")),
    };
    Ok(Paragraph {
        id: identity,
        style: None,
        text: String::new(),
    })
}

fn attribute(element: &quick_xml::events::BytesStart<'_>, name: &str) -> Result<Option<String>> {
    for attribute in element.attributes() {
        let attribute = attribute.context("reading an attribute")?;
        if attribute.key.local_name().as_ref() == name {
            let value = attribute
                .normalized_value(quick_xml::XmlVersion::Implicit1_0)
                .context("decoding an attribute")?;
            return Ok(Some(value.into_owned()));
        }
    }
    Ok(None)
}

fn count(report: &mut ReadReport, name: &str) {
    *report.unmodeled.entry(name.to_owned()).or_insert(0) += 1;
}

fn local_name(name: &str) -> &str {
    name.split_once(':').map_or(name, |(_, local)| local)
}

/// Elements that hold content this reader does not carry.
fn is_unmodeled(name: &str) -> bool {
    matches!(
        name,
        "w:tbl"
            | "w:drawing"
            | "w:pict"
            | "w:fldSimple"
            | "w:instrText"
            | "w:footnoteReference"
            | "w:endnoteReference"
            | "w:commentReference"
            | "w:sdt"
            | "w:hyperlink"
            | "w:smartTag"
            | "w:txbxContent"
    )
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;
    use crate::Doc;

    /// The body of a document, wrapped in the parts a reader needs.
    const HEAD: &str = concat!(
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>"#,
        r#"<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main""#,
        r#" xmlns:w14="http://schemas.microsoft.com/office/word/2010/wordml"><w:body>"#
    );
    const TAIL: &str = "</w:body></w:document>";

    /// A paragraph with an id, an optional style, and text.
    fn paragraph(id: &str, style: Option<&str>, text: &str) -> String {
        let style = style.map_or_else(String::new, |style| {
            format!(r#"<w:pPr><w:pStyle w:val="{style}"/></w:pPr>"#)
        });
        format!(
            r#"<w:p w14:paraId="{id}">{style}<w:r><w:t xml:space="preserve">{text}</w:t></w:r></w:p>"#
        )
    }

    /// A `.docx` holding `body`, with the parts Word always writes.
    fn docx(body: &str) -> Vec<u8> {
        let mut out = Vec::new();
        {
            let mut zip = zip::ZipWriter::new(Cursor::new(&mut out));
            let options = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Deflated);
            for (name, content) in [
                ("[Content_Types].xml", "<Types/>".to_owned()),
                ("word/document.xml", format!("{HEAD}{body}{TAIL}")),
                ("word/styles.xml", "<styles/>".to_owned()),
            ] {
                zip.start_file(name, options).unwrap();
                zip.write_all(content.as_bytes()).unwrap();
            }
            zip.finish().unwrap();
        }
        out
    }

    fn body() -> String {
        [
            paragraph("1A2B3C4D", Some("Heading1"), "Mutual agreement"),
            paragraph("5E6F7A8B", None, "The parties agree as follows."),
            paragraph("9C0D1E2F", None, "Neither party shall disclose it."),
        ]
        .concat()
    }

    #[test]
    fn paragraphs_become_blocks_by_their_own_ids() {
        let docx = Docx::read(&docx(&body()), "nda").unwrap();
        assert_eq!(docx.report.paragraphs, 3);
        assert_eq!(docx.report.identified, 3);
        assert_eq!(docx.report.synthetic, 0);
        assert_eq!(docx.paragraphs[1].text, "The parties agree as follows.");
        // Word's id is the block's counter, scoped to the document: re-reading an
        // edited file matches paragraphs instead of diffing them.
        assert_eq!(docx.paragraphs[0].id, BlockId::new(0x1A2B3C4D, "docx:nda"));
        assert_eq!(docx.paragraphs[0].style.as_deref(), Some("Heading1"));
        assert!(
            docx.report
                .parts
                .iter()
                .any(|part| part == "word/styles.xml")
        );
        assert_eq!(
            docx.markdown(),
            "# Mutual agreement\n\nThe parties agree as follows.\n\nNeither party shall disclose it.\n"
        );
    }

    #[test]
    fn importing_the_same_file_twice_writes_nothing() {
        // Byte equality is useless for an office file — every save rewrites the
        // archive — so the comparison is between block lists.
        let bytes = docx(&body());
        let docx = Docx::read(&bytes, "nda").unwrap();
        let mut doc = Doc::new("neo");
        let (ops, first) = docx.ops(&doc, "neo");
        assert_eq!(first.inserted, 3);
        doc.apply_all(&ops);

        let again = Docx::read(&bytes, "nda").unwrap();
        let (ops, second) = again.ops(&doc, "neo");
        assert!(
            ops.is_empty(),
            "{} ops from a file that did not change",
            ops.len()
        );
        assert_eq!(second.updated, 0);
    }

    #[test]
    fn an_edited_paragraph_is_updated_and_its_neighbours_keep_their_identity() {
        let mut doc = Doc::new("neo");
        let (ops, _) = Docx::read(&docx(&body()), "nda").unwrap().ops(&doc, "neo");
        doc.apply_all(&ops);
        let before: Vec<BlockId> = doc.live().iter().map(|b| b.id.clone()).collect();

        let edited = body().replace("The parties agree", "Both parties agree");
        let (ops, report) = Docx::read(&docx(&edited), "nda").unwrap().ops(&doc, "neo");
        assert_eq!(report.inserted, 0, "nothing was added");
        assert_eq!(report.updated, 1, "one paragraph was rewritten");
        assert_eq!(report.deleted, 0);
        assert_eq!(report.reordered, 0, "and nothing moved");
        doc.apply_all(&ops);

        let after: Vec<BlockId> = doc.live().iter().map(|b| b.id.clone()).collect();
        assert_eq!(
            before, after,
            "the same three paragraphs, in the same places"
        );
        assert!(doc.live()[1].text.starts_with("Both parties"));
    }

    #[test]
    fn a_removed_paragraph_leaves_a_tombstone() {
        // Deleted, not forgotten: the tombstone is what keeps a thread anchored to
        // that paragraph resolvable.
        let mut doc = Doc::new("neo");
        let (ops, _) = Docx::read(&docx(&body()), "nda").unwrap().ops(&doc, "neo");
        doc.apply_all(&ops);

        let shorter = [
            paragraph("1A2B3C4D", Some("Heading1"), "Mutual agreement"),
            paragraph("9C0D1E2F", None, "Neither party shall disclose it."),
        ]
        .concat();
        let (ops, report) = Docx::read(&docx(&shorter), "nda").unwrap().ops(&doc, "neo");
        assert_eq!(report.deleted, 1);
        doc.apply_all(&ops);
        assert_eq!(doc.live().len(), 2);
        assert!(
            doc.block(&BlockId::new(0x5E6F7A8B, "docx:nda"))
                .is_some_and(|block| block.deleted)
        );
    }

    #[test]
    fn a_paragraph_without_an_id_falls_back_to_its_position_and_says_so() {
        let anonymous = r#"<w:p><w:r><w:t>no identity</w:t></w:r></w:p>"#;
        let docx = Docx::read(&docx(anonymous), "notes").unwrap();
        assert_eq!(docx.report.identified, 0);
        assert_eq!(docx.report.synthetic, 1);
        assert_eq!(
            docx.paragraphs[0].id,
            BlockId::new(0, "docx:notes#position")
        );
        assert_eq!(docx.paragraphs[0].text, "no identity");
    }

    #[test]
    fn what_the_reader_does_not_model_is_counted() {
        // A reader that quietly drops a table is a reader that loses a contract.
        let with_table = format!(
            "{}<w:tbl><w:tr><w:tc><w:p><w:r><w:t>cell</w:t></w:r></w:p></w:tc></w:tr></w:tbl>{}",
            paragraph("1A2B3C4D", None, "before"),
            r#"<w:p w14:paraId="2B3C4D5E"><w:r><w:drawing/></w:r></w:p>"#
        );
        let docx = Docx::read(&docx(&with_table), "notes").unwrap();
        assert_eq!(docx.report.unmodeled.get("tbl").copied(), Some(1));
        assert_eq!(docx.report.unmodeled.get("drawing").copied(), Some(1));
        // The cell's paragraph is read as a paragraph of the body: the text
        // survives, the table around it does not, and the count says so.
        assert_eq!(docx.report.paragraphs, 3);
        assert!(docx.paragraphs.iter().any(|p| p.text == "cell"));
    }

    #[test]
    fn something_that_is_not_a_docx_is_an_error_not_a_panic() {
        let error = Docx::read(b"not a zip at all", "notes").unwrap_err();
        assert!(
            error.to_string().contains("container"),
            "the message should say what failed: {error}"
        );
    }

    #[test]
    fn a_body_with_no_word_prefix_is_refused_by_name() {
        let xml = r#"<document xmlns="urn:other"><body><p>text</p></body></document>"#;
        let mut out = Vec::new();
        {
            let mut zip = zip::ZipWriter::new(Cursor::new(&mut out));
            let options = zip::write::SimpleFileOptions::default();
            zip.start_file("word/document.xml", options).unwrap();
            zip.write_all(xml.as_bytes()).unwrap();
            zip.finish().unwrap();
        }
        let error = Docx::read(&out, "notes").unwrap_err();
        assert!(error.to_string().contains("w:"), "{error}");
    }
}
