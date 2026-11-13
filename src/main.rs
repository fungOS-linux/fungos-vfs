use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write,
    path::PathBuf,
};

use clap::{Parser, Subcommand};
use fungos_vfs::{
    Anchor, ApplyOutcome, BlockId, CheckoutOptions, CommentFile, ContentId, Doc, DocFile, Docx,
    Expect, Hydration, NodeRole, PathPolicy, RecordOptions, ReplicaConfig, ReplicaSpec, SourceMode,
    SyncServer, Transcript, Vfs, attach_project, checkout, export_dir, import_pi,
    import_pi_incremental, ingest_dir_with_policy, list_projects, load_doc, materialize,
    migrate_storage_with_cas, pull_over_quic, pull_over_ssh, record, to_markdown, to_operator,
    worktree_changes, worktrees,
};

#[derive(Parser)]
#[command(version, about = "Local-first content-addressed VFS for fungOS")]
struct Cli {
    #[arg(long, global = true, default_value = ".fungos-vfs")]
    root: PathBuf,
    /// The machine's content store. Defaults to `$FUNGOS_VFS_CAS`, then
    /// `$XDG_DATA_HOME/fungos-vfs`; every root on the machine shares it.
    #[arg(long, global = true)]
    cas: Option<PathBuf>,
    #[arg(long, global = true, value_enum, default_value_t = RoleArg::Full)]
    role: RoleArg,
    /// Stamp writes with this actor instead of the process user.
    #[arg(long, global = true)]
    actor: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum RoleArg {
    Full,
    Cache,
}

impl From<RoleArg> for NodeRole {
    fn from(value: RoleArg) -> Self {
        match value {
            RoleArg::Full => NodeRole::Full,
            RoleArg::Cache => NodeRole::Cache,
        }
    }
}

#[derive(Subcommand)]
enum Command {
    Put {
        file: PathBuf,
        path: String,
        #[arg(long, default_value = "application/octet-stream")]
        media_type: String,
        #[arg(long)]
        expected_version: Option<u64>,
        /// Fail unless the current head holds this content. Unlike a version,
        /// a content id means the same thing on every store.
        #[arg(long)]
        base: Option<String>,
    },
    Get {
        path: String,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    Ls {
        #[arg(default_value = "")]
        prefix: String,
        #[arg(long)]
        json: bool,
    },
    Stat {
        path: String,
        #[arg(long)]
        json: bool,
    },
    History {
        path: String,
        #[arg(long)]
        json: bool,
    },
    Rm {
        path: String,
    },
    Mkdir {
        path: String,
    },
    Rmdir {
        path: String,
    },
    Pin {
        path: String,
    },
    Unpin {
        path: String,
    },
    Lease {
        path: String,
        #[arg(long, default_value = "cli")]
        owner: String,
        #[arg(long, default_value_t = 60_000)]
        ttl_ms: u64,
    },
    Release {
        path: String,
        #[arg(long, default_value = "cli")]
        owner: String,
    },
    Pending {
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    Ack {
        ids: Vec<i64>,
    },
    Gc,
    /// Move this root's blobs into the shared content store and retire its
    /// `.fungos` directory. Reads fall back to the old location until then, so
    /// this is safe to run at any time and safe to run twice.
    Migrate,
    Evict {
        #[arg(long)]
        max_bytes: u64,
    },
    Placeholder {
        path: String,
        content_id: String,
        size: u64,
        #[arg(long, default_value = "application/octet-stream")]
        media_type: String,
    },
    Hydrate {
        path: String,
        file: PathBuf,
    },
    /// Copy a directory tree into the namespace under `prefix`.
    Ingest {
        source: PathBuf,
        #[arg(long, default_value = "")]
        prefix: String,
        #[arg(long, default_value = "git")]
        mode: String,
        #[arg(long)]
        dry_run: bool,
        /// A policy file to use instead of `<source>/.vfsignore`.
        #[arg(long)]
        policy: Option<PathBuf>,
        /// Ingest every path as a blob, ignoring any policy.
        #[arg(long)]
        no_policy: bool,
    },
    /// Attach a Git repository to this desktop, or list what is attached.
    ///
    /// Attach records where the worktrees are and ingests the in-between; the
    /// tracked tree becomes one Git pointer, never a blob per file.
    Project {
        /// `attach` or `list`.
        mode: String,
        /// A repository, or any path inside one, for `attach`.
        path: Option<PathBuf>,
        /// Name under the projects prefix; defaults to the directory's name.
        #[arg(long)]
        name: Option<String>,
        #[arg(long, default_value = "projects")]
        prefix: String,
        #[arg(long)]
        dry_run: bool,
    },
    /// Exchange ops with a peer store.
    Sync {
        /// `serve` answers a request on stdin; `pull` asks a peer; `compact`
        /// drops log rows a peer can no longer be caught up from.
        mode: String,
        /// ssh destination for `pull`, e.g. `agora-one`.
        #[arg(long)]
        remote: Option<String>,
        /// The peer's VFS root.
        #[arg(long)]
        remote_root: Option<String>,
        /// The peer's `fungos-vfs` binary.
        #[arg(long, default_value = "fungos-vfs")]
        remote_bin: String,
        #[arg(long, default_value_t = 500)]
        limit: usize,
        /// How old, in seconds, a log row must be before `compact` drops it.
        #[arg(long)]
        older_than: Option<u64>,
        /// Serve over QUIC on this address instead of answering one request on
        /// stdin, e.g. `127.0.0.1:7701`. A client needs the fingerprint this
        /// prints.
        #[arg(long)]
        quic: Option<String>,
        /// Where the QUIC identity lives. Created on first use and reused after
        /// that, so the fingerprint a peer pinned keeps working across restarts.
        #[arg(long)]
        cert: Option<PathBuf>,
        /// The fingerprint a QUIC peer must present, in hex.
        #[arg(long)]
        fingerprint: Option<String>,
        /// Path to settle, for `resolve`.
        #[arg(long)]
        path: Option<String>,
        /// Which side wins: `local` keeps what is here, `remote` takes the copy.
        #[arg(long)]
        keep: Option<String>,
        /// Which peer's divergence to settle, when several hold the same path.
        #[arg(long)]
        store: Option<String>,
    },
    /// Materialize everything under `prefix` into a directory.
    Export {
        prefix: String,
        dest: PathBuf,
    },
    /// Worktrees on a real disk, recorded by the difference.
    Worktree {
        /// `materialize`, `checkout`, `list`, `record`, `changes`, or `prune`.
        mode: String,
        #[arg(long)]
        name: Option<String>,
        /// The repository for `materialize`.
        #[arg(long)]
        project: Option<PathBuf>,
        /// The directory to check out into, for `materialize`.
        #[arg(long)]
        path: Option<PathBuf>,
        /// The commit to check out; defaults to the repository's HEAD.
        #[arg(long)]
        commit: Option<String>,
        /// Namespace prefix; defaults to `projects`, the same as `project attach`.
        #[arg(long, default_value = "projects")]
        prefix: String,
        /// Who holds the lease while `record` runs. Defaults to this actor and pid.
        #[arg(long)]
        owner: Option<String>,
        #[arg(long, default_value_t = fungos_vfs::DEFAULT_LEASE_MS)]
        lease_ms: u64,
        /// Do not carry the commit with the recording, even when no remote has
        /// it. The far side will then need the commit from somewhere else.
        #[arg(long)]
        no_carry: bool,
        /// Cursor from a previous `changes` page. `0` starts from the beginning.
        #[arg(long, default_value_t = 0)]
        since: i64,
        #[arg(long, default_value_t = 100)]
        limit: usize,
        /// Lay a recording over files that are neither its content nor the base
        /// commit's version.
        #[arg(long)]
        force: bool,
        /// Leave the checkout on whatever commit it is on, instead of moving it to
        /// the recording's base.
        #[arg(long)]
        stay: bool,
        /// Forget, rather than only report, what `worktree prune` finds safe.
        #[arg(long)]
        apply: bool,
    },
    /// The build cache: content this root may forget.
    Cache {
        /// `put`, `ls`, `pin`, `unpin`, or `evict`.
        mode: String,
        /// The file to file, for `put`.
        file: Option<PathBuf>,
        /// What produced it, for `put`. A label, never an address.
        #[arg(long)]
        path: Option<String>,
        /// The content id, for `pin` and `unpin`.
        #[arg(long)]
        content_id: Option<String>,
        #[arg(long, default_value = "application/octet-stream")]
        media_type: String,
        /// The ceiling to evict down to, for `evict`.
        #[arg(long)]
        max_bytes: Option<u64>,
        #[arg(long)]
        json: bool,
    },
    /// Where content is replicated besides this machine: disks, and adapters
    /// over the same trait for buckets and nodes.
    Replica {
        /// `add`, `list`, `remove`, `push`, or `serve`.
        mode: String,
        /// The replica's name, for `add` and `remove`.
        #[arg(long)]
        name: Option<String>,
        /// What kind it is. `disk` is built in.
        #[arg(long, default_value = "disk")]
        kind: String,
        /// Where a `disk` replica keeps its objects.
        #[arg(long)]
        path: Option<PathBuf>,
        /// Where a `provider` replica listens, `host:port`. For `serve`, where to
        /// listen.
        #[arg(long)]
        endpoint: Option<String>,
        /// `replica serve`: the address to serve on.
        #[arg(long)]
        listen: Option<String>,
        /// Read from this replica, never write to it.
        #[arg(long)]
        read_only: bool,
        /// Only the prefix to push.
        #[arg(long, default_value = "")]
        prefix: String,
        /// Report what `push` would upload, and upload nothing.
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        json: bool,
    },
    /// Conversations as records: import them from a harness, list them, show
    /// one, or project one into another harness's format.
    Session {
        /// `import`, `list`, `show`, `project`, or `sync`.
        mode: String,
        /// Which harness, for `import` (`pi`) and to filter `list`.
        #[arg(long)]
        harness: Option<String>,
        /// The harness's own store, for `import`.
        #[arg(long)]
        path: Option<PathBuf>,
        /// The session's native id, for `show` and `project`. Omit it to take
        /// every session the harness has.
        #[arg(long)]
        id: Option<String>,
        /// What to project into. `operator` is what this build writes.
        #[arg(long)]
        to: Option<String>,
        /// A document to grow instead of overwriting `--out`. With `markdown`
        /// this appends only the turns the document does not already hold, so a
        /// running session can be projected on a timer.
        #[arg(long = "doc")]
        doc: Option<String>,
        /// Where the target keeps its transcripts, for `project`. For
        /// `operator` that is `<workspace>/.operator/runs`.
        #[arg(long)]
        out: Option<PathBuf>,
        /// The workspace the projected session claims, for `project`. Defaults
        /// to the cwd the conversation was recorded in.
        #[arg(long)]
        workspace: Option<String>,
        /// Count what an import would write, and write nothing.
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        json: bool,
    },
    /// A markdown file and the document it is a view of: read the file's edits,
    /// merge them, and write the result back.
    Doc {
        /// `sync`, `comment`, or `comments`.
        mode: String,
        /// The document's identity. Two files with one id are one document.
        #[arg(long)]
        id: Option<String>,
        /// The file to reconcile.
        #[arg(long)]
        path: Option<PathBuf>,
        /// The markdown view to write for `import`, and to read for `view`.
        #[arg(long)]
        out: Option<PathBuf>,
        /// Who the edits, and the comments, are attributed to. Defaults to this
        /// store's actor, which has to be stable or every run starts a new stream.
        #[arg(long)]
        actor: Option<String>,
        /// The block a new thread hangs on: `counter@actor`, or `#N` for the Nth
        /// live block of the document.
        #[arg(long)]
        block: Option<String>,
        /// What to say. With `--block` it opens a thread; with `--thread` it
        /// replies.
        #[arg(long)]
        body: Option<String>,
        /// An existing thread, for a reply or a settle.
        #[arg(long)]
        thread: Option<String>,
        /// Mark the thread in `--thread` resolved.
        #[arg(long)]
        settle: bool,
        /// Reopen the thread in `--thread`.
        #[arg(long)]
        reopen: bool,
        #[arg(long)]
        json: bool,
    },
    /// Fill placeholders from the configured replicas.
    Fetch {
        /// One namespace path.
        #[arg(long)]
        path: Option<String>,
        /// Every placeholder under this prefix.
        #[arg(long, default_value = "")]
        prefix: String,
        /// Name what is missing without fetching it.
        #[arg(long)]
        dry_run: bool,
    },
}

/// Milliseconds since the epoch, as this process reads the clock.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

/// `#N` names the Nth live block of a document; `counter@actor` names one exactly.
///
/// The number is a convenience for a person at a terminal. Anything recorded
/// should use the identity: positions move when somebody inserts above them.
fn anchor_spec(doc: &Doc, spec: &str) -> anyhow::Result<Anchor> {
    let spec = spec.trim();
    if let Some(number) = spec.strip_prefix('#') {
        let number: usize = number
            .parse()
            .map_err(|_| anyhow::anyhow!("#{number} is not a block number"))?;
        let live = doc.live();
        let block = number.checked_sub(1).and_then(|index| live.get(index));
        let block = block.ok_or_else(|| {
            anyhow::anyhow!("the document has {} live blocks, not {number}", live.len())
        })?;
        return Ok(Anchor::Block {
            block: block.id.clone(),
        });
    }
    Ok(Anchor::Block {
        block: spec.parse().map_err(anyhow::Error::msg)?,
    })
}

/// The first line of a block, short enough to scan in a listing.
fn one_line(text: &str) -> String {
    let first = text.lines().next().unwrap_or_default().trim();
    if first.chars().count() > 96 {
        format!("{}…", first.chars().take(96).collect::<String>())
    } else {
        first.to_owned()
    }
}

fn anchor_label(anchor: &Anchor) -> String {
    match anchor {
        Anchor::Block { block } => block.to_string(),
        Anchor::Range { block, start, end } => format!("{block}[{start}..{end}]"),
    }
}

fn main() {
    if let Err(error) = run() {
        // `{:#}` rather than `{}`: the outermost message says what failed, the
        // causes say why, and a tool that hides the causes makes every incident a
        // guessing game.
        eprintln!("fungos-vfs: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    // `migrate` renames this root's state directory, so it has to run before
    // anything opens the root: an open database renamed out from under its own
    // connection is exactly the kind of surprise this command exists to avoid.
    if matches!(&cli.command, Command::Migrate) {
        let report = migrate_storage_with_cas(&cli.root, cli.cas.as_deref())?;
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }
    let mut vfs = Vfs::open_with(&cli.root, cli.role.into(), cli.cas.as_deref())?;
    if let Some(actor) = &cli.actor {
        vfs.set_actor(actor.clone());
    }
    match cli.command {
        Command::Put {
            file,
            path,
            media_type,
            expected_version,
            base,
        } => {
            let bytes = fs::read(&file)?;
            let base_id = base
                .as_deref()
                .map(ContentId::parse)
                .transpose()
                .map_err(|error| anyhow::anyhow!("--base: {error:?}"))?;
            let expect = match (&base_id, expected_version) {
                (Some(base), _) => Expect::Base(base),
                (None, Some(version)) => Expect::Version(version),
                (None, None) => Expect::Any,
            };
            let entry = vfs.put(&path, &bytes, &media_type, expect)?;
            println!("{}", serde_json::to_string_pretty(&entry)?);
        }
        Command::Get { path, output } => {
            let bytes = vfs.get(&path)?;
            if let Some(output) = output {
                fs::write(output, bytes)?;
            } else {
                print!("{}", String::from_utf8_lossy(&bytes));
            }
        }
        Command::Ls { prefix, json } => {
            let entries = vfs.list(&prefix)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&entries)?);
            } else {
                for entry in entries {
                    println!(
                        "{}\t{}\t{}\t{}",
                        entry.path, entry.size, entry.version, entry.content_id
                    );
                }
            }
        }
        Command::Stat { path, json } => {
            let entry = vfs
                .file(&path)?
                .ok_or_else(|| anyhow::anyhow!("not found: {path}"))?;
            if json {
                println!("{}", serde_json::to_string_pretty(&entry)?);
            } else {
                println!(
                    "{}\t{}\t{}\t{}\t{}\t{}",
                    entry.path,
                    entry.size,
                    entry.version,
                    entry.hydration,
                    entry.pinned,
                    entry.content_id
                );
            }
        }
        Command::History { path, json } => {
            let history = vfs.history(&path)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&history)?);
            } else {
                for entry in history {
                    println!(
                        "{}\t{}\t{}\t{}\t{}\t{}",
                        entry.version,
                        entry.size,
                        entry.created_at_ms,
                        entry.content_id,
                        entry.actor,
                        entry.origin
                    );
                }
            }
        }
        Command::Rm { path } => {
            vfs.remove(&path)?;
        }
        Command::Mkdir { path } => {
            vfs.mkdir(&path)?;
            println!("{path}");
        }
        Command::Rmdir { path } => {
            vfs.rmdir(&path)?;
        }
        Command::Pin { path } => vfs.set_pinned(&path, true)?,
        Command::Unpin { path } => vfs.set_pinned(&path, false)?,
        Command::Lease {
            path,
            owner,
            ttl_ms,
        } => {
            println!(
                "{}",
                serde_json::to_string_pretty(&vfs.lease(&path, &owner, ttl_ms)?)?
            );
        }
        Command::Release { path, owner } => vfs.release(&path, &owner)?,
        Command::Pending { limit } => {
            println!("{}", serde_json::to_string_pretty(&vfs.pending(limit)?)?);
        }
        Command::Ack { ids } => vfs.ack(&ids)?,
        Command::Gc => {
            println!("{}", vfs.gc()?);
        }
        Command::Evict { max_bytes } => {
            println!("{}", vfs.evict_to(max_bytes)?);
        }
        Command::Placeholder {
            path,
            content_id,
            size,
            media_type,
        } => {
            let entry =
                vfs.register_placeholder(&path, ContentId::parse(&content_id)?, size, &media_type)?;
            println!("{}", serde_json::to_string_pretty(&entry)?);
        }
        Command::Hydrate { path, file } => {
            let bytes = fs::read(file)?;
            let entry = vfs.hydrate(&path, &bytes)?;
            println!("{}", serde_json::to_string_pretty(&entry)?);
        }
        Command::Ingest {
            source,
            prefix,
            mode,
            dry_run,
            policy,
            no_policy,
        } => {
            let mode = SourceMode::parse(&mode)?;
            let policy = if no_policy {
                PathPolicy::store_everything()
            } else {
                match policy {
                    Some(path) => PathPolicy::parse(&fs::read_to_string(&path)?),
                    None => PathPolicy::load(&source)?,
                }
            };
            let report =
                ingest_dir_with_policy(&mut vfs, &source, &prefix, mode, dry_run, &policy)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::Project {
            mode,
            path,
            name,
            prefix,
            dry_run,
        } => match mode.as_str() {
            "attach" => {
                let path = path.ok_or_else(|| anyhow::anyhow!("project attach needs a path"))?;
                let report = attach_project(&mut vfs, &path, name.as_deref(), &prefix, dry_run)?;
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
            "list" => println!(
                "{}",
                serde_json::to_string_pretty(&list_projects(&mut vfs)?)?
            ),
            other => anyhow::bail!("project mode must be attach or list, not {other}"),
        },
        Command::Sync {
            mode,
            remote,
            remote_root,
            remote_bin,
            limit,
            older_than,
            quic,
            cert,
            fingerprint,
            path,
            keep,
            store,
        } => match mode.as_str() {
            // ssh stays the default carrier: without `--quic`, `serve` answers
            // one request on stdin, which is exactly what an ssh session gives
            // it, and `pull` shells out to `ssh` as it always has.
            "serve" => {
                if let Some(addr) = quic {
                    let server = match &cert {
                        Some(path) => SyncServer::bind_with_identity(&addr, path)?,
                        None => SyncServer::bind(&addr)?,
                    };
                    // Printed before serving, and flushed: a client cannot be
                    // configured without it, and stdout is block buffered when
                    // it is piped somewhere that is waiting for this line.
                    println!(
                        "{}",
                        serde_json::json!({
                            "listening": server.local_addr()?.to_string(),
                            "fingerprint": server.fingerprint(),
                        })
                    );
                    std::io::stdout().flush()?;
                    // One failed session is that peer's problem: a wrong
                    // fingerprint or a corrupt request must not stop the ones
                    // that are working.
                    loop {
                        if let Err(error) = server.accept_one(&mut vfs) {
                            eprintln!("sync serve over quic: {error}");
                        }
                    }
                } else {
                    let mut input = String::new();
                    std::io::Read::read_to_string(&mut std::io::stdin(), &mut input)?;
                    let request: fungos_vfs::SyncRequest = serde_json::from_str(&input)?;
                    let response = vfs.serve_sync(&request)?;
                    println!("{}", serde_json::to_string(&response)?);
                }
            }
            "pull" => {
                let (outcomes, report) = match quic {
                    Some(endpoint) => {
                        let fingerprint = fingerprint.ok_or_else(|| {
                            anyhow::anyhow!("sync pull --quic needs --fingerprint")
                        })?;
                        pull_over_quic(&mut vfs, &endpoint, &fingerprint, limit)?
                    }
                    None => {
                        let remote =
                            remote.ok_or_else(|| anyhow::anyhow!("sync pull needs --remote"))?;
                        let remote_root = remote_root
                            .ok_or_else(|| anyhow::anyhow!("sync pull needs --remote-root"))?;
                        pull_over_ssh(&mut vfs, &remote, &remote_root, &remote_bin, limit)?
                    }
                };
                println!("{}", summarize(&outcomes, &report));
            }
            "compact" => {
                let seconds =
                    older_than.ok_or_else(|| anyhow::anyhow!("sync compact needs --older-than"))?;
                let report = vfs.compact(std::time::Duration::from_secs(seconds))?;
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
            "diverged" => {
                println!("{}", serde_json::to_string_pretty(&diverged_report(&vfs)?)?);
            }
            "resolve" => {
                let path = path.ok_or_else(|| anyhow::anyhow!("sync resolve needs --path"))?;
                let keep = keep.ok_or_else(|| anyhow::anyhow!("sync resolve needs --keep"))?;
                let found: Vec<_> = vfs
                    .divergences()?
                    .into_iter()
                    .filter(|entry| entry.path == path)
                    .collect();
                let chosen = match (found.len(), store) {
                    (0, _) => anyhow::bail!("nothing is diverged at {path}"),
                    (1, None) => found.into_iter().next().expect("one"),
                    (_, None) => anyhow::bail!(
                        "{path} is diverged from {} stores; pass --store",
                        found.len()
                    ),
                    (_, Some(store)) => found
                        .into_iter()
                        .find(|entry| entry.store == store)
                        .ok_or_else(|| anyhow::anyhow!("no divergence at {path} from {store}"))?,
                };
                let settled = match keep.as_str() {
                    // Keeping ours leaves the copy: it is the only remaining
                    // record of what the peer wanted to write.
                    "local" => vfs.resolve_divergence(&chosen.store, chosen.hlc)?,
                    "remote" => vfs.accept_divergence(&chosen.store, chosen.hlc)?,
                    other => anyhow::bail!("--keep must be local or remote, not {other}"),
                };
                println!(
                    "{}",
                    serde_json::json!({
                        "path": path,
                        "store": chosen.store,
                        "kept": keep,
                        "settled": settled,
                        "copy": chosen.copy_path(),
                    })
                );
            }
            other => anyhow::bail!(
                "sync mode must be serve, pull, compact, diverged or resolve, not {other}"
            ),
        },
        Command::Export { prefix, dest } => {
            let report = export_dir(&mut vfs, &prefix, &dest)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::Worktree {
            mode,
            name,
            project,
            path,
            commit,
            prefix,
            owner,
            lease_ms,
            since,
            limit,
            force,
            apply,
            no_carry,
            stay,
        } => match mode.as_str() {
            "materialize" => {
                let project = project
                    .ok_or_else(|| anyhow::anyhow!("worktree materialize needs --project"))?;
                let name =
                    name.ok_or_else(|| anyhow::anyhow!("worktree materialize needs --name"))?;
                let path =
                    path.ok_or_else(|| anyhow::anyhow!("worktree materialize needs --path"))?;
                let report =
                    materialize(&mut vfs, &project, &name, &path, commit.as_deref(), &prefix)?;
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
            "list" => println!("{}", serde_json::to_string_pretty(&worktrees(&vfs)?)?),
            "record" => {
                let name = name.ok_or_else(|| anyhow::anyhow!("worktree record needs --name"))?;
                let owner = owner
                    .unwrap_or_else(|| format!("{}:{}", vfs.writer().actor, std::process::id()));
                let report = record(
                    &mut vfs,
                    &name,
                    &RecordOptions {
                        owner,
                        lease_ms,
                        carry: !no_carry,
                    },
                )?;
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
            "changes" => {
                let name = name.ok_or_else(|| anyhow::anyhow!("worktree changes needs --name"))?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&worktree_changes(&vfs, &name, since, limit)?)?
                );
            }
            "checkout" => {
                let name = name.ok_or_else(|| anyhow::anyhow!("worktree checkout needs --name"))?;
                let path = path.ok_or_else(|| anyhow::anyhow!("worktree checkout needs --path"))?;
                let owner = owner
                    .unwrap_or_else(|| format!("{}:{}", vfs.writer().actor, std::process::id()));
                // `--project` names a directory everywhere else in this CLI, and a
                // project is named after its directory; asking for the name a
                // second way would be two vocabularies for one thing.
                let project = project
                    .as_ref()
                    .and_then(|dir| dir.file_name())
                    .map(|name| name.to_string_lossy().into_owned());
                let report = checkout(
                    &mut vfs,
                    &CheckoutOptions {
                        name,
                        path,
                        project,
                        prefix,
                        commit,
                        force,
                        to_base: !stay,
                        owner,
                        lease_ms,
                    },
                )?;
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
            "prune" => {
                let report = fungos_vfs::prune(&mut vfs, name.as_deref(), apply)?;
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
            other => anyhow::bail!(
                "worktree mode must be materialize, checkout, list, record, changes or prune, not {other}"
            ),
        },
        Command::Cache {
            mode,
            file,
            path,
            content_id,
            media_type,
            max_bytes,
            json,
        } => match mode.as_str() {
            "put" => {
                let file = file.ok_or_else(|| anyhow::anyhow!("cache put needs a file"))?;
                let label = path.unwrap_or_else(|| file.to_string_lossy().into_owned());
                let bytes = fs::read(&file)?;
                let content_id = vfs.put_cache(&label, &bytes, &media_type)?;
                println!(
                    "{}",
                    serde_json::json!({
                        "path": label,
                        "content_id": content_id.to_string(),
                        "size": bytes.len(),
                        "cached_bytes": vfs.cached_bytes()?,
                    })
                );
            }
            "ls" => {
                let entries = vfs.cache_entries()?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&entries)?);
                } else {
                    for entry in entries {
                        println!(
                            "{}\t{}\t{}{}",
                            entry.content_id,
                            entry.size,
                            entry.path,
                            if entry.pinned { "\t(pinned)" } else { "" }
                        );
                    }
                }
            }
            "pin" | "unpin" => {
                let id =
                    content_id.ok_or_else(|| anyhow::anyhow!("cache {mode} needs --content-id"))?;
                let id = ContentId::parse(&id).map_err(|error| anyhow::anyhow!("{error:?}"))?;
                vfs.pin_cache(&id, mode == "pin")?;
            }
            "evict" => {
                let max =
                    max_bytes.ok_or_else(|| anyhow::anyhow!("cache evict needs --max-bytes"))?;
                println!(
                    "{}",
                    serde_json::json!({
                        "evicted": vfs.evict_cache_to(max)?,
                        "cached_bytes": vfs.cached_bytes()?,
                    })
                );
            }
            other => anyhow::bail!("cache mode must be put, ls, pin, unpin or evict, not {other}"),
        },
        Command::Replica {
            mode,
            name,
            kind,
            path,
            endpoint,
            listen,
            read_only,
            prefix,
            dry_run,
            json,
        } => match mode.as_str() {
            "add" => {
                let name = name.ok_or_else(|| anyhow::anyhow!("replica add needs --name"))?;
                let mut config = ReplicaConfig::load(&cli.root)?;
                config.upsert(ReplicaSpec {
                    name: name.clone(),
                    kind: kind.clone(),
                    path,
                    endpoint,
                    read_only,
                });
                // Built before it is saved: a replica this build cannot reach is
                // a typo to fix now, not a backup that has been failing for a week.
                let replicas = config.build()?;
                config.save(&cli.root)?;
                println!(
                    "{}",
                    serde_json::json!({
                        "added": name,
                        "kind": kind,
                        "replicas": replicas.names(),
                        "config": fungos_vfs::replica_config_path(&cli.root).display().to_string(),
                    })
                );
            }
            "list" => {
                let config = ReplicaConfig::load(&cli.root)?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&config.replicas)?);
                } else if config.replicas.is_empty() {
                    println!("no replicas; `replica add --name <name> --path <dir>` starts one");
                } else {
                    for spec in &config.replicas {
                        println!(
                            "{}\t{}\t{}{}",
                            spec.name,
                            spec.kind,
                            spec.path
                                .as_ref()
                                .map(|path| path.display().to_string())
                                .unwrap_or_default(),
                            if spec.read_only { "\t(read-only)" } else { "" }
                        );
                    }
                }
            }
            "remove" => {
                let name = name.ok_or_else(|| anyhow::anyhow!("replica remove needs --name"))?;
                let mut config = ReplicaConfig::load(&cli.root)?;
                if !config.remove(&name) {
                    anyhow::bail!("no replica named {name}");
                }
                config.save(&cli.root)?;
                println!("{}", serde_json::json!({ "removed": name }));
            }
            "push" => {
                let config = ReplicaConfig::load(&cli.root)?;
                let replicas = config.build()?;
                if replicas.is_empty() {
                    anyhow::bail!("no replicas configured; nothing to push to");
                }
                // Pushed by content id, not by path: the same bytes live under
                // many paths, and a namespace entry is a name for content, not
                // the content itself.
                let mut seen: BTreeSet<String> = BTreeSet::new();
                let mut ids: Vec<ContentId> = Vec::new();
                for entry in vfs.list(&prefix)? {
                    if entry.hydration != Hydration::Present {
                        continue;
                    }
                    if seen.insert(entry.content_id.to_string()) {
                        ids.push(entry.content_id);
                    }
                }
                let mut outcomes: Vec<serde_json::Value> = Vec::new();
                let mut added = 0;
                let mut already = 0;
                let mut failed = 0;
                for id in &ids {
                    if dry_run {
                        outcomes.push(serde_json::json!({
                            "content_id": id.to_string(),
                            "outcome": "would-push",
                        }));
                        continue;
                    }
                    let bytes = vfs.content(id)?;
                    for outcome in replicas.put_all(id, &bytes) {
                        match outcome.outcome {
                            "added" => added += 1,
                            "already" => already += 1,
                            "failed" => failed += 1,
                            _ => {}
                        }
                        outcomes.push(serde_json::to_value(&outcome)?);
                    }
                }
                println!(
                    "{}",
                    serde_json::json!({
                        "replicas": replicas.names(),
                        "content_ids": ids.len(),
                        "added": added,
                        "already": already,
                        "failed": failed,
                        "dry_run": dry_run,
                        "outcomes": outcomes,
                    })
                );
            }
            "serve" => {
                // One replica, served to whoever can reach the address. This is how a
                // RAID shelf or a peer's store becomes a replica on another machine
                // without either side learning a new protocol.
                let name = name.ok_or_else(|| anyhow::anyhow!("replica serve needs --name"))?;
                let listen = listen.unwrap_or_else(|| "127.0.0.1:7200".to_owned());
                let config = ReplicaConfig::load(&cli.root)?;
                let replicas = config.build()?;
                let source = replicas
                    .source(&name)
                    .ok_or_else(|| anyhow::anyhow!("no replica named {name}"))?;
                let server = fungos_vfs::ProviderServer::bind(listen.as_str())?;
                eprintln!(
                    "fungos-vfs: serving {name} on {} ({} bytes per range)",
                    server.local_addr()?,
                    fungos_vfs::MAX_RANGE_BYTES
                );
                server.serve(&*source)?;
            }
            other => {
                anyhow::bail!("replica mode must be add, list, remove, push or serve, not {other}")
            }
        },
        Command::Session {
            mode,
            harness,
            path,
            id,
            to,
            doc,
            out,
            workspace,
            dry_run,
            json,
        } => match mode.as_str() {
            "import" => {
                let root = path.ok_or_else(|| anyhow::anyhow!("session import needs --path"))?;
                let harness = harness.unwrap_or_else(|| "pi".to_owned());
                let report = match harness.as_str() {
                    "pi" => import_pi(&mut vfs, &root, dry_run)?,
                    other => anyhow::bail!("no importer for {other}; this build has pi"),
                };
                if json {
                    println!("{}", serde_json::to_string_pretty(&report)?);
                } else {
                    println!(
                        "{harness}: {} sessions ({} written, {} unchanged), {} turns, \
                         {} new payloads, {:.1} MiB",
                        report.sessions,
                        report.written,
                        report.unchanged,
                        report.turns,
                        report.blobs,
                        report.bytes as f64 / (1024.0 * 1024.0)
                    );
                    for (kind, count) in &report.unnormalized {
                        println!("  unnormalized {kind}: {count}");
                    }
                }
            }
            "list" => {
                let sessions = vfs.sessions(harness.as_deref())?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&sessions)?);
                } else {
                    for session in sessions {
                        println!(
                            "{}\t{}\t{}\t{}\t{} turns",
                            session.harness,
                            session.native_id,
                            session.name.as_deref().unwrap_or("-"),
                            session.started_at_ms,
                            session.turns
                        );
                    }
                }
            }
            // Import what changed, then project everything: one command, because a
            // timer that has to run two is a timer somebody will wire up wrong.
            "sync" => {
                let harness =
                    harness.ok_or_else(|| anyhow::anyhow!("session sync needs --harness"))?;
                let store = path.ok_or_else(|| anyhow::anyhow!("session sync needs --path"))?;
                let out = out.ok_or_else(|| anyhow::anyhow!("session sync needs --out"))?;
                let report = match harness.as_str() {
                    "pi" => import_pi_incremental(&mut vfs, &store, dry_run)?,
                    other => anyhow::bail!("no importer for {other}; this build has pi"),
                };
                let mut projected = 0usize;
                let mut rewritten = 0usize;
                let mut skipped = report.unnormalized.clone();
                let summaries = vfs.sessions(Some(&harness))?;
                // Rebuild only what moved. A timer fires whether or not anybody
                // wrote anything, and projecting 140 unchanged sessions costs a
                // minute of hashing to produce 140 identical files. A runs
                // directory that is missing bundles — first run, or somebody
                // cleaned it — gets everything again.
                let bundles = fs::read_dir(&out)
                    .map(|entries| {
                        entries
                            .filter_map(|entry| entry.ok())
                            .filter(|entry| {
                                entry.path().extension().is_some_and(|ext| ext == "ndjson")
                            })
                            .count()
                    })
                    .unwrap_or_default();
                let project_all = bundles < summaries.len();
                for summary in &summaries {
                    if !project_all && !report.changed.contains(&summary.native_id) {
                        continue;
                    }
                    let Some(session) = vfs.session(&harness, &summary.native_id)? else {
                        continue;
                    };
                    let projection =
                        to_operator(&vfs, &session, &out, workspace.as_deref(), dry_run)?;
                    projected += 1;
                    if projection.report.written {
                        rewritten += 1;
                    }
                    for (reason, count) in projection.report.skipped {
                        *skipped.entry(reason).or_default() += count;
                    }
                }
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&serde_json::json!({
                            "import": report,
                            "projected": projected,
                            "bundles_rewritten": rewritten,
                            "skipped": skipped,
                        }))?
                    );
                } else {
                    println!(
                        "{harness}: {} new or changed files ({} unchanged), {} turns, \
                         {} new payloads; {} bundles ({rewritten} rewritten){}",
                        report.sessions,
                        report.files_skipped,
                        report.turns,
                        report.blobs,
                        projected,
                        if dry_run { " (dry run)" } else { "" }
                    );
                    for (reason, count) in &skipped {
                        println!("  counted {reason}: {count}");
                    }
                }
            }
            "project" => {
                let harness =
                    harness.ok_or_else(|| anyhow::anyhow!("session project needs --harness"))?;
                let out = out.ok_or_else(|| anyhow::anyhow!("session project needs --out"))?;
                let target = to.as_deref().unwrap_or("operator").to_owned();
                match target.as_str() {
                    "operator" | "markdown" => {}
                    other => {
                        anyhow::bail!(
                            "no projector for {other}; this build writes operator and markdown"
                        )
                    }
                }
                // One session by id, or every session the harness has: the point
                // of a runs directory is a shelf of them, not a single file.
                let sessions = match &id {
                    Some(id) => vec![
                        vfs.session(&harness, id)?
                            .ok_or_else(|| anyhow::anyhow!("no session {harness}/{id}"))?,
                    ],
                    None => {
                        let mut sessions = Vec::new();
                        for summary in vfs.sessions(Some(&harness))? {
                            if let Some(session) = vfs.session(&harness, &summary.native_id)? {
                                sessions.push(session);
                            }
                        }
                        sessions
                    }
                };
                if target == "markdown" {
                    // A transcript is a document. With `--doc` it is grown in
                    // place: only the turns the document does not already hold
                    // are projected, so a running session can be projected on a
                    // timer instead of rewriting the file each time.
                    let session = sessions
                        .first()
                        .ok_or_else(|| anyhow::anyhow!("markdown projection needs one session"))?;
                    if let Some(doc_id) = &doc {
                        let actor = cli
                            .actor
                            .clone()
                            .unwrap_or_else(|| vfs.writer().actor.clone());
                        let mut file = DocFile::open(&vfs, doc_id, &actor, &out)?;
                        let synced = Transcript::sync(&mut file, &mut vfs, session)?;
                        if json {
                            println!(
                                "{}",
                                serde_json::json!({
                                    "document": doc_id,
                                    "path": out,
                                    "actor": actor,
                                    "synced": synced,
                                })
                            );
                        } else {
                            println!(
                                "{}\t{} turns, {} new blocks, {} rewritten{}, {}",
                                out.display(),
                                synced.turns,
                                synced.inserted,
                                synced.updated,
                                if synced.deleted > 0 {
                                    format!(", {} dropped", synced.deleted)
                                } else {
                                    String::new()
                                },
                                if synced.wrote_file {
                                    "file rewritten"
                                } else {
                                    "file unchanged"
                                }
                            );
                        }
                        return Ok(());
                    }
                    let text = to_markdown(&vfs, session)?;
                    fs::write(&out, &text)?;
                    println!("{}\t{} bytes", out.display(), text.len());
                    return Ok(());
                }
                let mut projected = Vec::new();
                for session in &sessions {
                    let projection =
                        to_operator(&vfs, session, &out, workspace.as_deref(), dry_run)?;
                    if json {
                        projected.push(serde_json::json!({
                            "path": projection.path,
                            "report": projection.report,
                        }));
                    } else {
                        println!(
                            "{}\t{} messages, {} attachments, {} bytes{}",
                            projection.path.display(),
                            projection.report.messages,
                            projection.report.attachments,
                            projection.report.bytes,
                            if dry_run { " (dry run)" } else { "" }
                        );
                        for (reason, count) in &projection.report.skipped {
                            println!("  skipped {reason}: {count}");
                        }
                    }
                }
                if json {
                    println!("{}", serde_json::to_string_pretty(&projected)?);
                }
            }
            "show" => {
                let harness =
                    harness.ok_or_else(|| anyhow::anyhow!("session show needs --harness"))?;
                let native_id = id.ok_or_else(|| anyhow::anyhow!("session show needs --id"))?;
                let session = vfs
                    .session(&harness, &native_id)?
                    .ok_or_else(|| anyhow::anyhow!("no session {harness}/{native_id}"))?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&session)?);
                } else {
                    print_session(&vfs, &session)?;
                }
            }
            other => {
                anyhow::bail!(
                    "session mode must be import, list, show, project or sync, not {other}"
                )
            }
        },
        Command::Doc {
            mode,
            id,
            path,
            out,
            actor,
            block,
            body,
            thread,
            settle,
            reopen,
            json,
        } => match mode.as_str() {
            "sync" => {
                let id = id.ok_or_else(|| anyhow::anyhow!("doc sync needs --id"))?;
                let path = path.ok_or_else(|| anyhow::anyhow!("doc sync needs --path"))?;
                let actor = actor.unwrap_or_else(|| vfs.writer().actor.clone());
                let mut doc = DocFile::open(&vfs, &id, &actor, &path)?;
                let report = doc.reconcile(&mut vfs)?;
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&serde_json::json!({
                            "path": path,
                            "actor": actor,
                            "ops": report.ops,
                            "wrote_file": report.wrote_file,
                            "deferred": report.deferred,
                            "replaced_unseen_text": report.replaced_unseen_text,
                            "snapshot": doc.snapshot(&vfs)?,
                        }))?
                    );
                } else {
                    println!(
                        "{}\t{} ops, {}{}{}",
                        path.display(),
                        report.ops,
                        if report.wrote_file {
                            "file rewritten"
                        } else {
                            "file unchanged"
                        },
                        if report.deferred > 0 {
                            format!(", {} deferred", report.deferred)
                        } else {
                            String::new()
                        },
                        if report.replaced_unseen_text {
                            ", replaced text this worktree had not seen"
                        } else {
                            ""
                        }
                    );
                }
            }
            "comment" => {
                let id = id.ok_or_else(|| anyhow::anyhow!("doc comment needs --id"))?;
                let actor = actor.unwrap_or_else(|| vfs.writer().actor.clone());
                let mut comments = CommentFile::open(&vfs, &id, &actor)?;
                match (block, thread) {
                    (Some(spec), _) => {
                        let body =
                            body.ok_or_else(|| anyhow::anyhow!("doc comment needs --body"))?;
                        // The document decides which block `#N` means, and the
                        // root has every actor's ops: the file is only a view of
                        // it and may not have been synced yet.
                        let doc = load_doc(&vfs, &id, &actor)?;
                        let anchor = anchor_spec(&doc, &spec)?;
                        let thread =
                            comments.open_thread(&mut vfs, anchor.clone(), &body, now_ms())?;
                        if json {
                            println!(
                                "{}",
                                serde_json::json!({
                                    "document": id,
                                    "thread": thread,
                                    "anchor": anchor_label(&anchor),
                                    "author": actor,
                                })
                            );
                        } else {
                            println!("{thread}\t{}\t{id}", anchor_label(&anchor));
                        }
                    }
                    (None, Some(spec)) => {
                        let thread: BlockId = spec.parse().map_err(anyhow::Error::msg)?;
                        if settle && reopen {
                            anyhow::bail!("a comment is settled or reopened, not both");
                        }
                        if settle || reopen {
                            let at = comments.settle(&mut vfs, &thread, settle, now_ms())?;
                            if json {
                                println!(
                                    "{}",
                                    serde_json::json!({
                                        "thread": thread,
                                        "settled": at,
                                        "resolved": settle,
                                    })
                                );
                            } else {
                                println!(
                                    "{thread}\t{}",
                                    if settle { "resolved" } else { "reopened" }
                                );
                            }
                        } else {
                            let body =
                                body.ok_or_else(|| anyhow::anyhow!("doc comment needs --body"))?;
                            let comment = comments.say(&mut vfs, &thread, &body, now_ms())?;
                            if json {
                                println!(
                                    "{}",
                                    serde_json::json!({
                                        "thread": thread,
                                        "comment": comment,
                                        "author": actor,
                                    })
                                );
                            } else {
                                println!("{thread}\t{comment}\t@{actor}");
                            }
                        }
                    }
                    (None, None) => anyhow::bail!(
                        "doc comment needs --block (open a thread on a block) or --thread \
                         (reply to one)"
                    ),
                }
            }
            "comments" => {
                let id = id.ok_or_else(|| anyhow::anyhow!("doc comments needs --id"))?;
                let actor = actor.unwrap_or_else(|| vfs.writer().actor.clone());
                let comments = CommentFile::open(&vfs, &id, &actor)?;
                // Resolution is against the document. The file is preferred when
                // it is here because it may hold edits the root has not seen yet.
                let doc = match &path {
                    Some(path) => DocFile::open(&vfs, &id, &actor, path)?.doc().clone(),
                    None => load_doc(&vfs, &id, &actor)?,
                };
                let rows: Vec<serde_json::Value> = comments
                    .threads(&doc)
                    .into_iter()
                    .map(|(thread, resolution)| {
                        serde_json::json!({
                            "document": id,
                            "thread": thread.id,
                            "state": resolution.state(),
                            "at": resolution.block(),
                            "resolved": thread.resolved(),
                            "anchor": anchor_label(&thread.anchor),
                            "comments": thread.comments.iter().map(|comment| {
                                serde_json::json!({
                                    "id": comment.id,
                                    "author": comment.author,
                                    "body": comment.body,
                                })
                            }).collect::<Vec<_>>(),
                        })
                    })
                    .collect();
                if json {
                    println!("{}", serde_json::to_string_pretty(&rows)?);
                } else if rows.is_empty() {
                    println!("{id}\tno threads");
                } else {
                    for row in &rows {
                        println!(
                            "{}\t{}\t{}\t{}",
                            row["thread"].as_str().unwrap_or_default(),
                            if row["resolved"].as_bool().unwrap_or(false) {
                                "resolved"
                            } else {
                                "open"
                            },
                            row["state"].as_str().unwrap_or_default(),
                            row["anchor"].as_str().unwrap_or_default(),
                        );
                        for comment in row["comments"].as_array().into_iter().flatten() {
                            println!(
                                "  @{}: {}",
                                comment["author"].as_str().unwrap_or_default(),
                                comment["body"].as_str().unwrap_or_default()
                            );
                        }
                    }
                }
            }
            "import" => {
                let id = id.ok_or_else(|| anyhow::anyhow!("doc import needs --id"))?;
                let source = path.ok_or_else(|| anyhow::anyhow!("doc import needs --path"))?;
                let actor = actor.unwrap_or_else(|| vfs.writer().actor.clone());
                // The markdown view is what a person and an agent edit; the
                // container stays where it is and stays the authority for the parts
                // this reader does not model.
                let view = out.unwrap_or_else(|| source.with_extension("md"));
                let bytes = fs::read(&source)?;
                if !fungos_vfs::docx::is_docx(&bytes) {
                    anyhow::bail!(
                        "{} is not a .docx container: a Word file starts with a zip header",
                        source.display()
                    );
                }
                let docx = Docx::read(&bytes, &id)?;
                let document = load_doc(&vfs, &id, &actor)?;
                let (ops, imported) = docx.ops(&document, &actor);
                let mut file = DocFile::open(&vfs, &id, &actor, &view)?;
                let reconciled = file.extend(&mut vfs, ops)?;
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&serde_json::json!({
                            "document": id,
                            "source": source,
                            "view": view,
                            "paragraphs": docx.report.paragraphs,
                            "identified": docx.report.identified,
                            "synthetic": docx.report.synthetic,
                            "inserted": imported.inserted,
                            "updated": imported.updated,
                            "deleted": imported.deleted,
                            "reordered": imported.reordered,
                            "unmodeled": docx.report.unmodeled,
                            "parts": docx.report.parts,
                            "wrote_view": reconciled.wrote_file,
                        }))?
                    );
                } else {
                    println!(
                        "{}\t{} paragraphs ({} by id, {} by position), {} new blocks, {} rewritten, {} deleted{}",
                        view.display(),
                        docx.report.paragraphs,
                        docx.report.identified,
                        docx.report.synthetic,
                        imported.inserted,
                        imported.updated,
                        imported.deleted,
                        if imported.reordered > 0 {
                            format!(", {} moved", imported.reordered)
                        } else {
                            String::new()
                        },
                    );
                    for (name, count) in &docx.report.unmodeled {
                        println!("  not modelled: {name} × {count}");
                    }
                }
            }
            // The read model a view renders: every block with the identity that
            // survives editing, and the threads hanging on it.
            "view" => {
                let id = id.ok_or_else(|| anyhow::anyhow!("doc view needs --id"))?;
                let actor = actor.unwrap_or_else(|| vfs.writer().actor.clone());
                let doc = match path.as_ref().or(out.as_ref()) {
                    Some(view) => DocFile::open(&vfs, &id, &actor, view)?.doc().clone(),
                    None => load_doc(&vfs, &id, &actor)?,
                };
                let comments = CommentFile::open(&vfs, &id, &actor)?;
                let threads = comments.threads(&doc);
                if json {
                    let blocks: Vec<serde_json::Value> = doc
                        .live()
                        .iter()
                        .map(|block| {
                            serde_json::json!({
                                "id": block.id,
                                "text": block.text,
                                "threads": threads
                                    .iter()
                                    .filter(|(_, resolution)| resolution
                                        .block()
                                        .is_some_and(|at| at == &block.id))
                                    .map(|(thread, resolution)| serde_json::json!({
                                        "id": thread.id,
                                        "state": resolution.state(),
                                        "resolved": thread.resolved(),
                                        "comments": thread
                                            .comments
                                            .iter()
                                            .map(|comment| serde_json::json!({
                                                "author": comment.author,
                                                "body": comment.body,
                                            }))
                                            .collect::<Vec<_>>(),
                                    }))
                                    .collect::<Vec<_>>(),
                            })
                        })
                        .collect();
                    println!("{}", serde_json::to_string_pretty(&blocks)?);
                } else {
                    let live = doc.live();
                    if live.is_empty() {
                        println!("{id}\tno blocks yet");
                    }
                    for block in live {
                        println!("{}\t{}", block.id, one_line(&block.text));
                        for (thread, resolution) in &threads {
                            if resolution.block().is_some_and(|at| at == &block.id) {
                                println!(
                                    "  [{}] {} {}",
                                    thread.id,
                                    if thread.resolved() {
                                        "resolved"
                                    } else {
                                        "open"
                                    },
                                    resolution.state()
                                );
                                for comment in &thread.comments {
                                    println!(
                                        "    @{}: {}",
                                        comment.author,
                                        one_line(&comment.body)
                                    );
                                }
                            }
                        }
                    }
                    for (thread, resolution) in &threads {
                        if resolution.block().is_none() {
                            println!(
                                "[{}] {} {} — nothing left to point at",
                                thread.id,
                                if thread.resolved() {
                                    "resolved"
                                } else {
                                    "open"
                                },
                                resolution.state()
                            );
                        }
                    }
                }
            }
            other => anyhow::bail!("doc mode must be sync, comment or comments, not {other}"),
        },
        Command::Fetch {
            path,
            prefix,
            dry_run,
        } => {
            let config = ReplicaConfig::load(&cli.root)?;
            let replicas = config.build()?;
            match path {
                Some(path) => {
                    if dry_run {
                        let entry = vfs
                            .file(&path)?
                            .ok_or_else(|| anyhow::anyhow!("no such path: {path}"))?;
                        println!(
                            "{}",
                            serde_json::json!({
                                "path": entry.path,
                                "content_id": entry.content_id.to_string(),
                                "hydration": format!("{:?}", entry.hydration),
                                "replicas": replicas.names(),
                            })
                        );
                    } else {
                        let hydrated = vfs.hydrate_from_replicas(&path, &replicas)?;
                        println!("{}", serde_json::to_string_pretty(&hydrated)?);
                    }
                }
                None => {
                    let missing = vfs.placeholders(&prefix)?;
                    let mut hydrated = 0;
                    let mut bytes = 0usize;
                    let mut sources: BTreeMap<String, usize> = BTreeMap::new();
                    if !dry_run {
                        for (path, _, _) in &missing {
                            let done = vfs.hydrate_from_replicas(path, &replicas)?;
                            hydrated += 1;
                            bytes += done.size;
                            *sources.entry(done.source).or_default() += 1;
                        }
                    }
                    println!(
                        "{}",
                        serde_json::json!({
                            "placeholders": missing.len(),
                            "hydrated": hydrated,
                            "bytes": bytes,
                            "sources": sources,
                            "dry_run": dry_run,
                        })
                    );
                }
            }
        }
        // Handled before the store is opened, above.
        Command::Migrate => unreachable!(),
    }
    Ok(())
}

/// One session as a readable transcript.
///
/// This is the CLI's own rendering, not a projector: it exists so `session show`
/// answers "what was said" without a second command, and it deliberately shows
/// payload ids rather than an opinion about formatting.
fn print_session(vfs: &fungos_vfs::Vfs, session: &fungos_vfs::Session) -> anyhow::Result<()> {
    println!(
        "{} {}{}{}",
        session.harness,
        session.native_id,
        session
            .name
            .as_deref()
            .map(|name| format!(" ({name})"))
            .unwrap_or_default(),
        session
            .cwd
            .as_deref()
            .map(|cwd| format!(" in {cwd}"))
            .unwrap_or_default(),
    );
    if !session.models.is_empty() {
        println!("models: {}", session.models.join(", "));
    }
    for turn in &session.turns {
        println!("\n#{} {:?}", turn.index, turn.role);
        if let Some(text) = vfs.turn_text(turn)? {
            println!("{text}");
        }
        if let Some(thinking) = &turn.thinking {
            println!("  (thinking {thinking})");
        }
        for tool in &turn.tools {
            println!(
                "  tool {} args {}{}",
                tool.name,
                tool.arguments,
                if tool.failed { " (failed)" } else { "" }
            );
        }
    }
    for (kind, count) in &session.unnormalized {
        println!("\nunnormalized {kind}: {count}");
    }
    Ok(())
}

/// What `sync diverged` prints: every refusal still held, and where its bytes
/// went. The age is here rather than in the store because when a divergence
/// expires is a policy of the caller running `compact`, not a fact of the log.
fn diverged_report(vfs: &fungos_vfs::Vfs) -> anyhow::Result<serde_json::Value> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default();
    let mut rows = Vec::new();
    for divergence in vfs.divergences()? {
        let copy = divergence.copy_path();
        rows.push(serde_json::json!({
            "store": divergence.store,
            "hlc": { "ms": divergence.hlc.ms, "counter": divergence.hlc.counter },
            "path": divergence.path,
            "copy": copy,
            "copy_present": vfs.file(&copy)?.is_some(),
            "detected_at_ms": divergence.detected_at_ms,
            "age_seconds": (now - divergence.detected_at_ms).max(0) / 1000,
        }));
    }
    Ok(serde_json::Value::Array(rows))
}

/// What `sync pull` prints, whichever carrier moved the ops.
///
/// The costs are reported, not just the outcomes: an exchange that moved a
/// snapshot should be recognisable as one, and the bytes are the number that
/// says whether the transfer was worth resuming.
fn summarize(outcomes: &[ApplyOutcome], report: &fungos_vfs::TransferReport) -> serde_json::Value {
    let mut applied = 0;
    let mut duplicate = 0;
    let mut diverged = 0;
    let mut snapshot = false;
    for outcome in outcomes {
        match outcome {
            ApplyOutcome::Applied => applied += 1,
            ApplyOutcome::Duplicate => duplicate += 1,
            ApplyOutcome::Diverged => diverged += 1,
            ApplyOutcome::Snapshot => snapshot = true,
        }
    }
    serde_json::json!({
        "applied": applied,
        "duplicate": duplicate,
        "diverged": diverged,
        "snapshot": snapshot,
        "session": report.session,
        "bytes": report.bytes,
        "entries": report.entries,
        "content_bytes": report.content_bytes,
        "resumed_from": report.resumed_from,
        "attempts": report.attempts,
    })
}
