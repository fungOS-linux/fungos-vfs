use std::{fs, io::Write, path::PathBuf};

use clap::{Parser, Subcommand};
use fungos_vfs::{
    ApplyOutcome, ContentId, Expect, NodeRole, PathPolicy, SourceMode, SyncServer, Vfs, export_dir,
    ingest_dir_with_policy, pull_over_quic, pull_over_ssh,
};

#[derive(Parser)]
#[command(version, about = "Local-first content-addressed VFS for fungOS")]
struct Cli {
    #[arg(long, global = true, default_value = ".fungos-vfs")]
    root: PathBuf,
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
    },
    /// Materialize everything under `prefix` into a directory.
    Export {
        prefix: String,
        dest: PathBuf,
    },
}

fn main() {
    if let Err(error) = run() {
        eprintln!("fungos-vfs: {error}");
        std::process::exit(1);
    }
}

fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let mut vfs = Vfs::open_with_role(&cli.root, cli.role.into())?;
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
                let outcomes = match quic {
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
                println!("{}", summarize(&outcomes));
            }
            "compact" => {
                let seconds =
                    older_than.ok_or_else(|| anyhow::anyhow!("sync compact needs --older-than"))?;
                let report = vfs.compact(std::time::Duration::from_secs(seconds))?;
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
            other => anyhow::bail!("sync mode must be serve, pull or compact, not {other}"),
        },
        Command::Export { prefix, dest } => {
            let report = export_dir(&mut vfs, &prefix, &dest)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
    }
    Ok(())
}

/// What `sync pull` prints, whichever carrier moved the ops.
fn summarize(outcomes: &[ApplyOutcome]) -> serde_json::Value {
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
    })
}
