use std::{fs, path::PathBuf};

use clap::{Parser, Subcommand};
use fungos_vfs::{ContentId, NodeRole, SourceMode, Vfs, export_dir, ingest_dir};

#[derive(Parser)]
#[command(version, about = "Local-first content-addressed VFS for fungOS")]
struct Cli {
    #[arg(long, global = true, default_value = ".fungos-vfs")]
    root: PathBuf,
    #[arg(long, global = true, value_enum, default_value_t = RoleArg::Full)]
    role: RoleArg,
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
    match cli.command {
        Command::Put {
            file,
            path,
            media_type,
            expected_version,
        } => {
            let bytes = fs::read(&file)?;
            let entry = vfs.put(&path, &bytes, &media_type, expected_version)?;
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
                for (version, content_id, size, created) in history {
                    println!("{version}\t{size}\t{created}\t{content_id}");
                }
            }
        }
        Command::Rm { path } => {
            vfs.remove(&path)?;
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
        } => {
            let mode = SourceMode::parse(&mode)?;
            let report = ingest_dir(&mut vfs, &source, &prefix, mode, dry_run)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::Export { prefix, dest } => {
            let report = export_dir(&mut vfs, &prefix, &dest)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
    }
    Ok(())
}
