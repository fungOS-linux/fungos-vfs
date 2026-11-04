//! Serve Unibus content fetches from a full-node VFS over the TCP hub.

use std::{fs, path::PathBuf};

use anyhow::Context;
use clap::Parser;
use fungos_vfs::Vfs;
use fungos_vfs_unibus::ContentService;
use unibus::client::{Client, ClientConfig};

#[derive(Parser)]
#[command(version, about = "Serve fungOS VFS content over Unibus")]
struct Cli {
    #[arg(long)]
    root: PathBuf,
    #[arg(long)]
    config: PathBuf,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("fungos-vfs-provider: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config: ClientConfig =
        serde_json::from_slice(&fs::read(&cli.config).context("read Unibus client config")?)
            .context("parse Unibus client config")?;
    let client = Client::connect(config).context("connect Unibus hub")?;
    let vfs = Vfs::open(&cli.root).context("open full-node VFS")?;
    let service = ContentService::new(vfs);
    service
        .serve_client(&client)
        .map_err(anyhow::Error::msg)
        .context("serve content requests")
}
