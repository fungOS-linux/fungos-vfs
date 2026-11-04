//! Hydrate one VFS path through Unibus and write the verified bytes.

use std::{fs, path::PathBuf, sync::Arc};

use anyhow::Context;
use clap::Parser;
use fungos_vfs::{NodeRole, Vfs};
use fungos_vfs_unibus::UnibusHydrator;
use unibus::client::{Client, ClientConfig};

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

#[derive(Parser)]
#[command(version, about = "Hydrate a fungOS VFS file through Unibus")]
struct Cli {
    #[arg(long)]
    root: PathBuf,
    #[arg(long, value_enum, default_value_t = RoleArg::Cache)]
    role: RoleArg,
    #[arg(long)]
    config: PathBuf,
    path: String,
    #[arg(long)]
    provider: Option<String>,
    #[arg(long)]
    output: Option<PathBuf>,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("fungos-vfs-fetch: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config: ClientConfig =
        serde_json::from_slice(&fs::read(&cli.config).context("read Unibus client config")?)
            .context("parse Unibus client config")?;
    let client = Arc::new(Client::connect(config).context("connect Unibus hub")?);
    let hydrator = UnibusHydrator::new(client, cli.provider);
    let mut vfs = Vfs::open_with_role(&cli.root, cli.role.into()).context("open VFS")?;
    let bytes = vfs
        .get_or_hydrate(&cli.path, &hydrator)
        .with_context(|| format!("hydrate {}", cli.path))?;
    if let Some(output) = cli.output {
        fs::write(&output, &bytes).with_context(|| format!("write {}", output.display()))?;
        println!("fungos-vfs-fetch: ok bytes={}", bytes.len());
    } else {
        print!("{}", String::from_utf8_lossy(&bytes));
    }
    Ok(())
}
