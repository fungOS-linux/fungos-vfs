//! Serve an S3/R2 bucket to this machine's VFS as a replica.
//!
//! The bucket credentials live where the network does, so the provider runs there.
//! `fungos-vfs` talks to it over the framed provider protocol and never holds a
//! signer.
//!
//! ```sh
//! export R2_ENDPOINT=https://<account>.r2.cloudflarestorage.com
//! export R2_ACCESS_KEY_ID=... R2_SECRET_ACCESS_KEY=...
//! fungos-vfs-r2-provider --bucket fungos-backup --listen 127.0.0.1:7202
//!
//! fungos-vfs --root ~/fungos replica add --name cold \
//!   --kind provider --endpoint 127.0.0.1:7202
//! ```

use std::process::ExitCode;

use clap::Parser;
use fungos_vfs::{ProviderServer, Replica};
use fungos_vfs_r2::R2Replica;

#[derive(Parser)]
#[command(about = "Serve an S3/R2 bucket as a fungos-vfs content replica")]
struct Cli {
    /// The bucket to keep copies in.
    #[arg(long)]
    bucket: String,
    /// Where to listen. Loopback by default.
    #[arg(long, default_value = "127.0.0.1:7202")]
    listen: String,
    /// What this replica is called in reports.
    #[arg(long, default_value = "r2")]
    name: String,
    /// Serve reads only, and refuse every write.
    #[arg(long)]
    read_only: bool,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let mut replica = match R2Replica::from_env(cli.name, cli.bucket) {
        Ok(replica) => replica,
        Err(error) => {
            eprintln!("fungos-vfs-r2-provider: {error}");
            return ExitCode::FAILURE;
        }
    };
    if cli.read_only {
        replica = replica.read_only();
    }
    let server = match ProviderServer::bind(cli.listen.as_str()) {
        Ok(server) => server,
        Err(error) => {
            eprintln!("fungos-vfs-r2-provider: {error}");
            return ExitCode::FAILURE;
        }
    };
    match server.local_addr() {
        Ok(addr) => eprintln!(
            "fungos-vfs-r2-provider: serving {} on {addr}",
            replica.name()
        ),
        Err(error) => eprintln!("fungos-vfs-r2-provider: {error}"),
    }
    match server.serve(&replica) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("fungos-vfs-r2-provider: {error}");
            ExitCode::FAILURE
        }
    }
}
