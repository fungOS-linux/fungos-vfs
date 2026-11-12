//! Serve a Kubo node to this machine's VFS as a replica.
//!
//! The node is the thing that holds the credentials and the network access, so it
//! is the thing that runs the provider. `fungos-vfs` talks to it over the framed
//! provider protocol and never holds a Kubo client itself — the same split as
//! `unibus-ipfs`: an adapter crate with a hardened third-party client, and a small
//! binary that hosts it.
//!
//! ```sh
//! kubectl-free, one process:
//! fungos-vfs-kubo-provider --listen 127.0.0.1:7201 --kubo-api http://127.0.0.1:5001
//!
//! # then, once:
//! fungos-vfs --root ~/fungos replica add --name kubo \
//!   --kind provider --endpoint 127.0.0.1:7201
//! ```

use std::process::ExitCode;

use clap::Parser;
use fungos_vfs::{ProviderServer, Replica};
use fungos_vfs_kubo::KuboReplica;

#[derive(Parser)]
#[command(about = "Serve a Kubo node as a fungos-vfs content replica")]
struct Cli {
    /// Where to listen. Loopback by default: a provider is reached by the machine
    /// that runs it, and everything else reaches that machine.
    #[arg(long, default_value = "127.0.0.1:7201")]
    listen: String,
    /// The Kubo API endpoint, e.g. `http://127.0.0.1:5001`.
    #[arg(long)]
    kubo_api: String,
    /// What this replica is called in reports.
    #[arg(long, default_value = "kubo")]
    name: String,
    /// Serve reads only, and refuse every write.
    #[arg(long)]
    read_only: bool,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let mut replica = KuboReplica::new(cli.name, cli.kubo_api);
    if cli.read_only {
        replica = replica.read_only();
    }
    let server = match ProviderServer::bind(cli.listen.as_str()) {
        Ok(server) => server,
        Err(error) => {
            eprintln!("fungos-vfs-kubo-provider: {error}");
            return ExitCode::FAILURE;
        }
    };
    match server.local_addr() {
        Ok(addr) => eprintln!(
            "fungos-vfs-kubo-provider: serving {} on {addr}",
            replica.name()
        ),
        Err(error) => eprintln!("fungos-vfs-kubo-provider: {error}"),
    }
    match server.serve(&replica) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("fungos-vfs-kubo-provider: {error}");
            ExitCode::FAILURE
        }
    }
}
