#!/bin/sh
# The gate. Run this before claiming a change is verified.
#
# The scope of each line is load-bearing, and every step below exists because a
# narrower version of it reported success while something else was broken:
#
#  - `cargo clippy --all-targets` without `--workspace` selects only the root
#    package when the root is itself a package, so member crates went unchecked.
#  - `cargo test --workspace` skips binaries gated behind `required-features`,
#    which is how `crates/fungos-vfs-fuse` stopped compiling without a signal.
#  - `cargo fmt --all` covers every member; the `--all` is not decoration.
#
# A gate that answers a narrower question than the one being asked is worse than
# no gate, because it is trusted.
set -eu

# The sccache wrapper breaks this build outright.
unset RUSTC_WRAPPER SCCACHE_CONF SCCACHE_BASEDIRS
export RUSTC_WRAPPER=

echo "== fmt (workspace) =="
cargo fmt --all -- --check

echo "== clippy (workspace, all targets) =="
cargo clippy --workspace --all-targets -- -D warnings

echo "== check (feature-gated binaries) =="
cargo check -p fungos-vfs-fuse --features fuse

echo "== test (workspace) =="
cargo test --workspace

echo "gate: ok"
