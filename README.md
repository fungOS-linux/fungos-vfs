# fungos-vfs

**Local-first content-addressed VFS for fungOS.**

A workspace is a SQLite namespace plus a content-addressed blob store. The
filesystem is local and fast; distribution is a separate sync/transport layer.

Status: **working slice.** Local full-node and pull-through cache behavior is
implemented. Two stores converge by pull-based anti-entropy over ssh or QUIC
(see [Sync](#sync)). Unibus ranged fetching and serving work over the TCP hub
and authenticated Iroh peer sessions. A FUSE adapter exposes the VFS as a
native filesystem on macOS and Linux.

## Model

- **Full node** — can write, retain versions, serve blobs, and pin content.
- **Cache node** — pull-through only: registers placeholders, hydrates blobs,
  leases open files, evicts old content, and refuses authoritative writes.
- File paths are workspace-relative and cannot escape the root.
- Each write creates a new immutable `ContentId: blake3:<hex>` version.
- Sync queue entries are durable and acknowledged explicitly.
- Leases protect open files from eviction.
- Eviction changes a file to `placeholder` but preserves namespace/history.

## CLI

```sh
cargo run -- --root /tmp/fungos put ./README.md docs/README.md
cargo run -- --root /tmp/fungos ls
cargo run -- --root /tmp/fungos stat docs/README.md
cargo run -- --root /tmp/fungos history docs/README.md
cargo run -- --root /tmp/fungos get docs/README.md
cargo run -- --root /tmp/fungos pending
cargo run -- --root /tmp/fungos evict --max-bytes 0
```

Pull-through cache:

```sh
cargo run -- --root /tmp/cache --role cache placeholder \
  big.bin blake3:<hex> 4096 --media-type application/octet-stream
cargo run -- --root /tmp/cache --role cache hydrate big.bin ./big.bin
cargo run -- --root /tmp/cache --role cache lease big.bin --owner app --ttl-ms 60000
```

## Pull-through hydration

`Hydrator` is the transport seam for cache nodes:

```rust
vfs.get_or_hydrate("big.bin", &source)?;
```

The VFS verifies the fetched bytes against the registered `ContentId` before
marking the file present. The first Unibus L2 wire types are included:

```text
unibus.content.fetch.v1
unibus.content.data.v1
unibus.content.error.v1
unibus.content.available.v1
```

The Unibus client adapter is `crates/fungos-vfs-unibus`. It sends bounded
`unibus.content.fetch.v1` frames and reconstructs from correlated
`unibus.content.data.v1` ranges:

```rust
use std::sync::Arc;
use fungos_vfs_unibus::UnibusHydrator;
use unibus::client::Client;

let client = Arc::new(Client::connect(config)?);
let source = UnibusHydrator::new(client, Some("content-node".into()));
let bytes = vfs.get_or_hydrate("big.bin", &source)?;
```

## Native filesystem

```sh
cargo build --release -p fungos-vfs-fuse --features fuse
~/.local/bin/fungos-vfs-fuse --root ~/fungos-node --mount ~/fungOS
```

See [`docs/native-fs.md`](docs/native-fs.md). macOS uses macFUSE; Linux uses
FUSE3.

## Sync

Two stores converge by exchanging **ops**, not files. The op log already records
who wrote, when, in what order, and against which head, so the exchange can say
"this write was made against a head you no longer have" — which is a fact a
file-level sync cannot express.

It is pull-based anti-entropy: a store asks a peer for what it is missing. The
carriers are ssh (default, no daemon, no port) and QUIC.

```sh
# On the peer: answer one request on stdin. This is what ssh gives it.
fungos-vfs --root ~/fungos-node sync serve

# Here: pull from that peer's store into this one.
fungos-vfs --root ~/fungos-local sync pull \
  --remote agora-one --remote-root /srv/fungos

# Same exchange over QUIC. `serve` prints the fingerprint to pin on the client.
fungos-vfs --root /srv/fungos sync serve --quic 0.0.0.0:7701 --cert /etc/fungos/quic.key
fungos-vfs --root ~/fungos-local sync pull --quic agora-one:7701 \
  --fingerprint <hex>
```

`--remote` is whatever `ssh` accepts, because `ssh` is what runs — unless
`FUNGOS_VFS_SSH` names another carrier. A Boat sandbox is reached with
`boat-ssh <id> <command>`, which takes the same two arguments and none of ssh's
options:

```sh
FUNGOS_VFS_SSH=boat-ssh fungos-vfs --root ~/fungos sync pull \
  --remote bx_j9txh4ms --remote-root /home/user/vfs --remote-bin /home/user/fungos-vfs
```

That is what lets a VM holding no keys for anything be a peer rather than a place
to copy files into. Verified end to end on a Boat sandbox: two edits recorded on
the VM, pulled here over `boat-ssh`, and laid over a checkout of the same base
with `checkout` — `replaced: 2`, eight files untouched.


`pull` reports what happened to each op:

```json
{"applied": 12, "duplicate": 0, "diverged": 1, "snapshot": false}
```

- **`applied`** — the op is now in this store.
- **`duplicate`** — already had it, recognised by `(origin, hlc)` before any work.
- **`diverged`** — the op names a head this store has moved past. It is **not
  merged**. Applying it would discard the intervening write, so it is held as
  unresolved state instead. This is the deliberate difference from a
  last-writer-wins layer: a filesystem that silently picks a winner loses an
  edit, and the person who lost it is never told.
- **`snapshot`** — this store had fallen behind a compaction horizon, so it was
  handed the live namespace at a watermark rather than a delta it could not
  recognise as short.

Content follows the ops: an op names content ids this store may not hold, and
the bytes are fetched by id and verified against it before being stored.

### Divergence, and where the other version went

A refused write is not thrown away. Its bytes are written beside the file as
`<name> (diverged from <store> <date>)<ext>`, so the other version is something
you can open rather than a number you have to take on faith:

```sh
fungos-vfs --root ~/fungos-local sync diverged
fungos-vfs --root ~/fungos-local sync resolve --path docs/plan.md --keep remote
fungos-vfs --root ~/fungos-local sync resolve --path docs/plan.md --keep local
```

- `diverged` lists every refusal still held: the path, the store it came from,
  where its bytes went, and how long it has been waiting.
- `--keep remote` makes the copy the file and forgets the refusal; the copy is
  removed because it is now redundant.
- `--keep local` keeps what is here, forgets the refusal, and **leaves the copy**
  — it is then the whole record of what the peer wanted to write.

### Compaction

An op log that only grows eventually costs more to scan than the work it
describes. `compact` drops rows older than a cutoff:

```sh
fungos-vfs --root ~/fungos-node sync compact --older-than 2592000  # 30 days
```

Dropping rows is the easy half. A peer whose cursor now sits below what was
dropped cannot be caught up from the log at all — the ops that would fill its
gap are the ones that are gone — so it is told so and given a snapshot instead.

Divergences are the other half, and they are why the copy above exists. An op
below the cutoff is either **forgotten** because its bytes are on disk, or
**reported as a pin** because they are not — the same clock, one decision.
That ordering is load-bearing: refusing a write deletes its receipt, so an op
the horizon has passed reads as a duplicate, and a write that never landed
would look like one that had. The copy is what lets the horizon advance
honestly. In practice this means an unresolved divergence costs **disk** (paid
at the moment of refusal) instead of a log that no one can compact.

The report says which happened: `expired_divergences` per store, and `pinned`
for the ones still holding the pass back.

### What sync is not

- **Not push.** Nothing watches the filesystem, so convergence happens when a
  pull runs. A live-sync daemon is future work.
- **Not a merge.** Concurrent writes to the same path surface as `diverged` for
  a human to settle. Neither side wins automatically; the version that lost is
  kept as a copy so the decision can be made by reading both.
- **Not a server.** No node is authoritative; any store can pull from any other.

## Projects

Attaching a repository is how a desktop gets content. It does **not** copy the
repository: Git already content-addresses its own tree, so a clean checkout
becomes one pointer.

```sh
fungos-vfs --root ~/fungos-local project attach ~/src/soil
fungos-vfs --root ~/fungos-local project list
```

What attach records:

- the **Git pointer** — commit and tree — instead of one blob per tracked file;
- the **worktrees** Git knows about, so the desktop can see what is checked out
  and where, without a second copy of any object;
the **in-between** Git cannot hold: a dirty worktree cannot be described by a
  tree id, so one that has not been recorded is stored as blobs — its whole
  checkout, not only the changed files — and the report says `git.dirty` so the
  difference is never implied. Record the worktree and it costs its diff instead;
  see [Worktrees](#worktrees).

The policy it attaches, in full:

```text
git: **

exclude: target/
exclude: node_modules/
exclude: .venv/
exclude: dist/
exclude: build/
```

Last matching rule wins, which is why the exclusions come second: a repository
that commits its build output must not have that output treated as source.

A *dirty* worktree cannot use the tree pointer, so attaching one stores its whole
checkout as blobs — unless it has been recorded, in which case only the
in-between is written (see [Worktrees](#worktrees)). The report says
`git.dirty` either way, so the difference is never implied.

A clean repository of one file attaches as **one tree pointer and zero bytes
written**. Change a file and the next attach writes that file — the rest is
still the tree's.

## Worktrees

A worktree is where work happens; the namespace is where it is recorded. A
worktree is a real directory on the local filesystem — `cargo`, an editor, and
`rg` behave exactly as they always have, and `target/` survives between builds
because nothing is remounting it. The VFS sits beside that directory, not under
it: it holds the base, the diff, and the history.

```sh
fungos-vfs --root ~/fungos-local worktree materialize \
  --project ~/src/soil --name fix-parser --path ~/src/soil-fix-parser
fungos-vfs --root ~/fungos-local worktree record  --name fix-parser
fungos-vfs --root ~/fungos-local worktree changes --name fix-parser
fungos-vfs --root ~/fungos-local worktree list
```

**Handing a worktree to another machine.** `record` writes the work, `checkout`
puts it back into a working tree somewhere else. The address and the base travel
in the project marker that `project attach` synced; the bytes travel as content.
Nothing is shared between the two machines but the wire:

```sh
# where the work is
fungos-vfs --root ~/fungos worktree record --name fix-parser
fungos-vfs --root ~/fungos project attach ~/src/soil

# where the work is wanted
fungos-vfs --root ~/fungos sync pull --remote agora-one --remote-root ~/fungos \
  --remote-bin fungos-vfs
git clone agora-one:src/soil ~/src/soil
git -C ~/src/soil checkout <the recording's base>
fungos-vfs --root ~/fungos worktree checkout --name fix-parser --path ~/src/soil
```

Verified both ways between a Mac and `agora-one`: three edits recorded on one
side reached the other as three modified files and nine untouched; an edit made
on the far side came back over QUIC and was laid over the near side's checkout
without disturbing its own three (`kept: 3, replaced: 1`).

`checkout` has three outcomes per path and one refusal: nothing there (write it),
already the recorded content (nothing to do), the base commit's version (replace
it, because that is what "changed this file" means), and anything else — **refuse**,
name the paths, and write nothing at all. A file that is neither what was recorded
nor what the commit says is somebody else's edit; a refusal that still wrote half
the plan would be worse than no checkout.

It writes files, never namespace entries, and it records a row so the far side
becomes a peer rather than a reader: its first `record` adopts the synced state in
one pass writing no ops, and the next edit reaches the other machine with that
machine's actor on it.

**Recording costs the diff, not the tree.** `record` asks Git which paths differ
from the base commit, hashes only those, and writes one op for each that differs
from what the namespace already holds. Three edits in a 678-file checkout cost
three ops and zero bytes for the other 675 — the rest is still the commit's
tree, which Git already content-addresses. A second `record` with nothing changed
is **zero ops and zero bytes written**.

**Materializing is nearly free.** Files the commit covers come from Git's own
checkout. Only the recorded in-between is streamed out of the content store, and
on APFS it is placed by `clonefile` — a reflink, not a copy. The report says which
happened (`placed` against `cloned`).

**Adopting is the usual case.** A worktree exists because an agent or an editor
made it; the recording arrives second. `materialize` pointed at an existing
checkout of the same repository at the same commit adopts it — it writes nothing,
records the address, and the next `record` describes whatever is already
different. Pointed at a directory that is not a worktree of that repository, it
refuses and says so.

**`project attach` prefers the recording.** A recorded worktree attaches as one
pointer plus its in-between, not as a walk of the checkout. That is what repairs
the old cost: attach a dirty checkout and it still stores the whole tree; record
that worktree, attach again, and the namespace becomes the three files that
actually changed, with the rest let go (`removed: 17` in a twenty-file checkout).
The report says `recorded: true` when this happened, so "attached cheaply" and
"attached the old way" are never confused.

A recording owns its namespace address, computed by one function for both
`materialize` and `attach`: the project's main worktree *is* `projects/<name>`,
and every other worktree hangs under `projects/<name>/worktrees/<label>`.

Two rules make an interrupted record safe:

1. **The plan is written before it is applied.** Every op lands in the journal in
   one transaction, with its content already filed. A run that dies leaves a plan
   the next run finishes — no duplicate ops, no lost file.
2. **Ops are committed before the base advances.** Each op is its own transaction;
   the recorded base moves only after they have all landed, in the same transaction
   that clears the journal. An op that already landed is recognised by looking at
   the namespace, not by trusting a flag that could itself be what was lost.

A record holds a **lease** on the worktree for as long as it runs. A second record
is refused and names the holder, which is the same rule as an open-file lease one
level up: two writers to one working tree is a conflict pretending to be a diff.

**The change feed** is what a harness polls. `changes --since <cursor>` returns
`{path, op, author, origin}` for everything that happened after a cursor, including
ops that arrived from a peer and removals — which are recorded as tombstones,
because Git's tree pointer covers files the namespace never held.

**Nothing cleans up after a worktree on its own.** Landing a PR, closing a thread,
or ending a session leaves the checkout and its row exactly where they were, which
is why `prune` exists and why it reports before it acts:

```sh
fungos-vfs --root ~/fungos worktree prune              # what would go, and why
fungos-vfs --root ~/fungos worktree prune --apply      # forget those
fungos-vfs --root ~/fungos worktree prune --name fix-parser --apply
```

There is no `--older-than`. Each row is decided by what it holds, not by its age:

| state | removable | why |
|---|---|---|
| `clean` | yes | the recording holds no in-between and Git reports nothing uncommitted, so every file is described by a commit |
| `missing` | yes | the directory is gone; the row was a promise about nothing |
| `recorded` | no | the in-between exists only as ops — dropping the row drops the only pointer to it |
| `dirty` | no | the disk holds work the recording has not seen |
| `leased` | no | something is recording or checking out right now; the refusal names the holder |
| `kept-by-git` | no | `git worktree remove` refused — the main worktree, the one you are standing in, or the one holding a file Git does not know |

The directory goes through `git worktree remove`, so git's own refusals are the
guard rather than a copy of them kept here. `clean` is deliberately **not** the same
question as *merged*: a prune loses a directory and a row, never a commit — the
commits stay in the repository behind their branch, and `worktree checkout` rebuilds
the directory from the row's base. Ignored files are the one thing `clean` cannot
see, and git removes them without complaint, which is right for `target/` and is why
`--name` exists for the one worktree holding something a commit does not describe.

What it deliberately does not do, yet:

- **Plain directories.** A worktree is a Git worktree; there is no cheap base for
  anything else, and inventing a per-file one would cost a hash of everything on
  every record. Refused rather than approximated.
- **Ignored files.** Git does not report them, and the attached policy excludes
  build output anyway. Same rule as `project attach`, same policy.
- **Renames** are recorded as a removal and an upsert — one blob, which is what a
  rename costs to store.
- **Non-Git directories.** There is no cheap base for one, and hashing everything
  on every record is the cost this design exists to avoid. Refused rather than
  approximated — a content-addressed store for plain files is the other half of
  the story, and it is a different one.

## The cache class

Build output is the largest thing on a developer machine and the only large thing
a namespace has no business versioning. A fourth class stores it without making it
a fact:

```text
cache: target/      stored in the CAS, never versioned, evictable
```

| | `Git` | `Blob` | `Cache` | `Exclude` |
|---|---|---|---|---|
| in the CAS | no (tree id) | yes | yes | no |
| version history | tree | yes | **no** | no |
| in the op log | yes | yes | **no** | no |
| a reader fetches it by | tree | path | **content id** | — |
| dropped by | nothing | reachability (`gc`) | **size (`evict`)** | — |

```sh
fungos-vfs --root ~/fungos cache put target/debug/app --path target/debug/app
fungos-vfs --root ~/fungos cache ls
fungos-vfs --root ~/fungos cache pin   --content-id blake3:<hex>
fungos-vfs --root ~/fungos cache evict --max-bytes 20000000000
```

Four rules, and each is the reason the class exists:

- **A cache write is not an op.** No namespace entry, no version, no author, no
  row for a peer to replay: a compiler's scratch files are not edits. What you get
  back is a `ContentId`, and that is the only handle — cache content is fetched by
  id, never by path.
- **`gc` keeps cache content; `evict` is what forgets it.** Collecting it in both
  places would be two collectors racing to delete the same bytes on different
  rules. Reachability is the rule for everything else; the ceiling is the rule for
  this.
- **Eviction is by size, oldest-use first.** Age alone is the wrong axis: a
  twelve-hour-old build tree may be the only copy of an hour of work. Pinned
  entries are skipped, so a changeset can name an artifact and have it survive the
  ceiling.
- **Eviction forgets the entry, not the bytes.** The store is shared, so dropping
  one root's claim leaves the content to the next `gc` — which is what makes
  eviction cheap and safe to run while other roots are working.

Two things are deliberately not done yet, and both are about a build that is
*running*: a tree-scoped lease (today `Lease` holds one path, so `evict` can drop
the tree under a build), and who renews it — the build by heartbeat, or the caller
for its lifetime. Neither changes the storage rule above; both matter before this
turns into a mount.

**`project attach` still says `exclude: target/`, and should.** Attaching walks
`git ls-files`, which never lists an ignored `target/`, so switching that rule to
`cache:` would only ever reach a repository that tracks its own build output — and
there the bytes are already in the git tree. *Source* is what attaching is for.
The cache is for the thing that produces build output, which is the builder: it
knows the ceiling and files ``PathClass::Cache`` entries itself.

## Pieces

A blob is fetched in pieces, from any source that has them — the one idea worth
taking from BitTorrent. The difference is not speed on a good day: it is whether
a machine holding **half** of something is useful to a machine holding the other
half.

```rust
let manifest = Manifest::of(&bytes);
let mut have = empty_have(&manifest);
let assembled = fetch(&manifest, &[&studio, &agora_one], &mut have)?;
```

- **256 KiB pieces**, the UnixFS default, so a UnixFS producer could become one
  more `PieceSource` without changing anything here.
- **Each piece carries its own blake3 id**, so it is verifiable on its own and no
  single peer has to be trusted. A source that answers with the wrong bytes is
  refused and the next one asked.
- **`have` is the resume state.** An interrupted fill keeps what it already
  verified, and a later attempt asks only for the gaps — resumption is a
  consequence of the representation, not a separate feature.
- **The whole thing is still checked at the end**: a complete set of individually
  valid pieces in the wrong order is a `WholeBlobMismatch`, not a corrupt file.

The wire already carried ranges (`unibus.content.fetch.v1`, bounded by
`MAX_RANGE_BYTES`); what was missing was a manifest making each range
independently checkable, which is what lets the ranges come from different peers
instead of being concatenated from one.

## Next

1. File Provider / Cloud Files adapter for a signed macOS app.
2. Sparse and page-granular hydration.
3. Blob provider adapters: local CAS, Kubo, iroh-blobs.
4. Conflict/version UI.
5. LOD-aware sync policy.
6. A live-sync daemon: watch the mount, push on change, so convergence is not
   something a person has to remember to ask for.

## License

GPL-3.0-or-later. See [LICENSE](LICENSE).

Deployment notes for studio and agora-one are in [`docs/deployment.md`](docs/deployment.md).
