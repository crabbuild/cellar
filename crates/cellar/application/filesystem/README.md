<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Per-cell filesystem primitive

`context.fs()` accesses the current root cell's filesystem in its existing
SQLite database. `context.storage()?.fs()` and `tx.fs()` address the same state.
Files, business data, receipts and alarms can commit together. The primitive is
implemented and remains under [release qualification](../../../../plans/per-cell-filesystem-implementation-status.md).

Enable it explicitly for an already registered cell class with
`Registry::filesystem(class, filesystem::Policy::default())`. Custom native
applications implement `Application::filesystem_policy`; its default refuses
filesystem access. JavaScript and Python hosts use `Source::filesystem` after
registering that class. Stateless handlers and facets cannot use this profile.
Capture host quotas through `CellLimits::filesystem` before admission.

Register `Policy { read_only: true, ..Policy::default() }` for a class that may
inspect existing files while continuing to update its local business state.
`capabilities().read_only` exposes this policy to every SDK. Application file,
namespace and metadata mutations return EROFS; descriptor access-mode errors
remain EBADF. Read-only open with `create(true)` can open an existing file,
while creating a missing file is refused. All-Omit timestamp updates only
resolve the path. Read/seek/dup/list and proof barriers remain available.
The policy does not set SQLite query-only mode or change persisted file modes;
framework initialization and owner-side garbage reclamation still run.
Explicit SDK `collect_garbage` is a mutation and is refused under this policy.

The public module is `cellar::sdk::filesystem`. `Filesystem` is the application
view; the existing SDK `FileSystem` host-storage abstraction is a different type.

## Operations and composition

Rust descriptor/path operations are bounded synchronous SQLite operations;
durability barriers are asynchronous. Guest facades use asynchronous bridge
calls for both. The [API contract](../../../../plans/per-cell-filesystem-api.md)
lists the complete initial operation profile.

```rust
use cellar::sdk::filesystem::{OpenOptions, Seek};

let fs = context.fs()?;
let file = fs.open("/notes.txt", OpenOptions::read_write().create(true))?;
file.write_all(b"hello\n")?;
file.lseek(Seek::Start(0))?;
let mut data = [0; 6];
file.read_exact(&mut data)?;
file.fsync().await?;
file.close()?;
```

Each standalone mutation has its own rollback boundary. For atomic file and
business-state changes, acquire the filesystem from the transaction view:

```rust
let db = context.storage()?;
db.tx(|tx| {
    tx.fs()?.write_file("/result.json", result_bytes)?;
    tx.put_json("result", &receipt)?;
    Ok(())
})?;
db.sync().await?;
```

Check a stable operation ID and request fingerprint before applying a repeated
command. Separately bound KV/SQL/queue services do not join this transaction.
A local return proves local completion; Cellar output gates or explicit
`sync`/`fsync` establish the configured external durability proof. A close/drop
does not supply proof. Barriers inside uncommitted transactions are refused.

Use stable operation receipts to reconcile a lost reply. File versions compare
the visible cell state; durability follows the cell's replication proof. After
recovery discards an unpublished prefix, a later mutation can reuse a provisional
version number from that prefix. A matching version alone cannot identify which
uncertain file effect survived.

File/directory handles belong to one invocation and optional exact transaction.
They expire at invocation retirement or transaction completion. Reopen paths in
later work; never persist a descriptor token. A preexisting ordinary descriptor
cannot join a transaction implicitly, and a sibling/nested transaction cannot
use another transaction's descriptor. `dup` shares offsets; independent opens
have independent offsets. `pread`/`pwrite` preserve offsets, including `pwrite`
on an append-open descriptor. Each accepted append prefix is atomic.

Rename and unlink preserve open inode identity. `write_file` replaces content
on the same inode; `replace_file` publishes a new inode and retains the old one
while open. `edit_file` checks both inode and version before a bounded byte edit.
Equal-size patches and tail replacement/append touch only bounded inserted data
even for large files; shifting an existing suffix uses the whole-file helper bound.
Empty edits still validate regular-file type, write permission and identity/version;
they preserve contents and metadata. Directories return EISDIR even for an empty
edit, and an escaping failure rolls back the surrounding transaction or guest batch.
`Directory::readdir` includes `.` and `..` for linked directories; stateless
`read_dir` pages contain only actual entries. An open directory removed by
`rmdir` or replacement returns EOF even after rewind/seek, while its inode stays
available for metadata and cleanup. Cookies are opaque positions, not consistent
snapshots. EOF preserves the cookie and a removed directory cannot create entries.

## Paths, identity and limits

Absolute paths start at the cell root; relative root methods also start there.
Directory-relative methods resolve from a checked `Directory`. `..` stops at
root. Names preserve raw bytes, are case-sensitive and have no Unicode
normalization. Components are limited to 255 bytes and exclude NUL and `/`.
Use Rust bytes/`FsPath`, JavaScript `Uint8Array` or Python `bytes` for names that
are not UTF-8. Guest integers preserve 64-bit values through decimal wire
strings; JavaScript returns bigint and Python returns int.

The initial profile has a single logical UID/GID of 1000, creation mask `0o022`
(configurable by class policy), owner permission checks and noatime metadata.
It supports regular files and directories, *at operations, modes, explicit
mtime, sparse data and typed errno/outcome errors. It has no links, locks,
ownership changes, ACL/xattrs, numeric OS descriptors or process facilities.
Use `capabilities()` rather than assuming unsupported facilities work.

Namespace errors are operation-specific. Creating an existing root or dot
directory returns EEXIST; ordinary unlink of a directory returns POSIX EPERM.
Root removal through any alias returns EBUSY. Removing a non-root `.` returns
EINVAL and removing `..` returns ENOTEMPTY. Missing and non-directory prefixes
retain ENOENT/ENOTDIR. Directory-relative methods apply these same rules from
their checked directory identity, without changing persistent state on refusal.

| Captured default | Bound |
| --- | --- |
| File / total logical / retained allocation | 1 GiB / 4 GiB / 1 GiB |
| Inodes / live SDK handles | 100,000 / 256 |
| Live filesystem handles across the execution domain | 16,384 |
| Transfer / whole-file helper | 1 MiB / 1 MiB |
| Path / depth | 4096 bytes / 64 components |
| Transaction filesystem work | 1024 operations / 4 MiB charged chunk work |
| Directory page / traversed namespace directories | 1024 / 1024 |
| GC batch / outstanding discarded ranges | 64 chunks or inode work / 64 ranges |

These are admission ceilings, not a qualified capacity/latency claim. A file's
size can exceed a transfer. Process large files with repeated bounded positional
I/O. Helpers reject oversized files before allocating whole-file buffers.

`max_handles` is per cell; `max_host_handles` is shared by that host's execution
domain. Open files, directories and duplicates count individually. EMFILE/ENFILE
refusals happen before create/truncate. Closed or expired capabilities release
capacity even when application code retains their old values. Independent
execution domains have separate registries. Host records are weak and bounded
by the captured host ceiling; background and transaction lifetimes remain part
of admission.
`write_all`/`read_exact` also have helper bounds; successful earlier calls in a
multi-call loop remain committed outside an explicit transaction. Transaction
work is charged across nested savepoints and is not refunded by rollback.
Looping file-helper failures report confirmed `completed_bytes`; a known
standalone write prefix is `LocallyCommitted`, while an unknown final operation
remains `Unknown`. Progress inside an uncommitted transaction is not a commit
claim. Empty helpers still check descriptor identity and access mode.

Guest capabilities report effective transfer/helper/page limits reduced for
the body and encoded result budgets. Their `transport` field reports the
bridge ceilings. Filesystem-enabled sources require `max_value_bytes >= 4096`.
Directory requests exceeding the effective page limit fail before moving the
cursor. Reads return a bounded short count that fits the result envelope.

File size, inode count, logical sizes of retained inodes and physical chunk
allocation are accounted separately. Sparse holes do not allocate chunks.
Pending garbage remains charged. Unlinked open inodes stay accessible until
their last live pin is released. Quotas permit reclamation when a lowered limit
is below current usage, while refusing growth beyond that limit. Positive growth
is checked before counter updates; overwriting existing allocation is permitted
at the quota. Integer-limit quota refusals preserve exact usage accounting.

## Backend, recovery and failure behavior

The [SQLite backend](../../storage/sqlite/filesystem/mod.rs) uses reserved
`__sys_fs_` tables on the isolate's owning connection. It stores fixed 64 KiB
chunks and metadata; application SQL cannot modify framework tables. Truncate
zeros the boundary tail and records revision-bounded discarded ranges before
bounded reclamation. Rewrites get newer revisions and cannot be erased by old
GC work. Existing durable work discovery resumes maintenance after recovery.

Files follow the cell's existing WAL/LTX, proof and recovery path. No descriptor
survives activation replacement. Stale authority cannot read or mutate state.
`delete_all` refuses live filesystem entries, retained inodes, live descriptors
or reclamation obligations; an empty quiescent filesystem retains its schema,
root and identity counters. It is not an unbounded recursive reset.

Errors include symbolic errno, operation context and effect outcome. Preserve
`NotApplied`, `LocallyCommitted` and `Unknown`; guest spellings are
`not_committed`, `locally_committed` and `unknown`. A lost reply is not evidence
of rollback. Reconcile an atomic receipt before repeating a mutation.

Failure to acquire a storage view in `Context::fs()` is `NotApplied`, with the
original cause retained: no filesystem backend work has started. This does not
reclassify backend initialization, commit or proof failures.

Authority/transaction admission on a retained filesystem or storage view also
returns `NotApplied` before the refused operation enters the backend. This covers
wrong-cell and retired-activation views as well as domain/source/transaction
refusals. Errors retain their cause and outer operation. The outcome describes
this call and cannot resolve an earlier lost reply.

The internal format marker remains 1. Enabling the primitive refuses development
databases whose work-table constraint predates `filesystem-maintenance`.
Recovery also refuses the older add-before-subtract resize-accounting trigger,
which could lose integer precision at extreme logical sizes. There is no
automatic legacy migration or destructive reset.

Owner-routed aggregate inspection is available through
`cellar::management::FleetClient::filesystem(scope)` and the authenticated
[console filesystem endpoint](../../../cellar-admin/README.md#cell-filesystem-usage).
Sampling never initializes the primitive or returns paths/content. It reports
captured quotas, revision, retained allocation and live handle/pin counts,
with fixed work budgets and explicit unavailable responses.

The [session agent example](../../../../examples/filesystem-agent/README.md)
uses checked file tools and saved decisions. Mounting and command execution
belong entirely to the separate [optional adapter design](../../../../plans/per-cell-filesystem-fuse-design.md).
