<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cellar filesystem primitive and POSIX API contract

Status: API design contract; see the
[implementation status](per-cell-filesystem-implementation-status.md) for
implemented behavior and qualification limits.
Prepared 2026-10-25; scope revised 2026-10-27. Inspected source baseline:
`d3206c924031a3f2ca3387b542c02877bb91423d`.
This extends the [filesystem design](per-cell-filesystem-design.md) and
[implementation plan](per-cell-filesystem-implementation.md).
This contract defines the context filesystem primitive only: acquisition,
POSIX-compatible file/directory operations, local transaction composition and
durability. It uses the same invocation and storage services as the other
Cellar application primitives.

## 1. Public primitive and naming

Expose the primitive alongside Cellar's existing storage, SQL, KV and queue
APIs. The canonical accessor is `context.fs()`:

```rust
use cellar::sdk::filesystem::{Filesystem, File, Directory, OpenOptions};

let fs = context.fs()?;
let same_fs = context.storage()?.fs()?;
```

Both accessors address the current root cell's filesystem. There is no binding
name, host directory or global filesystem implicit in this call. A stateless
context, a facet in v1 or a class without the filesystem capability is refused.
The primitive uses the existing cell runtime and storage path. Unlike named
KV/SQL/queue bindings, `fs()` takes no binding argument: it addresses the current
cell's single root. Configuration declares the capability and its quotas.

| Primitive | Accessor | Resource selected |
| --- | --- | --- |
| KV | `context.kv(name)` | Declared named KV binding |
| SQL | `context.sql(name)` | Declared named SQL binding |
| Queue | `context.queue(name)` | Declared named queue binding |
| Filesystem | `context.fs()` | Current root cell's filesystem |
| Filesystem in a local transaction | `tx.fs()` | Same filesystem, bound to that exact transaction |

Filesystem capability registration configures permission policy and captured
limits. `fs()` acquires a checked view of the current cell's state under the
existing invocation. This holds equally for Rust and `ctx.fs` guest facades.
The complete application surface is `Filesystem`, its file/directory handles
and the operations below. Registration and acquisition require no additional
service startup or platform integration.

The public configuration follows the same primitive pattern: `OpenOptions`
describes a file operation, class policy captures permission rules, capabilities
declare supported operations, and limits bound storage and work. Acquiring
`context.fs()` returns a usable view immediately under the current invocation.
There is no separate start stage in the filesystem API.

A path such as `/workspace` identifies a directory inside this cell's namespace.
It has no implicit relationship to a process working directory or host path.
The same path in two cells addresses independently owned state. Public handles
are explicit capabilities obtained from this view, rather than operating-system
file descriptors.

Do not export a new top-level `cellar::sdk::FileSystem`: that exact name already
identifies the host/LTX filesystem abstraction in [sdk.rs](../crates/cellar/sdk.rs).
Keep the new types under `cellar::sdk::filesystem`, with the application-facing
type spelled `Filesystem`. Existing host types and public imports remain intact.

`Filesystem` is a checked storage view, like `Storage`. It retains its original
activation, domain, source generation and optional transaction affinity. Each
operation checks the currently installed native invocation and those identities.
Cloning a view does not create another filesystem or grant new authority. A
retired view cannot silently rebind itself to a later activation.

`Context::fs()` is the normal application entry point.
`Storage::fs()` is needed for transaction composition:

```rust
let db = context.storage()?;
db.tx(|tx| {
    let fs = tx.fs()?;
    fs.write_file("/notes.txt", b"durable note\n")?;
    tx.put_json("note_status", &serde_json::json!({"saved": true}))?;
    Ok(())
})?;
db.sync().await?;
```

`Storage::tx`, `put_json` and `sync` are the existing composition points. Code
blocks in this document illustrate the target filesystem contract; they are
not a substitute for the implementation and test evidence in the status ledger.

Only local state in this cell participates in `tx.fs()` composition. Named KV,
SQL or queue bindings may address other cells/services and do not become part
of this SQLite transaction. Use existing outbox/intent contracts for those effects.
`fs()` is the only public accessor spelling in this design; no `filesystem()`
accessor alias is required.

## 2. One primitive, POSIX operations and bounded helpers

| Surface | Purpose | Atomicity/durability boundary |
| --- | --- | --- |
| POSIX-style SDK | Explicit paths, files, offsets, directories and metadata | One bounded operation or an explicit cell transaction; normal output gates/explicit sync |
| Application conveniences | `read_file`, `write_file`, conditional edits and bounded listing | Built from the same backend; documented whole-operation bounds |

The SDK uses POSIX names and semantics where supported, adapted to typed Rust
and asynchronous guest bridges. It is not a libc ABI, process-global descriptor
table or a promise to implement every POSIX facility. Supported filesystem
operations preserve POSIX behavior, with declared Cellar scope/lifetime limits.
SDK handles cannot be passed directly to `std::fs`, `os.read`, libc or Node `fs`.

Publish a capability profile before an application begins work. The initial
profile covers regular files, directories, metadata and durability barriers.
Links, locks and wider metadata have explicit later capability phases.
Unavailable operations return a defined unsupported error without partial work.

The operation profile is complete through the SDK alone. Its public types,
policy and capability schema describe file operations, storage limits and
authority. Core acceptance exercises that complete profile through SDK calls.

## 3. Core public types

| Type | Contract |
| --- | --- |
| `Filesystem` | Activation-/transaction-bound view of one cell's root |
| `File` | Owned descriptor capability referring to an open file description; no public integer OS fd |
| `Directory` | Owned directory descriptor and enumeration cursor |
| `OpenOptions` | Validated access, create, exclusive-create, truncate, append and creation-mode options |
| `FsPath` / `Name` | Validated byte-preserving paths/components with UTF-8 conveniences |
| `Stat` | Inode, kind, mode, logical uid/gid, size, nlink, mtime/ctime, content version and allocation information |
| `DirEntry` | Raw name bytes, inode identity and kind; not an implied fresh `stat` |
| `Seek` | `Start(u64)`, `Current(i64)` or `End(i64)` with checked arithmetic |
| `DirCookie` | Opaque enumeration position tied to one directory identity/generation |
| `Capabilities` | Supported operations/options plus captured limits and semantics |
| `FilesystemLimits` | Host-captured quotas, request sizes, handle counts and other storage limits |
| `Error` / `Errno` | Typed Cellar failure plus symbolic POSIX errno and known effect outcome |

Use byte paths and checked fixed-width integers in the backend. Public Rust
path arguments accept `FsPath` and ergonomic string/byte conversions. The tables
below abbreviate this as `path`. Cross-language encoding must not truncate
64-bit inode IDs, offsets, sizes, timestamps or versions into JS numbers.

`Stat` does not claim a meaningful access timestamp in the first profile;
capabilities report atime tracking as unsupported/noatime. Do not invent an
updated timestamp on every read. Stat exposes a documented fixed/untracked
atime representation and declares the noatime capability.
Allocation counts distinguish live file allocation from cell-wide retained GC
and receipt storage. `statvfs` reports quota-aware cell availability.

Add `FilesystemLimits` to the existing captured `CellLimits` family and validation
path. Bound storage, transfer, transaction-work and descriptor resources
separately from proof deadlines. An `unrestricted()` convenience cannot disable
integer/codec bounds, authority checks or bounded invocation admission.

Descriptor limits have two scopes: `max_handles` bounds one cell and
`max_host_handles` bounds all live filesystem descriptors in the owning
execution domain. Defaults are 256 and 16,384 respectively. Files, directory
handles and duplicates each occupy a slot. Per-cell exhaustion returns EMFILE;
host exhaustion returns ENFILE. Admission precedes create/truncate and descriptor
publication, so those refusals leave file contents and namespace unchanged.

The host registry contains at most `max_host_handles` weak records. It compacts
closed, transaction-expired and invocation-expired descriptors at the ceiling;
retaining an expired SDK value cannot hold capacity indefinitely. It retains
no invocation or activation owner and is separate for each execution domain.
Accepted background work continues to occupy capacity until its tracked event
ends. The host limit remains finite under `CellLimits::unrestricted()`.

## 4. POSIX-style operation catalog

Signatures show the intended method shape, omitting Rust generics and exact
future aliases. Unless stated otherwise, Rust methods are synchronous bounded
storage operations returning `filesystem::Result<T>`. Guest equivalents are
asynchronous RPCs. Durability barriers return futures in every language.

These synchronous operations target the initial SQLite chunk backend. Transfer
limits are separate from file-size quotas: use repeated bounded pread/pwrite
for large files. Whole-file helpers require bounded input/output. A future
remote-content tier must define asynchronous preparation and cold reads
separately; network I/O cannot occur inside these synchronous transaction scopes.

### File descriptors and I/O

| Proposed Rust method | POSIX equivalent | Initial contract |
| --- | --- | --- |
| `fs.open(path, options) -> File` | open | Open regular file, optionally create/exclusive-create/truncate |
| `fs.creat(path, mode) -> File` | creat | Convenience for write-only create + truncate |
| `file.read(&mut bytes) -> usize` | read | Read from shared open-description offset and advance by returned bytes |
| `file.write(bytes) -> usize` | write | Write at current offset, or atomically append when opened in append mode |
| `file.pread(&mut bytes, offset) -> usize` | pread | Positional read, leaving shared offset unchanged |
| `file.pwrite(bytes, offset) -> usize` | pwrite | Positional write, leaving shared offset unchanged |
| `file.lseek(Seek) -> u64` | lseek | Change shared offset; seeking past EOF does not extend the file |
| `file.fstat() -> Stat` | fstat | Metadata for the opened inode even after rename/unlink |
| `file.ftruncate(size)` | ftruncate | Change size; shrinking does not move the descriptor offset |
| `file.fchmod(mode)` | fchmod | Supported permission bits under the caller's filesystem identity |
| `file.dup() -> File` | dup | New descriptor sharing the same open file description and offset |
| `file.close()` | close | Consume descriptor, release its pin; no implicit durability promise |
| `file.fsync().await` | fsync | Wait for proven prior writes/metadata under current authority |
| `file.fdatasync().await` | fdatasync | May conservatively use the same full-cell proof barrier |

`File` is not implicitly cloneable; `dup()` makes offset-sharing intentional.
Two independent `open()` calls have independent offsets. The backend serializes
offset changes across duplicate handles, including when different tasks issue
operations concurrently. Types must remain usable inside Cellar's existing
`Send` handler futures; an unsafe raw SQLite pointer cannot be their storage.

Descriptor duplication sharing offsets follows the open-file-description model
documented by the [Linux man-pages project](https://man7.org/linux/man-pages/man2/dup.2.html).

Requests have a maximum transfer size. A large `read`/`write` can return a bounded
short count; EOF returns zero for a nonempty read. Each returned positive write
count identifies the committed prefix of that call. A no-progress write of
nonempty input returns an error. `write_all` is a looping convenience and is not
one atomic mutation outside an explicit bounded transaction. A zero-length write
does not extend a file, change contents, advance offsets or update timestamps.

For append, choose EOF and update data/size/offset under one transaction. An
ordinary write advances the offset only by committed bytes; failure before any
mutation leaves it unchanged. Replaying a recorded tool result does not rerun
the underlying write or advance an offset again.
Do not guarantee atomic append of an arbitrarily large buffer split into several
accepted operations. There is no process-global `errno`; errors are values.

SDK `pwrite` follows positional semantics even on an append-open descriptor.
An append option affects `write`, never the explicit offset supplied to `pwrite`.
Test both methods on an append-open descriptor. This follows the documented
POSIX positional contract; Linux has a known differing behavior for its syscall.
[pread/pwrite semantics](https://man7.org/linux/man-pages/man2/pread.2.html).

### Paths, directories and metadata

| Proposed Rust method | POSIX equivalent | Initial contract |
| --- | --- | --- |
| `fs.stat(path) -> Stat` | stat | Metadata for a path; no symbolic links in the first profile |
| `fs.lstat(path) -> Stat` | lstat | Same result as stat in the first profile; preserves the future no-follow distinction |
| `fs.truncate(path, size)` | truncate | Writable regular file only |
| `fs.mkdir(path, mode)` | mkdir | Create one directory; no implicit parent creation |
| `fs.rmdir(path)` | rmdir | Remove an empty directory; root removal refused |
| `fs.unlink(path)` | unlink | Remove a regular-file name; retain its inode while open |
| `fs.rename(old, new)` | rename | Atomic within the same cell; POSIX-style destination replacement rules |
| `fs.chmod(path, mode)` | chmod | Supported mode bits and logical identity rules |
| `fs.utimens(path, times)` | utimensat-style metadata | Explicit mtime updates; atime changes unsupported in initial noatime profile |
| `fs.opendir(path) -> Directory` | opendir | Pin a directory and start enumeration |
| `dir.readdir() -> Option<DirEntry>` | readdir | Next bounded entry or end of directory |
| `dir.telldir() -> DirCookie` | telldir | Current opaque cookie |
| `dir.seekdir(cookie)` | seekdir | Resume a cookie from this directory identity; stale/wrong cookie refused |
| `dir.rewinddir()` | rewinddir | Restart from the beginning |
| `dir.closedir()` | closedir | Consume descriptor and release its pin |
| `dir.fstat() -> Stat` | fstat on directory fd | Metadata for pinned directory |
| `dir.fsync().await` | fsync on directory fd | Await proof of earlier namespace operations |
| `fs.statvfs() -> StatVfs` | statvfs | Cell quota-aware space and file counts |
| `fs.sync().await` | Cellar extension | Existing whole-cell durability barrier, not a machine-wide POSIX sync |

`utimens` accepts explicit validated values and an injected-clock `Now`; omit
unsupported atime changes and permit `Omit`. ctime remains system-maintained
metadata-change time. Host-local process clocks do not override Cellar's injected
clock for SDK operations.

### Directory-relative operations

Path resolution preserves byte names and applies the same rules in every SDK.
Repeated separators and `.` resolve normally; `..` at the cell root stays at
that root. An empty path returns ENOENT, a NUL byte returns EINVAL, and an
overlong component, path or traversal returns ENAMETOOLONG. Intermediate regular
files and a trailing slash on a regular file return ENOTDIR. Rename refuses a
final `.` or `..` with EINVAL; root removal is refused with EBUSY. Creation and
removal apply their operation-specific POSIX error rules. These failures have a
known no-effect outcome.

For `mkdir`/`mkdirat`, an existing root or dot path returns EEXIST. Prefixes
still resolve normally: `mkdir("/missing/.")` returns ENOENT and a regular-file
prefix returns ENOTDIR. `unlink`/ordinary `unlinkat` refuses directories with
EPERM, following the [POSIX unlink contract](https://man7.org/linux/man-pages/man3/unlink.3p.html).
That rule also applies when a directory is reached through `.` or `..`.
Cellar protects the root identity with EBUSY through every alias. For non-root
directory removal, a final `.` returns EINVAL and a final `..` returns
ENOTEMPTY; neither removes an ancestor. Directory-relative forms use the same
rules after resolving the supplied directory capability.
See the [mkdir](https://man7.org/linux/man-pages/man3/mkdir.3p.html) and
[rmdir](https://man7.org/linux/man-pages/man3/rmdir.3p.html) references for the
corresponding POSIX rules.

An open directory that is removed by `rmdir` or destination replacement retains
its inode for `fstat` and cleanup, but enumeration returns EOF, including after
rewind or seeking an earlier cookie. It must not synthesize `.`/`..` after
removal. EOF leaves the current cookie unchanged. New entries cannot be created
through that removed directory handle. Linked directory enumeration continues
to include the two dot entries under the normal retained open rights.

Rename validates trailing-slash and type requirements before treating two paths
to the same inode as a no-op. In particular, `rename("/file", "/file/")`
returns ENOTDIR and preserves both file data and namespace metadata. A valid
same-entry rename changes neither timestamps nor versions. File replacement
keeps an open destination inode readable after its name is replaced; directory
replacement requires an empty destination and updates parent link counts.
These rules follow the [POSIX rename reference](https://man7.org/linux/man-pages/man3/rename.3p.html)
for the declared regular-file and directory profile.

Expose directory-relative operations so callers can pin a directory identity
while its path changes, rather than repeatedly resolving full paths:

```rust
fs.openat(&dir, path, options) -> Result<File>
fs.fstatat(&dir, path, flags) -> Result<Stat>
fs.mkdirat(&dir, path, mode) -> Result<()>
fs.unlinkat(&dir, path, flags) -> Result<()>
fs.renameat(&old_dir, old_path, &new_dir, new_path) -> Result<()>
```

Relative paths resolve from the supplied directory; absolute paths resolve from
the cell root. `..` cannot move above that root. Directory handles from different
cells, activations or transaction views are rejected before effects. No raw
`AT_FDCWD` integer is accepted: root-based methods have no ambient process cwd.
The supplied typed directory capability must be valid even when the path is
absolute. `renameat` validates both parents before namespace mutation; a valid
source parent cannot authorize an invalid destination parent, or the reverse.
`unlinkat(RemoveDirectory)` uses rmdir rules; `fstatat(NoFollow)` preserves the
link-following distinction for the future links profile. Unknown flags fail.

The supported subset uses separate typed `File` and `Directory` values.
`opendir` acquires directory identity for *at operations. Process-relative
`chdir`, `fchdir`, `getcwd`, numeric `dup2`, descriptor inheritance and
close-on-exec have no process-global equivalent in this context API.

### Open modes and permission policy

```rust
let options = OpenOptions::read_write()
    .create(true)
    .truncate(true)
    .mode(0o600);
let file = fs.open("/analysis.json", options)?;
```

Provide read-only, write-only and read-write access constructors plus `create`,
`create_new`, `truncate`, `append` and `mode`. `create_new` means exclusive
creation; on an existing name it fails without truncation. Read-only + truncate
is rejected deterministically. Creation mode does not chmod an existing file.
No-follow behavior and unknown flags are explicit even before symlinks exist.
Flags such as O_PATH, O_TMPFILE, arbitrary O_NONBLOCK and numeric platform bit
masks are outside the first SDK profile. O_SYNC/O_DSYNC are not accepted by the
local synchronous SDK methods; use an explicit asynchronous barrier.

The initial SDK profile uses fixed logical UID/GID 1000 and an immutable
creation mask captured from class policy; it has no process-global `umask()`
setter. The default mask is `0o022`,
configurable before activation. Rust/JS/Python request modes are filtered once
by that mask. Native and guest facades must use the same policy without applying
the mask twice.

Permission checks operate on that logical owner identity, parent search
permissions and file access mode. Permissions are not a claim that SDK-native
code is OS-sandboxed. Changing mode does not revoke an already-authorized open
file description's read/write access. Ownership transitions, setuid/setgid and
ACLs remain unsupported initially. File owner and ctime/mtime rules are part of
the native implementation, not repeated independently in language facades.

### Read-only filesystem policy

Declare `Policy { read_only: true, ..Policy::default() }` when registering the
cell class. `capabilities().read_only` reports the captured policy in every SDK.
It restricts application filesystem operations while local business state and
the owning SQLite connection retain their ordinary transaction semantics.

Valid namespace, data and metadata mutation calls return EROFS / NotApplied
before entering their backend effect. This includes file helpers, `fchmod`,
directory-relative mutations, transactional file effects and explicit
`collect_garbage`. Invalid descriptors/options and failed authority retain their
own errors; a read-only descriptor's write/ftruncate access errors remain EBADF.
Read, seek, duplicate, list, metadata inspection and durability barriers work
under the usual lifetime and authority checks. An all-Omit `utimens` only
resolves the path and is allowed.

An ordinary read-only open with `create(true)` succeeds for an existing regular
file; it returns EROFS if creation is needed. Exclusive create on an existing
entry remains EEXIST. Missing or non-directory prefixes retain ENOENT/ENOTDIR.
This follows the [POSIX open rules](https://man7.org/linux/man-pages/man3/open.3p.html).
Write-access opens are refused before descriptor admission or truncation.

The policy is part of the class configuration, not persisted file mode bits or
a read-only SQLite connection. Reopening a writer's cell under a reader class
policy preserves file contents, metadata and inode identity. A rejected file
effect rolls back its containing storage batch/transaction; a read-only file
check/read can compose with a successful local business-state update.
Framework initialization and durable reclamation remain owner-authorized host
work. The policy is not a point-in-time snapshot of retained allocation.

## 5. Handle lifetime, cleanup and transaction behavior

SDK open descriptors are owned by an invocation's tracked lifetime, including
accepted background work, and by an optional transaction scope. They are not
persisted or serializable and cannot be used from a different invocation even
if the cell is still active. Store a path and application identity for later
work; reopen in its new context. Version checks detect intervening changes.

Every descriptor has a non-reused capability token/generation. Closing one
duplicate releases that descriptor but not the shared open description while
other duplicates remain. Close consumes the Rust handle; further guest use of
its token yields EBADF. Drops schedule/release owned local references through
the existing lifecycle registry; they never silently fsync. Lifecycle shutdown
must release forgotten handles even when guest code omitted close.

`close` after rename/unlink still refers to the inode. Error reporting must
distinguish failed authority from an already-closed descriptor. Cleanup paths
can release memory/pins after retirement but cannot mutate files with stale
authority. Do not blindly retry close on a token that could refer to a newly
allocated object; tokens are never reused within an invocation registry.

### Transaction-scoped descriptors

`tx.fs()` returns a view bound to that exact transaction/savepoint. File
and directory handles opened from it become invalid when the scope commits or
rolls back. Even after a successful commit, callers reopen through a normal
view rather than retaining transaction-affine handles.

For the first release, a preexisting ordinary descriptor cannot be used to
perform filesystem work inside a transaction, and a transaction descriptor
cannot be used through a sibling/nested view. Return a transaction-affinity
error before effects. Open a descriptor through the current transaction view
instead. This avoids leaking speculative offsets or handles across rollback.
Pure handle cleanup remains allowed. Test these rules across async suspension,
savepoint rollback and descriptor duplication.

Do not accept a filesystem durability barrier inside an uncommitted
transaction. Commit first, then await `db.sync()` outside it. Transaction-end
cleanup invalidates escaped handles and releases pins regardless of whether a
language runtime runs destructors promptly.

### Durability and outcomes

Successful synchronous SDK writes mean a local commit, or a mutation of the
current still-open transaction. Handler output gates prove the commit before
an externally acknowledged response; callers can explicitly await `fsync` or
whole-cell `sync`. A closed descriptor is not a durable receipt.

Use the existing typed failure system with a filesystem-specific code/errno,
operation name and effect outcome. Preserve `NotApplied`, `LocallyCommitted`
and `Unknown` distinctions where supported by evidence. A generic I/O exception
does not establish rollback. Do not automatically retry append, truncate,
rename or a whole transaction after an ambiguous response. Applications use
stable operation IDs and atomic tool/effect receipts for reconciliation.

Native `Error::operation` names the outer SDK call, such as `filesystem.open`,
`filesystem.write_all` or `filesystem.fsync`. Acquiring the primitive uses
`filesystem.acquire`. Adding operation context preserves errno, the typed cause,
effect outcome and any confirmed partial progress. Operation names contain no
path or file contents. Guest envelopes use their filesystem bridge method names.

Facade refusals during path, exact-integer, byte-buffer or descriptor argument
conversion report `ValidationError`, symbolic `EINVAL`, the public filesystem
operation, `not_committed` and zero confirmed bytes. JavaScript returns a
rejected promise; Python raises from the awaited coroutine and retains any
buffer-conversion cause. These refusals precede submission of that filesystem
call. They do not change the outcome of any earlier submitted call.
Detached JavaScript views and released or noncontiguous Python buffers are
invalid byte inputs. Valid empty buffers retain normal empty-I/O semantics.

Guest wire validation reports `ValidationError` with `EINVAL` for malformed
shapes, access/seek enums, base64 and decimal syntax. Offset, size, timestamp and
expected file-identity arguments use strict decimal encoding: unsigned fields
contain one or more ASCII digits; signed fields allow one leading minus sign.
Whitespace, a leading plus, fractions, exponents and non-ASCII digits refuse.
Syntactically valid values outside `u64`/`i64` report `EOVERFLOW`. Filesystem
checks and mutations in `storage.apply` use the same unsigned grammar. These
pre-effect refusals retain the outer operation and zero progress; preparation
of a filesystem batch finishes before its transaction begins. Native backend
and proof failures retain their own evidenced outcomes.

Guest success acknowledgements are validated before exposing handles, metadata,
bytes, cookies or counts. Native decimal reply fields are bounded to 20 ASCII
characters, including a possible sign, before numeric conversion and must fit
their signed/unsigned type. This does not shorten the public input grammar.
Binary data must use canonical base64, with read lengths matching their counts.
Transfer and directory-page results must fit the submitted bounds. A malformed
reply reports `HostError`, `EIO`, the outer operation and `unknown`, retaining
the decoding cause. The failed transfer contributes no *confirmed* bytes; it
may have applied effects. Helpers retain earlier confirmed progress without
downgrading an unknown final call. Valid native failures retain their original
code, errno, outcome and progress. Malformed envelope decoding settles the
Python waiter with an error rather than leaving it pending. None of these
failures authorizes an automatic retry of a mutation.

If `Context::fs()` cannot acquire a storage view, acquisition reports
`NotApplied`: it has not entered the filesystem backend. This includes stateless
and retired-context refusals. A retained filesystem or storage view rejected by
cell, activation, source, domain or transaction admission also reports
`NotApplied` for the refused call, including path methods and sync. Its original
cause, public operation and zero confirmed byte progress are retained. This
does not settle an earlier operation whose reply was lost. Backend initialization,
SQLite effects and proof failures retain their own evidenced outcomes.

## 6. Error contract

| Situation | POSIX code | Additional Cellar information |
| --- | --- | --- |
| Missing path / component | ENOENT | No mutation |
| Exclusive create found existing entry | EEXIST | No mutation |
| Non-directory component / regular-file operation on directory | ENOTDIR / EISDIR | No mutation |
| Unlink of a directory | EPERM | Use rmdir or the typed directory-removal flag; no mutation |
| Nonempty directory removal or replacement | ENOTEMPTY | No mutation |
| Invalid/closed descriptor or incompatible descriptor access | EBADF | Handle identity retained for diagnosis, not permission to retry |
| Invalid options/offset arithmetic | EINVAL or EOVERFLOW | Validated before effects |
| Unsupported operation/flag | EOPNOTSUPP | Capability profile identifies support |
| Path/name bounds | ENAMETOOLONG | No mutation |
| Permission / read-only capability | EACCES / EROFS | No mutation |
| File too large / resource quota | EFBIG / EDQUOT | Report written prefix only if committed; otherwise no mutation |
| Host storage exhausted | ENOSPC | Preserve actual commit outcome |
| Too many open handles | EMFILE (cell) / ENFILE (host) | Admission failure before allocation |
| Cross-filesystem rename | EXDEV | Never implement by silent copy/delete |
| Stale invocation/activation | ESTALE or EIO equivalent | Typed ownership/domain/invocation failure preserved in SDK |
| Local transaction/version conflict | No mandatory POSIX equivalent | Typed Conflict/TransactionAffinity returned directly to the application |
| Proof failure or lost reply | EIO equivalent | Known local commit or unknown durable outcome, with receipt reconciliation |

Error errno values are symbolic in the cross-language protocol and never rely
on platform-specific numeric assignments. Paths are not automatically exposed
in telemetry/error messages. Guest errors retain structured fields so application
code need not parse English messages.

## 7. Convenience operations and typed I/O adapters

Provide a small layer over the POSIX backend:

- `read_file(path, max_bytes)` and explicit text decoding helpers.
- `write_file(path, bytes)` for one bounded atomic content replacement on the
  existing inode; preserve its identity and update its version.
- `replace_file(path, bytes)` for atomic publication of a new inode; existing
  open handles continue reading/writing the old unlinked inode until close.
- `edit_file(path, expected_version, patch)` for compare-and-edit in a transaction.
- `read_dir(path, limit, cursor)` for stateless bounded pages.
- `File::read_exact`, `write_all` and `Directory::read_page` as documented loops.

Do not ship unbounded recursive removal/search/copy as a convenience. An agent's
search tool owns a bounded traversal policy. A failed loop can have a completed
prefix outside an explicit transaction; helpers must not imply all-or-nothing
behavior when several primitive calls committed.
Looping file helpers report `completed_bytes` on failure. A failed `write_all`
after successful standalone writes reports a locally committed prefix; an
unknown final call stays unknown. Prefix progress inside an uncommitted
transaction cannot claim local commit. `read_exact` reports consumed bytes and
does not imply that its descriptor offset rolled back. Empty helper calls still
validate descriptor lifetime and access mode.
SDK failures during preliminary capability reads and helper-bound validation
identify the public `read_exact` / `write_all` operation. They report zero
completed bytes before byte I/O and preserve the underlying code, errno and
outcome. An unknown preflight outcome must not be rewritten as a no-commit
claim. Confirmed earlier byte transfers retain the prefix/offset rules above.
Cancellation while a Python filesystem coroutine is running preserves
`asyncio.CancelledError` and its message.
Filesystem calls attach `code=Cancelled`, `outcome=unknown`, the public
`operation` and confirmed `completed_bytes`; looping helpers retain their outer
operation and earlier confirmed prefix. A cancelled wait does not establish
rollback of its last native call. Cancelling a child task does not retire the
parent invocation's descriptor authority. Cancelling before the coroutine starts
cannot run the SDK's annotation code.
Descriptor `readdir` synthesizes `.` and `..`. Stateless `read_dir` returns only
actual entries so application traversals do not follow synthetic dot entries.
For conditional edits, equal-size patches and end-of-file tail replacement
(including append) touch only bounded inserted data, regardless of file size.
An edit that shifts an existing suffix uses the whole-file helper bound and
refuses larger files. These rules also apply inside atomic guest batches.
An empty edit still requires a writable regular file and a matching identity/
version. It leaves file bytes, size, version and timestamps unchanged. Editing
a directory returns EISDIR, including when the insertion and deletion are empty;
an escaping refusal rolls back the enclosing local transaction or guest batch.

Optional `std::io::Read`, `Write` and `Seek` adapters may wrap `File` after their
error/lifetime contract passes tests. `Write::flush` drains any local buffering;
it cannot synchronously perform Cellar's asynchronous durability proof. Document
and require `fsync().await` for that proof. Do not implement `AsRawFd`, pretend to
be `std::fs::File`, or silently replace process-global standard-library calls.

## 8. Rust usage examples

### POSIX-style file operations

```rust
use cellar::sdk::filesystem::{OpenOptions, Seek};

let fs = context.fs()?;
fs.mkdir("/reports", 0o700)?; // Caller handles EEXIST if reuse is intended.
let mut file = fs.open(
    "/reports/summary.txt",
    OpenOptions::read_write().create(true).truncate(true).mode(0o600),
)?;
file.write_all(b"rows=42\n")?;
file.lseek(Seek::Start(0))?;
let mut bytes = [0_u8; 8];
let count = file.read(&mut bytes)?;
file.fsync().await?;
file.close()?;
fs.rename("/reports/summary.txt", "/reports/latest.txt")?;
fs.sync().await?;
```

The file operations are independently committed unless placed in an explicit
transaction. This example does not promise atomic publication of the entire
sequence or crash-safe agent replay.

### One transaction for a file and a tool receipt

```rust
let db = context.storage()?;
db.tx(|tx| {
    // Production code validates stable operation ID and argument fingerprint.
    if tx.get_json(receipt_key)?.is_some() {
        return Ok(());
    }
    let fs = tx.fs()?;
    let mut file = fs.open(
        "/result.json",
        OpenOptions::write_only().create(true).truncate(true).mode(0o600),
    )?;
    file.write_all(result_bytes)?; // Input fits the transaction byte budget.
    file.close()?;
    tx.put_json(receipt_key, &receipt)?;
    Ok(())
})?;
db.sync().await?;
```

A complete agent implementation checks a matching fingerprint before returning
a prior receipt and returns the recorded tool result, not just success.

An agent's file tools acquire this primitive in each invocation. Read tools use
bounded reads and record the returned bytes/version; mutating tools use the
transaction above to commit file changes with their receipt. After recovery,
tools reopen paths with fresh handles and reconcile the saved operation ID.
This workflow uses the ordinary durable cell handler and the SDK alone.

These examples run inside Cellar's existing application invocation. File
contents can include source code, datasets or generated reports. Agents read
and edit those contents through explicit SDK file tools.

## 9. JavaScript and Python facade parity

Both facades expose `ctx.fs`, implemented through the native primitive.
Use `Uint8Array`/`bytes` for contents. File offsets and versions use bigint in the
JS facade with validated decimal-string wire encoding; Python uses bounded ints.
Byte paths remain byte-preserving with explicit UTF-8 conveniences.

Illustrative JavaScript:

```typescript
const fs = ctx.fs;
const file = await fs.open("/notes.txt", {
  access: "read_write", create: true, truncate: true, mode: 0o600,
});
try {
  await file.writeAll(new TextEncoder().encode("hello\n"));
  const bytes = await file.pread(6, 0n);
  await file.fsync();
} finally {
  await file.close();
}
```

Illustrative Python:

```python
fs = ctx.fs
async with await fs.open(
    "/notes.txt", access="read_write", create=True, truncate=True, mode=0o600
) as file:
    await file.write_all(b"hello\n")
    content = await file.pread(6, 0)
    await file.fsync()
```

The Python async context manager closes the descriptor; it does not silently
fsync. Python built-in `open`, `os.*`, and Node `fs` continue to use their host OS
environment. Durable agent file tools explicitly use `ctx.fs`; the primitive
does not redirect those standard-library calls or start an execution environment.

Cross-language descriptor tokens are opaque, invocation-scoped and checked
against the originating domain/cell/generation on every native call. Do not
trust a token string as authorization. Public bridge operations include bounded
`filesystem.open/read/write/pread/pwrite/lseek/fstat/close`, directory operations,
metadata and barriers, with structured outcomes. Opening/closing/dup and
offset-changing reads require handle-state serialization; only side-effect-free
positional reads/metadata may use the guest read concurrency classification.
Guest capabilities report effective transfer/helper/page bounds that fit both
native limits and the encoded bridge envelope; `transport` reports the bridge
value/body/batch ceilings. Sources enabling the filesystem require at least
4096 value bytes for metadata/capabilities/errors. Oversized directory pages
fail before moving a cursor; large descriptor reads return a short count whose
entire result fits. Whole-file reads reject excess size before buffering.

### Atomic guest batches

Sequential descriptor calls across `await` do not establish a transaction.
Extend `storage.apply` with path/inode-version checks and bounded filesystem
mutations, committed with ordinary storage updates. Proposed JSON shape after
the facade encodes bytes:

```json
{
  "checks": {"run": {"present": true, "value": {"phase": "tool"}}},
  "filesystem_checks": [
    {"path": "/notes.txt", "present": true, "inode": "12", "version": "7"}
  ],
  "filesystem_mutations": [
    {"op": "write_file", "path": "/notes.txt", "data_base64": "aGVsbG8K"}
  ],
  "puts": {"tool_receipt": {"operation": "write-note", "result": "saved"}}
}
```

The real agent includes its stable tool identity, digest and complete checked run
state; this abbreviated batch illustrates composition only. Match inode as well
as version to detect path replacement. Batch ops do not accept live descriptor
tokens in v1. Nested filesystem descriptors across multiple guest messages would
require another explicit transaction protocol, not an implicit open transaction.

Admission must include the full checked request and each saved value's future
storage-read envelope. In a monolithic journal, the next checkpoint sends both
the previous snapshot and the new state; checking only the new state's size is
insufficient. Reserve bounded room for retry/terminal state before admitting a
pending checkpoint. File effects must not commit when their journal cannot be
saved and read back. The guest value budget applies to JSON metadata; the body
budget applies to binary bodies. Language-specific JSON escaping counts toward
the request budget. Shared checkpoint validators enforce these bounds for file
tools and the agent examples without changing the core transaction contract.

## 10. Compatibility phases and explicit exclusions

| Phase | API coverage | Delivery requirement |
| --- | --- | --- |
| Core | File/directory catalog, *at variants, dup, modes, noatime, barriers and typed errors | FS-02/03/05 native and guest conformance |
| Links | link/linkat, symlink/symlinkat, readlink/readlinkat, stat/lstat and nofollow distinctions | FS-06 namespace, link-count and path-resolution tests |
| Locks and wider metadata | Advisory lock ownership, ownership changes, ACL/xattr policy where supported | FS-06 invocation/owner-loss release and concurrent-caller tests |
| Additional filesystem operations | Allocation, optimized copy or change observation when justified | A bounded per-operation contract and native/guest parity tests |

Core exposes unsupported-operation errors where capability discovery requires
them; no advertised operation may silently succeed without its effect. Link
support must define absolute targets relative to the cell root, loop/depth
limits, cross-cell EXDEV, rename/link-count rules and traversal after directory
rename. Unsupported links cannot be created through raw framework-table SQL.

The compatibility contract covers explicit SDK file and directory operations.
C ABI compatibility, host-fd export, process-global cwd/umask and
pipes/sockets/devices are outside that profile. Supported operations retain
their declared POSIX semantics within the cell root and SDK handle lifetime.
The capability profile is explicit; it is not a claim of POSIX certification.

## 11. Existing storage clearing and reset behavior

The current [delete-all implementation](../crates/cellar/storage/sqlite/mod.rs)
enumerates tables and drops those not explicitly retained. A `__sys_` name alone
does not preserve new filesystem tables on this path. Implementing the new
primitive must extend this behavior before enabling it.

For core v1, `Storage::delete_all()` refuses with typed `FilesystemPresent` when
there are file entries, non-root inodes, live SDK descriptors or pending
filesystem GC. This follows the existing pattern
of refusing a clear when durable obligations remain. On an initialized but
fully empty/quiescent filesystem it may clear ordinary business state while
preserving the filesystem schema, identity counters and empty root.

Normal `unlink`/`rmdir` operations and bounded GC can reach that empty state.
Do not add an unbounded recursive clear as a shortcut. A future administrative
whole-cell reset needs invocation cancellation, descriptor revocation,
root-generation replacement and bounded reclamation; it is outside the core
POSIX API. Cell deletion/reset must never leave old handles authorized against
reused inode IDs or capability tokens.

## 12. Implementation acceptance additions

The [operation conformance audit](per-cell-filesystem-conformance.md) maps the
public catalog to selected assertions and distinguishes remaining audit work
from the executed evidence in the status ledger.

In addition to the existing filesystem plan, require tests for:

1. `context.fs()`, `storage.fs()` and `tx.fs()` identity and
   permission parity; no collision with the existing SDK `FileSystem` type.
2. Independent-open versus dup-shared offsets; positional I/O preserving offsets;
   seek overflow/negative results; truncate not moving an offset; close of one
   duplicate retaining the inode through another.
3. Short transfer, zero-length I/O and `write_all` partial-progress behavior;
   atomic append boundaries and positional pwrite on append-open descriptors.
4. Creation masks and modes; create-new conflict without truncation; permission
   checks on parents; existing open rights after chmod; unsupported flags.
5. Directory-relative access after rename, root traversal, wrong-cell handles,
   cookie affinity and snapshot limitations during concurrent enumeration.
6. Transaction handle invalidation, ordinary-handle refusal within transactions,
   nested savepoint affinity, offset behavior on rollback and proof-barrier refusal
   inside uncommitted transactions.
7. Invocation end/domain close releasing SDK descriptors; stale guest tokens,
   repeated close, binary filenames and integers beyond JS safe-number range.
8. Guest mutation/receipt batches, file replacement versus content update,
   expected inode/version checks and native/guest error-code parity.
9. `delete_all` refuses live filesystem state and preserves an empty initialized
   schema; it cannot accidentally drop metadata, receipts or recovery state.
10. All core examples, guest operations and file-tool workflows run directly
    through the SDK using the existing Cellar runtime and storage services.
    The public API and capability schema contain only the primitive defined here.

Publish runnable Rust/JS/Python examples only after the APIs exist and are tested.
Until then, label these signatures and examples as proposals in the plans.
