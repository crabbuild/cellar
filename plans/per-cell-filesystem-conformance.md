<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Filesystem operation conformance audit

Status: partial audit, 2026-10-27. This inventory follows the
[public API contract](per-cell-filesystem-api.md). Each row names behavioral
assertions inspected in the current fixtures; it does not establish complete
POSIX conformance or completion of the corresponding requirement group.
Executed suites and qualification limits belong in the
[status ledger](per-cell-filesystem-implementation-status.md).

## Fixture boundaries

| Label | Source and boundary |
| --- | --- |
| Native | [Native filesystem harness](../crates/cellar/tests/native_filesystem.rs): `/exercise`, `/contracts`, authority/lifetime routes and restart |
| Contracts | [Native semantic helpers](../crates/cellar/tests/support/filesystem_contracts.rs): `transactions`, `metadata`, `directory_relative`, `foreign_directory`, `directory_mutation`, `namespace`, `reserved_namespace` and `preflight_authority` |
| Read-only | [Writer/reader harness](../crates/cellar/tests/support/filesystem_readonly.rs): recovery, EROFS/EBADF, unchanged metadata/usage and transaction rollback |
| Admission | [Native descriptor admission](../crates/cellar/tests/support/filesystem_admission.rs): EMFILE/ENFILE before create/truncate and capacity release |
| Guests | [JavaScript](../crates/cellar-javascript/tests/support/filesystem.js) and [Python](../crates/cellar-python/tests/filesystem.py): real bridge calls driven by the [shared host client](../crates/cellar/tests/support/filesystem_guest.rs) |
| Budget | [Node](../crates/cellar-javascript/tests/support/filesystem-budget.js) and [Python](../crates/cellar-python/tests/filesystem_budget.py): short transfers, encoded pages and receipt/checkpoint admission |
| Backend | [SQLite tests](../crates/cellar/storage/sqlite/filesystem_tests.rs): persistent-table comparisons, chunk/GC invariants, quotas and exhaustion |

Rust uses snake_case names. The JavaScript facade uses camelCase where the SDK
declares it; Python uses snake_case. Guest transaction composition is the checked
`storage.apply` batch, rather than a callback transaction held across bridge calls.

## Access and file descriptors

| Audit ID | Public operation | Selected assertions inspected |
| --- | --- | --- |
| O01 | `context.fs()` | Native and Guests acquire the current cell; stateless access refuses. Native activation replacement and facet tests check admission boundaries |
| O02 | `Storage::fs()` / `tx.fs()` | Contracts checks exact nested transaction affinity, rollback and expiry; Read-only checks policy parity; guest file/state/receipt effects use one native batch |
| O03 | `fs.capabilities()` | Native/Guests check unsupported links and captured limits; Read-only checks the policy flag |
| O04 | `fs.open()` | Native/Guests exercise independent offsets, modes and create/exclusive flags; Read-only and Admission check errors before namespace/content effects |
| O05 | `fs.creat()` | Contracts `directory_relative` and guest metadata profiles verify mode and contents; Native/Read-only check admission/access refusals |
| O06 | `file.read()` | Native/Guests check dup-shared offsets and bytes; Native checks zero I/O; guest metadata checks EOF beyond file size |
| O07 | `file.write()` | Native/Guests check append versus ordinary writes; Native checks zero I/O; Read-only checks EBADF |
| O08 | `file.pread()` | Native/Guests assert exact bytes without moving offsets; unlinked open files remain readable; Budget checks bounded short reads |
| O09 | `file.pwrite()` | Native/Guests check positional writes on append-open handles; Native checks retained rights after chmod and zero I/O |
| O10 | `file.lseek()` | Native checks independent/shared offsets and failed-seek offset preservation; Guests check exact offsets beyond 2^53 and signed overflow |
| O11 | `file.fstat()` | Native/Guests check inode identity through rename, replacement and unlink; expired/closed handles refuse |
| O12 | `file.ftruncate()` | Native/Guests check shrinking and zero extension without moving the offset; Backend checks truncate/rewrite before GC; Read-only checks EBADF |
| O13 | `file.fchmod()` | Contracts checks injected-clock ctime without changing mtime/content version; Native/Guests check retained open rights; Read-only checks EROFS |
| O14 | `file.dup()` | Native/Guests check shared offsets and closing one duplicate; Admission checks duplicate slots and lifetime release |
| O15 | `file.close()` | Native/Guests close duplicates and pinned unlinked inodes; stale guest wrappers refuse subsequent operations; Admission checks immediate slot reuse |
| O16 | `file.fsync()` | Read-only and Guests await successful barriers; Native `/contracts` refuses proof inside an uncommitted transaction |
| O17 | `file.fdatasync()` | Read-only and Guests await successful barriers; Native `/contracts` checks transaction-affinity refusal |

## Namespace and directory handles

| Audit ID | Public operation | Selected assertions inspected |
| --- | --- | --- |
| O18 | `fs.stat()` | Contracts/Guests compare metadata and exact path errno; Read-only preserves metadata through refused effects |
| O19 | `fs.lstat()` | Contracts `directory_relative` and Guests compare stat identity in the no-links profile; Native checks the outer operation on missing paths |
| O20 | `fs.truncate()` | Contracts `directory_relative` checks exact shortened bytes; Guests check shrink/zero-extension and unchanged descriptor offset; Read-only checks EROFS |
| O21 | `fs.mkdir()` | Native/Guests check creation mask; reserved-path cases check root/dot/prefix errors and unchanged state |
| O22 | `fs.rmdir()` | Contracts/Guests check nonempty/type/root/dot errors and removed open-directory behavior |
| O23 | `fs.unlink()` | Native/Guests retain open inode contents through unlink/GC; reserved paths check EPERM for directories and root protection |
| O24 | `fs.rename()` | Contracts/Guests check replacement, cycles, type/slash errors, unchanged self-renames and pinned victims; Backend also compares persistent accounting |
| O25 | `fs.chmod()` | Native checks parent search/create permissions; Guests check path mode changes and retained descriptor access; Read-only checks EROFS |
| O26 | `fs.utimens()` | Contracts checks signed extremes, injected Now, Omit and unsupported atime; Guests check signed codecs and noatime; Read-only checks Omit versus mutation |
| O27 | `fs.opendir()` | Native/Guests check dot entries and pinned identity; Admission checks capacity before publication; Native checks missing-path operation naming |
| O28 | `dir.readdir()` | Contracts/Guests check bytes, continuation during mutation and EOF after removal/replacement; Budget checks result admission before cursor advancement |
| O29 | `dir.telldir()` | Native/Guests save and reuse opaque positions; removed-directory cases check EOF leaves cookies unchanged |
| O30 | `dir.seekdir()` | Native checks another descriptor's cookie is refused; Contracts/Guests check seek after mutation and removal |
| O31 | `dir.rewinddir()` | Contracts reserved paths and Guests check rewind after unlink; Contracts `directory_relative` checks linked dot entries |
| O32 | `dir.closedir()` | Native/Guests release pins; Admission checks directory capacity and expired-handle reclamation |
| O33 | `dir.fstat()` | Contracts/Guests retain directory inode identity after rename and nlink zero after replacement/removal |
| O34 | `dir.fsync()` | Read-only/Guests await namespace proof; Native `/contracts` checks transaction-affinity refusal |
| O35 | `fs.statvfs()` | Contracts `directory_relative` and Guests check quota/availability sums; refusal profiles compare exact usage; Backend checks retained GC accounting |
| O36 | `fs.sync()` | Native/Guests use the whole-cell proof path; Native checks transaction refusal and cached-view authority failure |
| O37 | `fs.openat()` | Contracts/Guests use byte names after directory rename; Contracts checks an absolute path uses the cell root; authority profiles refuse foreign/expired/closed/wrong-kind parents before create or truncate |
| O38 | `fs.fstatat()` | Contracts/Guests check Follow/NoFollow identity and ENOENT after move; Native checks a removed directory's retained parent; authority profiles refuse invalid parents in both follow modes |
| O39 | `fs.mkdirat()` | Contracts/Guests check captured creation mask; shared reserved-path cases check root/dot errors; removed and authority-invalid directory creation refuses |
| O40 | `fs.unlinkat()` | Contracts/Guests remove byte-named files and empty directories using distinct flags; reserved-path cases check operation-specific errors; authority profiles refuse invalid parents with either removal flag |
| O41 | `fs.renameat()` | Contracts/Guests move byte-named entries between pinned parents after a parent rename; authority profiles reject either invalid parent and preserve source/victim metadata and contents; native transaction effects roll back |

## Helpers, transactions and agent tools

| Audit ID | Public operation | Selected assertions inspected |
| --- | --- | --- |
| O42 | `fs.read_file()` / `read_file_bounded()` | Native/Guests check exact bytes and whole-file bounds; Native checks directory/missing-path failures |
| O43 | `fs.write_file()` | Native/Guests check bounded content updates and file/state rollback; Read-only checks EROFS; guest effects compose with receipts |
| O44 | `fs.replace_file()` | Native/Guests verify a new inode at the path while the old open inode retains its contents |
| O45 | `fs.edit_file()` | Native/Guests check version conflicts and changed bytes; Native checks bounded patches/tail edits on a large sparse file; namespace profiles check empty regular-file no-ops, directory EISDIR and transaction/batch rollback |
| O46 | `fs.read_dir()` | Contracts checks continuation across namespace mutations and rejects another directory's cursor; Native/Guests check bounded pages without synthetic dot entries |
| O47 | `file.read_exact()` | Native/Budget check consumed prefix and offset on UnexpectedEof; Native/Guests check closed empty-helper refusal; Guests check expired-context and bound failures identify the helper with zero progress |
| O48 | `file.write_all()` | Native/Budget check confirmed committed prefixes; Python Budget verifies a saved prefix after restart; Guests check expired-context and bound failures identify the helper with zero progress |
| O49 | `dir.read_page()` | Contracts checks mutation/seek continuation and linked dot entries; removed-directory profiles check empty pages after rewind |
| O50 | `fs.collect_garbage()` | Backend checks pins, discarded revisions and quota retention; native/guest hosts check automatic maintenance/restart; Read-only refuses explicit application GC |
| O51 | Guest `storage.apply` file checks/effects | Native batch validation checks identity/version; Guests check conflict and rollback after a preceding file mutation; file effects/receipts/checkpoints commit together |
| O52 | Durable-agent read/write/edit/list/search/rename/delete | Guest tool profiles check results, saved reads, conflicts, stable IDs, request snapshots and receipt caps. Directory-edit refusals replay after namespace deletion and host restart; process crash evidence remains separately scoped in the ledger |

## Directory-relative authority profiles

`foreign_directory` runs eighteen relative/absolute-path refusals through a
still-live directory from another cell, then two transaction rollback cases
with each foreign `renameat` parent in turn. The source invocation remains
accepted as background work during the test. On resumption it verifies its
root metadata and usage before performing its own next mutation. The caller
checks exact ESTALE, outer operation, NotApplied and zero completed bytes;
sampled namespace metadata, quota usage and the valid directory cookie remain
unchanged. Preceding business/file writes roll back in both transaction cases.

The guest `authorityProfile` / `authority_profile` runs eighteen cases each for
a closed directory, a directory token retained from an earlier invocation and
a live regular-file token supplied as a directory. Each real Node, V8 and
CPython bridge checks EBADF, operation, not_committed and zero completed bytes,
plus metadata/usage/cursor preservation and unchanged source/victim bytes.
Absolute paths still require valid directory capabilities. These guest cases
establish token expiry/type checks.

The live guest profile adds a nested call from `authority-owner` to
`authority-caller` while the owner's invocation remains pending. Both cells
use the same path names with different contents. The owner retains a directory,
a filesystem view and an open, unlinked file throughout the call. The caller
checks 45 refusals:

| Authority boundary | Cases | Expected refusal |
| --- | --- | --- |
| Owner's file wrapper | 14: read/write, positional I/O, seek, stat, truncate, chmod, dup, close, both barriers and empty looping helpers | ContextMismatch, public operation, not_committed and zero confirmed bytes |
| Owner's directory wrapper | 8: readdir/read_page, tell/seek/rewind, close, stat and barrier | Same caller-scope refusal |
| Owner's filesystem view | 5: capabilities, stat, read_file, sync and collect_garbage | Same caller-scope refusal |
| Foreign directory token passed through the caller's filesystem | 18 relative/absolute *at cases, including both rename parents | EBADF, public operation, not_committed and zero confirmed bytes |

Every refusal preserves the caller's sampled metadata, usage and directory
cookie. After the nested call the owner verifies its metadata/usage, descriptor
position, directory cookie and original bytes, including the unlinked inode,
then closes its descriptors. CPython checks actual GIL state before and after
the nested call. Executed runtime/ABI combinations are recorded in the ledger.
These cases cover live invocation isolation; cancellation and closed-domain
qualification remain separate.

## Guest looping-helper error profiles

The Node and CPython `/expired` cases reproduced a missing public operation
field before capability preflight joined the helper error path. The shared
guest fixture now checks empty/nonempty reads and writes against a retained
descriptor from an earlier invocation. It preserves the original context
failure code/outcome/errno, reports the helper operation and zero bytes, and
checks unchanged file metadata/content. `/metadata` also checks negative or
over-bound reads and an over-bound write, with unchanged offsets and usage.

`filesystem_helpers_preserve_preflight_failure_outcomes` in the
[V8 runtime tests](../crates/cellar-javascript/runtime_tests.rs) supplies eight
capability error replies through the real SDK protocol. The CPython fixture's
`helper_preflight_outcomes` supplies corresponding typed errors at the facade
boundary. Both preserve not_committed/unknown, errno and code with zero bytes;
neither may dispatch byte I/O after the refusal. These are client error-contract
tests, not actual host-capacity or network-failure qualification.

Node and Python budget profiles check 4,096 consumed bytes on UnexpectedEof
with an advanced offset and unchanged observed file version. Their bounded write reaches
the file-size ceiling with one 1,024-byte accepted prefix, then returns EFBIG
with locally_committed and that exact count. The Python restart checks the
saved inode/version/size and the original head and written tail. This is a
sparse file and local host replacement; it does not qualify dense capacity or
empty-local-state restore.

## Guest argument-conversion profiles

`argumentProfile` / `argument_profile` checks malformed path, offset/size/time,
byte-data, directory-argument and expected-version values. The JavaScript
profile has 57 cases, including malformed edit objects and seven detached-view
refusals. The Python profile has
51 cases, including released and noncontiguous buffers. Every refusal checks
ValidationError/EINVAL, public operation, not_committed and zero confirmed
bytes, plus unchanged root/file/directory metadata, quota usage, file offset,
directory cookie and original content. JavaScript also checks that each call
returns a promise before observing its rejection. Python checks the original
buffer exception remains the cause, including through `write_all`, and accepts
valid bytearray/memoryview writes afterward. JavaScript distinguishes detached
views from valid empty arrays and checks empty writes preserve the baseline.

These profiles cover the facade's argument converters. Native wire validation
has the separate profiles below; client-side refusal alone does not prove it.

## Guest wire-validation profiles

`wireProfile` / `wire_profile` splits the following checks into four invocation
groups so each retains the fixture's normal handler budget:

| Group | Cases | Selected assertions |
| --- | --- | --- |
| Unsigned | 81 | Nine descriptor/path/edit fields reject eight malformed decimal forms and one u64 overflow; EINVAL versus EOVERFLOW |
| Signed | 30 | Current/end seeks and timestamps reject eight malformed forms and both i64 overflow boundaries |
| Options | 26 | Open/access/seek enums, flag and mode types, counts, page argument types, malformed cookies/cursors and helper bound validation; includes a client serialization refusal for NaN |
| Batch | 21 | Strict unsigned checks/edits/truncation, base64/path decoding and inconsistent checks refuse before any file or business put |

All 158 cases check ValidationError, symbolic errno, outer operation,
not_committed and zero progress, plus unchanged metadata, usage, offsets,
directory cookie and exact original bytes. Batch cases also check the business
put is absent; their preceding valid file effect is never applied. The decoder
retains typed parse causes. A separate guest unit checks that an uncertainty
from the native backend wins over a validation marker, and that a filesystem
marker does not establish the effect outcome of an enclosing cell fetch.

These selected cases do not complete every option/precedence combination or
post-dispatch result, cancellation, domain and fault profile.

## Open audit work

Every row records selected positive or negative behavior. The following work
remains before this inventory can support a complete conformance claim:

1. Enumerate applicable type, access, option, size/overflow and namespace errors
   for each operation, including precedence only where the contract specifies it.
   Shared backend coverage alone does not prove every public wrapper's operation
   name, effect outcome or guest serialization. The looping-helper preflight
   and progress profiles above cover their specified SDK error paths; remaining
   operation/option combinations still need their own assertions. The argument
   profiles cover selected facade conversions. The wire profiles map the
   selected syntax/range, shape and option refusals above; remaining combinations
   and precedence still need their own assertions.
2. Complete transaction, cancellation and closed-domain cases for each
   handle-bearing family. Extend the selected live cross-cell cases where
   additional operation families require distinct authority assertions. The
   native foreign-directory and guest live/expired/closed/wrong-kind profiles
   above cover both `renameat` parents, with their distinct scopes.
3. Keep transfer/page/transaction admission, cleanup and ambiguity tests linked
   to actual source and executable evidence. Invocation-bound descriptors and
   activation-bound filesystem views have distinct lifetime contracts.
4. Finish fleet fault, sustained load, captured-package, target/ABI and capacity
   qualification from the implementation plan. This operation inventory does
   not replace those gates or include the separate optional FUSE proposal.
