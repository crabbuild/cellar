<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Filesystem operation conformance audit

Status: partial audit, 2026-10-28. This inventory follows the
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
| Guest concurrency | [JavaScript waves](../crates/cellar-javascript/tests/support/filesystem-concurrency.js) and [Python waves](../crates/cellar-python/tests/filesystem_concurrency.py): concurrent known reads with sequential handle-state controls, host record checks and saved restart snapshots |
| Budget | [Node](../crates/cellar-javascript/tests/support/filesystem-budget.js) and [Python](../crates/cellar-python/tests/filesystem_budget.py): short transfers, encoded pages and receipt/checkpoint admission |
| Lost replies | [Node IPC harness](../crates/cellar-javascript/tests/filesystem_io.rs): real successful byte replies withheld through native cancellation, reconciliation and cold recovery |
| Python task cancellation | [Python coroutine fixture](../crates/cellar-python/tests/filesystem_cancellation.py): real native byte calls, cancelled waits, exact confirmed prefixes and local reopen |
| Installed Vercel | [Production-package harness](../crates/cellar-javascript/tests/support/fleet_package_vercel.rs): restored runtime archive, model HTTP boundary, proven edit/receipt cut and empty-local-state recovery |
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
| O06 | `file.read()` | Native/Guests check dup-shared offsets and bytes; Native checks zero I/O; guest metadata checks EOF beyond file size; Lost replies preserves unknown outcomes after a native read |
| O07 | `file.write()` | Native/Guests check append versus ordinary writes; Native checks zero I/O; Read-only checks EBADF; Lost replies reconciles an unacknowledged committed prefix without repeating it |
| O08 | `file.pread()` | Native/Guests assert exact bytes without moving offsets; unlinked open files remain readable; Budget checks bounded short reads; Lost replies refuses to expose undelivered data |
| O09 | `file.pwrite()` | Native/Guests check positional writes on append-open handles; Native checks retained rights after chmod and zero I/O; Lost replies checks native version progress and retained bytes |
| O10 | `file.lseek()` | Native checks independent/shared offsets and failed-seek offset preservation; Guests check exact offsets beyond 2^53 and signed overflow |
| O11 | `file.fstat()` | Native/Guests check inode identity through rename, replacement and unlink; expired/closed handles refuse |
| O12 | `file.ftruncate()` | Native/Guests check shrinking and zero extension without moving the offset; Backend checks truncate/rewrite before GC; Read-only checks EBADF |
| O13 | `file.fchmod()` | Contracts checks injected-clock ctime without changing mtime/content version; Native/Guests check retained open rights; Read-only checks EROFS and unsupported-mode EOPNOTSUPP precedence |
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
| O25 | `fs.chmod()` | Native checks parent search/create permissions; Guests check path mode changes and retained descriptor access; Read-only checks EROFS and unsupported-mode EOPNOTSUPP precedence |
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
| O47 | `file.read_exact()` | Native/Budget check consumed prefix and offset on UnexpectedEof; Native/Guests check closed empty-helper refusal; Guests check preflight operation/zero progress; Lost replies checks a confirmed prefix with a second processed but undelivered transfer |
| O48 | `file.write_all()` | Native/Budget check confirmed committed prefixes; Python Budget verifies a saved prefix after restart; Guests check preflight operation/zero progress; Lost replies recovers more committed bytes than the caller can confirm |
| O49 | `dir.read_page()` | Contracts checks mutation/seek continuation and linked dot entries; removed-directory profiles check empty pages after rewind |
| O50 | `fs.collect_garbage()` | Backend checks pins, discarded revisions and quota retention; native/guest hosts check automatic maintenance/restart; Read-only refuses explicit application GC |
| O51 | Guest `storage.apply` file checks/effects | Native batch validation checks identity/version; Guests check conflict and rollback after a preceding file mutation; file effects/receipts/checkpoints commit together |
| O52 | Durable-agent read/write/edit/list/search/rename/delete | Guest tool profiles check results, saved reads, conflicts, stable IDs, request snapshots and receipt caps. Directory-edit refusals replay after namespace deletion and host restart; process crash evidence remains separately scoped in the ledger |

The installed Vercel profile exercises O52 through the captured production entry
and its installed OpenAI adapter, using a private HTTP model fixture. The fixture
holds the next provider response only after two isolated cells have proved their
edit, rename and seven receipts. The predecessor is killed and its local data
deleted. An empty replacement must retain exact edited inode/version/bytes and
all seven saved receipts, including the original read; it completes nine tools
and five saved decisions per task. A third task verifies new-owner writes, and
twelve duplicate submissions preserve results and provider-call counts. Direct
SQLite inspection verifies final deletion before joined shutdown. The recovery
source is the development bucket. Installed external-store, physical
fault, streaming-provider and deployment-capacity profiles remain separate.

## Concurrent guest descriptor profile

O03, O06–O11, O14–O15, O18–O19, O28–O29, O33, O35, O38, O42 and O46
have three dedicated invocation cases. Read waves submit at most twelve calls
without awaiting individual replies, within the existing admission limits.
All calls settle before inspection. Mutating and handle-state calls remain
sequential; handler, lease and runtime budgets are unchanged.

| Case | Assertions |
| --- | --- |
| Shared file description | Sequential reads through a file and its duplicate advance both offsets to 384; an independent open retains its own position. Twelve concurrent preads return exact ordered records without changing offsets or version. A mixed wave exercises all ten known-read operations while preserving metadata, usage and a directory cookie. Sequential writes and duplicate cleanup remain usable afterward. |
| Append and positional writes | Two independently opened append descriptions, each with a duplicate, sequentially append twelve unique 32-byte records starting 16 bytes before a 64 KiB chunk boundary. Each pair shares its last append offset. Sequential pwrites through append-open handles preserve offsets and sparse prefix zeros. Concurrent preads through all four handles return exact patched records without extending the file. |
| Directory cursor | Sequential readdir returns twelve names and two dot entries exactly once. Twelve concurrent stateless listings return the complete real-name set. Twelve concurrent telldir calls retain the same cookie, and a subsequent EOF preserves it. |

Each case saves its metadata and bytes or names with an exact native
inode/version check, then awaits filesystem proof. After a preserving node
restart, a fresh invocation reopens the path and compares full metadata and
contents against that saved snapshot. The Rust HTTP client independently checks
record lengths, uniform record bytes, unique IDs, positional ordering, file
sizes and directory names. Optional `CELLAR_FS_CONCURRENCY_REPORT_DIR` captures
the responses for qualification reports.

A separate Node/V8 admission test in
[the shared runtime harness](../crates/cellar-javascript/synchronous_runtime_tests.rs)
keeps a storage mutation unacknowledged while all ten known reads are admitted.
Thirty-five mutating or unknown filesystem names must report
ConcurrentMutation/not_committed without native dispatch. A marker submitted
after those refusals prevents an early host acknowledgement from hiding a slot
release. This covers both Node's pre-IPC guard and the native runtime guard.

These cases cover read concurrency within one guest invocation. They do not
promise concurrent guest mutations, a multi-call snapshot, atomicity for a
multi-transfer append helper, fairness under sustained load, or process-crash
recovery. Executed runtime/ABI profiles belong in the status ledger.

## Mode-option precedence profile

O13/O25 cover mode bits outside `0o777` under both writable and read-only
policies. Native cases include sticky, setgid, setuid, file-type bits and
`u32::MAX`; Node/V8 and Python use setuid and the maximum unsigned mode. The
read-only fixtures require EOPNOTSUPP, the public operation, NotApplied and zero
completed bytes while preserving bytes, full metadata, quota usage, file offset
and directory cookie. Supported mode mutations still return EROFS. The writer's
persisted state is reopened with a read-only class before these assertions.

Native transaction cases require rollback of preceding business writes for both
metadata APIs. Separate retained-view, nested-transaction and expired-descriptor
cases prove that unsupported modes do not bypass authority checks. Native
writable-policy checks retain metadata/accounting/offsets after refusal and then
exercise supported metadata changes. These cases cover the selected mode-option
boundary; they do not close the complete O13/O25 error and lifetime matrix.

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

`wireProfile` / `wire_profile` splits the following checks into six invocation
groups so each retains the fixture's normal handler budget:

| Group | Cases | Selected assertions |
| --- | --- | --- |
| Unsigned | 81 | Nine descriptor/path/edit fields reject eight malformed decimal forms and one u64 overflow; EINVAL versus EOVERFLOW |
| Signed | 30 | Current/end seeks and timestamps reject eight malformed forms and both i64 overflow boundaries |
| Options | 26 | Open/access/seek enums, flag and mode types, counts, page argument types, malformed cookies/cursors and helper bound validation; includes a client serialization refusal for NaN |
| Batch | 21 | Strict unsigned checks/edits/truncation, base64/path decoding and inconsistent checks refuse before any file or business put |

Unsigned cases run as three groups of 27. Every one of the original 81
refusals retains its metadata/usage/offset/cookie/byte comparison. The former
single unsigned invocation made more than 648 bridge calls including assertions
and exceeded the unchanged two-second handler budget in full Node-suite runs
on both the parent and reply-validation sources. The status ledger retains that
dense-invocation observation; this conformance grouping is not a throughput
qualification or a longer runtime allowance.

All 158 cases check ValidationError, symbolic errno, outer operation,
not_committed and zero progress, plus unchanged metadata, usage, offsets,
directory cookie and exact original bytes. Batch cases also check the business
put is absent; their preceding valid file effect is never applied. The decoder
retains typed parse causes. A separate guest unit checks that an uncertainty
from the native backend wins over a validation marker, and that a filesystem
marker does not establish the effect outcome of an enclosing cell fetch.

These selected cases do not complete every option/precedence combination or
post-dispatch result, cancellation, domain and fault profile.

## Guest reply-validation profile

The shared [reply inventory](../crates/cellar/tests/support/filesystem_reply_cases.json)
contains 181 cases: 132 failures and 49 valid controls. The failures include
three valid native error envelopes whose classifications must be preserved.
The [Node factory test](../tools/javascript/filesystem-replies.test.mjs),
[embedded V8 driver](../crates/cellar-javascript/runtime_tests.rs) and
[Python protocol profile](../crates/cellar-python/tests/filesystem_replies.py)
drive the public facades with controlled host replies. Node and V8 share the
[public-call probe](../crates/cellar-javascript/tests/support/filesystem-replies.js).
Python also exercises `Context._reply` settlement, including unparseable JSON.

| Reply family | Selected audit IDs | Assertions |
| --- | --- | --- |
| Handles and metadata | O03–O05, O11, O14, O18–O19, O27, O33, O35, O37–O38, O45 | Invalid/missing handle fields, decimal syntax/ranges, kinds, modes, identity and capability shapes report structured uncertainty; valid extreme decimal values and extra metadata fields remain usable |
| Bytes and progress | O06–O10, O42, O47–O48 | Canonical base64, counts and request bounds are checked before returning bytes or advancing a helper; a later malformed reply retains the earlier two-byte confirmed prefix |
| Directories and reclamation | O28–O32, O40–O41, O46, O49–O50 | Entry/page/cursor/cookie shapes, page bounds and GC counts are checked; valid EOF and empty pages succeed |
| Unit acknowledgements and failures | O12–O13, O15–O17, O20–O26, O34, O36, O39–O44 | Non-null unit acknowledgements, corrupt JSON, missing fields and invalid failure metadata cannot establish rollback; valid native not_committed/locally_committed/unknown failures retain their code, errno and outcome |

Malformed replies report HostError/EIO, the outer operation and unknown, with
zero confirmed bytes from that call and the original decoder cause. The helper
prefix cases report two confirmed bytes and still report unknown. Valid native
decimal fields fit at most 20 ASCII characters; overlong fields refuse before
integer conversion, including padded values that would otherwise fit.
Valid native Overloaded errors retain their type in Python. Invalid JSON settles
the Python future before it is awaited. The protocol fixtures assert every expected call
and complete only after consuming the full reply sequence.

These are controlled protocol faults. They do not prove real write commit or
durability after loss of a production reply; native/fleet fault qualification
and receipt reconciliation remain separate. Real-host SDK suites check that
ordinary generated replies still work through the stricter facade boundary.

## Guest deadline cleanup profile

The shared host client drives `/lifetime/cancel-start` to the native handler
deadline, then invokes `/lifetime/cancel-check` and, after node restart,
`/lifetime/cancel-recovered`. A [native-future observer](../crates/cellar/tests/support/filesystem_lifetime.rs)
waits for the original cell future to drop before cleanup assertions; an HTTP
caller timeout alone cannot establish cell expiry. The observer leaves the
future's polling and destruction in its original native execution environment.
The guest fixtures retain the original filesystem
view, file, duplicate and removed-directory wrappers in global variables.
No explicit close or guest collection accounts for their released resources.

Before suspension, an atomic batch saves `/kept`, business state and a receipt.
An explicit barrier proves those writes. A separate 256 KiB file and an empty
directory are removed while their descriptors remain open; attempted GC leaves
all four inodes accounted for. Following cancellation, bounded GC must leave
exactly the root and `/kept`, nine logical bytes and one 64 KiB chunk. Four fresh
descriptors can fill the captured four-slot host ceiling, and the next open
returns ENFILE.

| Cases | Selected audit IDs | Assertions |
| --- | --- | --- |
| 16 retained file/duplicate calls | O06–O17, O47–O48 | Reads, writes, positional I/O, seek, metadata, resize, chmod, dup, close, barriers and empty helpers refuse retired invocation authority |
| 8 retained directory calls | O28–O34, O49 | Enumeration, page reads, cookies, seek/rewind, close, stat and proof refuse retired invocation authority |
| 7 retained filesystem-view calls | O03, O18, O23, O36, O42–O43, O50 | Capability/metadata/content reads, writes, unlink, proof and GC cannot borrow the later invocation |
| 10 retired directory-token calls | O37–O38, O40–O41 | Relative and absolute open/truncate, stat, unlink and both rename parents return EBADF through the fresh filesystem view |

Each of the 41 refusals reports the outer operation, not_committed and zero
confirmed bytes. Fresh root/file metadata, usage, file offset and directory
cookie stay unchanged after each call. The committed file contents, business
state and receipt survive both cancellation and node restart. Retained wrappers
remain reachable through the shutdown boundary.

This profile cancels a handler after completed, proven operations. It does not
qualify cancellation during an in-flight byte operation or durability proof,
activation takeover, closed execution domains, or resource overload generally.
Runtime/ABI executions and artifact identities are recorded in the status
ledger.

## Python in-flight proof cancellation profile

The [Python proof harness](../crates/cellar-python/tests/support/proof_authority.rs)
reuses real three-node fleet proof and the
[authenticated peer/bucket gates](../crates/cellar/tests/support/proof_fault_gates.rs).
The [filesystem fixture](../crates/cellar-python/tests/support/proof_filesystem.py)
commits a four-chunk file, business generation and SHA-256 receipt in one
`storage.apply` batch. A preceding generation is explicitly proven and
acknowledged. Peer append acknowledgements and bucket publications are held
before the next batch; the client must remain pending after local commit.

Eight modes select `file.fsync`, `file.fdatasync`, `fs.sync`, or final output
proof (O16, O17, O36 and the cell output boundary), followed by either the
original handler deadline or an observed native node-lease mismatch fence.
Release of the actual transports after authority retirement cannot turn the
failed client response into success. The interrupted generation may survive or
be absent; the test does not infer rollback from a lost acknowledgement.

The original task resumes only to check 31 retained file/duplicate/directory/view
refusals and one raw filesystem submission refusal. SDK refusals require
ContextExpired, the public operation, not_committed and zero confirmed bytes.
Two existing storage/raw-storage authority assertions also remain in the
shared harness. Retained guest wrappers remain live while four fresh descriptors
reuse the captured host ceiling; a fifth open must return ENFILE.

Warm recovery must select an internally consistent file, generation, receipt,
inode and content version. An explicit barrier proves that selected readback.
After the original nodes stop with Preserve and their unchanged five-second
leases expire, three new logical nodes start with new local directories. Their
cold readback must equal the warm result exactly. A subsequent atomic file/state
write proves fresh capabilities still work. The business handler executes once,
and all six native outcomes and interpreter/bridge cleanup must be accounted for;
the fence case retains its expected incomplete native fence outcome. The new
profile has a 90-second outer process watchdog for both three-node lifecycles;
existing handler, proof, lease and older-profile watchdog budgets are unchanged.

This is supplied development-store and loopback-peer evidence. It does not
qualify external-store outages, physical network faults, process crashes, byte-I/O
cancellation, retained-wrapper handoff shutdown, closed domains generally, or the
corresponding Node/V8 proof profiles. Executed ABIs and frozen executable/source
identities belong in the implementation status ledger.

## Python handoff and closed-domain profile

Eight `--runtime-filesystem-handoff-{barrier}-{deadline|fence}` modes extend
the Python in-flight proof profile. Three successors start with new local
directories and reach readiness before any predecessor begins Handoff. After
proof interruption and warm coherent readback, each remaining predecessor must
return a complete native Handoff report while successor capacity remains live.
The successors recover the exact proven file/state/receipt result and admit a
new atomic write. They stop with Preserve after verification; the fixture does
not attempt to hand the final cell to an empty fleet.

The old file, duplicate, directory and filesystem-view wrappers remain retained
through predecessor domain shutdown. A live successor invocation repeats the
31 SDK refusals and one raw submission refusal, taking the profile's cumulative
count from 32 to 64. Each SDK call must retain ContextExpired, its public
operation, not_committed and zero confirmed bytes. After every refusal, fresh
root/file metadata, usage, file/duplicate offsets, directory cookie and a
positional byte prefix must remain identical. Full file/state/receipt checks
follow. Four fresh descriptors fill the successor's host ceiling; a fifth open
still returns ENFILE, so a stale close cannot release its slots.

The earlier all-node Handoff experiment stopped its final donor without a live
successor. Targeted diagnostics observed successful epoch-advancing transfers on
the first two nodes, followed by occupied=0, quiescing=0, evicting=0,
releasing=0 and adopting=1 on the last node. It had no pending application events
or dispatches. Keeping ready successors before departure made the original
coherence and shutdown assertions pass with the wrappers still retained. This
resolves that fixture topology observation without a production runtime change,
a timeout extension or a weaker shutdown assertion.

Executed runtime/ABI and executable identities belong in the status ledger.
These selected Python handle families do not qualify all closed-domain paths,
Node/V8 handoff behavior, physical outages or process crashes.

## JavaScript in-flight proof and closed-domain profile

The [Node/V8 proof harness](../crates/cellar-javascript/tests/filesystem_proof.rs)
uses the [native controller](../crates/cellar-javascript/tests/support/filesystem_proof.rs),
[guest fixture](../crates/cellar-javascript/tests/support/filesystem-proof.js),
shared peer/bucket gates and the existing native-future observer. Forty cases
cover each engine, five barriers (file fsync/fdatasync, directory fsync,
filesystem sync and final output proof), deadline/fence interruption, and
Preserve/rolling Handoff. Handler/proof/lease limits remain 10/30/5 seconds;
the V8 pump has the existing filesystem fixture's explicit one-second bound.

An acknowledged seed precedes the cut. A single checked batch commits four
chunks, business generation and a SHA-256 receipt locally while peer append and
bucket publication are held. Both real gates must be entered and the HTTP
client must remain pending. A real native node-lease mismatch or the handler
deadline retires authority. For awaited barriers, the abandoned promise must
retain ContextExpired/unknown when host retirement settles it, or the native
Storage/EIO/unknown proof failure when that reply wins the race. The public
operation and zero confirmed bytes remain exact; HostError and rollback claims
are refused.
An HTTP timeout alone is insufficient; the native observer waits for the
original cell future to drop.

The original JavaScript continuation resumes after a separate application
promise is released and checks 31 retained SDK calls plus one raw filesystem
submission. Those calls require ContextExpired/not_committed and zero progress.
After all predecessor domains close, a live successor repeats the refusals with
ContextMismatch/not_committed; it must not lend the stale wrappers its own
invocation authority. The cumulative count is 64 per case. Every successor-side
refusal preserves full metadata/usage, file and dup offsets, directory cookie
and positional bytes. Four fresh descriptors remain allocated and a fifth open
returns ENFILE after stale close attempts.

Warm readback must keep the file, business state, receipt and captured
inode/version coherent. Before predecessor shutdown, that readback also reuses
all four host slots while the original wrappers remain retained. Rust
independently verifies the returned full bytes and SHA-256, then cold readback
must match the proven warm result. Handoff cases
start and ready successors before departure; Preserve cases honor the original
lease before starting new logical identities. A fresh write advances the same
inode's version without changing preceding state. Six native outcomes are
checked, with the exact forced fence as the sole allowed incomplete outcome.
Preserve additionally requires clean-reload preparation. After each runtime
joins, stopped=true, active_invocations=0 and retained_bytes=0 are required.

The before-fix Node and V8 fsync cases exposed a producer/validator mismatch:
framework expiry replies lacked the required error string, so strict filesystem
validation returned HostError/EIO/unknown. The reply producers now include that
field while preserving ContextExpired/unknown. The validator remains strict;
the corresponding synchronous expiry producers use the same canonical shape.
Asynchronous filesystem cases do not qualify synchronous transport loss.
Executed artifacts and remaining limits belong in the status ledger.

These are development-bucket/loopback-peer cases. Byte-I/O cancellation,
remaining operation/domain/overload combinations, external faults, sustained
load and final package/target qualification remain separate gates.

## Node lost byte-reply profile

The [Node IPC harness](../crates/cellar-javascript/tests/filesystem_io.rs) uses a
[native controller](../crates/cellar-javascript/tests/support/filesystem_io.rs),
[guest workload](../crates/cellar-javascript/tests/support/filesystem-io.mjs) and
[TCP relay](../crates/cellar-javascript/tests/support/filesystem-io-relay.mjs).
The supervisor loads the unmodified production runtime source and forwards its
authentication in memory. It changes the connection's port to the relay and
withholds one real successful filesystem reply. Heartbeats, cancellation and
unrelated invocations continue through the actual framed transport.

Twenty-four cases cover read/pread/write/pwrite/read_exact/write_all, each with
the original handler deadline or an actual native lease-mismatch fence, followed
by Preserve/lease-expiry or rolling Handoff. The 10/30/5-second handler/proof/lease
budgets match the existing proof fixtures. A four-byte transfer ceiling creates
bounded short calls. Single-call cases lose their first reply and confirm zero
bytes. Helpers receive one reply, then lose the second: they must preserve the
outer helper operation, ContextExpired/unknown and exactly four confirmed bytes.
Neither a success reply retained by the relay nor a processed transfer can be
counted as delivered progress.

Before cancellation, another real invocation proves the actual content while
the original HTTP caller remains pending. Native Rust checks exact bytes,
SHA-256, inode identity and the exact content-version increment for the one or
two writes. Read cases preserve full persistent file metadata. A proven seed
receipt remains unchanged until a fresh invocation reconciles the uncertain
operation with a checked inode/version transaction. It saves a receipt for the
observed content without replaying the write. Thus a helper write recovers eight
processed bytes while reporting only four confirmed bytes to its original caller.

The original native cell future must retire before the controller releases the
held frame. Its SHA-256 must remain unchanged, and delivery of that late reply
cannot change the original failure or repeat the application operation. Nine
retained byte/close calls then refuse with ContextExpired/not_committed and zero
progress. Warm recovery reuses all four host descriptor slots. After predecessor
domains close, a live successor repeats the nine refusals with ContextMismatch;
each preserves fresh full metadata/usage, offsets, directory cookie and bytes.
The fifth open still returns ENFILE after stale close attempts.

Cold file/state/receipt readback must equal the reconciled proven warm result.
A new atomic write advances the same inode's version. All six native outcomes
are checked; only the deliberately forced fence may be incomplete. The joined
Node bridge must have no active invocations or retained native bytes. Structured
reports and executed source/artifact identities belong in the status ledger.

This profile qualifies cancellation while awaiting a real Node byte reply. It
does not pause SQLite inside its synchronous call, qualify V8/Python reply loss,
or replace between-transfer helper cancellation, physical peer/store/disk faults
and sustained-load qualification.

## S3-backed native recovery profile

The [native fault harness](../crates/cellar/tests/support/node_filesystem_faults.rs)
runs the same scenario against either its development bucket or an explicit
private S3-compatible bucket. The S3 case is opt-in, requires a bucket name
starting with `cellar-filesystem-`, and uses an explicit endpoint and static test
credentials. The JavaScript SDK workflow provisions a pinned private RustFS
container, creates the bucket and retains the structured fault report.

The test proves a first file/state/receipt transaction, blocks replica publication
before a second local commit, independently refuses owner-store operations and
requires actual lease-expiry fencing without acknowledging the second effect.
It removes old local state, refuses unreadable replica admission without calling
the application factory, restores the first acknowledged transaction under a new
owner, rejects stale capabilities and proves a third transaction. A second
empty-local-state restore must match that new receipt. The same deadlines and
shutdown assertions apply to both backends.

The S3 case adds real provider conditional writes, object publication and cold
replica reads beneath the supplied-client fault gates. Those gates do not simulate
physical network partition, SDK socket timeouts, credential expiry or disk stalls.
Results and exact backend scope belong in the release ledger.

The `filesystem_s3_socket_cuts_fence_unproven_writes_and_refuse_failed_recovery`
variant adds real TCP connection closure beneath the unmodified S3 client. Three
[bounded proxies](../crates/cellar/tests/support/filesystem_socket_fault.rs)
isolate replica publication, owner lease access and recovery reads independently.
Cuts cancel and join existing pooled connections before rejecting new ones.
The scenario retains the same file/state/receipt ledger, ambiguous response,
lease-expiry fence, failed-constructor admission, stale-capability checks, two
empty-node recoveries and joined node shutdown. Proxy counters establish actual
forwarding before the publication/lease cuts and denied connections afterward;
all three servers and their forwarding tasks must join. A separate loopback
control tests both directions of an established connection and refusal of a new
one. This closes the selected connection-closure boundary.

The `filesystem_s3_socket_stalls_fence_unproven_writes_and_refuse_failed_recovery`
variant pauses both forwarding directions while retaining the accepted sockets.
Its fault acknowledgement waits until every admitted connection has parked or
finished; newly accepted connections park without connecting upstream. The
control test requires blocked reads rather than EOF on both existing endpoints
and the fresh client, then verifies that all parked tasks join at shutdown.
The proxy bounds active connections at 128 and reads its remaining-task counter
after joining rather than assuming zero.

This variant retains the same unknown-write, lease-fence, failed-admission,
stale-capability and two cold-recovery assertions. It additionally requires
stalled connections at all three boundaries and zero denied connections. Its
failed-recovery HTTP observer has a 60-second deadline so it can observe the
existing S3 request/retry budget before requiring HTTP 500 and a clean drain.
The other profiles keep their 15-second observer. Cell restore has no aggregate
activation deadline; the node-log startup recovery timeout is a different gate.
Runtime, provider, ownership and shutdown budgets are unchanged. The report
records the selected observer bound and actual response time. This covers TCP
forwarding stalls on local S3; kernel packet loss/delay, peer transport failures,
expired credentials and host disk faults remain separate profiles.

## Python task cancellation profile

The [Python filesystem supervisor](../crates/cellar-python/tests/filesystem.rs)
selects `--task-cancellation` to run twelve cases and twelve local-reopen checks
per ABI. The fixture starts real SDK coroutines and suspends them before a
first/second transfer, after the second native transfer, or during capabilities
reply delivery. Four direct read/pread/write/pwrite cases suspend delivery after
the native call. Native transfers and the host descriptor ceiling are both four;
file contents are 32 bytes, with a twelve-byte request starting at offset three.
No native storage operation is simulated.

`Task.cancel(message)` must retain `asyncio.CancelledError`, the message and
the task's cancelled state. Errors carry `Cancelled`, `unknown`, the public
operation and zero or four confirmed bytes. Python's `CancelledError` derives
from `BaseException`, so handling only `Exception` cannot establish this contract.
The SDK annotates and re-raises cancellation without converting it to SDKError
or changing native work or parent-invocation authority. A coroutine cancelled
before it starts cannot run this annotation code.

The fixture independently verifies actual zero/one/two native replies, full
file bytes and SHA-256, exact version increments, inode identity, duplicate-shared
offsets and unchanged directory cookies. Positional calls preserve the offset;
reads preserve the complete stat. The parent can still use its original handles,
occupy four slots, observe ENFILE at the fifth and close all handles. A checked
inode/version receipt is saved and proven. Local reopen must recover the exact
same bytes, identity, version, progress and receipt. This is an SDK coroutine
cancellation/local-reopen profile, separate from withheld IPC replies, native
invocation retirement and empty-local-state fleet recovery.

The shared proof fixture separately records cancellation and native storage
failure. A native proof failure can retire the activation before Python consumes
its queued error reply. Both exact outcomes are permitted: CancelledError with
Cancelled/unknown, or SDKError with Storage/EIO/unknown; each must name the public
barrier and report zero confirmed bytes. Unexpected errors and success fail the
case. The transport-failure/fence profile fails both real transports after their
held local-commit cut, observes settlement, then forces the actual lease mismatch.
It preserves the original deadlines, recovery, stale-wrapper and complete cleanup
assertions. It does not require one side of that reply/cancellation race to win.

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
   and precedence still need their own assertions. The reply-validation profile
   adds controlled post-dispatch faults and helper-prefix evidence. The Node
   byte-reply profile adds selected real transport losses; remaining lost-reply
   families and production fault outcomes retain their separate gates.
2. Complete transaction, cancellation and closed-domain cases for each
   handle-bearing family. Extend the selected live cross-cell cases where
   additional operation families require distinct authority assertions. The
   native foreign-directory and guest live/expired/closed/wrong-kind profiles
   above cover both `renameat` parents, with their distinct scopes. The deadline
   cleanup profile adds retained-wrapper refusal, slot/pin release and committed
   batch recovery. The Python in-flight proof profile above adds the selected
   barrier/deadline/fence combinations. The Node/V8 profile adds five barriers,
   both interruption modes and Preserve/Handoff with stale-context checks.
   The Node byte-reply profile adds six byte APIs through real reply loss and
   native cancellation. Other byte-cancellation/runtime combinations and
   remaining domain families stay open. The
   Python handoff profile adds selected retained-wrapper refusals after predecessor domain
   shutdown and coherent successor writes; it does not complete every lifetime
   family.
3. Keep transfer/page/transaction admission, cleanup and ambiguity tests linked
   to actual source and executable evidence. Invocation-bound descriptors and
   activation-bound filesystem views have distinct lifetime contracts.
4. Finish fleet fault, sustained load, captured-package, target/ABI and capacity
   qualification from the implementation plan. This operation inventory does
   not replace those gates or include the separate optional FUSE proposal.
