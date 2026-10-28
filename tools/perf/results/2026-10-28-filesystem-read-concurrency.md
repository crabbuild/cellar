<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Filesystem guest read concurrency, 2026-10-28

The filesystem's known reads now use the guest read admission lane. Before this
fix, a bounded wave of `pread` calls returned `ConcurrentMutation`: the shared
guest classifier and Node's pre-IPC classifier contained no filesystem names.
The source baseline is `ba15b5021ca414ba6e444e3d4244d51fada1888a`; the
[structured record](2026-10-28-filesystem-read-concurrency.json) pins the changed
sources, executable identities, commands, logs and observations.

Both classifiers now admit exactly `capabilities`, `statvfs`, `pread`, `fstat`,
`stat`, `lstat`, `fstatat`, `read_file`, `read_dir` and `telldir`. Offset-changing
reads, handle allocation/closure, namespace/data mutations, proof waits and
unknown operations retain the single-mutation admission rule. A read overlapping
a mutation has no multi-call snapshot or additional durability guarantee.

## Regression and controls

An initial fixture incorrectly expected simultaneous offset reads and writes to
succeed. That contradicted the existing guest contract and was corrected before
the production probe. With mutations sequential, the concurrent positional-read
wave still failed with HTTP 500 and `ConcurrentMutation`. The allowlist fix
removed that failure; a subsequent fixture-only correction used the public kind
name `Directory` instead of `directory`. These distinct failures remain in the
raw evidence and are not treated as passing runs.

The controlled Node/V8 admission test keeps a storage write unacknowledged. It
requires all ten filesystem read operations to reach the host, and all 35
mutating or unknown filesystem calls to settle as
`ConcurrentMutation/not_committed` without host dispatch. Only after a marker
confirms those refusals does the host acknowledge the outstanding requests.
This tests admission separately from payload validation and real storage.

The real-host fixtures use at most four handles and twelve concurrent reads:

| Profile | Verified behavior |
| --- | --- |
| Shared descriptions | Sequential reads/writes share duplicate offsets; independent opens retain separate offsets. Concurrent positional reads return exact records and preserve offset/version. A mixed wave covers all ten known reads, metadata, allocation and a directory cookie. |
| Append and positional edits | Sequential appends cross a 64 KiB boundary from a sparse prefix. Positional writes through append-open handles replace exact records without extending the file or moving offsets. Concurrent reads return the edited records; sampled sparse ranges stay zero. |
| Directory cursors | Sequential enumeration returns all names and dot entries once. Concurrent stateless listings and cookie reads preserve the directory cursor; EOF retains its cookie. |
| Restart | Each profile stores a native-checked inode/version snapshot, requests sync and compares exact metadata and content after local restart. The host independently verifies record identities, lengths and directory names. |

## Validation scope

The recorded matrix runs on macOS arm64 with Rust 1.98.1, Node 24.13.1 and
CPython 3.14.5 with both GIL and free-threaded builds. It uses unoptimized
development/test artifacts with debug information disabled and local development
storage. Python linkage and actual GIL state are checked for each frozen binary.
All six JavaScript filesystem cases and both Python normal/read-only profiles
pass. The four normal runtime profiles emit 24 structured observations before
and after restart. All 36 guest-library tests and three selected JavaScript
filesystem-library tests pass, including the held-mutation control. Strict
Clippy passes for the guest library, both adapters and the RPC qualifier.
Formatting, module layout, native dependency boundaries, JavaScript formatting
and documentation checks pass; both edited workflows parse and 217 changed
documentation references resolve. The quality record retains the initial
harness lint findings and the checks after their correction: report writes now
have a narrow client-side fixture exemption, and fixed-width record checks use
array chunks.

The RPC qualifier also referenced the old location of the fault adapter moved
earlier in this branch. The import now points to the shared native module; its
existing ordering tests remain enabled. This fixes the observed missing-source
compile failure; all 41 local qualifier tests pass. This does not establish a
passing full remote RPC matrix.

Raw evidence is under the checkout-specific target's `qualification/concurrency/`,
`concurrency-final/` and `concurrency-qualified/` directories. CI registers the
same fixtures and retains their JSON. The
[run instructions](../filesystem.md#guest-read-concurrency) describe reproduction.
This closes the identified read-admission defect and selected operation cases.
It does not qualify concurrent guest mutations, process crashes, external-store
failures, sustained load or optimized deployment resource bounds. Full CI and
the remaining operation/error audit retain separate acceptance gates.
