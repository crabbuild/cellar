<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Per-cell filesystem primitive implementation and qualification plan

Status: delivery and qualification plan; package completion requires source and
test evidence in the [status ledger](per-cell-filesystem-implementation-status.md).
Prepared 2026-10-25; scope revised 2026-10-28. Inspected source baseline:
`d3206c924031a3f2ca3387b542c02877bb91423d`.

Read the [technical design](per-cell-filesystem-design.md) and
[POSIX API contract](per-cell-filesystem-api.md) before implementation. This plan
delivers `context.fs()` as a standalone application primitive: SQLite file state,
POSIX-compatible SDK operations, guest parity and durable agent file tools.
The optional [FUSE and execution design](per-cell-filesystem-fuse-design.md)
owns every mounting, container and command-runner work package. Those packages
are independent of this primitive's completion criteria.

## Qualification target

Filesystem is a first-class Cellar application primitive alongside KV and SQL.
Its acceptance target is the framework contract: per-cell isolation, POSIX
operation semantics within the declared capability profile, atomic file/KV/SQL
transactions, existing durability proofs and recovery, stale-authority refusal,
typed failures, bounded admission and cleanup, and native/guest SDK parity.
Use Cellar's existing storage, fleet and runtime qualification profiles, adding
file/state/receipt assertions at the filesystem integration boundary.

Primitive completion does not depend on selecting an application's object store,
cell population, traffic rate or latency/recovery SLO. Publish tested artifact
identities, configured limits and evidence boundaries. Deployment sizing,
provider-specific tuning and application SLO/RPO/RTO acceptance use the separate
[deployment qualification process](../docs/19-testing-qualification.md).
Performance experiments below inform limits and optimization. Evidence for
declared resource and progress contracts remains required for primitive acceptance.

## Delivery sequence

Keep the primitive opt-in until its native, guest and fleet gates pass. No new
OS mounting library or execution runtime is a prerequisite for any FS-xx gate.

| Package | Deliverable | Depends on | Release gate |
| --- | --- | --- | --- |
| FS-00 | Source audit and context/POSIX contract | None | Accessor, transaction, authority and capability decisions recorded |
| FS-01 | Schema, bounded file/directory operations, quotas and GC | FS-00 | Namespace/content invariants and rollback pass |
| FS-02 | Rust context primitive, descriptors and transaction composition | FS-01 | POSIX operation profile, file-plus-state atomicity and authority pass |
| FS-03 | Guest APIs and durable agent file tools | FS-02 | Native/JS/Python parity and receipt replay survive crashes |
| FS-04 | Lifecycle, recovery and resource qualification | FS-02; agent cases also FS-03 | Stale capabilities refused, acknowledged state recovers, work/cleanup bounded |
| FS-05 | Packaging, published bounds and public release | FS-03/04 | Exact artifacts pass the SDK/platform matrix and declared primitive contracts |
| FS-06 | Additional filesystem capability profiles | Core baseline plus per-profile design | Links/locks/metadata expansion passes its own conformance tests |

FS-02 provides a native primitive; FS-03 adds guest/agent use. FS-04/05 establish
the core production release. FS-06 is optional expansion and cannot silently
broaden the supported operation matrix before qualification.

### Scope guard for every core package

Core packages deliver `context.fs()` acquisition, POSIX-compatible SDK
operations, `tx.fs()` composition and durable agent file tools. Public types
and captured configuration contain file-operation policy and storage limits.
All examples call the primitive directly in ordinary cell invocations.

Build and run the core feature matrix using the normal Cellar SDK/runtime and
SQLite storage services. Core tests qualify this complete application surface
without additional operating-system services. A stored script is ordinary file
content; the agent demonstrates reading, editing and recovering it through SDK
tools. Executing the script is outside FS-03 acceptance.

The separate extension plan owns host integration work and its release gates.
Changes for that extension preserve the primitive's public surface, descriptor
lifetimes and independently testable delivery sequence.

Review API changes against the primitive contract at FS-00 and again at FS-05.
Keep registration, capability discovery and examples usable through the normal
context acquisition path. Any proposal that introduces an OS mount, execution
builder or command runner is tracked under FUSE-xx in the separate design;
it does not add a dependency to an FS-xx package.

## FS-00: Verify context and POSIX boundaries

Read current sources linked in the design, applicable AGENTS.md files and the
storage/SDK/guest lifecycle tests. Compare the inspected snapshot with the
implementation checkout and update source drift before coding.

Record the `Context::fs()` / `Storage::fs()` naming, per-root-cell scope,
`tx.fs()` affinity, byte-path model, captured identity/quotas, explicit errno
profile and local-commit/proof boundaries. Confirm that named KV/SQL/queue
bindings cannot be treated as participants in a local filesystem transaction.

Define the supported POSIX behavior and deterministic unsupported cases before
adding methods. Check capabilities against API-based agent read/write/edit/search
workloads. Select initial bounded request, chunk and transaction-work parameters
for measurement. Document that cold object-backed content would need a later
async/preparation contract, not hidden I/O in synchronous transaction methods.

Exit: a source-grounded contract and meaningful behavior matrix, with no
platform-device feasibility probe required for the context primitive.

## FS-01: Implement the storage core

Suggested new component: `crates/cellar/storage/sqlite/filesystem/` with
`mod.rs`, `schema.sql`, `namespace.rs`, `content.rs`, `quota.rs`, `gc.rs` and
focused tests. Final boundaries should follow responsibilities, not this list
mechanically. Register it from existing SQLite storage; avoid a parallel
connection pool or a filesystem-specific replication stack.

Implementation steps:

1. Add lazy format-1 schema and root initialization; verify the existing
   `__sys_` protection in `crates/cellar-ltx/src/system_schema.rs` and public SQL
   authorization covers the new tables. Add regression coverage; do not change
   LTX merely to register tables already covered by that prefix. Add typed
   inode, name, offset and revision values.
2. Implement byte-name validation, root-confined path traversal, indexed lookup,
   mkdir/rmdir, create/stat and indexed directory cookies.
3. Add bounded chunks, offset read/write, atomic append, truncate/zero extension
   and exact logical/allocated usage accounting. Implement revision-bounded
   discarded ranges so extend/rewrite before truncate GC cannot expose old
   bytes or cause GC to delete new bytes.
4. Add rename/replace with one transaction and checked parent/type/cycle rules.
   Add unlink with explicit inode pins supplied by the owning lifecycle service.
5. Add persistent reclamation cursors and bounded owner-driven GC. Ensure quota
   pressure includes pending garbage and failure cannot free referenced data.

Exit: core tests pass on SQLite using meaningful state invariants and
fault-triggered rollback. Record write amplification for several chunk sizes;
keep 64 KiB only if the measurements support it. Large GC is resumable, not a
single unbounded transaction hidden behind unlink.

## FS-02: Expose the Rust SDK

Add `crates/cellar/application/filesystem/` with a small public facade and private
validation/codec helpers. Wire exports through `crates/cellar/lib.rs` and
`sdk.rs` consistent with their existing physical-module mapping. Expose
`Context::fs()` and `Storage::fs()`, including transaction-derived
views. Use `cellar::sdk::filesystem::Filesystem`; preserve the existing public
`FileSystem` host abstraction. Restrict v1 to root cells; stateless and facet
access returns a typed refusal.

Implement typed errors and declared capability/limit validation. Document local
commit versus external durability. Add bounded read/list/write helpers and
expected-version editing. Do not expose raw SQL access to framework tables.

Add `crates/cellar/tests/native_filesystem.rs` using existing SDK harnesses.
Test file + KV + receipt + alarm/outbox composition where those APIs apply,
rollback on a later failure, retired contexts, closed domains and conflicting
updates. A dedicated filesystem sync must delegate to the current cell's
existing durability barrier.

Land FS-02 in these reviewable slices, each with its owning contract tests:

1. **Accessor and types:** `Filesystem`, `File`, `Directory`, byte paths,
   `OpenOptions`, metadata, capability profiles and typed errno/outcome values.
   Integrate `FilesystemLimits` into `application/limits.rs` and class policy.
2. **POSIX descriptor operations:** open/creat, read/write, pread/pwrite,
   lseek, fstat/ftruncate/fchmod, dup and close. Implement shared open-description
   offsets, permissions/creation mask, short transfers and owner-checked pins.
3. **Namespace and directory operations:** stat/lstat, mkdir/rmdir/unlink/rename,
   chmod/time updates, opendir/readdir/cookies, *at variants and statvfs. Add
   fsync/fdatasync/whole-cell sync with explicit transaction restrictions.
4. **Composition and lifecycle:** transaction-affine descriptors, event-end
   cleanup, conditional writes/conveniences and existing storage-clear behavior.
   `delete_all()` must refuse live filesystem state and preserve an empty
   initialized filesystem's schema/identity as specified in the API contract.

Use a pure, bounded reference model to compare operation traces where useful;
do not mirror SQL statements in tests. Add Rust compile/type tests once real
API code exists. POSIX-style signatures in plan Markdown are not compile tests.

Exit: a context-API example writes and reads a report, restarts with empty node
storage against the preserved recovery source and verifies its content/hash and
business receipt. It runs directly through the SDK and existing cell harness.

## FS-03: Extend guest batches and integrate file tools

Update `crates/cellar-guest/storage.rs`, operation validation, limits, codecs and
failure mapping. Add filesystem checks/mutations to `storage.apply`, with all
validation before effects. Add bounded read/stat/list operations. Update the
Python and JavaScript SDK facades and bridge tests together. At the inspected
baseline these include `crates/cellar-python/python/cellar.py` and
`packages/cellar-sdk/index.js` / `index.d.ts`; confirm source drift before edits.

Expose `ctx.fs` and the POSIX descriptor/directory operation family,
not only whole-file agent helpers. Add opaque per-invocation native handles and
correct cleanup on cancellation, descriptor close and domain shutdown. JS
facades use bigint where required and validated decimal-string encoding on the
wire; Python ints receive the same native bounds. Mutating handle state such as
read-offset updates, open, dup and close cannot be classified as concurrent
side-effect-free reads. Positional reads can use the known-read lane after
validation. Test native/JS/Python parity and malformed/stale tokens.

Extend the Vercel agent's `Save`/receipt effect descriptor so file changes join
the journal update in the same cell transaction. Tools use the current
invocation's `ctx.fs` for reads and the native atomic batch for mutations and
receipts. Add read/list/write/edit/rename/delete tools, version conflicts,
bounded output and durable read receipts. Adjust the agent's
configuration fingerprint because the tool schema changed; unfinished runs
from the old configuration still require their original release.

Keep the existing example's one-cell-per-turn identity at this stage and label
its files turn-local. Add a new session-owned example with indexed turn journals
and serialized mutating turns, demonstrating follow-up access through the context
API. Do not copy the previous turn's filesystem as an implicit follow-up mechanism. Add Pi and
Python parity after the common native contract passes.

Exit: crash tests before/after file commit and before result delivery reproduce
the original tool result and one mutation. A read replay after a later edit
returns the original recorded bytes. A file mutation cannot survive without its
receipt, nor a receipt without its file effect, in the same committed batch.
Exercise reduced guest value budgets as well as default limits: account for
old snapshot plus new journal, JSON escaping and future read envelopes. Verify
that rejecting a large tool result changes no files, retains saved decisions
and receipts, and leaves enough room to persist a bounded terminal result.

## FS-04: Qualify lifecycle, recovery and bounded work

Use the existing execution-domain and invocation registries for SDK descriptors,
accepted background work and cleanup. Every operation validates current cell,
domain, epoch, activation, invocation and exact transaction affinity. Create/open
pins are allocated under the owner boundary; rollback and cancellation release
provisional state. End-of-invocation cleanup invalidates tokens even if guest
code omits close. Dup keeps the inode alive until the last description reference
is released. Fenced cleanup may release resources but cannot issue stale writes.

Recovery restores files and business state through the existing LTX/proof path.
Reopen under fresh authority; never deserialize an old file capability. Reclaim
unlinked inodes only after former handle owners are fenced. Test the truncate
range/revision scheme through extension, rewriting, crash and GC restart.

Exercise real failover with a client ledger of acknowledged writes, local commits
and ambiguous responses. Validate file-plus-receipt transactions against empty
node storage, bucket/peer failures, disk exhaustion and new-owner writes.
Do not replace the cell durability machinery with a filesystem-specific replica.

Charge bytes, descriptors, operations, decode buffers and pending reclamation
before unbounded allocation. Long helper loops yield through the existing
invocation scheduler outside explicit transactions. Transactions have bounded
work budgets; do not split an advertised atomic operation silently. Stress one
hot cell alongside many independent cells and ordinary business transactions.
GC uses bounded batches and durable wake discovery, with supervised task cleanup.

Exit: all declared lifecycle/fleet cases pass; no stale operation succeeds,
acknowledged state recovers, quota failures preserve outcomes, and domain close
releases or explicitly reports all remaining resources.

## FS-05: Measure, package and release the primitive

Add native and guest SDK CI coverage on supported storage/runtime platforms.
Package the filesystem capability and captured limits. Update SDK/storage guides,
primitive exports, example catalog and readiness documentation with the exact
supported POSIX profile, local-commit semantics and explicit proof APIs.

Provide authenticated owner-routed inspection for filesystem usage, quota
pressure, retained GC, current revision and live SDK handles. Keep file contents,
paths and tool arguments out of default metrics/logs.

The implementation uses `FleetClient::filesystem(scope)` and the console's
`GET /api/v1/fleets/{fleet}/cells/{scope}/filesystem` with `data_read` permission.
The private signed sample has an exact scope/method-bound read capability and
uses normal owning-cell RPC gates. Inspecting an unused filesystem creates no
filesystem state. Return unavailable on exceeded work bounds; do not substitute
zero counters. See the [operator contract](../crates/cellar-admin/README.md#cell-filesystem-usage).

Run the operation, recovery and bounded-resource matrices against an exact artifact.
Record source SHA, toolchain, feature configuration, backend and raw outcomes.
Verify transfer limits, file/workspace admission, quota accounting and recovery
at the declared boundaries. Dense GB claims require measured recovery/resource
evidence; a large sparse logical offset does not establish dense-file capacity.
Chunk-size comparisons and application throughput targets are optimization and
deployment work, independent of the initial supported operation profile.

Exit: Rust and guest examples use `context.fs()` / `ctx.fs`, survive recovery,
publish their capability and resource limits, and pass the affected feature CI.
Core completion requires this plan's primitive gates; adapter deployment and
application performance qualification remain separately tracked.

## FS-06: Expand filesystem capability profiles

Choose additions from real API workloads and specify their semantics first:

- Links: link counts, unlink lifetime, symlink bytes, stat/lstat, follow/nofollow,
  *at variants, cell-root resolution, loop/depth limits and directory renames.
- Locks: open-description/invocation ownership, blocking/cancellation, deadlock
  policy, duplicate handles and release on invocation or authority loss.
- Wider metadata: bounded xattrs and explicit ownership/ACL policy.
- Allocation/copy/change observation: bounded work, atomicity, quotas and clear
  errors for unsupported flags; no implicit host-process facilities.

Update capability discovery only after native and guest conformance passes.
OS mapped I/O, kernel caching and process execution belong to separate adapters.
Core-v1 support does not imply universal compatibility with external programs.

## Required verification matrix

These are behavioral requirements, not claims of existing tests. Assign each a
stable case ID in the eventual test ledger and link its implementation.
The [operation conformance audit](per-cell-filesystem-conformance.md) maps each
public operation to inspected assertions and records the remaining audit work.

| IDs | Cases | Owning layer |
| --- | --- | --- |
| N01–N06 | Root rules; raw/UTF-8 names; invalid depth; trailing slash/type checks; exclusive create; inode overflow | Storage |
| N07–N12 | Rename replacement; nonempty destination; ancestor cycle; open/unlink; open/rename; readdir continuation during mutation | Storage/SDK |
| D01–D08 | Cross-chunk I/O; zero I/O; sparse holes; truncate tail zeroing; serialized append; bounded large writes; extend before truncate GC; rewrite before GC | Storage/SDK |
| Q01–Q06 | Logical quota; retained physical quota; inode quota; limit rollback; GC restart/pins; transaction work bound | Storage/runtime |
| T01–T07 | File + local state + receipt atomicity; later rollback; version conflict; expired tx view; guest prevalidation; invalid codec; remote bindings excluded | SDK/guest |
| A01–A07 | Stateless/facet refusal; retired invocation; same-epoch retired activation; ownership loss; closed domain; protected SQL | SDK/runtime |
| H01–H06 | Inode pin lifecycle; dup and last close; stale tokens; recovery orphan cleanup; cookie validity; provisional open/create cleanup | SDK/runtime |
| S01–S04 | Two-cell isolation; guessed capability; wrong-cell *at handle; path traversal and no ambient host-file access | SDK/guest |
| C01–C06 | Bounded operation/decode bytes; transaction work limits; cross-cell fairness; helper cancellation; GC admission; complete shutdown accounting | Runtime |
| G01–G06 | Saved decision replay; atomic mutation/receipt crash; recorded read after edit; journal caps; follow-up session files; serialized mutating turns | Agent file tools |
| F01–F06 | Empty-local-state recovery; stale-owner refusal; bucket outage; peer-proof failure; disk full/stall; proven post-takeover write | Fleet |
| P01–P08 | Accessor identity; independent/dup offsets; positional offsets; seek/truncate offsets; short/zero transfer; modes/mask; *at identity; close one duplicate | POSIX SDK |
| P09–P16 | Tx-handle expiry; ordinary descriptor in tx refused; savepoint affinity; fsync in tx refused; stale guest tokens; integer/name codecs; delete_all policy; pwrite on append descriptor | SDK/guest |
| B01 | Build the core primitive and SDK facades without adapter libraries or features | SDK/packaging |
| B02 | Public types and captured configuration expose only the context primitive; no mount or execution methods, fields or capability flags | SDK/packaging |
| B03 | Run Rust/JS/Python file-operation conformance using ordinary SDK invocations, with no mount service, device or helper | SDK/guest |
| B04 | Recover agent read/write/edit/list/search effects and receipts using the primitive alone, with no process runner | Agent file tools |

Lost response tests retain ambiguous outcomes until application receipts or state
checks reconcile them. Replaying a tool result must not rerun file mutations or
advance descriptor offsets. Atomicity claims apply to one bounded mutation or
explicit transaction, not an arbitrary multi-call agent program.

## Performance and capacity experiments

Measure at least:

- Small-file create/stat/read/unlink at 1,000, 10,000 and 100,000 entries where
  quotas permit, separating metadata and payload traffic.
- Sequential and random writes with 4 KiB, 16 KiB and 64 KiB chunks; small edits
  to dense large files; append-heavy workloads; repeated truncate/unlink GC.
- Rust and guest bridge costs, bounded range reads versus whole-file helpers,
  one hot cell versus many independent cells, and concurrent business state.
- Single-node bucket proof and fleet proof, including outages/backpressure;
  p50/p95/p99 operation/SQL/proof latency.
- WAL/LTX bytes per application byte, peak disk including WAL/GC, decode/request
  memory, and live descriptor/task counts per invocation and active cell.
- Dense 10 MiB, 100 MiB and 1 GiB workspaces, increasing only within configured
  limits; cold recovery to first admitted read and first proven new write.
- Sparse gaps independently of dense payload tests; no gap-sized allocation;
  strict memory bounds for repeated streaming reads/writes and rejection by
  bounded whole-file helpers.
- Durable agent file-tool workflow, follow-up turns, interrupted SDK work and
  repeated activation/cleanup at configured limits.

Use isolated state and a client ledger of acknowledged writes; compare recovered
file content/hashes and receipts. Reconcile ambiguous outcomes separately.
A throughput benchmark does not establish correctness or durability.

## Repository checks and completion evidence

For implementation, use the checkout-specific target directory required by
[AGENTS.md](../AGENTS.md). Run formatting, layout, native dependency and docs
checks, then affected native/guest tests and Clippy. Expand to the supported
SDK feature matrix at FS-05. Do not enable unqualified internal-test harnesses.

The existing `tools/docs/check.py` does not enumerate all `plans/` files; check
local links in this package explicitly. Documentation edits require document
validation, without claims that proposed Rust snippets compile.

Each package completion records source SHA, changed boundaries, test IDs/results,
environment/features, fault evidence, cleanup and remaining limits. Keep pending
gates open. Estimate delivery after FS-00/01 establish API and storage costs.
