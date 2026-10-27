<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Per-cell filesystem implementation status

Status: implementation in progress, 2026-10-27. The full
[API contract](per-cell-filesystem-api.md) and
[implementation plan](per-cell-filesystem-implementation.md) remain the delivery
scope. This ledger distinguishes implemented behavior from verified behavior.
FUSE remains a separate proposal.

## PR integration baseline

The PR is rebased onto `origin/main` at
`a9da48acf139b15b7a989c12f1237983bc333532` on 2026-10-27. Integration preserves
the newer guest/container dispatch and RPC admission paths, uses the current
RPC payload-size helper, and updates filesystem fixtures to the current host
configuration API. The session example's npm lockfile and linked capacity
reports are included in the repository.

Earlier measurements and captured-package results below retain their original
source/artifact scope. They do not qualify the rebased native host or constitute
a completed remote CI run. Final packaged-agent captures and target/ABI checks
must use the release candidate built from the integrated sources.

Selected post-rebase local checks passed on macOS arm64 with Rust 1.98.1:

| Boundary | Observed checks |
| --- | --- |
| Native filesystem | 21 filesystem unit tests, one page-reader owner regression, three SDK integration cases and five node cases run serially; dense capacity remains opt-in |
| Guest facades | Six combined Node/V8 filesystem cases, 20 V8 runtime cases, one transport-authority case, one guest budget test and Python SDK normal/read-only/small-budget modes with CPython 3.14.5 GIL |
| Agent units | Pi 16, Vercel 37 and Python 15 each under GIL and free-threaded CPython 3.14.5 |
| Management and tooling | 17 console cases, 97 JavaScript tooling cases, TypeScript declarations, example builds and SDK archive-content checks |
| Repository gates | Strict all-target Clippy for cellar/admin/guest/JavaScript/Python with combined guest features; formatting, layout, native dependency and documentation checks |

These selected checks do not complete the operation/error audit or production
qualification. The failed parallel stress observation remains recorded under
remaining requirement 3 below.

## Implemented boundaries

- Root-cell capability registration through native `Registry::filesystem` and
  JavaScript/Python `Source::filesystem`; `Context::fs`, `Storage::fs`, exact
  transaction affinity, and captured `CellLimits::filesystem`.
- Protected SQLite inodes, byte-name entries, 64 KiB chunks, sparse reads,
  positional writes, atomic append, namespace changes, modes and timestamps.
- Revision-bounded truncate ranges and bounded reclamation; open-inode pins,
  quota accounting, and storage-clear guards.
- Rust file/directory descriptors, explicit dup and shared offsets, directory
  cookies and *at methods, invocation expiry and asynchronous proof barriers.
- Common guest descriptor registry, symbolic errors and effect outcomes,
  base64 byte paths, decimal integer encoding, and JavaScript/Python facades.
- Atomic `storage.apply` filesystem checks and path mutations composed with
  local state and tool receipts.
- Bounded replacement/version-checked edits, stateless directory pages, descriptor
  dot entries and guest parity. Guest transfer/page admission accounts for the
  encoded result envelope before advancing offsets/cursors.
- Persistent filesystem-maintenance work, directory ancestry pins and bounded
  namespace traversal, plus per-transaction work budgets.
- `@cellar/sdk/filesystem-tools` provides bounded tools with canonical request
  fingerprints, atomic effect/receipt/checkpoint batches, saved read replay and
  typed refusal receipts. The new session example saves model decisions before
  tools, serializes mutating turns and retains files across follow-up turns.
- Looping helpers now expose confirmed `completed_bytes`, preserve uncertain
  outcomes and validate empty I/O against the descriptor. Whole-file helpers
  use their own byte/work bound instead of repeatedly rewriting chunks when
  descriptor transfers are small. Transactional GC charges its worst-case batch
  work. Native, the combined Node/V8 suite and CPython pass against final sources
  after removal of temporary investigation diagnostics.
- Conditional edits support bounded equal-length patches and tail append/shrink
  without buffering the existing file. Edits that shift an existing suffix
  retain the whole-file helper bound. A 100 MiB sparse-file regression covers
  cross-chunk patching, append, tail deletion and refusal of an oversized shift;
  this is not dense-file capacity qualification.
- Native errors now identify the outer public filesystem operation, including
  accessor admission, aliases, looping helpers and asynchronous barriers, while
  retaining the original cause, outcome and confirmed byte progress.
- The Vercel agent uses the shared bounded file-tool preparation API. File
  checks/effects join its existing run/receipt save transaction; proven refusals
  are saved without repeating file mutations. Its strict schemas and contract
  revision participate in the run configuration fingerprint. Files remain
  turn-local; the separate session example owns cross-turn workspaces.
- Pi now uses the same prepared-effect/receipt contract with strict TypeBox
  schemas and saved tool results. Its model/tool replay and native process
  recovery checks pass, including recorded file refusals.
- The embedded `cellar_filesystem_tools` Python module supplies the same bounded
  tool family, standalone atomic receipts and preparation for an existing
  journal. PydanticAI defers its tools, then commits each file effect with its
  receipt, partial results and next phase/alarm. Explicit barriers prove intent
  before provider I/O. A configuration fingerprint fences incompatible runs.
- Standalone JS/Python tools validate the complete future storage-read receipt
  envelope, including double-escaped request text, before submitting file
  effects. Oversized results can become recorded refusals; requests too large
  for even a refusal receipt fail without a file mutation. Both languages pass
  the 8 KiB regression, including Python restart/replay.
- Shared JS/Python checkpoint validators now admit the complete checked JSON
  request and every put's future read envelope before file effects. Pending
  monolithic journals reserve room for a bounded terminal update; the examples
  retain their 512 KiB application cap and additionally enforce captured bridge
  limits. Python accounts for request-side Unicode escaping. The PydanticAI
  tools phase can record `checkpoint_limit` when even a small refusal cannot
  fit, preserving prior saved decisions and receipts. Session journals use the
  same admission checks for decisions, observations and phase updates.
- `Context::fs()` now reports `NotApplied` if acquiring the storage capability
  fails before backend work. The original cause and acquisition operation are
  retained. An actual stateless invocation reproduced the earlier `Unknown`
  classification before the fix; stateless and retired-context regression
  checks now pass. Backend initialization/commit/proof outcomes are unchanged.
- Authenticated aggregate inspection now uses `FleetClient::filesystem(scope)`
  and the console's `data_read` endpoint. Signed scope/method-bound requests
  follow normal owner routing and proof gates without invoking application
  constructors or initializing unused file state. Read-only metadata accounting
  has fixed SQLite VM/time and handle-registry work limits. It reports explicit
  unavailability rather than zero usage, and preserves exact decimal counters.
- Positive logical/allocation/inode growth is now admitted before SQLite
  counter arithmetic. A regression reproduced `EIO / Unknown` when an aggregate
  logical size at the signed-integer limit grew by one byte; it now returns
  `EDQUOT / NotApplied`. Resize accounting subtracts the old size before adding
  the new size, avoiding precision loss during shrinking and overwriting.
  Overwrites at the allocation ceiling and reuse of pending truncated chunks
  remain legal. Existing development databases with the older resize trigger
  refuse recovery without migration or reset.
- Generic replica page reads now retain their activation's execution-domain
  owner while the LTX helper polls the supplied store on its separate runtime.
  A small production-development-store regression reproduces the earlier
  closed-domain error and verifies successful ranged reads plus refusal after
  owner closure, including from another domain. This fixes the GiB cold-open
  failure exposed by the new dense-capacity harness. Native S3 and custom reader
  factories retain their separate contracts.
- Class `Policy::read_only` is enforced by the native primitive and exposed in
  all SDK capabilities. Filesystem mutations return EROFS without changing
  local file state; business-state transactions remain writable. Existing-file
  read-only opens with create succeed, while missing-file creation is refused.
  Descriptor access errors remain EBADF. Trusted initialization and owner-side
  reclamation retain their own framework authority.
- Cached filesystem/storage admission now marks a refused call `NotApplied`
  before entering the filesystem backend. A native regression reproduced the
  former `Unknown` classification for a wrong-cell view. The fix retains the
  cause, outer operation and byte progress; SQLite/backend failures keep their
  own outcome classification. Native integration regressions and the Node/V8/
  CPython filesystem suites pass after this fix.
- Conditional edits now validate regular-file type before the empty-patch
  shortcut. A native regression reproduced a successful empty directory edit
  before the fix. Root, empty and nonempty directories now return EISDIR with
  zero completed bytes and known no effect; empty regular-file edits preserve
  bytes and metadata. Native transactions and guest batches roll back preceding
  file effects, and agent refusal receipts replay after deletion and restart.
- JavaScript/Python looping helpers now include capability reads and helper
  bound validation in their structured SDK error path. Targeted Node/CPython
  regressions reproduced missing operation fields before the fix. Empty and
  nonempty retired-context failures retain their original classification and
  report the helper operation with zero bytes; unknown protocol outcomes stay
  unknown. Explicit counters retain confirmed transfer progress through later
  errors. Guest EOF and partial-write cases check offsets, bytes and Python
  prefix identity/version through host restart.

Primary sources:
[SDK](../crates/cellar/application/filesystem/mod.rs),
[handles](../crates/cellar/application/filesystem/handles.rs),
[backend](../crates/cellar/storage/sqlite/filesystem/mod.rs),
[guest protocol](../crates/cellar-guest/filesystem.rs),
[guest batches](../crates/cellar-guest/filesystem_batch.rs).

## Evidence recorded so far

Commands use target directories unique to this checkout. Initial compilation
used Rust 1.94.1 to check the manifest minimum. The checks below ran with SDK CI's
Rust 1.98.1 and debug info disabled, against the uncommitted implementation.
The final affected-feature matrix and fault/capacity gates remain open.

| Check | Observed evidence | Scope/limit |
| --- | --- | --- |
| SQLite backend tests | Sixteen backend cases passed; all 21 filesystem-filtered library cases passed | Sparse data, truncate/rewrite GC at the allocation ceiling, quota rollback and signed-integer aggregate limits, namespace replacement, same-inode trailing-slash refusal and operation-specific root/dot errors, pinned unlink/ancestors and allocation-pressure fairness, depth/work limits, protected SQL, inode/cookie/revision exhaustion with complete rollback, real SQLITE_FULL during chunk growth, both development-format refusals, read-only open policy and bounded inspection |
| Authenticated filesystem inspection | Real-node test and all 17 console tests passed | Signed forwarding through another node, exact authority scope/method, refusal of application-minted inspection, unused/disabled classes without constructors or initialization, live handles/pins, retained allocation, management client, and observations after owner handoff. Console checks anonymous/read/write/configuration/fleet permissions and unavailable responses; existing event-state node regression also passed |
| Pending GC cold recovery | Writable and read-only successor cases passed | An output-proven file has an unlinked open inode when shutdown reaches its deadline. Joined retirement revokes the old handles; the old local directory is deleted. A differently named node restores only from the development bucket, resumes persisted maintenance with a one-chunk batch and a lowered allocation quota, and reclaims garbage before any application invocation. A read-only successor also refuses application mutation, reads the surviving sparse/rewritten content inside a successful business-state transaction, proves it and joins shutdown |
| Reclamation under concurrent cell traffic | All five filesystem node cases passed together; the dense capacity case remains explicitly opt-in | A hot cell makes 32 garbage-producing transactions under 1 MiB retained-allocation and 2 MiB logical quotas with one-chunk automatic GC. An older 768 KiB orphan stays open while newer garbage is reclaimed and four independent cells commit 96 file/state transactions. Quota refusal rolls back business state; the old handle retains its bytes. After release, the hot cell reaches an empty root without application GC calls. A new node with empty local storage restores all five cells' file/state checkpoints from the private development bucket and both shutdowns join. This is a bounded native progress/recovery case, not a throughput or broad fairness qualification |
| Replica refusal, fencing and recovery admission | Real native file/state/receipt case passed in the combined filesystem node suite | Actual replica publications are cut and drained before a locally committed second transaction; no client success escapes the proof gate. An independent lease-store outage fences the owner, retires its events and preserves the caller executor. After old local data is deleted, a node with an unreadable replica refuses cell admission without constructing application code. A healthy owner restores the acknowledged first transaction, rejects the old descriptor/context, proves a new third transaction, then another empty-node restore recovers it. Hashes, inode/version and receipts agree with the client ledger; unpublished transaction 2 is absent. The application propagates recovery failure as a root-handler 500. Supplied-store injection against the development bucket; no physical network, peer-proof or host disk-stall claim |
| Quota-fix runtime regression | Native filesystem plus both filesystem node cases, all four Node/V8 cases and the CPython harness passed | Current subtract-before-add accounting and growth admission, actual native/guest hosts, Node 24.13.1 and CPython 3.14.6 GIL. The node cold-recovery poll retries only the documented 503 while per-cell admission recovers; malformed successful responses and authentication failures remain fatal |
| Dense native capacity and recovery | 10 MiB, 100 MiB and 1 GiB passed in independent processes against the same executable | Real payloads, one-MiB file/state/receipt transactions, bounded 4 KiB edits, duplicate write replay, every-block digest comparison, empty-local-state recovery, exact recovered inode/version and receipts, proven new-owner append, automatic truncate reclamation and complete joined shutdowns. Captured profile uses 2 GiB file/allocation limits and explicit recovery budgets. Unoptimized macOS/private development bucket; no production latency or hard memory claim. [Report and artifact hashes](../tools/perf/results/2026-10-27-filesystem-capacity.md) |
| Paged replica owner regression | Focused failing/passing reproduction and all six native replication tests passed | Arbitrary-thread default range reads retain the original owner for metadata and streamed body I/O; closed owners cannot rebind. Existing supplied-store namespace, custom reader and generic/native S3 credential-provider cases pass |
| Native filesystem tests | All three passed through production turns after the cached-view admission fix | Descriptors/dot entries, bounded helpers with an 8-byte descriptor transfer ceiling, known/transactional partial-write outcomes, EOF progress, operation names, permissions and retained open rights after chmod, zero-I/O metadata/offsets, failed seek offsets, cell/host handle admission before create/truncate, cookie affinity, async-transaction barrier refusals, GC byte-work rollback, local rollback, expired contexts, isolation, proof requests, read-only restart and activation replacement. Wrong-cell and same-epoch retired filesystem/storage views preserve known-no-effect outcomes, causes, operations and current metadata/usage through read/write/mkdir/acquire/sync refusals |
| Node and V8 filesystem tests | All six cases passed together after the cached-view admission fix | Actual runtimes/hosts, binary names, atomic batches, version conflicts, stale wrappers, automatic GC, restart, file-tool replay, interrupted session recovery, follow-up reads, empty-helper refusals and read-only recovery |
| Native transaction and lifetime profile | Expanded filesystem harness plus existing facet and transaction suites passed | Nested synchronous/async savepoints, child/parent rollback, explicit rollback, exact handle affinity across suspension, parent offsets, nonrefunded operation/byte budgets, injected-clock metadata, directory mutation, stateless/facet refusal, accepted background I/O after response, wrong-cell/invocation handles, retained-handle expiry/pin release and pending root/child rollback on activation retirement |
| Guest metadata and directory-relative profile | Passed through Node, V8 and CPython native hosts | Signed timestamp extremes, noatime/omit/now, invalid creation flags/modes without effects, retained open rights after chmod, exact offsets beyond 2^53, overflow refusals, truncate/zero extension, creat/lstat/fdatasync, raw-byte *at operations after parent rename, unlink of an open inode, rewind/pages, directory proof and quota availability. Stateless calls preserve a known no-commit outcome |
| Namespace refusal and replacement profile | Passed through Rust, Node, V8 and CPython native hosts | Empty/NUL/overlong paths, depth, non-directory traversal, exclusive creation, type errors, nonempty removal/replacement, ancestor cycles and same-file trailing slash return exact errno/operation/no-effect outcomes and preserve sampled metadata/accounting. Valid self-renames are no-ops; directory replacement preserves pinned identities and link counts, and file replacement/unlink retains both old contents through GC until descriptors close. This is a selected namespace profile, not completion of all POSIX error cases |
| Conditional-edit file type and rollback | Native regression failed before the check; all three native tests, all six Node/V8 tests and CPython normal/read-only/small-budget modes passed after it | Empty regular-file edits at start/middle/EOF preserve bytes, identity, version, timestamps and usage. Empty/nonempty edits against root, empty and nonempty directories return exact EISDIR/operation/no-effect errors. Native transactions roll back preceding business/file changes; guest batches roll back preceding file mutations and omit business puts. Guest tools record the refusal, replay after directory removal, and replay again through a restarted host |
| Native directory-relative success profile | All three native filesystem tests passed after adding `directory_relative` | Byte-name open/stat/mkdir/rename/unlink through pinned parents after rename; absolute openat resolution from the cell root; unlinked inode reads, directory removal flags and linked dot pages. Also checks creat mode/content, path truncation, stat/lstat equality and quota/availability sums. Guest metadata profiles provide corresponding byte-name and namespace checks |
| Directory-relative authority profile | All three native filesystem tests, all six Node/V8 filesystem tests and normal CPython mode passed | Eighteen relative/absolute cases refuse a still-live foreign native directory across openat/fstatat/mkdirat/unlinkat/renameat, including both rename parents; two native transactions roll back preceding file/business writes. Original owner metadata/usage remain unchanged before its background continuation. Each guest runtime also passes 54 cases across closed, prior-invocation and wrong-kind tokens, preserving exact errors, metadata/usage/cursors and source/victim bytes. Guest token tests have a different scope from concurrent native cross-cell tests |
| Guest looping-helper preflight and progress | Targeted Node/CPython red reproductions; all six Node/V8 tests, V8 protocol case and CPython normal/read-only/small-budget modes passed after the fix | Retired-context and helper-bound failures report read_exact/write_all with zero bytes and preserve code/errno/outcome. V8 protocol replies and CPython facade injection preserve not_committed/unknown without byte dispatch. Real budget profiles check 4096 consumed bytes/Unknown on EOF, an unchanged observed file version, a 1024-byte locally committed prefix before EFBIG, checked original head bytes and Python inode/version/head/tail after local host restart. Protocol injection is distinct from real overload/fleet fault qualification |
| Root/dot and removed-directory profile | All 42 shared cases and removed-directory enumeration passed through Rust, Node, V8 and CPython native hosts | Existing root/dot creation returns EEXIST, directory unlink returns POSIX EPERM, root aliases remain EBUSY, non-root dot/dot-dot removal returns EINVAL/ENOTEMPTY, and missing/non-directory prefixes retain their own errors. Native/guest paths include directory-relative operations and assert exact known-no-effect failures with unchanged metadata/accounting. Removed or replaced open directories retain fstat identity but return EOF without cookie advancement, including rewind/seek; new entries remain refused |
| Host filesystem descriptor admission | Both native filesystem tests, all four Node/V8 cases and the CPython harness passed | Captured `max_host_handles` bounds all SDK file/directory/dup descriptors in one execution domain. ENFILE precedes create/truncate and preserves metadata, offsets and bytes. The native four-slot/two-domain case checks cell EMFILE precedence, cross-cell exhaustion, immediate close reuse, retained handles after commit/rollback/event expiry, and independent-host admission. Guest cases exercise native four-slot exhaustion and typed no-effect replies through their real bridges |
| Host service regression | All five native cell-limit tests and all 16 native domain tests passed | Captured configuration, unrestricted policy, host authority, clocks, input gates, cancellation and joined shutdown regressions after adding the domain-owned weak descriptor registry |
| Read-only filesystem policy | Passed through Rust, Node, V8 and CPython native hosts | Writer-state restart preserves bytes, metadata and usage through twenty refused mutation paths; EROFS/outcome/operation parity, open/create/exclusive flags, EBADF access checks, all-Omit timestamps, reads/dup/offsets, barriers and successful business-state transactions. A refused filesystem effect rolls back preceding business updates in both native transactions and guest batches |
| Small guest transport | Passed with an 8192-byte value limit; CPython mode repeated after the shared-case entry-point change | Bounded short reads preserve offsets; 255-byte binary names fit paged results; over-budget pages preserve the cursor; partial writes report confirmed local progress |
| CPython filesystem test | Normal, small-budget and read-only standalone modes passed with captured CPython 3.14.5 GIL and 3.14.5t on macOS arm64 | Primitive/batch/restart/automatic-GC, namespace/authority, helper error/progress and read-only profiles through the actual selected ABI. Separate executables/linkage evidence distinguish the ABI builds; no tracing or increased handler budget |
| Agent SIGKILL / cold recovery | All nine process cases repeated against the main-integrated source-entry host, including helper preflight normalization, busy-abandon and both ambiguity branches | Five boundaries cover acceptance, provider dispatch, saved decision, atomic file/receipt and completion. Four control cases supply unknown outcomes after a proven native batch or before dispatch, exhaust five attempts, then SIGKILL and remove old local storage. Resume reconciles the saved decision/receipt and completes exactly one native file-effect batch overall. Abandon preserves the observed committed or absent state and unknown outcome; a follow-up turn reads the file or records ENOENT. No-commit cases retain zero allocation/receipts and one root inode until resumed. Duplicate input alone does not resume; an in-flight step rejects abandon with 409. Source-entry SDK dependency bytes match current sources. These are guest-caller fault injections, not transport/storage fault qualification |
| Vercel agent file tools / cold recovery | Both filesystem cases and the existing ticket case passed on Node 24.13.1 | New node and empty local storage; nine receipts, unchanged write version, saved read replay after a later edit, stale-version refusal, rename/delete, five provider-fixture decisions without repeats and unchanged duplicate acceptance. Private development bucket; no paid provider or external S3 qualification |
| Pi agent file tools / cold recovery | All eight process cases and 16 unit tests passed on Node 24.13.1 | Existing six interruption points plus file-commit and after-edit cases; old node data erased, new node, alarm-only continuation, nine file receipts, unchanged version/read replay, saved conflict, rename/delete, five provider decisions and complete shutdown. A reduced-budget unit case rejects an oversized decision and still records retry exhaustion |
| Python SDK file tools | Native harness passed through restart and joined shutdown | Standalone receipts, whole-request replay checks, binary names, saved reads/refusals after mutation, search/list/edit, receipt caps, captured mutable requests, prepared effects and oversized decimal-offset validation; CPython 3.14.6 GIL build |
| PydanticAI agent file tools | 15 unit tests and both production-package crash cases passed on each captured Python ABI | CPython 3.14.5 GIL and 3.14.5t, hash-locked dependencies, archived runtime and current embedded SDK/tool/application sources. Entire runtime prefixes were moved away before installation, restored and verified. SIGKILL after proven file commit/edit, old local-state deletion, new node and alarm-only completion preserve nine receipts, five model decisions, saved reads/conflict and namespace deletion. Runtime health before/after recovery verifies GIL mode, including disabled GIL for the free-threaded build; native/Python shutdown joins. Exact packages are recorded below |
| Private receipt transport admission | Passed in actual Node and CPython runtimes with an 8192-byte value budget | Large read results become replayable validation refusals; double-escaped requests that cannot fit a receipt reject before changing an existing file. Python checks the same receipt after restart |
| Checked agent-journal admission | Passed in actual Node and CPython runtimes with an 8192-byte value budget | A new journal can fit one request yet fail its next compare-and-save bound; rejection occurs before submitting file effects. A terminal update remains writable and preserves the prior payload. Python also rejects escaped non-BMP input before submission and verifies the terminal journal after restart. Agent unit cases cover retry exhaustion and a full tool journal retaining prior decisions/receipts |
| Guest wire budget unit test | Passed | Worst-case base64 reads and 255-byte binary directory entries/cursors fit fixed result envelopes; configuration rejects inadequate metadata budgets |
| Installed agent package, historical | Refreshed through regular-file edit validation and passed with its capture directory unavailable | Native executable `dfd0f540ed6526883a507b9e5ea7a88d3963c2ac0b6cc9d0ce91664ba71b4455`, package version `0377d3184376af35ad986e045dbdb4040687a94157f435ab3224ef155d53663a`, inventory `24ea9a09c743388b7b1f9f941ddb3c11de3881dc60d16dca1476f507fa614a26`; Node 24.13.1 on macOS arm64. Runtime files were moved away before installation, restored from the archive and verified; SDK helper/session bytes matched that checkpoint's sources. Four isolated production-entry sessions, SIGKILL, actual predecessor-data removal, new-node recovery, unchanged completed runs, conflict/duplicate checks, follow-up file reads and joined shutdown. The nine interruption/control cases use the separate rebuilt source-entry host; this production package contains no fault hooks |
| V8 refusal / budget checks | Both isolated checks passed | Minimal closed empty-helper case uses default pump limits; separate runaway-code test terminates and joins at 40 ms |
| Strict Rust Clippy | Passed for core/guest/admin/JS/Python all targets after the quota fixes; core all-target checks repeated through read-only policy. JS/Python libraries/filesystem targets and the native-node target passed after read-only changes; Node-only fleet-package target passed after adding actual predecessor-data removal | Rust 1.98.1, JavaScript Node + V8 and Python embed features, `-D warnings` |
| Edit validation source checks | Strict Clippy passed for cellar, cellar-guest, cellar-javascript and cellar-python with all targets after the regular-file check and native directory-relative profile | Combined Node + V8 + Python embed feature graph, Rust 1.98.1 and `-D warnings`; this compiles affected targets and does not imply all their tests ran |
| Directory-relative authority source checks | The same strict all-target Clippy matrix passed after adding the foreign/expired/closed/wrong-kind profiles and shared guest driver routes | Core/guest/JavaScript/Python, Node + V8 + Python embed, Rust 1.98.1, `-D warnings` |
| Vercel unit/build checks | All 37 unit tests passed on Node 24.13.1; strict TypeScript build and formatter passed | Includes existing model/tool replay and ambiguity checks plus reduced-budget rejection with durable retry exhaustion; production-process cases exercise filesystem composition through the real JSON bridge |
| Existing SDK streaming tests | Three passed | Regression check for the shared JavaScript context facade |
| TypeScript/Python source checks | Passed | Strict original and filesystem declaration fixtures; Python facade/fixture compilation |
| Documentation and layout | Formatting/layout, strict core all-target and focused guest Clippy passed through the cached-view and native-failure cases | Formatting and module layout checked for 1099 Rust files; native dependency check reports 465 reachable packages with no JavaScript engine; docs checker validates 761 references and explicit filesystem/performance plan/guide/report links pass separately. Both SDK and Python workflows register the CPython read-only mode; SDK workflow includes dense qualification and preserves the native proof-fault client ledger. Workflow parses locally; remote CI and broader artifact/feature qualification remain open |

The combined V8 SDK/tool/session conformance fixture uses an explicit one-second
pump allowance. Its earlier default-allowance reruns encountered runtime
termination under concurrent builds. A reduced refusal case also encountered a
node-authority startup timeout, then passed when rerun in isolation. Production
runtime defaults and the independent 40 ms termination test remain unchanged.
These observations do not establish filesystem latency or load qualification.

The Python packages below include the native changes and guest helper
normalization before the PR's main-branch integration. Earlier Python observations using package
`af335c386176f83d82b854d1ac105f59724e0d626ce04f93d9345997d96693a1`
used the host interpreter/dependencies without a captured runtime; they remain
historical checkpoints. The refreshed Node session package below includes the
main-branch integration and JavaScript helper normalization. Other agent
artifacts and platforms remain open. New runs use the current development
accounting schema; older databases
with the previous resize trigger are deliberately refused.

### Captured session-agent checkpoint after main integration

This checkpoint uses source
`fbe59a850800e482e8bb03eee949d3d3d8cdc728`, Rust 1.98.1, Node 24.13.1 and the
development Cargo profile with debug info disabled on macOS arm64. The existing
Node packager captured and verified 29 files. Application, session, model,
lockfile and SDK/helper/declaration bytes matched the current sources.

| Artifact | SHA-256 or version |
| --- | --- |
| Package version | `0b67cc3e32325e64fb16365c8ec5de99fb33af181995cb167e64739b4955ac11` |
| Native executable | `a6536ec3f93f7c75f24f5886eef19a399fe40030d2a6cf189e5a10ee4e727843` |
| Runtime inventory | `4f9d7d64baee2bbcdc348508c31839f9ef6b9898ea6b2f30f0d8d0a8c1aa949a` |
| Runtime archive, 46,367,095 bytes | `db8a10aa502762d4ef04d07cc8bd4de36473bbffb4b51cb8a9ec68334e75e799` |
| Source-entry host | `08e32bd362bb71e98b85b38a70179869629ad8857aed6b59c0d4e4d3a07eaa96` |

The complete runtime prefix was moved away before installation. The production
entry restored it from the verified archive and ran four independent session
workspaces. SIGKILL, actual predecessor-data deletion and new-node recovery
preserved completed results, duplicate/conflict behavior and follow-up reads.
The successor host joined; the restored runtime passed its verifier again.
This test used the unmodified production entry and no fault hooks.

All nine interruption/control cases also passed against the fresh source-entry
host. An owned bootstrap probe verified that the actual script sent to Node
contained the complete current 65,575-byte runner, SHA-256
`a229ebf0f40fdfa9c0d42bb821391d435c8f1191a0b182beff606a52024b0a9c`.
The probe retained source hashes, not the private bootstrap configuration.
Its script forwarding is separate from the production-entry archive test.

The task-local `.cellar/filesystem-session-delivery-pr144.json` records the
source/package identities, actual-bootstrap proof, closure hashes, process
report and absent-prefix restoration. These are macOS/development-bucket
results; Linux, final Pi/Vercel captures, refreshed Python hosts and broader
failure/load qualification remain open.

### Captured Python ABI checkpoints

Both packages use Rust 1.98.1, uv 0.11.19, the development Cargo profile with
debug info disabled, and the pinned Python-agent wheel hashes. They target
`aarch64-apple-darwin`. Full SDK facade, file-tool and application source bytes
were checked inside each native executable.

| Runtime | Captured files | Native executable SHA-256 | Runtime inventory SHA-256 |
| --- | --- | --- | --- |
| CPython 3.14.5 GIL | 7,766 | `7a014a80982cb3805dc4dcfd2ef661f825523f8c3952dde8be33c7a67d4ce0e8` | `019ac54b9d071a702d68b00a38bf7733db7978db73cab5dd6313e5d13e1e0269` |
| CPython 3.14.5t | 7,770 | `270521d048ea7d1bd9079b25c240130514f536eb802a01a31e99d1dcc84f319e` | `9016bd3bdb0526bb5e73debf61232ec901b88b83cd783d3149d94f20dcdc75ba` |

Package versions are
`4e28e9be0c9d98284337611b086152b99c3cd4bcb4c31acfd5ead96242af1ffd`
for GIL and
`0cdd4d8bfc98618d1e5df405ff26f091bb4a72fc6af98ded09711c460109e96e`
for free-threaded. Each inventory/archive pair was produced by the existing
[package builder](../examples/python-agent/package.py), not by a host-runtime
fallback.

An independent Rust process driver has no loader dependency on either captured
prefix, allowing the complete prefix to be absent before installation. The
production host receives no ambient site-packages override. Both interruption
cases restore from a preserved development bucket into empty node storage;
the replacement's alarm completes the job before a task HTTP request. The
[harness](../crates/cellar-python/tests/agent_filesystem.rs) checks runtime health
before the first job and after model/tool/recovery work, preserving the mode.
The free-threaded agent and its unit-test imports keep the GIL disabled.

Separate SDK executions cover normal, read-only and small-budget modes against
each selected runtime. Frozen executable hashes and libpython linkage evidence
distinguish them because Cargo reuses the test filename across ABI switches.
Inventory verification also checks that SDK/agent execution leaves the captured
runtime files unchanged. These are local macOS/ABI results; Linux CI, other
agent captures, external stores, peer failures and load/resource qualification
remain separate gates.

The task-local `.cellar/filesystem-inspection-quota-evidence.json` records source
hashes and command-log hashes for this inspection/quota checkpoint, including
both failing integer-boundary reproductions and the passing regressions. Its
limits distinguish local runtime conformance from final artifact, capacity and
fleet-fault qualification. All processes started for those checks have joined.

The task-local `.cellar/filesystem-namespace-evidence.json` records source,
executable and log hashes for the trailing-slash regression and selected
cross-language namespace cases. The final fixture first drains earlier
scenarios' garbage, so unrelated reclamation cannot change its quota baseline.
It preserves the no-effect/accounting assertions through the actual SDK hosts.
The local CPython run uses 3.14.6 with the GIL; supported-ABI and final captured
artifact qualification remain separate open gates.

The task-local `.cellar/filesystem-reserved-namespace-evidence.json` records
the next namespace checkpoint: two backend failures before the root/dot/EPERM
fixes, the native failure before suppressing removed dot entries, and final
runtime/check logs with source and executable hashes. The shared
[42-case inventory](../crates/cellar/tests/support/filesystem_reserved_paths.json)
is compiled into the Rust, JS and Python fixtures; it requires no host-file reads
from a running cell. Each backend refusal compares every persistent filesystem
table and maintenance rows. This remains local SDK evidence, with the broader
release gates below open.

The task-local `.cellar/filesystem-host-admission-evidence.json` records the
failing host-cap reproduction and the passing native/guest/runtime checks,
source fingerprints and final executable hashes. The production registry is
owned by `HostServices`, retains only weak descriptors, and compacts at its
captured ceiling. The ordinary per-cell registry remains responsible for inode
pins. Defaults are 256 descriptors per cell and 16,384 per execution domain;
`unrestricted()` retains finite descriptor ceilings. TypeScript declarations and
the native guest capability replies expose the same host limit. This is live
descriptor admission, not an OS-file-descriptor or hard-RSS qualification claim.

The task-local `.cellar/filesystem-readonly-evidence.json` records the original
EROFS failure and passing backend/native/Node/V8/CPython checks, including the
read-only successor's automatic cold-recovery maintenance. It also records the
bounded concurrent-cell load case, final executed artifact hashes and source
fingerprints. The CPython modes use the local GIL build; external stores,
supported-ABI artifacts and the remaining fault/resource matrix stay open.

The task-local `.cellar/filesystem-agent-controls-evidence.json` records the
seven process cases, the final busy-abandon regression, captured production
package/inventory hashes and the restoration test. It preserves a copy of the
executed source-entry host before packaging rebuilt that executable. Earlier
runtime/package directories remain separately backed up. The production capture
was renamed away before restoration, and the installed runtime's SDK/session
bytes are checked against the source after the test.

The task-local `.cellar/filesystem-proof-fault-evidence.json` records the later
cached-view admission regression and fix, native replica/lease/recovery fault
case, nine current-host agent cases and refreshed production package. Its
`.cellar/filesystem-proof-fault-runtime.json` client ledger distinguishes
acknowledged transactions 1 and 3 from the locally committed, unacknowledged
transaction 2, including recovered hashes, inode/version, real refusal counters
and lifecycle checks. Provisional versions can be reused after an unpublished
prefix is lost, so the ledger and stable receipts establish identity. This is
local development-store evidence; the remaining target, peer/network, disk and
performance gates below stay open.

### Native semantics and lifetime cases

These are covered subcases of the implementation-plan IDs, rather than a claim
that every case in each group is complete.

| Case ID | Verified contract | Executing boundary |
| --- | --- | --- |
| P09/P10/P11-nested | Ordinary/parent handles cannot enter child work; child and outer handles expire at their exact boundary, including across async suspension | `transactions` in [native semantic helpers](../crates/cellar/tests/support/filesystem_contracts.rs), invoked by `/transactions` in [native harness](../crates/cellar/tests/native_filesystem.rs) |
| T01/T02-savepoints | Child rollback restores bytes and parent offsets; released child data joins parent commit/rollback; explicit rollback immediately invalidates handles | Same `transactions` helper |
| Q06/C02-savepoints | Rolled-back children do not refund root operation or byte admission; a fresh root gets a new budget and rejected root state does not persist | Same `transactions` helper, including bounded maintenance admission |
| P06/P14-metadata | Logical identity/modes, noatime, signed nanosecond extremes and explicit/omitted/clock-derived times preserve content versions | `metadata` in native semantic helpers; `/metadata` in [JS](../crates/cellar-javascript/tests/support/filesystem.js) and [Python](../crates/cellar-python/tests/filesystem.py) fixtures |
| P04/P07/P14-native-at | Native byte-name directory-relative operations preserve identity across parent rename and unlink; absolute paths resolve at the cell root. Creat/truncate/lstat and statvfs expose their declared values | `directory_relative` in native semantic helpers, invoked by `/metadata` in the native harness; corresponding guest metadata profiles |
| P07/S02/S03-at-authority | Both renameat parents and every directory-relative family refuse invalid authority before effects, including absolute paths; a refusal rolls back preceding native transaction effects | `foreign_directory` under `/foreign-handles` in the native harness and `/authority-{capture,closed,expired,file}` in guest fixtures; [operation audit](per-cell-filesystem-conformance.md#directory-relative-authority-profiles) records exact case counts and native/guest scope distinctions |
| P05/T02/G02-edit-kind | Empty regular-file edits preserve metadata; directory edits refuse even when empty. Refusal rolls back the native transaction/guest batch, and an agent replays its saved refusal after deletion and restart | `namespace` / `namespaceProfile` / `namespace_profile` and guest `tools-recovered`. The native root-directory assertion failed with `filesystem.edit_file unexpectedly succeeded` before adding the kind check |
| P05/P13/C01-helper-preflight | Guest helper preparation preserves outer operation, zero progress and original SDK failure classification; later EOF/size failure retains exact consumed/committed prefixes | `/expired` and `/metadata` in guest fixtures; `filesystem_helpers_preserve_preflight_failure_outcomes` in V8 runtime tests; Node/Python budget fixtures. See the [operation audit](per-cell-filesystem-conformance.md#guest-looping-helper-error-profiles) for the protocol-versus-real-host scope |
| G01/G02/P14/A05-captured-python | Both Python ABIs restore an absent runtime and recover file/state/receipt work into empty node storage, preserving runtime/GIL mode and joined shutdown; SDK modes execute against the corresponding captured libpython | [Captured ABI checkpoints](#captured-python-abi-checkpoints), production Python-agent package harness, its runtime-profile assertions and the standalone filesystem SDK test |
| P06/T01/T02-readonly | Read-only class policy preserves file/metadata/usage through mutations, flags and barriers; local reads compose with business updates, while refused file effects roll back the whole transaction/batch | [Writer/reader native harness](../crates/cellar/tests/support/filesystem_readonly.rs), native filesystem test, Node/V8 read-only tests and CPython `--read-only`; `readonly_open_preserves_existing_files_and_refuses_creation_without_effects` compares every persistent filesystem/work table |
| N12/H05-mutation | Directory cookies and stateless pages continue across removal/addition and parent rename; another directory cannot use the cursor | `directory_mutation` in native semantic helpers |
| N04-trailing-slash | Renaming a regular file to its own slash-suffixed name returns ENOTDIR / NotApplied. Failed source/destination slash cases and successful same-entry no-ops preserve metadata, revision, identity allocation, quotas and bytes | `rename_trailing_slash_requires_a_directory_even_for_the_same_inode` in [backend tests](../crates/cellar/storage/sqlite/filesystem_tests.rs); the test failed before moving destination validation ahead of the same-inode fast path |
| N01-traversal/N03/N04/N05/N07–N11-namespace | Root-clamped traversal, bounded invalid paths and selected namespace refusals preserve sampled state. File/directory replacements preserve inode identity, detached open victims and parent link counts; close and bounded GC complete cleanup | `namespace` in native semantic helpers and `/namespace` in [native](../crates/cellar/tests/native_filesystem.rs), [JS](../crates/cellar-javascript/tests/support/filesystem.js) and [Python](../crates/cellar-python/tests/filesystem.py) fixtures; the shared [guest driver](../crates/cellar/tests/support/filesystem_guest.rs) runs it in Node, V8 and CPython |
| N01-root/dot/P07-at | Forty-two operation-specific root, dot and prefix cases preserve exact errors and persistent state, including mkdirat/unlinkat from parent and child directory identities | [Shared case inventory](../crates/cellar/tests/support/filesystem_reserved_paths.json), `reserved_namespace` in native semantic helpers and `/reserved-{mkdir,rmdir,unlink,at}` in guest fixtures. `reserved_namespace_creation_and_removal_follow_operation_specific_errors` and `reserved_namespace_unlink_refuses_directory_identities_with_eperm` in backend tests compare all persistent filesystem/work rows |
| N12/H05-unlinked-directory | Removal and destination replacement suppress dot entries on retained directory handles; EOF preserves cookies, rewind/seek remain at EOF, fstat retains the inode and mkdirat remains refused | Native `namespace`/`reserved_namespace` and matching JS/Python profiles. The native replacement assertion failed before adding the linked-state check to descriptor enumeration |
| H01/H03/P08/C01-host-admission | Per-cell EMFILE and domain-wide ENFILE have known no-effect outcomes. Closing, committing, rolling back or retiring an invocation frees capacity even with retained SDK values; one domain cannot exhaust another | `host_filesystem_handle_admission_is_domain_owned` in [native harness](../crates/cellar/tests/native_filesystem.rs), implemented by [admission fixture](../crates/cellar/tests/support/filesystem_admission.rs); `/host-admission` in JS/Python fixtures with a four-slot captured host ceiling |
| A01/A02-acquire | Stateless/facet/retired contexts refuse access; capability acquisition failures before backend work have a known no-commit outcome | Native harness, [facet suite](../crates/cellar/tests/native_facets.rs), and `/stateless-fs` in both guest fixtures |
| A02/A03-scope-admission | Cached filesystem/storage views in another cell or a replaced activation fail before backend work with NotApplied, zero bytes, outer operation and retained cause; failed path mutations change neither root metadata nor usage | `preflight_authority` in [native semantic helpers](../crates/cellar/tests/support/filesystem_contracts.rs), invoked by `/foreign-views` and `/retired-views` in the native filesystem harness. The first wrong-cell stat assertion reproduced Unknown before the scope-admission mapping fix |
| H01/A02/C04-background | Accepted background work can use its descriptor after response delivery; another invocation/cell cannot borrow it; retained values lose authority and pins when the event ends | `/background`, `/foreign-handles` and `/background-expired` in native harness |
| A03/T02/C04-retire | Retiring a suspended root/child transaction drops owned work, rolls back files/state/quotas and prevents old descriptors from using a new activation | `/retire-transaction` and `/retired-transaction-check` in native harness |
| N06/H05/P14-exhaustion | Last representable inode/cookie/revision transitions succeed; the next create, rename replacement or write returns EOVERFLOW with no counter, inode, namespace, chunk or work-row changes | `exhausted_identity_cookie_and_revision_leave_no_partial_mutation` in [backend tests](../crates/cellar/storage/sqlite/filesystem_tests.rs) |
| Q02/Q05-pins | Retained allocation causes EDQUOT without a partial write; one-chunk GC skips an earlier pinned orphan, frees later garbage and makes space usable without changing pinned bytes | `pinned_orphans_do_not_starve_reclamation_at_the_allocation_quota` in backend tests |
| Q01/P14-aggregate | Logical size at i64::MAX remains exact through overwrites and shrinking; further aggregate growth is EDQUOT / NotApplied without changing file state; lowering the quota still permits shrinking | `aggregate_logical_quota_at_integer_limit_is_a_known_refusal` in backend tests; the huge file is sparse and is not dense-capacity evidence |
| F05-sqlite-full | A real SQLite page ceiling causes ENOSPC during chunk growth; observed rollback preserves prior content/accounting, the caller outcome stays Unknown, and writes succeed after removing the ceiling | `sqlite_full_during_chunk_growth_rolls_back_and_allows_later_writes` in backend tests; does not qualify host disk stalls or fleet proof failures |
| F01/F02/F03/F06/A03/A05-store-fault | Acknowledged file/state/receipt transactions recover through a blocked publication, owner expiry, failed recovery and healthy takeover; the deliberately unpublished local transaction cannot escape as success. A failed replica reader admits no application constructor. Old descriptors/context refuse new-owner use, and a proven post-takeover transaction survives another cold restore | `filesystem_unproven_write_is_fenced_and_failed_recovery_refuses_admission` in [native failure fixture](../crates/cellar/tests/support/node_filesystem_faults.rs), using the existing supplied-store decorators. `CELLAR_FS_FAULT_REPORT` records the client ledger, recovered hashes/inode/versions, real refusal counters and joined lifecycle assertions |
| FS-04-format | An older work constraint refuses first-use initialization; recovery of existing files refuses an old work constraint or resize trigger and preserves the database without migration/reset | `old_maintenance_constraint_refuses_initialization_and_recovery_without_reset` and `old_resize_accounting_refuses_recovery_without_changing_file_state` in backend tests |
| Q05/H04/F01-maintenance | Pending orphan GC survives forced retirement and empty-local-state recovery, runs without application code, and can reduce usage while above a newly lowered quota | `filesystem_pending_reclamation_recovers_without_application_access` in [real-node tests](../crates/cellar/tests/support/node_filesystem.rs) |
| Q05/H04/F01-readonly-maintenance | A read-only successor performs the same recovery and automatic GC without application construction, then reads the surviving file and commits/proves business state while refusing file mutation | `filesystem_readonly_recovery_reclaims_without_application_access` in real-node tests |
| Q02/Q05/C03/C05/S01/F01-load | One-chunk automatic GC makes space beyond an older pinned orphan while four cells complete file/state transactions; allocation/logical admission, failed-transaction rollback, final reclamation and five-cell cold recovery preserve checkpoints | `filesystem_reclamation_under_pressure_preserves_other_cells_and_recovers` in [bounded native load fixture](../crates/cellar/tests/support/node_filesystem_fairness.rs); 32 hot rounds plus 96 independent-cell transactions, with exact byte/counter checks and joined shutdown |
| FS-05-inspection | Owner-routed aggregate reads preserve absent state, exact integers, bounded work and permissions; samples after handoff carry the new owner epoch | `filesystem_inspection_is_authenticated_owner_routed_and_read_only` in real-node tests, [sample validation](../crates/cellar/observability/filesystem_state.rs), backend inspection tests and [console tests](../crates/cellar-admin/tests/console.rs) |
| D01/D06/F01/F06-dense | Dense payloads through 1 GiB survive cold recovery with exact file/state/receipts; a new owner can append, read and reclaim a trimmed tail | Opt-in `filesystem_dense_capacity_cold_recovery` in [capacity harness](../crates/cellar/tests/support/node_filesystem_capacity.rs), with [captured results](../tools/perf/results/2026-10-27-filesystem-capacity.json) |
| A05/C04-paged-owner | A page fault outside an installed domain uses its captured owner; owner closure refuses later faults even under another ambient domain | `default_paged_reader_retains_its_owner_and_refuses_after_owner_closure` in [replica storage tests](../crates/cellar/durability/replication/storage_tests.rs) |

### G04 checkpoint transport cases

| Case ID | Observed behavior | Test boundary |
| --- | --- | --- |
| G04-01 | An 8 KiB bridge refuses a pending journal with insufficient compare-and-save space before file submission; the previous payload remains available for a terminal update | [Node budget fixture](../crates/cellar-javascript/tests/support/filesystem-budget.js), run by `node_filesystem_encoded_budget_preserves_offsets_and_directory_cursors` |
| G04-02 | The same admission rule accounts for Python request-side non-BMP escaping, preserves file bytes and recovers the saved terminal journal after restart | [Python budget fixture](../crates/cellar-python/tests/filesystem_budget.py), run by `cargo test --locked -p cellar-python --features embed --test filesystem -- --small-budget` |
| G04-03 | Oversized model decisions produce no tool effects; charged retries still reach a saved terminal failure | The `small transport refuses a large decision and still saves retry exhaustion` cases in [Vercel](../examples/vercel-agent/test/durable.test.mjs) and [Pi](../examples/pi-agent/test/durable.test.mjs) |
| G04-04 | If a tool result and its refusal cannot fit, a terminal checkpoint retains prior model decisions, calls and receipts | `test_full_tool_journal_records_terminal_failure_without_losing_decisions` and `test_small_transport_reserves_a_terminal_checkpoint` in [PydanticAI tests](../examples/python-agent/test_app.py) |

### Agent interruption and explicit control cases

| Case ID | Observed behavior | Test boundary |
| --- | --- | --- |
| G01/G02-control-resume | A proven file/receipt commit with its reply suppressed remains at tool-receipt after retry exhaustion. Cold recovery preserves unknown outcome; explicit resume does not repeat the decision or file version and completes with two receipts | `uncertain-resume` in [process driver](../examples/filesystem-agent/test/process.mjs), using [guest-caller fault fixture](../examples/filesystem-agent/test/crash-fixture.mjs) and the actual native host |
| G05/G06-control-abandon | Abandon preserves the committed file, receipt and unknown outcome, releases workspace ownership, and permits a new turn to read the file; resume cannot resurrect the abandoned turn | `uncertain-abandon` in the same process driver |
| G06-control-busy | Abandon returns 409 while provider I/O is suspended, preserving the pending model phase; subsequent SIGKILL/cold recovery still completes normally | Additional assertions in the `provider` process case |
| G01/G02-control-before-resume | An unknown reply before native dispatch leaves the saved decision intact with no file, allocation or receipt. After cold recovery, explicit resume submits exactly one native file-effect batch and completes with two receipts without repeating the saved model decision | `uncertain-before-resume` in the same process driver; native batch-completion log plus namespace/usage, journal and model-call assertions |
| G05/G06-control-before-abandon | Abandon retains unknown outcome without creating a file or receipt; resume cannot resurrect the abandoned turn. A new turn records the ENOENT read refusal with no native file-effect batch | `uncertain-before-abandon` in the same process driver |

The native test proof adapter records real host proof requests but does not
substitute for fleet replication/fault qualification. Language fixtures run
actual native hosts, language runtimes, requests, local storage and shutdown.
A successful local restart is not an empty-node recovery drill. The separate
process suite performs cold recovery through the development bucket; it does
not qualify external object-store behavior or multi-node peer failures. Its five
cases each recover the original content and two receipts, preserve the file's
version after a saved write, reject conflicting IDs, isolate another session,
read the workspace on a follow-up turn and return a clean shutdown. Only an
uncheckpointed provider request repeats. Pending turns resume by alarm before a
session HTTP request; the completed case needs no continuation alarm.

## Release priorities

The primitive is available for review, with production qualification still
open. Prioritize the remaining work in this order; the detailed requirements
below remain the acceptance checklist.

| Priority | Next deliverable | Completion evidence |
| --- | --- | --- |
| P0 | Finish the POSIX operation, error, authority and resource contract | O01–O52 mapped to assertions across Rust, Node/V8 and Python; remaining option, cancellation, closed-domain and overload cases resolved |
| P0 | Complete durability and reclamation fault qualification | File/state/receipt client ledger across external-store/peer failures, disk stalls, takeover and sustained GC; explicit ambiguous outcomes and joined cleanup |
| P1 | Qualify final packages and supported runtime/ABI targets | Fresh Node agent captures, final-source Python captures, absent-runtime restoration, cold recovery and passing affected-feature CI |
| P1 | Establish supported capacity and performance limits | Optimized chunk-size comparisons, dense and small-file workloads, guest overhead, many-cell fairness, amplification and resource bounds |
| P2 | Consider additional capability profiles after the core baseline | Separate workload justification and contracts for links/locks/wider metadata or object-backed content; independent conformance before advertising support |

FUSE and command execution remain in the separate
[optional integration proposal](per-cell-filesystem-fuse-design.md); they are
independent of these primitive release priorities.

## Remaining requirements

1. Complete the operation-by-operation conformance/error audit and case-ID map.
   The [operation inventory](per-cell-filesystem-conformance.md) now maps the
   public surface to inspected assertions with O01–O52 audit IDs. Its rows are
   selected coverage, not complete per-operation error matrices; remaining
   overload, option/error and handle-authority cases stay open. Root/final-dot mutation
   errors and removed-directory enumeration now have the explicit cases above.
   Host-wide ENFILE now has domain-owned native/guest admission and lifecycle
   evidence; the read-only-filesystem EROFS boundary now has native, guest and
   cold-recovery maintenance evidence.
   Inode, cookie and
   revision exhaustion and signed-integer aggregate quota limits now have
   explicit rollback/accounting coverage.
   Timestamp/options parity, selected namespace refusals/replacements, nested
   affinity, directory mutation and bounded
   checkpoint cases have the explicit subcase mappings above. Do not infer full
   group completion from those mappings.
2. Finish final captured-runtime/package qualification across the supported ABI
   matrix. Vercel/Pi/Python tool parity and cold recovery now have local evidence;
   the Python run used a staged native package plus host runtime/dependencies.
   The session example's captured artifact has been refreshed and restored with
   its capture directory unavailable, with actual predecessor-data removal.
   Explicit uncertain-outcome, resume/abandon and busy-abandon now have local
   Node source-entry evidence for both committed and uncommitted effects.
   Captured Python-agent packages include both 3.14.5 ABI variants on macOS
   arm64 before the main-branch integration, with absent-prefix restoration,
   cold agent recovery and direct SDK-mode evidence. The Node session package
   and nine-case source-entry host have now been refreshed against the PR's
   integrated sources, including helper preflight normalization. Final
   Pi/Vercel captures and Python hosts built from the integrated sources remain
   open. The corresponding Linux CI/target gates remain unverified.
   Finish the remaining agent captures and supported target/ABI matrix. The production-entry
   restore case and qualification-only fault cases retain their distinct scopes.
3. Complete the maintenance stress/fault matrix. Pending-work cold recovery,
   pinned-orphan fairness and reclamation above a lowered quota now have the
   bounded backend and real-node evidence above. The bounded native load case
   adds automatic reclamation under quota pressure and concurrent file/state
   progress in four independent cells, followed by five-cell cold recovery.
   Longer-duration stress, larger cell populations, guest traffic and fault
   injection remain open. Root operation and byte
   admission across rolled-back savepoints has native evidence.
   A post-rebase local run executed independent node fixtures in parallel,
   contrary to the [qualification guide](../docs/19-testing-qualification.md).
   Four filesystem cases passed; the reclamation/fairness fixture self-fenced
   after its one-second node lease expired. The unchanged case passed alone,
   and all five filesystem node cases passed in the documented serial mode.
   Preserve the failed invocation as an observation outside that qualification
   profile; it does not close or replace the larger-population stress gate.
4. Finish the authority/resource case map and remaining cancellation, closed-domain
   and metadata-bound cases. Native background lifetime, wrong-cell/invocation
   refusal, event-end pin release and pending-transaction retirement now have
   evidence; extend the applicable cases across supported guest runtimes.
5. Complete the performance/capacity matrix. Native dense 10 MiB/100 MiB/1 GiB
   file-plus-state recovery now has same-executable local evidence, sampled RSS
   and retained file/WAL/LTX observations. Finish optimized 4/16/64 KiB chunk
   comparisons, small-file density, actual traffic amplification, hard resource
   bounds, guest overhead, external-store/peer proof and larger-population
   fairness; the four-cell bounded progress case is not that performance matrix.
6. Qualify durability failures and recovery under existing native/fleet harnesses;
   verify committed file-plus-state-plus-receipt recovery and stale-owner refusal.
   The native supplied-store case now covers blocked bucket publication,
   independent owner-store isolation/fencing, failed replica-read admission,
   stale capabilities and two empty-local-state recoveries with a client ledger.
   It does not qualify physical network isolation, external-store outages,
   peer-proof failures or host disk stalls. The SQLite page-ceiling case covers
   one local disk-full boundary; complete the remaining fault matrix.
7. Finish final packaged-example delivery and affected-feature checks;
   run affected Rust/guest tests and strict Clippy, formatting/layout/native
   dependency checks, declarations and documentation links against final sources.
   SDK guides, example catalog and Node CI registration have been added.
   SDK CI now also registers the 10 MiB native capacity case and its structured
   report; remote CI completion remains unverified.
8. Audit every initial-profile operation and acceptance item against the final
   sources and observed tests before declaring completion. Optional links/locks
   remain explicitly unsupported capability phases, as specified by the design.

## Development format boundary

The new maintenance reason extends the framework work-table constraint. The
resize-accounting trigger also requires subtract-before-add arithmetic. In
accordance with the repository's internal format-1 hard-cutover policy, enabling
this primitive refuses a development database created with the older constraint
instead of silently losing reclamation work; recovery refuses an older resize
trigger rather than accepting inexact accounting. No legacy migration was added.
The backend format tests now exercise first-use and recovery refusal while
preserving existing state. Broader artifact compatibility remains part of release
qualification.
