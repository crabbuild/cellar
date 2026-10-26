<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# 10 GB per-cell implementation and qualification

This tracks the active implementation of the user-requested 10,000,000,000-byte
per-cell storage limit. The preceding 256 MiB extension is not completion of this
work. The architecture and product policy are in
[large-cell recovery](large-cell-recovery.md).

## Completion requirements

- [ ] R1: Admit logical SQLite images through 10,000,000,000 bytes, with exact
  page rounding and configurable smaller quotas. Preserve independent value,
  command-delta, response, transfer-memory and concurrency limits.
- [ ] R2: Remove whole-image memory allocation from command freezing, snapshot
  encoding, transfer assembly, archive download/verification and recovery.
  Anonymous scratch files and blocking work retain their owners through failure,
  cancellation and shutdown. Resource admission covers actual buffers and disk.
- [ ] R3: Preserve SQLite lock-page holes, length, full-image identity, LTX file
  checksums, ordered frame/index coverage and checked offsets across 1 GiB and
  4 GiB. Invalid, corrupt or incomplete input cannot produce a publication pin.
- [ ] R4: Preserve exact Raft/epoch/membership/disk-lineage proofs. Qualify warm
  takeover, fresh authorized peer/S3 recovery, lost local state, worker restart
  and exactly-once durable receipt reconciliation at the requested size.
- [ ] R5: Quota exhaustion rejects growth without changing the committed image;
  reads, cleanup and durable retries remain usable at capacity. Reserve bounded
  framework housekeeping/receipt capacity without bypassing output proofs.
- [ ] R6: Run actual SQLite images near the decimal quota with compressible and
  incompressible data. Verify data, version, subsequent writes, oversize refusal,
  corruption, disk/I/O faults and interruption. Exercise the actual RustFS/S3
  backend, not only in-process storage or a small database with a large bound.
- [ ] R7: Address warm first-write and cold activation work proportional to the
  complete image using incremental/COW publication and bounded recovery. Measure
  peak memory, transferred bytes and first-read/first-write time. Do not claim a
  recovery-time objective or production capacity from configuration-only tests.
- [ ] R8: Pass owning unit/native/crash suites, affected feature-mode checks,
  Clippy, formatting/layout, native dependencies and documentation checks.
- [ ] R9: Open a follow-up PR from updated `main` after merged PR #119, and retain a
  reproducible validation record with explicit scope and remaining limitations.

## Work in progress

- Added a file-backed v0.5.2 physical snapshot codec with a disk-spooled page
  index. Source reads and decoded destination writes are bounded at 64 KiB.
  The default codec uses 64 KiB buffers; explicitly admitted large archives use
  2 MiB codec buffers and existing 4 MiB transport windows. Native files operate
  directly on these windows, with a 64 KiB fallback for existing providers.
  No profile changes encoded bytes or removes integrity checks. The codec omits
  the reserved SQLite lock page from LTX
  while binding canonical zero bytes into the complete physical image digest.
- Added independent logical and encoded size limits using u64 byte offsets.
  Frame/index/image/encoded integrity remains mandatory before physical output
  can be handed to the generation owner for sealing.
- Command freezing now hashes the independently read committed main-file/WAL
  overlay rather than allocating SQLite's serialized image. Tests compare its
  result with SQLite's pager across rollback, growth, shrink and checkpoints.
  Capture checks decoded/page/image bounds before page collection, including
  compression-heavy writes and fallback snapshots.
- Native snapshot sources/downloads use the file-backed codec. Transport moves
  file reads and appends into registered blocking work, retaining each scratch
  owner through cancellation. Installation tickets and membership/epoch checks
  remain separate from physical verification. The generation owner independently
  re-reads the output and shares the existing reservation/cleanup/sync barriers.
- Archives now store a small authenticated physical index and immutable 4 MiB
  parts, each using conditional create and exact read-back confirmation. Small
  snapshots inline their encoded bytes in the index to retain upload coalescing.
  The index is a format-1 hard cutover; no legacy archive reader is added.
  Recovery overlaps bounded part reads and spools the complete stream to disk.
  Snapshot plus bounded tail still shares a single private generation/publication.
- The opt-in materialized quota accepts decimal 10 GB without raising per-command
  delta, decoded-page, response or object-part buffers. Generation and unarchived
  admission are explicit disk/retention bounds, separate from transfer memory.
- Generation import, private writers and delta publication use exclusive COW
  clones on supported filesystems, with bounded copying elsewhere. Destination
  verification remains mandatory before callbacks or publication. Clone tests
  cover source isolation, collisions, unsupported fallback, corrupt output and
  uncertain creation. Full-image SHA-256 scans remain on these paths.
- Scratch sources live outside generation inventories; native receive and archive
  spools use the configured replica volume. Large transfers may explicitly use
  finite deadlines up to one hour; defaults remain unchanged. Longer deadlines
  are qualification controls, not a recovery-time objective.
- Added shared node snapshot scratch admission. Encoders reserve their entire
  body/index bound before creating files and shrink the charge after closing
  the index; receivers reserve their declared complete file. Source clones,
  partial-write failures and blocking owners retain the charge until the actual
  last file closes. Native source, receive and archive paths share one log-owned
  pool, separate from immutable generation and memory admission. Pressure maps
  to overload refusal. The large-image profile supplies a conservative four-body
  scratch floor; deployments may configure the independent byte/file limits.
- Private `SQLITE_FULL` failures return `StorageFull` before proposal submission.
  The disposable writer still requires rebuilding; an error alone does not
  prove rollback or permit reuse. Reads and durable receipts retain their normal
  committed-image proof boundaries.
- The materialized profile reserves virtual receipt/control headroom within the
  image quota (one sixteenth, capped at 32 MiB). Transactional accounting visits
  only the reserved btrees and their indexes; application Blob/outbox data uses
  the application allowance. Freelist pages remain reusable. Pointer maps and
  the reserved lock page have a separate, quota-sized structural allowance.
  Admission runs before commit and again after capture's control writes. A
  refusal never publishes private writes and requires rebuilding that writer.
  Receipt-space exhaustion reports overload independently of application quota.

## Validation so far

- The bounded-I/O follow-up passes 858 ordinary cases: 605 host library,
  68 archive, four native large-image, 27 native LTX, 22 native read,
  44 snapshot and 88 LTX unit/integration cases. Eight native and two LTX
  qualification/child entries remain ignored by their parent invocations.
  Tests compare exact encoded bytes across compact/buffered profiles and native
  and 64 KiB fallback providers, restore incompressible SQLite overflow data at
  three page sizes, preserve scratch ownership after uncertain window writes,
  and reject forged transport/physical data. Native sparse reads and index
  varints cross 4 GiB offsets and codec-window boundaries; truncated,
  noncanonical and overflowing varints fail closed. Real compressed SQLite
  images above 256 MiB retain exact identity and integrity across cache policies
  and buffer profiles. Rust 1.98.1 library/test Clippy, no-default-feature LTX
  all-target checks, native-only/no-Python libraries, formatting/layout (1,059
  files), native dependencies and documentation checks pass. The source-bound
  `d5f12630` release passes its 12 MiB preflight in 9.39 seconds (47,595,520-byte
  maximum RSS) and the actual 272 MiB RustFS recovery/write/restart preflight
  in 48.54 seconds (89,866,240-byte maximum RSS). Its independently authenticated
  selected archive head reaches index/term 9/4. A fresh full-size rerun is live
  under the frozen `d5f12630` binary. Ordinary tests and smaller geometry do not
  qualify production 10 GB recovery or an RTO.
- The frozen `bbdc6b2a` production RustFS drill at 9,999,998,976 SQLite bytes
  failed its unchanged deadline after 2,567.14 seconds (59,375,616-byte maximum
  RSS). Owner-state erasure, warm takeover/read/write, retained-receipt retry
  and quota cleanup/reuse passed. Repair admitted required index 9 in 0.894
  seconds. The subsequent 1,800-second archive wait retained selected head
  index/term 2/2. There was one index-2 encoder, followed by an index-9 encoder;
  both uploads were confirmed, but index 9 reached neither independent replay
  completion nor confirmed head selection before failure. A read-only RustFS
  inventory found 4,813 objects totaling 20,116,303,274 bytes and independently
  authenticated the selected 327-byte head envelope at 2/2. Uploaded bytes do
  not establish a recovery head. No cold restore, replacement write or restart
  was reached. Concurrent validation load and cache state were not controlled;
  these timings do not establish an RTO or a controlled performance comparison.
- The release-mode physical codec drill passed at the exact page-rounded quota:
  9,999,998,976 SQLite bytes and 10,083,173,257 encoded bytes. It exercised
  incompressible overflow data, offsets above 4 GiB, the reserved lock page,
  quota refusal, deletion/page reuse, restored payload hashes and full SQLite
  integrity checking. Current 64 KiB batched output finished in 695.60 seconds
  on the development filesystem, with 25,378,816 bytes maximum process RSS.
  Stages completed at 78.51 seconds (filled), 215.10 (encoded), 360.67 (received),
  501.20 (restored/synced), and 693.33 (SQLite integrity/payload verification).
  This is codec evidence, not native 10 GB or production RTO qualification.
- Native archive/execution/LTX/batched-apply/snapshot suites passed 149 tests,
  including process-kill snapshot and peer/S3 seed matrices, exact retained
  checkpoint recovery, partial transfers, shutdown, lost responses, missing
  quorum, corrupted archive parts and independent bundle publication. These
  ordinary native fixtures are small; they do not qualify native 10 GB recovery.
- Fast codec/capture/committed-image tests passed, including uncertain partial
  append poisoning, scratch-handle release, short reads, corrupted destination
  write detection and rejection of highly compressible oversized command output.
- LTX Clippy passed with warnings denied after COW and scratch placement changes.
- A 12 MiB full-capacity native template drill passed owner takeover, duplicate
  receipt reconciliation, quota refusal, cleanup/page reuse, an empty authorized
  S3 learner and local historical reads after restarting that learner before its
  peers. This validates the full-size harness at small scale. Full native 10 GB
  peer/RustFS runs and final-revision checks remain pending.
- The first full-size native RustFS attempt reached the exact quota-filled
  template, then correctly refused initialization at a remaining 256 MiB
  committed-marker guard. It failed after 48.93 seconds (26,591,232-byte maximum
  RSS); no native 10 GB recovery is claimed from that run. The guard now uses
  the decimal quota, with a regression test for the rounded last page and the
  first page beyond it. A rebuilt full-size run remains required.
- Shared scratch ownership tests pass for atomic refusal before source I/O,
  source cloning, completed downloads, uncertain partial appends and encoder
  errors. After native pool integration, 537 host library tests, four ordinary
  native recovery drills and 18 focused LTX tests pass. Clippy for both crates'
  libraries/tests with warnings denied, formatting, layout, native dependencies
  and documentation checks pass.
- The rebuilt full-size native RustFS baseline at commit `e46b0577` passed in
  2,197.01 seconds with 119,308,288-byte maximum process RSS. Its real SQLite
  image was 9,999,998,976 bytes. It covered owner service stop, warm takeover,
  retained-receipt retry, quota refusal, cleanup/page reuse, an empty S3-seeded
  replacement and local historical reads after restarting that replacement
  before peers. Stages completed at 263.28 seconds (native ready), 1,135.94
  (takeover/retry), 1,409.41 (quota/cleanup/reuse), 1,855.72 (empty-worker read)
  and 2,136.24 (restart read). This baseline did not erase the original owner's
  disk, promote the replacement or execute a new command on it. A strengthened
  harness adds those checks, distinct first/middle/last payload hashes and
  separate first-read/first-write timings. Both strengthened 12 MiB peer/S3
  drills pass (23.34 seconds total); the full-size reruns remain pending.
- The receipt/control accounting tests pass across 18 page-size, auto-vacuum
  and reserved-byte combinations, including index and freelist accounting. A
  protected temporary native dbstat table prevents application schema names
  from substituting allocation data. The native execution suite passes 21
  tests, including no-page-free cleanup at capacity, page reuse, duplicate
  receipt reconciliation and receipt-space refusal before publication.
- After the receipt policy and strengthened harness changes, all 540 host
  library tests pass serially. Clippy for both crates' libraries/tests with
  warnings denied, module layout, native dependency and documentation checks
  pass. Formatting and the CI native-only/no-Python library feature check also
  pass. This does not replace the remaining full-size and fault qualification.

## CI follow-up

- Rust 1.98.1 Clippy rejected constant-sized `chunks_exact` calls shared by the
  language streaming and native fixtures. Fixed array chunks preserve the same
  coverage. The streamed snapshot admission fixture now rejects insufficient
  archive/transport allowances before member storage opens and positively tests
  smaller independent command buffers and archive object parts. Both Rust 1.98.1
  Clippy target sets and all 29 native Raft host tests pass.
- The event-owner outage failure showed a healthy node expiring before the
  injected outage; a local replay also observed a healthy successor expiry.
  A deterministic dev-store regression found that a metadata read's final
  connection close checkpointed unrelated committed WAL data. Read operations
  now use read-only, non-creating connections with no close-time checkpoint.
  Writers retain their existing checkpoint and FULL durability behavior. The
  tests also require missing-store reads to leave the database absent, recovery
  after it returns, conditional updates and explicit checkpoint/reopen.
- At commit `0966e7fc`, all 542 host library tests (73.37 seconds), 29 native
  Raft host tests (42.15 seconds) and all 93 native-node tests (662.81 seconds)
  pass serially on the final reader policy. The original outage fixture also
  passes independently and in 20 further fresh-process repetitions (274.02
  seconds total). Its deadlines and fencing assertions are unchanged. Both
  Rust 1.98.1 Clippy target sets, the native-only/no-Python feature check,
  formatting, layout, dependency and documentation checks pass. Removing an
  identified read-side I/O stall does not establish that every intermittent
  lease expiry has the same cause; Linux CI qualification remains pending.

## Restart serving qualification

The full-capacity fixture now checks serving after restart as well as historical
reads. Once quorum returns, it transfers leadership to the restored replacement,
reconciles a pre-restart receipt without another callback, executes a new command
locally, verifies payload hashes at the resulting version and retries that new
command without another callback. Both small peer/S3-route drills pass with these
assertions (24.38 seconds total). The strengthened full-size runs remain pending;
this small-fixture result does not qualify 10 GB restart serving or fast recovery.

The strengthened native RustFS run at `f18f1c11` failed on the first linearizable
read with `VersionUnavailable`. It reached a 9,999,998,976-byte image and native
readiness at 508.43 seconds, but did not reach a successful first read/write,
owner erasure or empty-worker recovery. Total elapsed time was 990.79 seconds;
maximum process RSS was 44,417,024 bytes. All simulated workers used the workspace
volume and RustFS used the main volume. The runner stopped on this failure, so
the peer drill did not run. Logs, executable/source identities, a unique retained
object prefix and resource samples are preserved privately.

Investigation found full snapshot encoding inside the serialized SQL worker's
checkpoint/source capture. A blocked capture can therefore delay reads; this
needs a deterministic gated regression and a separate bounded encoding owner,
without relaxing read deadlines or checkpoint/publication proofs. This is a
working diagnosis, not a qualified fix. The extent-root contract is being built
in an isolated checkout while the existing native path is repaired.

The small gated regression reproduced `VersionUnavailable` while a checkpoint
encoder held the SQL worker (5.61 seconds). Snapshot metadata capture now freezes
the exact durable cut and retains its image pin, then a separately admitted
execution-domain blocking job performs encoding. Source, scratch and application
work ownership persist through completion and shutdown; encoding completion does
not release a newer SQL operation's busy flag. The snapshot descriptor, physical
verification and runtime checkpoint/install proofs are unchanged. Three gated
tests pass (2.33 seconds): reads/new writes during held encoding, joined close
with retained reservations, and a late encoding completion during a newer private
callback. `snapshot_encode` records this work separately from SQL. Broader owning
suites and the full-size peer/RustFS reruns remain required; these focused tests
do not establish that the earlier 10 GB failure has been eliminated.

At runtime revision `1a387317`, serial qualification passes 600 host library,
32 native host, 44 snapshot, 54 archive, 21 execution and four ordinary large-image
tests. The operations suite initially passed 22 cases and rejected its old
26-operation metrics inventory. Its updated fixed inventory explicitly includes
`snapshot_encode` (27 operations and 657 Prometheus performance samples), and all
23 operations tests then pass. This totals 778 passing cases; seven ignored
fixture entries remain outside the default parent invocations. Native all-target
Clippy with Rust 1.98.1 and warnings denied, the native-only/no-Python library
feature check, formatting, layout, dependency and documentation checks pass.
These suites retain the existing small-fixture crash,
corruption, exact-cut and shutdown coverage; they do not replace the still-required
full-size RustFS and peer reruns after the encoding separation.

The fresh full-size RustFS rerun at `552b3979` also failed on its first read with
`VersionUnavailable`. The exact 9,999,998,976-byte image reached readiness at
391.22 seconds; total elapsed time was 657.37 seconds and maximum process RSS
was 61,292,544 bytes. The peer drill did not start. Before unwinding, diagnostics
recorded one running leader SQL query, idle follower SQL workers, no snapshot
encoder starts, and incoherent reported images on all three replicas. Their
image-view positions remained at index 2/term 2 while durable application had
advanced to index 3/term 3. This disproves snapshot encoding as the complete
explanation for this failure. Query/VFS work and protocol-only image-position
reporting now require independent investigation; deadlines and assertions remain
unchanged. The failed log, frozen executable identity and unique RustFS prefix
remain preserved privately.

At `552b3979`, all-target JavaScript Clippy passes with Rust 1.98.1, both
`embed,node` features and warnings denied. All 16 focused V8 termination cases
and the complete 96-case local lifecycle matrix pass. The corresponding
[Linux lifecycle job](https://github.com/crabbuild/cellar/actions/runs/40181416781/job/121133098989)
also passes qualifier checks and the owned Node/V8/Python matrices. This is
language lifecycle evidence, not full-size recovery qualification.

The diagnostic work also reproduced an independent readiness-reporting defect:
a healthy protocol-only apply retained its verified image but made query
readiness false because its original publication position was older. Readiness
now shares query admission's complete-marker and monotonic applied-position
checks. The public three-node leadership-transfer regression fails against the
original `552b3979` release components (6.12 seconds), and passes with the
correction. Negative cases retain full marker binding, term/index coherence,
committed boundaries and the sampled runtime position. No read deadline, source
freshness policy or per-query quorum proof changes.

Owning behavior checks pass 603 host library, 32 native host, 21 execution,
23 operations and 22 read tests: 701 cases, with two ignored execution fixtures
outside their default invocation. The read suite includes the new leadership
transfer case without another application write. This reporting correction does
not establish a fix for the separately observed full-size physical I/O stall.

At `9946d022`, the JavaScript/Node, Python examples and typed-RPC lifecycle
workflows pass. The SDK's free-threaded Python job instead fails the native
`automatic_plan_recovers_controller_and_leader_loss` test while waiting for a
command-ready leader after repair. This is distinct from the originally
reported event-owner outage. The same membership case passes in 20 additional
fresh local processes, and the complete membership suite passes 27 cases with
one ignored process fixture. Its leader wait now retains the existing status
samples for failure output, without additional probes or a longer deadline.
The Linux timeout remains unresolved; local repetitions do not establish its
cause or a fix. Membership-target Rust 1.98.1 Clippy and formatting pass.

## Committed-image integrity and full-size diagnostic

Committed-image observation now binds its full main/WAL scan to SQLite's live
committed WAL-index cut. Both matching native-endian header copies, their
checksums, geometry, salts and final-frame checksum are required. A borrowed
write lock protects the 96-byte metadata copy; contention refuses immediately,
and the lock is released before image I/O. Rechecking the cut after inspection
retains the quiescent-owner boundary. A new regression failed before this change:
corrupting or truncating a newer committed frame could previously select an older
transaction when the database length stayed unchanged. Corrupt checksums, salts
and incomplete committed frames now refuse; active writers refuse and rolled-back
spilled frames remain excluded. No bad source bytes become a publication proof.

The final ordinary scope passes 603 host library, 21 native execution, 32 native
host, 27 native LTX, 44 snapshot, four LTX library and three committed-image cases:
734 cases with six ignored native process/fixture entry points. Rust 1.98.1
all-target Clippy for `cellar` and `cellar-ltx`, formatting, module layout,
native dependencies and documentation checks pass. This correctness evidence
does not replace full-size recovery qualification.

The private cache-pressure experiment against frozen `552b3979` runtime
components completed with a failure after 2,598.74 seconds and 82,837,504-byte
maximum RSS. Only snapshot-scratch files used macOS `F_NOCACHE`; application
code, timeouts and proof gates were unchanged. First read took 2.494 seconds and
first write 87.091 seconds. The joined former owner's local state was erased;
warm first read took 4.586 seconds and warm first write 78.888 seconds.
Takeover/retry completed at 559.222 seconds and quota/cleanup/page reuse at
778.372 seconds. It then exceeded the unchanged 1,800-second wait for an archive
head covering the newly admitted repair. Empty-worker activation, local cold
read, promotion and restart were never reached. The retained prefix is
`full-capacity-qualification/b74a73f6b84724a79e4f90e7eafe5a64`.
The query-time improvement is diagnostic evidence; this isolated filesystem
variant is not a production fix or a successful full-size recovery run.

## Archive publication work reduction

Direct inspection of that retained RustFS prefix found 4,812 objects, including
4,797 snapshot parts totaling 20,115,602,176 bytes and two full snapshot indexes.
The selected head stayed at index/term 2/2; a newer manifest for 9/5 was present
before the deadline, but its physical publication had not confirmed. The worker
sample was encoding a full snapshot. This is evidence of substantial pending
archive work, not an object-store outage or a successful cold activation.

Fresh publication now retains a private confirmed-source proof after every
part/index is conditionally created and exactly read back. The actual uploaded
stream must match the source's encoded SHA-256. Full physical validation and
independent destination replay/sync still run before head CAS, using that same
confirmed source instead of downloading its parts again. Dependencies still come
from their exact stored references. Tail extension and covering-head verification
retain their prior closure checks.

A protocol/configuration-only checkpoint can rebind a fully validated prior
snapshot index when its complete image state is identical to the new durable cut.
The new manifest captures the current exact position and configuration/member
bindings; no image parts are uploaded. Missing/corrupt prior material refuses
instead of being repaired. Wire markers, object layout, timeouts and publication
barriers are unchanged.

The fresh three-part regression fails before this change (six part GETs for
three read-back confirmations) and passes afterward with three. Configuration
rebinding previously uploaded three unchanged parts and now uploads zero.
Independent recovery still fetches all three parts. Six new tests also cover
transient source-read corruption, missing confirmations, source scratch lifetime,
independent destination sync refusal and missing prior parts during rebind.

The final ordinary scope passes 764 cases: 603 host library, 60 archive, 21
execution, 32 host, four ordinary large-image and 44 snapshot tests. Seven
child/opt-in fixture entries remain ignored by those parent invocations.
Rust 1.98.1 all-target native Clippy, the native-only/no-Python library check,
formatting/layout (1,037 Rust files), native dependencies and documentation pass.
The release diagnostic's 12 MiB end-to-end preflight passes through restart and
receipt reconciliation in 9.33 seconds (46,170,112-byte maximum RSS). It retains
the diagnostic9 cache policy for comparison; this is not production 10 GB or RTO
qualification. The fresh full-size archive/recovery rerun remains required.

## Large snapshot cache policy

The controlled macOS experiment showed that sequential snapshot spools can
interfere with first-query I/O even though codec buffers remain bounded. The
production codec now requests the injected file provider's optional streaming
cache policy for encoder body/index and receive files when logical image size
is at least 256 MiB. This selection also covers highly compressed large images;
configured maximum quotas do not select the policy for small snapshots. The
direct macOS provider uses `F_NOCACHE`. Other direct providers explicitly return
`Unsupported` and retain the ordinary path. Real advice I/O errors abort and
close files before refunding scratch admission. No path-name heuristic, global
OS setting, sync omission, integrity shortcut or deadline increase is used.

The selection regression fails before the codec requests advice. Five new cases
cover threshold geometry, invalid declarations, failure on either encoder file,
receive failures and actual native file I/O. Real SQLite round trips above
256 MiB and at small size compare the complete encoded identity across accepted,
unsupported and native-provider advice. They independently hash restored bytes,
check SQL values and run SQLite integrity checking. The large image compresses
below the policy threshold, and all instrumented reads/writes stay within 64 KiB.
Cloned receive owners retain their exact file/byte charge until the last close.
The synthetic 10 GB metadata case deliberately refuses incomplete receive;
it does not qualify a 10 GB physical snapshot.

The current ordinary scope passes 794 cases: the preceding 764 native cases
and all 30 LTX integration cases, including the five cache-policy cases. Eight
child/opt-in entries are ignored by these parent invocations. Rust 1.98.1 native
all-target Clippy, the final cache-test Clippy target, native-only/no-Python
libraries, formatting/layout (1,038 files), native dependency and documentation
checks pass. The release libraries also build. These checks do not establish a
bounded OS cache footprint or qualify production 10 GB recovery. The unchanged
full-size archive diagnostic remains live under its separate filesystem variant;
the checked-in production filesystem still needs its own full-size run.

## Full-size archive diagnostic after confirmation reuse

The frozen `3c3e68e5` runtime was rerun with the controlled diagnostic9
filesystem policy and the same independently hashed, incompressible
9,999,998,976-byte input. This diagnostic again failed with a deadline error;
the process returned 101 after 2,875.96 seconds with 79,151,104-byte maximum RSS.
It passed first read (10.173 seconds), first write (101.773 seconds), owner-state
erasure, warm first read/write (9.226/83.109 seconds), receipt retry and quota
cleanup/reuse. It did not reach cold local read, replacement promotion/write or
restart, so it does not qualify those full-size paths.

The retained test prefix contains 4,797 snapshot parts totaling 20,115,602,176
bytes, two snapshot indexes, two manifests and one head. The selected head still
binds index/term 2/2; an unselected manifest binds 9/4. Diagnostic inspection
verified metadata framing/SHA-256 and the selected head's exact manifest digest
and length. Uploaded bytes or a newer immutable manifest do not establish a
published recovery head. The separate production cache policy at `ce65404e`
was not part of this frozen executable.

The original generic deadline message did not identify its precise pending
phase. The full-capacity fixture now prints repair start/admission, the required
archive index and each observed head-position change. It issues the same
metadata requests and retains the same checks, polling interval and deadlines.
The ordinary peer/S3 preflights pass with these observations; the new full-size
production run must distinguish admission delay from archive publication delay.

## Production full-size archive observation

The frozen `3573f8e3` fixture (runtime code `ce65404e`) completed its checked-in
`DirectFileSystem` run against RustFS with the independently hashed
9,999,998,976-byte input. It returned 101 after 2,990.24 seconds with
77,807,616-byte maximum RSS. No debugger, private filesystem substitution or
deadline increase was used. First read/write took 4.549/174.868 seconds; warm
first read/write took 7.818/179.113 seconds. Owner-state erasure, retained-receipt
retry and quota cleanup/reuse completed before repair.

Repair admission reached required index 9 in 0.127 seconds. The unchanged
1,800-second archive wait then expired: the selected head appeared at index/term
2/2 and never covered index 9. No archive-ready, cold read, replacement promotion/
write or restart was reached. This isolates the pending phase to archive
coverage rather than repair admission; it does not establish a more specific
archive root cause. The run predates the later extent components and does not
qualify their integration. Repeating the unchanged runtime is not qualification
progress; native incremental publication remains necessary work.

## Incremental image data path

The [extent image contract](extent-root-contract.md) now includes typed,
domain-separated roots, verified private extent files, independent committed
SQLite/WAL observations and actual SQLite demand reads. A bounded file cache
fetches and verifies only requested extents through admitted downloads. It
shares the existing immutable VFS boundary with materialized generation pins;
the ordinary native read path retains its existing authority and semantics.
Real small/larger SQLite hot-set tests fetch one 4 MiB extent, while broad reads,
cache eviction/refetch and injected failures preserve the exact root and
physical file charges.

The combined scope at `eb459388` passes 873 ordinary cases, including 60 LTX and 813
native cases. Ten child/qualification entries are ignored by their parent
invocations. Native/LTX all-target Rust 1.98.1 Clippy with warnings denied,
native-only/no-Python libraries, formatting/layout and dependency/docs checks
pass. These components still do not supply durable native root inventory,
archive/peer placement, an authenticated cold writer base, incremental checkpoint
continuity or native installation authority. They do not close R7 or establish
a fast-recovery claim.

A later extent-read regression reproduces corrupt bytes escaping after a file
was initially sealed. Extent files now retain up to 2 KiB of derived block
digests and verify every later read, using at most two 64 KiB reads for an
unaligned request. The regression fails before the fix; all 62 LTX integration
cases and LTX all-target Clippy pass afterward. Actual SQLite also refuses a
faulty read from an already verified cache entry. This changes the extent data
path only; the 813 materialized native cases above remain prior-revision
evidence, and full native extent installation remains unimplemented.

The persistent `NodeExtentStore` now adds one shared node inventory, independently
verified imports, exact metadata views and local closure seals, monotonic
references, retained closed-root reachability and synced reclamation. Fifteen
owning cases pass, including actual SQLite reopen/demand reads and an abrupt
child-process exit during root publication and source-panic quarantine. The
combined scope passes 77 ordinary LTX cases; two child/full-size entries remain
ignored by parent invocations. All 49 native LTX/read regressions and LTX/native
all-target Rust 1.98.1 Clippy also pass; the other native baseline results above
remain prior-revision evidence.
The owner distinguishes recovered metadata from independently verified local
data and never treats inventory names as native authority. See the
[persistent owner contract](extent-root-contract.md#persistent-node-extent-owner).
Native selection, COW writes and archive/peer placement remain open.

## Bounded snapshot publication window

A gated archive regression reproduced serial part publication: holding one
exact read-back prevented a second part from entering. Snapshot publication now
overlaps two conditional PUT/read-back operations while keeping source reads,
whole-stream hashing and manifest part order sequential. Each source buffer
moves into its upload instead of being copied for object hashing and upload.
The fixed window fits the existing physical working-memory allowance; producer
admission bounds node-wide windows independently of image size. After any
failure, admission stops and every entered confirmation drains before the
source is released. Index/head publication and independent destination replay
retain their existing proofs.

The new regression passes, alongside an entered-window failure test that holds
the second confirmation after the first fails, checks retained scratch, then
releases it and verifies no head, complete cleanup and a successful exact retry.
All 62 archive cases pass, including transient source corruption and independent
destination sync refusal. Coarse `raft_archive_phase` observations distinguish
encoding, confirmed upload, retained-material reads, physical replay and exact
head confirmation. The full-capacity fixture enables these observations without
changing its checks, requests or deadlines. A new full-size run is still needed;
the bounded window alone establishes no recovery-time or production claim.

## Full-size publication phase observation

The frozen `29817781` production RustFS drill failed its unchanged deadline
after 3,128.52 seconds, with 78,954,496-byte maximum RSS. Its SQLite input was
9,999,998,976 bytes. Owner-state erasure, warm takeover/new writes, receipt retry
and quota cleanup/reuse passed; repair admitted required index 9 in 0.130 seconds.
The subsequent 1,800-second archive wait expired with selected head index/term
2/2. No archive-ready, cold read, replacement promotion/write or restart was
reached. This attempt used a separate RustFS instance on the workspace volume;
its store filesystem differs from the preceding run, so timings are not a
controlled comparison.

Coarse phases show three overlapping encoders for the same index-2 cut. Two
publishers completed their 10,058,329,319-byte encodings, uploads and independent
replays, then both started reading retained material at the old cut. Duplicate
cross-replica work is observed; a lower-level filesystem bottleneck is not
established. Bounded two-part publication alone did not qualify full recovery.

## Advisory cross-replica publication scheduling

Native archive builders now enable conditional scheduling for actual images
of at least 64 MiB. A bounded `publication` object beside the archive head binds
the birth history and a node/disk/process-derived publisher identity. Exact CAS
and exact read-back reconcile acquisition, renewal and release, including lost
replies. This is optional scheduling input, never ownership or recovery proof.
An old publisher cannot release a newer publisher's conditional record.

Peers age unchanged versions on local monotonic clocks, using a configurable
100..=60,000 ms grace period (default 5,000 ms). Renewal requires completed data
I/O. A process-alive timer alone cannot renew a stalled operation indefinitely.
The renewal future is joined; no detached heartbeat survives the data owner.
Malformed/unavailable advisory records leave ordinary publication available.
Covering heads still require each replica's independent material verification
and physical replay. All data digests, destination syncs and exact head gates
remain mandatory. Native incremental publication and full-size qualification
remain open.

The final ordinary scope passes 848 cases: 605 host library, 68 archive, four
large-image, 27 native LTX, 22 native read, 44 snapshot and 78 LTX integration
cases. Seven native child/full-size entries and two LTX child/full-size entries
remain ignored by their parent invocations. Six new archive cases cover
competing publishers, lost replies, active progress, abandoned work, a live but
stalled publisher, late release and corrupt advisory metadata. Independent
destination-sync refusal still exposes no new head. A file-observation case
proves pending/failed reads and metadata inspection cannot fake data progress.

An intermittent WAL fixture assumed every uncommitted spill grows the file.
SQLite may instead restart and reuse its allocation. The fixture now pins the
committed reader and explicitly flushes dirty pages while keeping the write
transaction uncommitted. Its original growth, refusal and rollback-digest
assertions remain intact; the corrected case passes 20 additional fresh
processes. This changes the fixture setup, not production WAL inspection.

## Buffered archive preflight

The actual 272 MiB SQLite image (285,212,672 bytes) exercises the native buffered
profile, including archive confirmation/replay, owner-state loss, replacement
promotion, a new command and receipt reconciliation after restarting the
replacement. It creates an incompressible fixture on the supplied worker volume;
`CELLAR_LARGE_IMAGE_FIXTURE` cannot substitute a different image in this case.
The release run at `d5f12630` produced 142 objects / 505,543,090 stored bytes in
its isolated RustFS prefix. An exact bounded GET independently verified the
selected head envelope and its SHA-256 at position 9/4. This is a native/backend
preflight, not evidence of full-size recovery or a recovery-time guarantee.

Supply the ordinary AWS environment and a disposable S3-compatible endpoint,
bucket and writable worker volume through `CELLAR_LARGE_IMAGE_ENDPOINT`,
`CELLAR_LARGE_IMAGE_BUCKET` and `CELLAR_LARGE_IMAGE_WORKDIR`. Use the
checkout-specific workspace target directory required by the contributor guide.
The owning fixture can be invoked explicitly with:

```sh
CARGO_BUILD_JOBS=1 CARGO_PROFILE_RELEASE_PANIC=unwind \
  cargo test --locked -p cellar --release \
  --test native_raft_large_images \
  full_capacity::buffered_archive_rustfs_recovers_after_owner_and_local_state_loss \
  -- --ignored --exact --nocapture --test-threads=1
```

The subsequent 9,999,998,976-byte run uses the preflight's source-bound
`d5f12630` release, retains the prior full-size input hash and uses a fresh RustFS
prefix with unchanged requests, deadlines and proof checks. Read-only process disk-I/O
counters supplement phase/RSS observations; they do not alter the target binary
or establish transferred bytes or a controlled performance comparison. It failed
after 4,080.31 seconds with `VersionUnavailable`, with 91,586,560-byte maximum
RSS. Warm takeover, owner-state erasure, receipt retry and quota cleanup/reuse
passed. The archive selected the required index/term 9/4 after a 931.957-second
wait; a completely empty replacement subsequently served a local historical
read after 912.264 seconds. The run did not reach the replacement's first-write
or restart proof. Its coarse log does not identify which intervening operation
returned `VersionUnavailable`; the fixture now adds operation-specific error
context and stage markers for payload verification and promotion. Requests,
deadlines and assertions are unchanged. This is a failed full-size recovery
qualification, not a passing capacity or recovery-time result.

## Remaining work

The completion checklist remains open. Full native 10 GB warm takeover, exact
peer and RustFS recovery after local state loss, restart/retry reconciliation,
quota housekeeping headroom, full qualification of aggregate scratch admission, and incremental
identity/lazy restoration still require implementation or qualification. COW
avoids full copies on supported filesystems, but full-image SHA-256 scans and
portable materialized copies remain on important paths; no
large-cell recovery-time objective is claimed yet. PR #119 has merged; follow-up
[PR #120](https://github.com/crabbuild/cellar/pull/120) contains the bounded
snapshot foundation and remains open for this work.
