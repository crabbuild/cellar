<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Plan 010: Bound recovery time for 10 GB cells

Status: proposed implementation strategy; no recovery-time target is qualified.
Prepared 2026-10-26 against `c6338ad45f64425e44ba54a9ad04f2058b947089`.
Priority P1; effort L; correctness risk HIGH. RT-01 through RT-03 are in progress;
RT-04 through RT-06 remain TODO. Implement them in dependency order and record evidence before
updating their status. This plan does not close the existing MR or R1–R9 gates.

The deliverable is faster, fault-safe native peer and RustFS recovery, through
the first durable write on the replacement. Improving only historical reads or
showing a faster file download does not satisfy it. The current 10 GB logical
quota and independent command/value limits remain unchanged.

## Baseline and current implementation

The configured native materialized paths pass at 9,999,998,976 SQLite bytes
(the page-rounded decimal 10 GB quota). The clean source is `106a961f`; all
1,265 tracked Rust/Cargo inputs at the plan revision match that executable.
These are individual functional drills on a documented two-volume layout,
not latency distributions or controlled comparisons between backends.

| Observation | RustFS path | Direct peer path |
| --- | ---: | ---: |
| Complete functional drill | 1,873.19 s | 2,434.59 s |
| Empty-worker join through first local read | 125.56 s after archive confirmation | 1,383.78 s including source preparation |
| Required archive-head catch-up wait | 1,112.36 s | No latest-head wait before peer seed |
| Restored owner's first write acknowledgement | 27.40 s | 30.94 s |

The functional drill contains other work between stages; do not add selected
numbers and label the sum failure-to-service RTO. See the exact source, log
hashes, scope and caveats in the
[current qualification](10gb-implementation.md#current-qualification).
The previously fixed synchronous snapshot-close defect is a separate regression;
the complete archive job, including failure cleanup, must remain off the driver.

Confirmed constraints at the proposal revision:

- Public activation constructs `CellStorage { rooted: None, ... }` in
  `crates/cellar/durability/raft/host.rs`.
- Materialized execution/apply still copies or clones and verifies complete
  images. Bounded RAM does not remove full-image disk/CPU work.
- `snapshot.rs::encode_image` calls `pin.snapshot(limits.physical())`; its
  default codec profile is Compact. Archive encoding already selects buffered
  I/O for large images. Measure the peer path before choosing its profile.
- `snapshot_transport.rs` receives one chunk, verifies it, awaits a blocking
  append, then requests the next. The default and maximum chunk are 1 MiB.
- Both `SealSeed` and `SealArchiveSeed` in `host_sql/mod.rs` reject rooted
  storage with `extent seed transport is not installed`.
- `NodeExtentStore::seal_image_root` requires a verified local dependency
  closure. `stage_root` deliberately does not claim missing extents are durable.
  A staged manifest cannot substitute for a sealed local image.
- `performance.rs::BOUNDS_US` ends at 30 seconds followed by infinity; current
  aggregate histograms cannot resolve 60–120-second recovery percentiles.

Two complete source spools were observed concurrently during the peer run.
Their consumers and the shares of source encoding, transfer and destination
verification remain unproven. Investigate before adding deduplication. The
existing native file adapter already supports caller-owned large I/O windows;
quota accounting already restricts dbstat work to framework btrees. Neither is
a missing implementation to redo.

## Targets and measurement contract

These are proposed acceptance targets on a declared reference profile, not
universal service promises. Record the profile and source with every result.

| Gate | Proposed acceptance | Meaning |
| --- | --- | --- |
| M1: complete recovery | Observed p95 <=120 s, independently for peer and RustFS | Blank authorized replacement reaches first verified read and durable small write; complete base is local |
| M1 stretch | Observed p95 <=60 s; explore 30–60 s | Same contract after reducing materialization and verification overhead |
| M2: incremental activation | Observed p95 <=30 s; aim for 10–30 s | Exact installed root serves the declared hot read/write workload before full hydration |
| Full hydration | Separate measured duration and bytes | All dependencies are locally verified; never equate this with M2 readiness |

Define timestamps in the qualification controller's monotonic clock:

- `t_fault`: injected owner loss, or the maintenance handoff request.
- `t_repair`: replacement admission starts with an empty worker data directory.
- `t_read`: replacement returns the required version and expected indexed data.
- `t_write`: replacement, after authorized promotion/leadership, acknowledges a
  new small command and passes an version-bound follow-up read. Verify its
  receipt and duplicate callback count as part of the trial's correctness oracle.
- `t_hydrated`: the entire selected root's local dependency closure is verified.

Report service failover on an existing warm voter separately. Report
`cold_ready = max(t_read,t_write)-t_repair` and
`cold_failure_to_ready = max(t_read,t_write)-t_fault`. The M1/M2 target applies
to the latter in an immediate-repair failure drill. Include detection, placement,
queue waits, grant acquisition, archive lag, source preparation, transfer,
verification, replay and promotion. The smaller metric helps diagnosis but
cannot replace the target. A maintenance drill reports its own timing.

The preliminary 120-second engineering budget is 15 seconds for detection,
admission and authority; 15 seconds for source availability; 65 seconds for
transfer/physical installation; 10 seconds for remaining committed-tail replay;
and 15 seconds for promotion, first operations and scheduling margin. This is
a budgeting hypothesis. RT-01 must measure critical-path overlap and revise
allocations without silently raising the overall target. A missing prepared
snapshot consumes the same budget; its preparation is never excluded.

Reference profile, initially proposed and then calibrated in RT-01:

- Three original voters across independent failure domains, with a surviving
  majority, plus one empty replacement. Fresh disk loss uses a fresh identity
  and authorized learner repair. Object bytes do not recreate a lost quorum.
- Replacement compute and the executable are provisioned for this reference
  profile; the replacement process starts cold and its entire local data directory
  is absent. Include process startup in the timer. If production also waits for
  VM provisioning or executable distribution, report those delays in actual
  failure-to-ready time and qualify that broader envelope separately.
- Near-quota deterministic incompressible data and separately compressible data.
  A timed indexed read touches a declared hot set <=256 MiB; the small counter
  command and receipt use ordinary delta/value limits. Full payload checks run
  afterward and can still fail the entire sample. Broad scans get separate
  results and do not inherit the hot-workload M2 target.
- Record CPU count/model/quotas, memory, storage/fsync semantics, topology,
  RTT/loss, TLS, store version, build profile and process/filesystem cache state.
  Component preflight targets are >=250 MB/s sustained source object/TLS reads
  and >=500 MB/s local sequential reads/writes and checksum processing. Measure
  simultaneous read/write contention too; these figures alone do not prove RTO.
- One active large recovery initially; then qualify two and a fleet repair burst.
  Bound the tail by both bytes and measured replay cost (initial budget 10 s).
  If write rate exceeds archive/catch-up capacity, report overload and target
  ineligibility; do not claim a fixed RTO for an unbounded backlog.

One 10,000 MB transfer takes at least 100 s at 100 MB/s, 40 s at 250 MB/s or
20 s at 500 MB/s. Repeated passes, source preparation and durable installation
add work. M2 removes complete hydration from first-use latency; it does not
remove eventual bandwidth, disk capacity or quorum requirements.

## Scope, conventions and source map

Keep the pure `cellar-logic` boundary free of I/O and async work. Use existing
execution-domain clocks, blocking jobs, cancellation and joined shutdown. Keep
typed errors, immutable generations, schema/history/position checks and output
proofs. Match the bounded ownership pattern in `host_snapshot.rs::encode_snapshot`
and the complete-job cleanup pattern in `host_archive.rs::publish`.

Production work belongs beside its owning component, with private focused
modules and preserved public paths. Never create both `feature.rs` and a Rust
`feature/` directory. New dependencies go through workspace Cargo.toml only if
an existing contract cannot serve the requirement. No JavaScript engine belongs
in this native recovery work.

| Responsibility | Existing implementation to inspect before editing |
| --- | --- |
| Timers, socket/store bytes | `crates/cellar/durability/raft/{performance,transport_metrics,object_metrics}.rs` |
| Public activation and ownership | `crates/cellar/durability/raft/{host,host_seed,host_snapshot}.rs` |
| Source and peer receiver | `crates/cellar/durability/raft/{snapshot,snapshot_capture,snapshot_transport,seed_transport,transport_options}.rs` |
| Archive selection/publication/replay | `crates/cellar/durability/raft/{host_archive.rs,archive/mod.rs,archive/publication.rs,archive/physical.rs,archive/replay.rs,seed_archive.rs}` |
| Root image execution/apply | `crates/cellar/durability/raft/{rooted/,execution/rooted_working.rs,execution/images.rs,host_sql/mod.rs,host_sql/images.rs}` |
| Immutable storage/codec | `crates/cellar-ltx/src/{generation.rs,stream_snapshot/,extent/}` |
| Functional acceptance fixtures | `crates/cellar/tests/{native_raft_large_images.rs,support/raft_full_capacity.rs}` |
| Native process/fault patterns | `crates/cellar/tests/{native_raft_daemon.rs,support/raft_seed_crash.rs,support/raft_seed_fault.rs}` |
| Physical process harness | `tools/perf/raft.py`, `tools/perf/raft-README.md`, `tools/multi-raft/README.md` |

Scope also includes new owning-component tests, a proposed recovery campaign
runner/analyzer under `tools/perf/`, explicit recovery profiles, and updated
component/operations documentation when behavior lands. Implementation changes stay within these owning components. Unrelated RPC, language adapters, UI, dependency
upgrades, higher quotas and LTX format redesign are outside the implementation.
Coordinate any required root/snapshot contract change in one format-1 hard
cutover; do not introduce legacy readers, silent backend fallback or migrations.

## Execution order and status

| Package | Deliverable | Depends on | Effort / risk | Status |
| --- | --- | --- | --- | --- |
| RT-01 | Stage evidence and a focused cold-recovery oracle | Existing materialized functional passes | M / medium | IN PROGRESS |
| RT-02 | Bounded bulk pipeline and shared prepared snapshot bytes | RT-01 | M / high | IN PROGRESS |
| RT-03 | Archive freshness, recovery scheduling and warm handoff | RT-01; integrate RT-02 | M / high | IN PROGRESS |
| RT-04 | Complete public extent snapshot/peer/archive recovery | RT-01; integrate RT-02/03 | L / high | TODO |
| RT-05 | Verified demand loading, incremental first write and hydration | RT-04 | L / high | TODO |
| RT-06 | Hard-failure, concurrency and percentile qualification | Fault cases accompany every package; final gate needs RT-02–05 | L / high | TODO |

RT-02 and RT-03 need not wait for the entire extent backend. Establish M1 after
they land. RT-04 first restores a complete local extent closure, preserving the
existing sealed-image contract; RT-05 separately introduces remote dependency
capabilities. Establish M2 only after its first-write and fault gates pass.

## RT-01: Measure and establish a focused recovery oracle

Implementation began from updated `main` at `b3ff49a6` after PR #128 merged.
The campaign guide at `tools/perf/recovery-README.md` describes the native
phase ledger, strict source/profile-bound runner and analyzer. Minute-scale
histograms and native peer/archive observations are implemented; the focused
ordinary-size pilots and broader regression checks are being verified. Full-size
stage attribution, repeated physical pilots and all later milestones remain open.
The measurement-core run passed 648 library, 70 archive, six ordinary large-image,
23 operations and 44 snapshot cases, including existing small process-crash
matrices. The fixed metric inventory is 41 operations, 39 counters and 22 buckets;
its native cardinality test checks exactly 1,230 performance series independent
of group count. The subsequently strengthened fixture recreates the replacement
SDK runtime inside the timer and passes three small cases, including a follower
seed source after a leader change. The initial fixed-source repair orchestrator
failed with `NotLeader`; the corrected orchestrator retains the same plan and
resolves the current leader for each mutation. The earlier uninstrumented stall
is not fully attributed by that deterministic regression. All-target/all-feature Clippy for `cellar` and `cellar-ltx` passes with warnings
denied. Formatting and layout checks cover 1,188 Rust files, and the native-only
dependency guard reports 466 packages with no JavaScript engine. The runner
owns per-trial worker directories through interruption and requires a source-bound
release build manifest. Full-size physical pilots remain open. These counts do not qualify a new
10 GB recovery time.

The first optimized smoke campaign at `40b4a540` failed all three trials after
the first local read with `NotLeader`. A controlled leader change reproduced
the final handoff controller's stale source: transfer admission can step the
source down before the target reports command readiness. The fixture now
refreshes the leader until the target is ready; server-side term, membership,
replication and readiness checks are unchanged. This does not attribute the
earlier uninstrumented stall. The three failed samples remain in the original
campaign. Separately, a real SIGTERM probe found an orphaned runner child. The
CLI now unwinds process and directory ownership for SIGTERM and Ctrl-C, with a
regression checking that child shutdown precedes worker-directory removal.
The current Python suite passes 231 cases with one skip. The optimized native
run passes all three small recovery cases, including the forced stale handoff
source, in 29.76 seconds; the three physical cases remain ignored. The focused
native target also passes Clippy with warnings denied. Repeated source-bound
pilots and new physical stage measurements remain open.

At `605cd8e5`, three separate 12 MiB peer trials passed (failure-to-first-write
median 3.929074 s, maximum 5.884990 s), and three 272 MiB RustFS trials passed
(median 17.926337 s, maximum 18.311141 s). The first full-size peer attribution
trial passed at 9,999,998,976 bytes: failure-to-first-verified-write 224.186171 s,
with source encoding 42.329 s, authenticated fetch/append 70.011 s and destination
verification 61.047 s. Fetch contains 55.342 s of receive time and 14.495 s of
serial append time. The first write/read-back interval is 26.776 s. The complete
drill is 694.31 s, including untimed fixture preparation and 52.461 s of cleanup;
it is not the recovery-time sample. One trial does not establish a percentile
or qualify the 120-second target.

The matching full-size RustFS trial failed its existing 1,800-second archive
coverage wait (2,297.15 s complete drill). The stored head remained at index 3,
before the acknowledged command and repair admission. Replacement restoration
never started. A new 4 MiB store write succeeded; 719 returned store-write errors
in that trial's log were expected conditional-create conflicts, and 33 part PUTs
logged more than five seconds in flight. These observations do not yet prove
the cause of the publication stall. The failed sample remains in its campaign.
Archive phase tracing and failure-path observations are installed. At `75949958`,
the 1,280 MiB RustFS diagnostic passed: failure-to-first-verified-write
85.568223 s, including 64.158376 s waiting for archive coverage, 13.081198 s
installing the seed and 4.084760 s through the first write/read-back. This crosses
SQLite's reserved-lock page boundary without reproducing the full-size stall;
it does not establish the cause of that stall or a size-dependent latency curve.
Repeated full-size qualification remains open. The separately passing older
full-capacity functional drills retain their narrower scope. Publication tracing
now identifies the publisher and requested cut, and
host debug events retain typed failure causes without changing retry policy.

At `986d7a7f`, the traced 9,999,998,976-byte RustFS immediate-fault trial passed all
recovery, write/read-back, receipt, payload and joined-cleanup checks. Failure to
first verified write was 1,921.131523 s: archive waiting 1,580.795023 s, seed
installation 246.375447 s, promotion 2.329619 s and first write/read-back
37.054271 s. The complete functional drill was 2,568.87 s, including untimed
setup; it is not the RTO sample. The 120-second target was missed.

The same publisher/cut trace attributes 26.978973 s to encoding cut 5,
978.571748 s to upload/confirmation and 34.545645 s to its physical replay. The
protocol-only successor cut 6 took another 557.408309 s, including a 214.359441 s
read of the prior material and 225.200802 s of physical replay. Its event material
index names the prior base, so correlate publisher and requested cut too. The
13,364 failed publication events were advisory `Unconfirmed` deferrals; this run
did not reproduce a timeout or permanent verifier failure. It identifies slow
full-image I/O in this run, not the cause of the earlier untraced timeout.

This was a shared-development-host diagnostic. The task's RustFS process was
restarted from its existing data before bucket preflight; separate-checkout
validation ran with one Cargo job. A one-second setup sample had no Rust symbols
in the stripped release image. A later two-second kernel/network observation
showed roughly 8.4 MB/s of upload progress and corresponding RustFS physical
writes; these counters do not isolate one object or qualify sustained throughput.
No object-store probes ran during the trial. Source/cache/hardware and hard-kill
qualification remain unverified.

The RustFS attribution trial included read-only store audits and a fresh 4 MiB
write probe during/around the observed archive wait. It is diagnostic evidence,
not a controlled latency comparison; the stall needs an unobserved reproduction
with the new archive tracing. The source-bound retention probe also found that
`--keep-node-state` kept an empty parent while Rust TempDir cleanup removed every
database. The flag now reaches the native fixture. A repeated real small trial
keeps nine SQLite files while its original-owner erasure check still passes.

1. Extend the existing finite metric families with source admission/encoding,
   fetch queue, actual received bytes, append, destination verification/sync,
   install, tail replay, promotion, first read/write and cleanup observations.
   Add useful long-duration histogram buckets, or a separate bounded recovery
   histogram. Update registry/cardinality tests. Do not add cell IDs, root IDs
   or request IDs as unbounded metric labels. Exact per-trial samples belong in
   the private evidence ledger, joined by a bounded trial identity.
2. Attribute source spools to capture purpose and exact generation. Count bytes
   scanned, hashed, encoded, transferred, written and retained separately. Keep
   kernel socket bytes, object payload bytes, logical DB bytes and fsync counts
   distinct. Record queue time even when no progress bytes are produced.
3. Add a focused immediate-repair fixture alongside the existing full-capacity
   drill. Reuse its source template, known payload oracle, receipts and native
   SDK paths. Preserve the existing long functional drill unchanged. The new
   fixture times the entire replacement read/write path without interleaved
   quota experiments; its later full verification still gates acceptance.
4. Add `tools/perf/recovery.py`, `analyze_recovery.py`, `test_recovery.py` and
   `recovery-profiles.json` (proposed new artifacts). Follow existing isolated
   process/container ownership patterns. Provide one owner for all child
   processes and backend resources; cancel and join them on failure. The runner
   needs four worker slots for immediate repair; the existing three-node harness
   is a starting pattern, not an already complete 10 GB recovery runner.

Verification: existing large-image, archive and snapshot targets below pass;
new ordinary-size tests prove stage completion/failure/cancellation accounting.
The analyzer's unit tests reject zero selected tests, missing phases, bad values,
source/profile drift and a timed-out sample disguised as success. A pilot with
three actual 10 GB trials per path produces a complete stage ledger. It is
baseline evidence, not a qualified p95. Do not choose pipeline parameters until
source/transfer/destination measurements identify the dominant work.

## RT-02: Remove avoidable materialized-path work

The initial pipeline is committed at `e9937227` while archive diagnostics proceed.
The peer receiver uses two admitted payload windows and one
registered physical owner through download, sealing and discard, with a joined
stop observer and shared memory charge. Native adapters append complete admitted
windows; injected providers retain the bounded fallback. Six ownership/window
tests, all 44 native snapshot cases and seven ordinary native large-image cases
pass. The library passes all 654 cases
serially; an earlier four-thread run failed one unchanged Python loader-inventory
case with `No such file or directory`, which also passes in isolation. All-target,
all-feature Clippy passes with warnings denied. An additional incompressible
12 MiB native TLS seed passes and checks that one receiver appended more than
two windows with matching received/appended byte counts. That test establishes
multi-window integration; the held-I/O tests establish the two-window bound and
overlap.

The next change selects Buffered source encoding and peer destination verification
on transports configured for images of at least 256 MiB. The source reservation
carries the selected codec policy, and receive admission precedes scratch-file
creation. The codec/wire reservation is 16.25 MiB with 1 MiB chunks; smaller
transports retain the default 8 MiB. Source chunks now fill the existing admitted
native read window directly, preserving injected-provider fallbacks. Eight
ownership/admission tests and nine codec cases pass, including cross-profile byte
identity and corrupt-frame rejection. The corrected 272 MiB buffered and 12 MiB
compact native TLS recovery cases both pass (111.42 s combined functional test
duration, not an RTO comparison). All 44 native snapshot cases pass again
(231.05 s), including exact-ticket installs, partial-body retries and process
crash matrices. All-target/all-feature Clippy passes with
warnings denied. An initial fixture incorrectly paired a 12 MiB physical template
with a 272 MiB quota and was correctly refused; its subsequent small case failed
preflight with a closing node. The small case passes in isolation, and both cases
pass together after generating the actual requested physical quota. The failed
run is retained; test-context fallout is plausible but not proven.

Prepared payload reuse and the full-size buffered performance comparison remain
open. These checks establish no 10 GB recovery-time improvement or qualification.

1. Where RT-01 proves benefit, select the existing Buffered snapshot codec for
   large peer snapshots under explicit memory admission. Account for codec,
   transport and decoded buffers together. Preserve Compact for providers that
   cannot admit the larger profile; encoded bytes and proof checks must match.
2. Overlap receiving chunk N+1 with writing verified chunk N using a bounded
   queue. Start with two 1 MiB slots; measure 2/4/8 slots. If RTT remains dominant,
   add bounded request pipelining with exact response correlation. Keep the
   existing frame bound initially. Wire-window changes require coordinated
   capability checks and independent tests, not simply a larger constant.
3. Coalesce expensive immutable payload preparation only for identical source
   generation/content, geometry, schema/history and compatible codec limits.
   A small node-owned cache pins prepared bytes under explicit byte/file limits.
   Each consumer still gets its own current cut, grant, lease and membership
   checks; shared payload bytes must never share authority. Do not assume the
   two observed encoders necessarily represent reusable work.
4. Cancellation detaches a waiter without dropping buffers/files still in use.
   The last physical owner releases the charge after blocking cleanup joins.
   Preserve authentication, ordered offsets, per-chunk digest, complete encoded
   integrity, destination verification and sync before publication. Keep full
   image verification wherever the current contract requires it; reducing such
   scans requires a separately specified proof, not trusting a filename/reflink.

Verification: snapshot tests add slow-disk/network, wrong-order/tag/digest,
expired lease, shared-consumer cancellation, cache eviction and shutdown cases.
Measured buffers stay within the declared window plus existing codec allowance.
Re-run both exact 10 GB drills and the focused campaign. Report saved source
encodes, transfer throughput and total RTO, not throughput alone. Any retained
whole-image copy/hash pass gets an explicit count and cost.

## RT-03: Keep a usable recovery source ready

The first implementation removes the unconditional checkpoint rotation for
configuration changes. A data update followed by learner admission now remains a
bounded tail over the existing base. Manifests carry separate base and final
configuration/member state, and recovery replays Raft's configuration changer
plus the live revision-bound member intents to the exact final cut. Joint leave
retains outgoing bindings until retirement; stale intents, unknown fields and
oversized changes are refused. A source metadata grant still cannot activate a
replacement before complete replay and independent physical sealing.

This is a format-1 hard cutover under the `/images-1/config-tails-1` protocol
identity; earlier namespaces and state are refused. The new regression failed
before the implementation because learner admission rotated the base. All 662
library, 71 archive, 44 native snapshot and nine ordinary large-image/recovery
tests pass after the change. A real RustFS preflight also passes with an actual
285,212,672-byte image, owner loss, local-state erasure, cold replacement,
promotion, a new verified write, receipts, callback counts and joined cleanup.
That preflight uses a debug executable and supplies no latency qualification.
Forced-checkpoint tests use
an explicit tail limit and keep their no-upload, exact-state, missing-part and
failed-destination-sync assertions. The initial full archive run recorded one
obsolete implicit-rotation expectation; the first native snapshot run found the
same tail-phase assumption in its exact-reference fault fixture. Both corrected
suites pass with their original refusal assertions preserved, and the failed
logs are retained. This establishes no full-size latency gain. Prepared payload reuse,
measured replay-cost rotation, foreground I/O and verified warm handoff remain
open below; full-image download/verification/replay still runs.

1. In the archive producer and controller, expose durable archived position,
   committed/applied position, tail bytes and estimated replay time. Archive lag
   is not just elapsed age or entry count. Maintain the existing conditional
   publication and independently verified dependency proof.
2. Keep a reusable verified checkpoint and bounded committed tail ready during
   normal operation. Coalesce obsolete pending work only when its retained
   history is covered by a newer verified recovery set. Rotate on measured
   replay bytes/time as well as entry count; avoid creating another full 10 GB
   snapshot on every small command.
3. Make recovery/foreground I/O scheduling explicit, node-wide and bounded.
   Reserve capacity for Raft control, queries and small-cell commands; bulk
   encoders and hydration share a separate queue. Include queue delay in RTO.
   Choose peer or archive based on authorized coverage and measured source
   readiness. Avoid unbounded speculative duplicate full-image downloads.
4. Preserve write durability semantics. Quorum acknowledgement does not prove
   current RustFS coverage; report degraded archive readiness and apply existing
   backlog admission rather than inventing an archive proof. If the source is
   behind, fetch the necessary committed tail through an authorized path or
   refuse readiness. Do not return stale state to meet the timer.
5. For maintenance, require the target voter's exact applied position, prepared
   writer and capacity before leadership transfer and drain. A blank standby
   must first seed and catch up. Keep at least a surviving voting majority.

Verification: archive tests cover slow/stalled store, competing publishers,
independent cells, backlog pressure and retained dependencies during rotation.
Controller/operations tests cover stale applied state, interrupted handoff,
quorum loss and concurrent repair. Campaigns include both ready and deliberately
lagged archives; waiting for the head must remain in their reported duration.
If the 10-second replay budget is infeasible, record the measured bound and
redesign snapshot frequency/catch-up before accepting M1.

## RT-04: Connect complete extent recovery end to end

1. Reuse the existing domain-separated `ImageRoot`, 4 MiB `ExtentId` and
   canonical manifest; do not invent another page index. A 10 GB image has
   2,385 extents and a 76,432-byte manifest. Roots bind schema, history,
   geometry and LTX state; the authenticated native record binds scope,
   incarnation and exact committed term/index/membership separately.
2. Define peer and archive snapshot descriptors containing the exact root,
   bounded manifest and immutable extent identities. Persist/upload missing
   extent objects with verified exact content and lengths. Publish the root
   conditionally only after its dependency closure has the required durability.
   Keep a bounded subsequent tail and protect references before log truncation.
3. Implement both source and receive/seal paths in snapshot/seed, archive replay,
   rooted apply and host SQL. For this package, receive and verify all missing
   extents locally before producing `SealedImageRoot` or installing a durable
   engine marker. Duplicate transfers reuse only independently verified objects.
4. Wire node-owned extent inventory, workspace budgets, root-reference recovery
   and GC through startup, checkpoints, retirement, membership repair and joined
   shutdown. Rebuild the complete reference set before reclaim. A single cell's
   restore must never reclaim another cell's live roots.
5. Enable public backend selection only after both peer and archive recovery
   have passed the same admission/receipt/fault oracles. Persist an unambiguous
   representation/capability contract, keep Cellar-owned markers at 1, and make
   mismatches refuse startup. Use a coordinated fresh-deployment hard cutover;
   no automatic conversion of an existing materialized history is in scope.

Verification: existing extent tests plus new native rooted peer/archive seed
and restart tests pass. Corruption, missing dependencies, truncation/growth,
reserved lock-page holes and offsets above 4 GiB must be tested. Repeat both
near-quota drills with the public extent path and the same payload/receipt
oracle. Backend flags without remote recovery are not completion.

## RT-05: Serve safely before complete hydration

1. Introduce an explicit installed-root capability backed by durable retained
   remote dependencies. This is a new contract, not a relaxed
   `SealedImageRoot`. Stage/sync metadata, verify the authenticated root and
   restore the exact committed tail before installation. Prove what base
   durability permits voting, log truncation and write acknowledgement before
   allowing a partially hydrated voter to contribute to those proofs.
2. Pin remote dependencies for each installed root, snapshot, seed and in-flight
   reader/writer. Retention must outlive a temporary source lease and survive
   worker restart. GC and promotion must inspect authoritative references;
   neither latest-head-only GC nor a cache-presence bit is sufficient.
3. Supply `ExtentSource` through the owning execution domain with bounded,
   coalesced fetches, deadlines and priority over background hydration. Verify
   exact length/content before exposing bytes. The synchronous SQLite VFS must
   not block the async driver; source unavailability/corruption yields a typed
   read/admission failure, never a zero-filled or different-version page.
4. Fetch schema/root pages and the declared hot set first; hydrate the rest with
   bounded background concurrency. Atomically record verified local presence or
   rediscover verified objects after restart. Resumption is by exact extent ID,
   never an unverified raw download prefix. Reads remain pinned to their root.
5. Complete incremental private-writer and apply integration so a small first
   command fetches/hashes only touched extents plus bounded metadata, publishes
   its new root under ordinary output proofs and retains unchanged dependencies.
   Do not fall back to full-image hashing on writer open, receipt access, quota
   checks or WAL recycling. Respect existing WAL/workspace/delta limits.

Verification: while hydration is intentionally paused below 100%, the authorized
replacement reads an uncached extent, performs and verifies a new durable small
command, reconciles a duplicate receipt and survives a fresh process restart.
Kill it before and after each root/extent/marker sync boundary. Delete its cache
and verify recovery at the same root. Loss of a required remote extent fails
safely. A read-only or learner-only demonstration does not complete M2.
For the same hot workload, first-use bytes/RAM stay bounded as total database
size increases from 256 MiB to 10 GB. Full-scan workloads are reported separately.

## RT-06: Harden and qualify the targets

Use deterministic small fault tests in ordinary CI and source-pinned physical
campaigns for actual size, process loss and performance. Runtime tests precede
each package's rollout; do not defer all fault coverage to this final gate.

| Matrix axis | Required cases |
| --- | --- |
| Physical recovery | Live peer and real RustFS, new process and empty local disk, then retained-disk restart |
| Crash boundaries | Hard process kill during source preparation, download, extent sync, root sync, engine install, first write and background hydration |
| Authority | Old owner returns, stale epoch/incarnation, expired grant, membership change, lost quorum, source changes mid-transfer |
| Integrity/capacity | Missing/corrupt objects or manifests, partial writes, disk full, growth/truncation, >4 GiB offsets, quota refusal and cleanup |
| Retry/lifecycle | Lost replies, same command ID, canceled waiters, overloaded queues, retries, joined shutdown with no premature refunds |
| Workloads | 256 MiB, 1 GiB, 4 GiB, near decimal 10 GB; compressible/incompressible; fixed hot set and broad/random reads |
| Interference | One/two recoveries and repair burst with foreground small cells; source/store stalls; realistic RTT and bandwidth limits |

Begin with three pilot trials per backend, then at least 100 timed trials for
each claimed backend/storage/reference-profile combination. Report nearest-rank
p50/p95/p99, sample count, spread, failures and censored deadlines. Treat failed
or timed-out samples as target misses, never discard them from the denominator.
The ordinary per-operation deadlines and all correctness assertions stay intact.
One hundred observations provide an empirical percentile, not a universal tail
probability guarantee; retain confidence/variability information with the claim.

Bind every result to source/tree/executable hashes, exact selected tests, profile,
limits and complete phase ledger. Keep backend credentials and cell contents out
of published artifacts. No SQL readers or full payload samplers may warm the
replacement before its timed first operations. Label source/store OS caches
separately; an empty worker is not evidence of a cold source cache. Use disposable
VMs for cold OS-cache experiments; do not drop the shared development host cache.

A scoped pass requires zero durability/integrity/ownership violations and the
chosen target met in its declared envelope. Separately report unavailable or
underprovisioned runs. Backpressure may refuse excess work safely, but a refused
recovery is not a successful RTO sample. Measure foreground latency/throughput
against a same-source control; initially require <=10% degradation at the declared
non-saturating load. This interference bound is proposed and must be calibrated.

## Verification commands and expected results

Use a writable Cargo target unique to the implementation checkout on the workspace
volume. For example, choose a fresh name before the first build:

```sh
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellar-recovery-time-010"
cargo format --check
cargo check-layout
python3 tools/check_native_dependencies.py
python3 tools/docs/check.py
cargo test --locked -p cellar-ltx --all-features --lib --tests
cargo test --locked -p cellar --lib
cargo test --locked -p cellar --test native_raft_large_images
cargo test --locked -p cellar --test native_raft_archive
cargo test --locked -p cellar --test native_raft_snapshot
cargo test --locked -p cellar --test native_raft_rooted
cargo test --locked -p cellar --test native_raft_execution
cargo test --locked -p cellar --test native_raft_operations
cargo test --locked -p cellar --test native_raft_daemon
cargo clippy --locked -p cellar -p cellar-ltx --all-targets --all-features -- -D warnings
```

Expected: nonzero selected tests for each named target, all pass, guards and
Clippy exit zero; no lint allowances or weaker timeout/proof assertions. Apply
owning-crate feature checks from `.github/workflows/native-sdk.yml` and its wider
release gates before deployment. Do not enable `cellar_internal_tests` manually.

Existing explicit full-size commands (supply the documented private RustFS/S3
environment and work directories; never print credentials):

```sh
cargo test --locked --release -p cellar --test native_raft_large_images full_capacity::decimal_ten_gb_rustfs_restores_native_serving_after_owner_and_local_state_loss -- --ignored --exact --nocapture --test-threads=1
cargo test --locked --release -p cellar --test native_raft_large_images full_capacity::decimal_ten_gb_peer_restores_native_serving_after_owner_and_local_state_loss -- --ignored --exact --nocapture --test-threads=1
```

Expected: exactly one selected test per command, both pass with payloads, quota,
new writes, restart, receipts and joined shutdown. These commands alone do not
qualify M1/M2. They currently select the materialized representation; RT-04 must
add equally explicit extent commands and verify their actual selected backend.

The following initial campaign interface is implemented for materialized
embedded-node pilots. Qualification still requires the remaining fault and
hardware gates; see `tools/perf/recovery-README.md` for current behavior:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tools/perf -p 'test_recovery.py' -v
python3 tools/perf/recovery.py --backend rustfs --storage materialized --profile reference-10gb --repetitions 3 --output /tmp/cellar-recovery-010-rustfs
python3 tools/perf/recovery.py --backend peer --storage materialized --profile reference-10gb --repetitions 3 --output /tmp/cellar-recovery-010-peer
python3 tools/perf/analyze_recovery.py /tmp/cellar-recovery-010-rustfs --mode pilot --target-seconds 120 --minimum-samples 3
```

The pilot analyzer must clearly label its result unqualified. For acceptance,
use fresh private run directories, `--repetitions 100` and
`--mode qualify --minimum-samples 100`; require all phases, the exact target-sized fixture,
post-recovery value/receipt checks, hard-kill suite evidence and source/profile
identity. Repeat independently for peer and RustFS. RT-05 adds `--storage extents`
and a distinct activation profile/30-second target; a hydrated-first fixture
cannot satisfy that profile. Qualification mode must exit nonzero with inadequate
evidence or an unmet target; pilot mode must never emit a qualified result.

## Completion, stop conditions and maintenance

Before editing, compare the current implementation with the source map and
record `git diff --stat c6338ad45f64425e44ba54a9ad04f2058b947089..HEAD -- crates/cellar/durability/raft crates/cellar-ltx tools/perf`.
Reconcile drift in these contracts before executing a step. Use a `codex/`
branch and reuse an active relevant PR; after it merges, start from updated main.
Keep each package independently reviewable. Update this status table and the
current qualification ledger with exact evidence; never mark the entire existing
multi-Raft delivery complete from a recovery-only pass.

Stop the affected rollout and report a contract/design decision when a step
would require trusting unverified bytes, losing a live root reference, counting
a partially hydrated voter without a valid durability proof, changing history
or weakening ownership/output/deadline checks. Safe work on independent packages
can continue. Hardware that cannot meet the profile should yield an honest
measured envelope and revised design, not a concealed timing exclusion.

All of these must hold before claiming this plan implemented:

- [ ] RT-01–RT-06 evidence is source-bound and their table rows are DONE.
- [ ] Peer and RustFS M1 each meet the 120-second observed-p95 gate.
- [ ] Public extent peer/archive recovery passes the full native correctness oracle.
- [ ] Both extent sources satisfy M2 with verified durable first writes before full hydration.
- [ ] Process-loss, corruption, stale authority, replay, GC, retry, resource and interference cases pass.
- [ ] Ordinary tests/Clippy/guards pass, including unchanged archive-close regressions.
- [ ] Operators can inspect the selected root/cut, source freshness, tail cost,
      queue/phase progress, verified cache bytes and full-hydration status.
- [ ] Documentation names the qualified hardware, workload, concurrency, cache
      state and sample population; deferred matrix items remain explicit.

Review future changes to command envelopes, root formats, archive compaction,
log retention, WAL policy, storage providers and execution-domain lifecycle against
these gates. Source lag and per-small-write full-image work are recovery costs
that must remain visible even after the bulk network path becomes faster.
