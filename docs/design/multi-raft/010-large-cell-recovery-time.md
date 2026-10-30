<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Plan 010: Bound recovery time for 10 GB cells

Purpose: Sequence bounded 10 GB recovery work, measurement contracts, failure oracles, and unqualified recovery-time targets.

[Design library](../README.md) · [Topic contract](README.md)

| Implementation | Validation or example |
| --- | --- |
| [Code and API](../../../crates/cellar/durability/raft/README.md) | [Checks and usage](../../../examples/raft-counter/README.md) |

The map summarizes this document’s scope; the recorded gates determine completion.

```mermaid
flowchart LR
    A["Verified recovery source"] --> B["Bounded transfer and image work"]
    B --> C["Readiness and failure qualification"]
```

## Reading map

Start with these sections; subsequent entries retain their original source and evidence scope:

- [Baseline and current implementation](#baseline-and-current-implementation)
- [Targets and measurement contract](#targets-and-measurement-contract)
- [Scope, conventions and source map](#scope-conventions-and-source-map)
- [Execution order and status](#execution-order-and-status)
- [RT-01: Measure and establish a focused recovery oracle](#rt-01-measure-and-establish-a-focused-recovery-oracle)
- [RT-02: Remove avoidable materialized-path work](#rt-02-remove-avoidable-materialized-path-work)

- Status: proposed implementation strategy; no recovery-time target is qualified.
- Prepared 2026-10-26 against `c6338ad45f64425e44ba54a9ad04f2058b947089`.
- Priority P1; effort L; correctness risk HIGH.
- RT-01 through RT-03 are in progress; RT-04 and RT-05 are in progress; RT-06 remains TODO.
- Implement them in dependency order and record evidence before updating their status.
- This plan does not close the existing MR or R1–R9 gates.

The deliverable is faster, fault-safe native peer and RustFS recovery, through the first durable write on the replacement. Improving only historical reads or showing a faster file download does not satisfy it. The current 10 GB logical quota and independent command/value limits remain unchanged.

## Baseline and current implementation

The configured native materialized paths pass at 9,999,998,976 SQLite bytes (the page-rounded decimal 10 GB quota). The clean source is `106a961f`; all 1,265 tracked Rust/Cargo inputs at the plan revision match that executable. These are individual functional drills on a documented two-volume layout, not latency distributions or controlled comparisons between backends.

| Observation | RustFS path | Direct peer path |
| --- | ---: | ---: |
| Complete functional drill | 1,873.19 s | 2,434.59 s |
| Empty-worker join through first local read | 125.56 s after archive confirmation | 1,383.78 s including source preparation |
| Required archive-head catch-up wait | 1,112.36 s | No latest-head wait before peer seed |
| Restored owner's first write acknowledgement | 27.40 s | 30.94 s |

The functional drill contains other work between stages; do not add selected numbers and label the sum failure-to-service RTO. See the exact source, log hashes, scope and caveats in the [current qualification](10gb-implementation.md#current-qualification). The previously fixed synchronous snapshot-close defect is a separate regression; the complete archive job, including failure cleanup, must remain off the driver.

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

- Two complete source spools were observed concurrently during the peer run.
- Their consumers and the shares of source encoding, transfer and destination verification remain unproven.
- Investigate before adding deduplication.
- The existing native file adapter already supports caller-owned large I/O windows; quota accounting already restricts dbstat work to framework btrees.
- Neither is a missing implementation to redo.

## Targets and measurement contract

These are proposed acceptance targets on a declared reference profile, not universal service promises. Record the profile and source with every result.

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

- Report service failover on an existing warm voter separately.
- Report `cold_ready = max(t_read,t_write)-t_repair` and `cold_failure_to_ready = max(t_read,t_write)-t_fault`.
- The M1/M2 target applies to the latter in an immediate-repair failure drill.
- Include detection, placement, queue waits, grant acquisition, archive lag, source preparation, transfer, verification, replay and promotion.
- The smaller metric helps diagnosis but cannot replace the target.
- A maintenance drill reports its own timing.

- The preliminary 120-second engineering budget is 15 seconds for detection, admission and authority; 15 seconds for source availability; 65 seconds for transfer/physical installation; 10 seconds for remaining committed-tail replay; and 15 seconds for promotion, first operations and scheduling margin.
- This is a budgeting hypothesis.
- RT-01 must measure critical-path overlap and revise allocations without silently raising the overall target.
- A missing prepared snapshot consumes the same budget; its preparation is never excluded.

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

One 10,000 MB transfer takes at least 100 s at 100 MB/s, 40 s at 250 MB/s or 20 s at 500 MB/s. Repeated passes, source preparation and durable installation add work. M2 removes complete hydration from first-use latency; it does not remove eventual bandwidth, disk capacity or quorum requirements.

## Scope, conventions and source map

Keep the pure `cellar-logic` boundary free of I/O and async work. Use existing execution-domain clocks, blocking jobs, cancellation and joined shutdown. Keep typed errors, immutable generations, schema/history/position checks and output proofs. Match the bounded ownership pattern in `host_snapshot.rs::encode_snapshot` and the complete-job cleanup pattern in `host_archive.rs::publish`.

Production work belongs beside its owning component, with private focused modules and preserved public paths. Never create both `feature.rs` and a Rust `feature/` directory. New dependencies go through workspace Cargo.toml only if an existing contract cannot serve the requirement. No JavaScript engine belongs in this native recovery work.

| Responsibility | Existing implementation to inspect before editing |
| --- | --- |
| Timers, socket/store bytes | `crates/cellar/durability/raft/{performance,transport_metrics,object_metrics}.rs` |
| Public activation and ownership | `crates/cellar/durability/raft/{host,host_seed,host_snapshot}.rs` |
| Source and peer receiver | `crates/cellar/durability/raft/{snapshot,snapshot_capture,snapshot_transport,seed_transport,transport_options}.rs` |
| Archive selection/publication/replay | `crates/cellar/durability/raft/{host_archive.rs,archive/mod.rs,archive/publication.rs,archive/physical.rs,archive/replay.rs,seed_archive.rs}` |
| Root image execution/apply | `crates/cellar/durability/raft/{rooted/,execution/rooted_working.rs,execution/images.rs,host_sql/mod.rs,host_sql/images.rs}` |
| Immutable storage/codec | `crates/cellar-ltx/src/{generation.rs,stream_snapshot/,extent/}` |
| Functional acceptance fixtures | `tools/cellar-recovery-tests/tests/native_raft_large_images.rs`, shared `crates/cellar/tests/support/raft_full_capacity.rs` |
| Native process/fault patterns | `crates/cellar/tests/{native_raft_daemon.rs,support/raft_seed_crash.rs,support/raft_seed_fault.rs}` |
| Physical process harness | `tools/perf/raft.py`, `tools/perf/raft-README.md`, `tools/multi-raft/README.md` |

- Scope also includes new owning-component tests, a proposed recovery campaign runner/analyzer under `tools/perf/`, explicit recovery profiles, and updated component/operations documentation when behavior lands.
- Implementation changes stay within these owning components.
- Unrelated RPC, language adapters, UI, dependency upgrades, higher quotas and LTX format redesign are outside the implementation.
- Coordinate any required root/snapshot contract change in one format-1 hard cutover; do not introduce legacy readers, silent backend fallback or migrations.

## Execution order and status

| Package | Deliverable | Depends on | Effort / risk | Status |
| --- | --- | --- | --- | --- |
| RT-01 | Stage evidence and a focused cold-recovery oracle | Existing materialized functional passes | M / medium | IN PROGRESS |
| RT-02 | Bounded bulk pipeline and shared prepared snapshot bytes | RT-01 | M / high | IN PROGRESS |
| RT-03 | Archive freshness, recovery scheduling and warm handoff | RT-01; integrate RT-02 | M / high | IN PROGRESS |
| RT-04 | Complete public extent snapshot/peer/archive recovery | RT-01; integrate RT-02/03 | L / high | IN PROGRESS |
| RT-05 | Verified demand loading, incremental first write and hydration | RT-04 | L / high | In progress; private writer and ownership registry groundwork |
| RT-06 | Hard-failure, concurrency and percentile qualification | Fault cases accompany every package; final gate needs RT-02–05 | L / high | TODO |

RT-02 and RT-03 need not wait for the entire extent backend. Establish M1 after they land. RT-04 first restores a complete local extent closure, preserving the existing sealed-image contract; RT-05 separately introduces remote dependency capabilities. Establish M2 only after its first-write and fault gates pass.

## RT-01: Measure and establish a focused recovery oracle

- Implementation began from updated `main` at `b3ff49a6` after PR #128 merged.
- The campaign guide at `tools/perf/recovery-README.md` describes the native phase ledger, strict source/profile-bound runner and analyzer.
- Minute-scale histograms and native peer/archive observations are implemented; the focused ordinary-size pilots and broader regression checks are being verified.
- Full-size stage attribution, repeated physical pilots and all later milestones remain open.
- The measurement-core run passed 648 library, 70 archive, six ordinary large-image, 23 operations and 44 snapshot cases, including existing small process-crash matrices.
- The fixed metric inventory is 41 operations, 39 counters and 22 buckets; its native cardinality test checks exactly 1,230 performance series independent of group count.
- The subsequently strengthened fixture recreates the replacement SDK runtime inside the timer and passes three small cases, including a follower seed source after a leader change.
- The initial fixed-source repair orchestrator failed with `NotLeader`; the corrected orchestrator retains the same plan and resolves the current leader for each mutation.
- The earlier uninstrumented stall is not fully attributed by that deterministic regression.
- All-target/all-feature Clippy for `cellar` and `cellar-ltx` passes with warnings denied.
- Formatting and layout checks cover 1,188 Rust files, and the native-only dependency guard reports 466 packages with no JavaScript engine.
- The runner owns per-trial worker directories through interruption and requires a source-bound release build manifest.
- Full-size physical pilots remain open.
- These counts do not qualify a new 10 GB recovery time.

- The first optimized smoke campaign at `40b4a540` failed all three trials after the first local read with `NotLeader`.
- A controlled leader change reproduced the final handoff controller's stale source: transfer admission can step the source down before the target reports command readiness.
- The fixture now refreshes the leader until the target is ready; server-side term, membership, replication and readiness checks are unchanged.
- This does not attribute the earlier uninstrumented stall.
- The three failed samples remain in the original campaign.
- Separately, a real SIGTERM probe found an orphaned runner child.
- The CLI now unwinds process and directory ownership for SIGTERM and Ctrl-C, with a regression checking that child shutdown precedes worker-directory removal.
- The current Python suite passes 231 cases with one skip.
- The optimized native run passes all three small recovery cases, including the forced stale handoff source, in 29.76 seconds; the three physical cases remain ignored.
- The focused native target also passes Clippy with warnings denied.
- Repeated source-bound pilots and new physical stage measurements remain open.

- At `605cd8e5`, three separate 12 MiB peer trials passed (failure-to-first-write median 3.929074 s, maximum 5.884990 s), and three 272 MiB RustFS trials passed (median 17.926337 s, maximum 18.311141 s).
- The first full-size peer attribution trial passed at 9,999,998,976 bytes: failure-to-first-verified-write 224.186171 s, with source encoding 42.329 s, authenticated fetch/append 70.011 s and destination verification 61.047 s.
- Fetch contains 55.342 s of receive time and 14.495 s of serial append time.
- The first write/read-back interval is 26.776 s.
- The complete drill is 694.31 s, including untimed fixture preparation and 52.461 s of cleanup; it is not the recovery-time sample.
- One trial does not establish a percentile or qualify the 120-second target.

- The matching full-size RustFS trial failed its existing 1,800-second archive coverage wait (2,297.15 s complete drill).
- The stored head remained at index 3, before the acknowledged command and repair admission.
- Replacement restoration never started.
- A new 4 MiB store write succeeded; 719 returned store-write errors in that trial's log were expected conditional-create conflicts, and 33 part PUTs logged more than five seconds in flight.
- These observations do not yet prove the cause of the publication stall.
- The failed sample remains in its campaign.
- Archive phase tracing and failure-path observations are installed.
- At `75949958`, the 1,280 MiB RustFS diagnostic passed: failure-to-first-verified-write 85.568223 s, including 64.158376 s waiting for archive coverage, 13.081198 s installing the seed and 4.084760 s through the first write/read-back.
- This crosses SQLite's reserved-lock page boundary without reproducing the full-size stall; it does not establish the cause of that stall or a size-dependent latency curve.
- Repeated full-size qualification remains open.
- The separately passing older full-capacity functional drills retain their narrower scope.
- Publication tracing now identifies the publisher and requested cut, and host debug events retain typed failure causes without changing retry policy.

- At `986d7a7f`, the traced 9,999,998,976-byte RustFS immediate-fault trial passed all recovery, write/read-back, receipt, payload and joined-cleanup checks.
- Failure to first verified write was 1,921.131523 s: archive waiting 1,580.795023 s, seed installation 246.375447 s, promotion 2.329619 s and first write/read-back 37.054271 s.
- The complete functional drill was 2,568.87 s, including untimed setup; it is not the RTO sample.
- The 120-second target was missed.

- The same publisher/cut trace attributes 26.978973 s to encoding cut 5, 978.571748 s to upload/confirmation and 34.545645 s to its physical replay.
- The protocol-only successor cut 6 took another 557.408309 s, including a 214.359441 s read of the prior material and 225.200802 s of physical replay.
- Its event material index names the prior base, so correlate publisher and requested cut too.
- The 13,364 failed publication events were advisory `Unconfirmed` deferrals; this run did not reproduce a timeout or permanent verifier failure.
- It identifies slow full-image I/O in this run, not the cause of the earlier untraced timeout.

- This was a shared-development-host diagnostic.
- The task's RustFS process was restarted from its existing data before bucket preflight; separate-checkout validation ran with one Cargo job.
- A one-second setup sample had no Rust symbols in the stripped release image.
- A later two-second kernel/network observation showed roughly 8.4 MB/s of upload progress and corresponding RustFS physical writes; these counters do not isolate one object or qualify sustained throughput.
- No object-store probes ran during the trial.
- Source/cache/hardware and hard-kill qualification remain unverified.

- At `78d37a18`, the configuration-tail 9,999,998,976-byte RustFS trial also passed the complete functional oracle.
- Failure to first verified write was 1,348.780124 s (22m29s): admission 14.746032 s, archive waiting 1,077.730958 s, seed installation 209.863706 s, promotion 0.691219 s and first write/read-back 44.899275 s.
- The full drill took 1,928.95 s, including untimed setup.
- The 120-second target was missed.
- The release executable hash is `72d1f4ff274edab4629ad0e8a96be4319240a143ed6dba96d9b640f2ad9f1753`; source-input hash is `a764dbb3e0564a9203b264a9280764ed66b2812c209b6888b9b22a1beab1295e`.

- A later capacity cleanup retired only this completed configuration-tail trial's isolated synthetic bucket prefix: 2,419 objects and 10,058,695,872 logical bytes.
- The successful oracle, closed worker directories and retained executable hash were checked first; exact keys, sizes and ETags were revalidated before deletion.
- Inventory, batch responses and an empty-prefix audit remain with the campaign's source, executable and logs.
- The newer prepared-body RustFS trial's objects, failed samples and golden input remain available.
- This is fixture retirement, not production remote retention or garbage collection.

- The same-publisher/requested-cut trace shows cut 2 encoded before the fault but never confirmed as an archive base.
- Cut 3 then took 26.613036 s to encode, 485.333165 s to upload/confirm and 33.813185 s to replay.
- Configuration-only cut 6 reused that base without a new full encode/upload, but still took about 515.77 s from acquisition to confirmed head; prior-material reading accounted for 193.514499 s and later replay for 270.291608 s.
- The 10,824 publication failures were `Unconfirmed` deferrals; no permanent verification error or archive timeout was reproduced.
- Archive readiness and repeated whole-image verification remain the dominant costs.

- This single shared-host sample cannot establish a causal speedup over the `986d7a7f` observation.
- Upload code was unchanged despite a different observed confirmation rate; fault timing, initial base cut, OS caches and competing work were uncontrolled.
- Separate-checkout validation overlapped initial setup.
- The fault used joined domain shutdown followed by local erasure, not a hard process kill.
- The frozen trial predates `33d897d6` destination batching and prepared-body reuse, so it measures neither.
- Campaign JSON, native ledger/log, executable manifest and same-publisher attribution are retained in the task's private `recovery-time-rustfs-10gb-config-tails-1` evidence directory.
- One passing sample remains unqualified; the analyzer exits nonzero for the missed target.

- The prepared-body peer pilot freezes native source at `bc3b95aa`.
- Its first package-wide release build did not finish and produced no native recovery observation.
- Cargo had already emitted a completed release artifact for the selected `native_raft_large_images` test target (`fresh=false`, compiled during that build).
- The retained compiler record, unchanged source-input inventory, copied executable and verified binary manifest bind the restarted prebuilt-target pilot to that exact artifact; this does not turn the aggregate build into a pass.
- The executable SHA-256 is `b9fd52951e57ae681dfe6bf36d632f97417af77fb25d45bca1b9b368ce220086` and source-input SHA-256 is `981e474f3cc55ee5d65f3510140e98aa633e1a067ab2a9527f51c1e521f090df`.
- The `recovery-time-peer-10gb-prepared-2` campaign passes all functional checks and joined cleanup, with unchanged source inputs.
- Failure to first verified write is 697.738511 s (11m38s): repair admission 105.851003 s, seed installation 558.496863 s, promotion 1.089778 s and first write/read-back 31.795360 s.
- The complete native drill takes 1,441.54 s including untimed setup; cleanup finishes 865.879726 s after the fault.
- The 120-second target is missed, so the analyzer returns nonzero despite the native process returning zero.

- The source records one prepared-body miss, no hits, no checkpoint encoding and no reused bytes.
- Its seed encoding takes 277.578266 s after 0.006967 s of encoding queue time.
- The receiver records 10,058,313,680 exact received/appended bytes, 228.864360 s of fetch, 227.493027 s summed receive time, 15.840001 s summed append time and 42.041766 s of destination verification.
- Append overlaps receive; owner-clock sums are not a controller-clock critical-path sum.
- This sample does not measure a pre-existing prepared body's benefit.
- Shared-host compilation and archive work overlap the trial; source/store cache state and background load remain uncontrolled.
- Neither a speedup nor a regression against the older peer pilot is established by this observation.
- Logs, executable, exact source and operation attribution are retained; after the complete oracle and closed-handle audit, the two synthetic local worker trees were retired.
- A later capacity cleanup inventoried and retired only this completed peer campaign's isolated bucket prefix (1,796 objects, 7,478,450,006 logical bytes), confirming no current objects remained under that exact prefix.
- Its full object inventory, deletion responses and audit join the retained source, executable, ledger and logs.
- The latest completed 10 GB RustFS prefix, failed samples and golden source remain available; this fixture cleanup is not the production remote retention/GC work.

- The matching prepared-body RustFS pilot at the same frozen `bc3b95aa` source and executable passes all nine correctness checks and joined cleanup at 9,999,998,976 bytes.
- Failure through first verified write is 903.362692 s (15m03s): repair admission 14.336060 s, archive coverage wait 684.489903 s, seed installation 165.541886 s, promotion 0.690167 s and first write/read-back 37.422583 s.
- The complete native drill is 1,398.78 s; cleanup finishes 965.631203 s after the fault.
- The analyzer correctly returns nonzero for the missed 120-second target and insufficient qualification population; the native process returns zero and source inputs remain unchanged.
- This pilot predates the archive replay memo.
- Shared load and uncontrolled source/store caches prevent causal comparisons with earlier observations.
- The native executable, source inventory, ledger, logs and bucket prefix are retained in the private `recovery-time-rustfs-10gb-prepared-1` campaign; only its closed synthetic local worker trees were retired after joined cleanup and an open-handle audit.

- Publisher-correlated attribution of that same frozen RustFS trial separates the successful cut-3 publication (23.696246 s encoding, 202.360348 s from encode completion to upload confirmation, 27.103586 s replay; 269.208142 s from publication acquisition to confirmation) from the successful metadata-only cut-6 publication.
- Cut 6 uses a different publisher and takes 175.312080 s to read material and 208.755513 s to replay; its publication-to-confirmation interval is 415.123302 s.
- Other owners independently read/replay cut 6 as well.
- These publisher wall-clock spans are not additive with controller intervals.
- The later per-verifier replay memo starts empty on a new owner and cannot eliminate that owner's first physical proof.
- Public extent recovery and source readiness remain necessary; this attribution is not evidence of a memo speedup.
- The raw log digest and complete phase groups are retained in `publication-attribution.json` beside the campaign.

- The RustFS attribution trial included read-only store audits and a fresh 4 MiB write probe during/around the observed archive wait.
- It is diagnostic evidence, not a controlled latency comparison; the stall needs an unobserved reproduction with the new archive tracing.
- The source-bound retention probe also found that `--keep-node-state` kept an empty parent while Rust TempDir cleanup removed every database.
- The flag now reaches the native fixture.
- A repeated real small trial keeps nine SQLite files while its original-owner erasure check still passes.

1. Extend the existing finite metric families with source admission/encoding, fetch queue, actual received bytes, append, destination verification/sync, install, tail replay, promotion, first read/write and cleanup observations.
  - Add useful long-duration histogram buckets, or a separate bounded recovery histogram.
  - Update registry/cardinality tests.
  - Do not add cell IDs, root IDs or request IDs as unbounded metric labels.
  - Exact per-trial samples belong in the private evidence ledger, joined by a bounded trial identity.
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

- Verification: existing large-image, archive and snapshot targets below pass; new ordinary-size tests prove stage completion/failure/cancellation accounting.
- The analyzer's unit tests reject zero selected tests, missing phases, bad values, source/profile drift and a timed-out sample disguised as success.
- A pilot with three actual 10 GB trials per path produces a complete stage ledger.
- It is baseline evidence, not a qualified p95.
- Do not choose pipeline parameters until source/transfer/destination measurements identify the dominant work.

## RT-02: Remove avoidable materialized-path work

- The initial pipeline is committed at `e9937227` while archive diagnostics proceed.
- The peer receiver uses two admitted payload windows and one registered physical owner through download, sealing and discard, with a joined stop observer and shared memory charge.
- Native adapters append complete admitted windows; injected providers retain the bounded fallback.
- Six ownership/window tests, all 44 native snapshot cases and seven ordinary native large-image cases pass.
- The library passes all 654 cases serially; an earlier four-thread run failed one unchanged Python loader-inventory case with `No such file or directory`, which also passes in isolation.
- All-target, all-feature Clippy passes with warnings denied.
- An additional incompressible 12 MiB native TLS seed passes and checks that one receiver appended more than two windows with matching received/appended byte counts.
- That test establishes multi-window integration; the held-I/O tests establish the two-window bound and overlap.

- The next change selects Buffered source encoding and peer destination verification on transports configured for images of at least 256 MiB.
- The source reservation carries the selected codec policy, and receive admission precedes scratch-file creation.
- The codec/wire reservation is 16.25 MiB with 1 MiB chunks; smaller transports retain the default 8 MiB.
- Source chunks now fill the existing admitted native read window directly, preserving injected-provider fallbacks.
- Eight ownership/admission tests and nine codec cases pass, including cross-profile byte identity and corrupt-frame rejection.
- The corrected 272 MiB buffered and 12 MiB compact native TLS recovery cases both pass (111.42 s combined functional test duration, not an RTO comparison).
- All 44 native snapshot cases pass again (231.05 s), including exact-ticket installs, partial-body retries and process crash matrices.
- All-target/all-feature Clippy passes with warnings denied.
- An initial fixture incorrectly paired a 12 MiB physical template with a 272 MiB quota and was correctly refused; its subsequent small case failed preflight with a closing node.
- The small case passes in isolation, and both cases pass together after generating the actual requested physical quota.
- The failed run is retained; test-context fallout is plausible but not proven.

Prepared payload reuse and the full-size buffered performance comparison remain open. These checks establish no 10 GB recovery-time improvement or qualification.

- A follow-up batches decoded destination pages into the selected profile's positional window: 2 MiB for Buffered and 64 KiB for Compact.
- Native files accept the admitted window directly; existing injected providers retain bounded 64 KiB random writes.
- It does not rely on sequential-cursor behavior or raise the existing codec working-memory allowance.
- Verification-only calls omit the unused destination buffer.
- Five host-window, ten streamed-snapshot and one I/O-activity tests pass, including cross-profile exact SQLite bytes, partial-write refusal, offsets beyond 4 GiB and independent destination re-read.
- The regression failed before the decoder change because the native window was never used.
- An initial fixture correctly hit the nonempty-destination refusal and was fixed to supply the required private empty file; both failure logs are retained.
- The full-size RustFS campaign frozen at `78d37a18` predates this follow-up, so it cannot measure this destination-write change.
- The follow-up also passes 27 native LTX cases, five native cold-recovery cases and a real RustFS 272 MiB owner-loss/local-erasure preflight through first write, receipts and joined cleanup.
- All-target/all-feature Clippy for both affected crates and repository format/layout/dependency/docs checks pass.
- These are functional checks on a shared host; the physical preflight overlapped the frozen full-size campaign's setup and supplies no RTO comparison.

- Prepared peer/checkpoint bodies now have a single node-owned physical cache.
- Keys bind the engine owner, complete replica identity, birth/schema and exact immutable image; compatible caller limits are checked again.
- A hit reuses bytes while rebuilding the current cut and membership.
- One operation, one key and two handle metadata slots add a fixed 256 KiB bound; ordinary scratch and source admission remain held through physical work.
- Async consumers never own the file.
- Idle replacement, peer/archive receive reclamation and joined shutdown close it on the registered blocking owner.
- Complete source-stream hashes invalidate corrupt cache entries; destination physical verification is unchanged.
- Archive publication is not yet a consumer of this cache.
- Fixed hit/miss/reused-byte counters supplement the existing purpose timers; a reuse timer adds no newly encoded bytes.
- Physical archive publishers release idle cached bytes on typed scratch exhaustion before their ordinary retry, including wrapped image errors; other errors and live consumers preserve the cached file.

- Eight cache tests pass: compatible reuse, identity separation, active-key and limit refusal, corrupt-stream invalidation/rebuild, physical-close ownership, held-handle shutdown, cancellation retention and real scratch-pressure recovery.
- The final source passes all 671 library, 71 archive, 45 native snapshot and nine ordinary large-image/recovery cases.
- The native reuse case prepares before learner admission, then requires fresh membership, no second seed encoding, zero seed callbacks and subsequent version-bound reads.
- The metrics case requires 42 fixed counters and exactly 1,233 performance series.
- A real RustFS preflight passes at 285,212,672 bytes through owner loss, local erasure, cold replacement, new writes, receipts, payloads and joined cleanup.
- Its retained debug executable hash is `9dc81eaf71375b8638ac8c9e3ccc1724f9a03b2e443e00400e13fd9227e0ef84`; source-input hash is `981e474f3cc55ee5d65f3510140e98aa633e1a067ab2a9527f51c1e521f090df`.
- Clippy for both affected crates with all targets/features and warnings denied, format/layout (1,196 Rust files), native dependency and docs guards pass.

- The resource regression first reproduced a stale nine-resident-task expectation; the fixture now explicitly requires eleven, including the physical cache pair, and retains its original overload and deadline assertions.
- The first pressure fixture had only one spool slot and correctly failed before creating its initial snapshot.
- Two slots admit the body/index encoder, then the retained cached body causes an actual subsequent encoder refusal until eviction.
- An unchanged daemon route timing case failed once in the broad suite before receiving its actor request; its isolated rerun and the final complete 671-case run pass.
- Its cause remains unqualified.
- All failed/obsolete-source logs are retained.
- These are functional checks on a shared host; full-size latency after the cache remains unmeasured and unqualified.

- CI at `bc3b95aa` caught the Python measurement consumer's stale 39-counter vocabulary.
- Its existing native-contract test refused the three prepared-body counters before the SDK build.
- The consumer now names all 42 counters; the full 231-case performance tooling suite passes with one skip.
- The running full-size measurement checkout stays frozen at `bc3b95aa`: the recovery runner/analyzer do not import that counter consumer, and its raw native observations still bind the recorded source and executable.
- The tooling-only follow-up supplies no new native timing result.

- At `fb518fa6`, the Python application examples and typed-RPC workflows pass.
- The SDK free-threaded lane fails an event-settlement race: obsolete ack and extension requests return `Ambiguous` after their transports close, rather than the result required by that fixture.
- The JavaScript lane fails the published container lifecycle fixture on a `/wake` request timeout.
- These CI failures are retained with their exact head/job identities.
- Their causes are not established by the passing native recovery suites; full SDK CI remains an open gate.

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

- Verification: snapshot tests add slow-disk/network, wrong-order/tag/digest, expired lease, shared-consumer cancellation, cache eviction and shutdown cases.
- Measured buffers stay within the declared window plus existing codec allowance.
- Re-run both exact 10 GB drills and the focused campaign.
- Report saved source encodes, transfer throughput and total RTO, not throughput alone.
- Any retained whole-image copy/hash pass gets an explicit count and cost.

## RT-03: Keep a usable recovery source ready

- The first implementation removes the unconditional checkpoint rotation for configuration changes.
- A data update followed by learner admission now remains a bounded tail over the existing base.
- Manifests carry separate base and final configuration/member state, and recovery replays Raft's configuration changer plus the live revision-bound member intents to the exact final cut.
- Joint leave retains outgoing bindings until retirement; stale intents, unknown fields and oversized changes are refused.
- A source metadata grant still cannot activate a replacement before complete replay and independent physical sealing.

- This is a format-1 hard cutover under the `/images-1/config-tails-1` protocol identity; earlier namespaces and state are refused.
- The new regression failed before the implementation because learner admission rotated the base.
- All 662 library, 71 archive, 44 native snapshot and nine ordinary large-image/recovery tests pass after the change.
- A real RustFS preflight also passes with an actual 285,212,672-byte image, owner loss, local-state erasure, cold replacement, promotion, a new verified write, receipts, callback counts and joined cleanup.
- That preflight uses a debug executable and supplies no latency qualification.
- Forced-checkpoint tests use an explicit tail limit and keep their no-upload, exact-state, missing-part and failed-destination-sync assertions.
- The initial full archive run recorded one obsolete implicit-rotation expectation; the first native snapshot run found the same tail-phase assumption in its exact-reference fault fixture.
- Both corrected suites pass with their original refusal assertions preserved, and the failed logs are retained.
- This establishes no full-size latency gain.
- That source predates the RT-02 prepared-body cache.
- Measured replay-cost rotation, foreground I/O and verified warm handoff remain open below; full-image download, verification and replay still run.

- The `78d37a18` trace prioritizes a separately verified physical replay memo before increasing bulk concurrency: configuration-only cut 6 reused its base but still reread and replayed the complete image.
- A proposed node-owned memo may reuse only a prior successful physical replay of the exact scope/birth/schema, encoded base identity, ordered data-bearing entry projection and final image state.
- Configuration and membership validation still run at every new cut.
- Every publication must still load and authenticate its exact remote dependency set; a memo cannot turn missing/corrupt objects into a successful head or replace the receiver's independent physical seal.
- Keep one bounded metadata key, never cache failures, and preserve verifier quarantine after physical errors.
- First prove cross-history, changed SQL, missing-part, corruption, configuration and shutdown refusals with deterministic physical-I/O counts.
- This work is under verification and supplies no measured 10 GB replay improvement yet.

- The implementation makes the validation boundary explicit.
- A recovery set receives a private record only after complete source decoding, object verification and current metadata/configuration validation.
- Its full fingerprint binds the manifest/reference, logical position and all framed entries; the separate physical key omits only metadata-only transitions.
- Before memo lookup both fingerprints are recomputed over bounded resident material, with streamed metadata serialization.
- A stale or missing record refuses replay; no second full-image scan is needed to establish that the validated input values still match.
- Physical misses retain the original full replay/seal/cleanup path; only a successful empty workspace can record a key.
- Physical errors keep the existing recovery-required latch.
- Fresh remote reads and the receiver's own independent seal remain mandatory.
- The native no-op regression failed before the change because it created another destination image.
- The first positive regression and all five private fingerprint tests pass.
- All five native memo cases pass, including missing/corrupt parts, failed physical verification, fresh verifier ownership and identical SQLite bytes under another history.
- The complete 76-case archive and 676-case library suites pass, as do all 45 ordinary snapshot cases (two ignored), nine ordinary large-image/cold-recovery cases (eight ignored) and the native metric inventory check.
- All-target, all-feature Clippy for `cellar`/`cellar-ltx`, formatting/layout (1,199 Rust files), native dependency and documentation guards pass.
- A misspelled snapshot target was refused before test selection; the corrected complete target passes.
- The failed selector, original no-op regression and initial compile repair logs are retained without assertion or deadline changes.

- A source-bound debug RustFS preflight at 285,212,672 bytes also passes owner loss, local erasure, cold replacement, promotion, new durable writes, exact receipts, payload checks, single callbacks and joined cleanup.
- Its trace records three performed replays and one reuse; the receiver still restores independently.
- Its retained executable SHA-256 is `0cae442bcd864d34a92e0c57acade4974ddbfae47e4eaf6180c9f2a277695962` and validated source-input SHA-256 is `ce5184dded8c75813e33e634368c16999a418bb7018468f06b31a50c199e3b0c`.
- This establishes functional behavior only.
- The completed full-size peer pilot uses the preceding `bc3b95aa` source and contains no replay memo; neither this preflight nor the deterministic I/O-count regression proves a 10 GB latency gain.

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

- Verification: archive tests cover slow/stalled store, competing publishers, independent cells, backlog pressure and retained dependencies during rotation.
- Controller/operations tests cover stale applied state, interrupted handoff, quorum loss and concurrent repair.
- Campaigns include both ready and deliberately lagged archives; waiting for the head must remain in their reported duration.
- If the 10-second replay budget is infeasible, record the measured bound and redesign snapshot frequency/catch-up before accepting M1.

### Node-wide local archive work admission

- Archive producer count bounds retained cuts but previously let each producer submit local encoding/verification work independently.
- A deterministic owning test holds one physical archive job and submits another cell's job through a second host clone.
- Before admission is enforced, both pass the supposed one-job boundary; the test fails with two successful admissions against an expected one.

- The native owner now shares a separate physical budget across captured publication clones and their verifier.
- `max_blocking_jobs` accepts one to four and defaults to one.
- Foreground archive restoration retains its original host; the adapter preserves the actual filesystem, clock and execution domain.
- A waiting job is not submitted to the physical executor; its already admitted publication owner and capture reservation remain held.
- A slot moves into the dispatched job and outlives caller cancellation, including while its executor has not begun the job.
- Owner cancellation/drop refuses queued admission, while entered work still joins through domain shutdown.
- Producer and memory admission remain separate.

- `archive_physical_admission` and `archive_physical_queue` distinguish the new admission delay from executor dispatch delay; both remain in overall publication and recovery timing.
- The physical test runner records the explicit setting and refuses continuation with a changed or missing value.
- This bounds local archive work, not object-store bandwidth.
- Network scheduling, measured foreground interference bounds, source freshness/replay-time rotation, verified warm handoff and remote-retention/demand-loading proof remain open.
- No full-size RTO gain or percentile qualification follows from the admission tests alone.

- The admission implementation passes 13 owning tests and five performance tests.
- Its runtime source passes 76 archive, 20 ordinary large-image and 32 rooted integration cases; 14 physical cases remain ignored.
- The additional public operations target passes all 23 cases after its exact metrics inventory is updated to 43 operations, 42 counters and 1,291 performance series.
- Before that expectation update, the test refuses the observed 43-operation inventory against the old 41-operation assertion.
- The qualification package now wraps the existing public archive and operations suites without requiring the host CLI binaries.
- All-feature Clippy, 246 Python tooling cases (one skipped), formatting/layout over 1,314 Rust files and dependency/documentation checks pass.
- The final test-only changes preserve every production runtime input from the archive and recovery validation.
- Source-bound physical preflights and full-size timing remain separate gates.

### Work-conserving physical dispatch

- The first real RustFS materialized 272 MiB preflight after replay-budget work stalled at archive catch-up: surviving publications stopped at material reads before replacement startup.
- Its release attempt at `b1e7eaae` was intentionally canceled through the owning runner after capturing evidence; that interrupted requested trial remains unsuccessful.
- A same-source lab diagnostic completed, showing scheduling sensitivity rather than establishing a release recovery time.

- A deterministic owning test holds the only physical job, polls a read-ahead waiter once, releases the first job and then drives an append without polling the read-ahead future again.
- The old fair semaphore reserves the released permit for that unpolled waiter; the append times out.
- This is the same dependency cycle as a bounded receive/upload window whose caller performs intervening physical I/O.

- Admission now arms a capacity notification before a nonblocking acquisition.
- A wakeup owns no permit; only a polled dispatch can claim capacity.
- The physical job's guard releases its permit before notifying waiters and still outlives caller cancellation or executor queueing.
- Owner stop/drop remains the cancellation boundary.
- This retains the node-wide concurrency cap and existing pipeline windows without spawning a task per job.
- Before integrating updated `main`, all 14 owning tests and 15 private archive tests pass, together with 80 archive, 20 ordinary large-image, 23 public operations and 32 rooted cases; 14 physical cases remain ignored.
- All-feature host/qualification/performance-node Clippy, 246 Python tooling cases (one skipped), format/layout over 1,315 Rust files and dependency/documentation guards pass at source inputs `0161e54d978b27f05920c6f094c9608734c8ff21c71b5481136364b6bd2e2679`.
- PR #145 merged during validation.
- The updated-main checks and repeated real-store preflights below close the recorded scheduling reproduction; the 10 GB and hard-failure gates remain open.

- At `45f024994b267b47119e8ee7d0cec68c0e802201`, based on updated `main` `3ffbadcc`, all 12 checks pass again: 14 owning and 15 private archive tests, 155 native integration cases (80 archive, 20 ordinary large-image, 23 operations and 32 rooted), three all-feature Clippy modes and 246 Python cases with one skip.
- Fourteen physical cases remain ignored in the ordinary suite.
- Formatting, layout over 1,350 Rust files and dependency/documentation guards pass.

All eight requested release preflights pass with nine correctness checks and six valid phase windows per trial, unchanged source inventories and executable bytes. Failure to first verified durable write is:

| Representation/source | Physical image | Trials | Seconds |
| --- | ---: | ---: | ---: |
| Materialized peer | 12 MiB | 3 | 3.200869–3.939944 |
| Materialized RustFS | 272 MiB | 3 | 12.153734–15.435117 |
| Extent peer | 12 MiB | 1 | 3.199940 |
| Extent RustFS | 272 MiB | 1 | 7.062406 |

Inputs are `ea617f247e3e596bab5b51fc22b3353179d29db2f0819e21e7a1fd5eb32198a6`, with release executable `c1e59bbe471edf679f1c90a48822122c57697614bbee4f309b9ae8628bcbbe36`. The original stalled/interrupted release attempt remains unsuccessful. These shared-host, uncontrolled-cache, joined-domain-loss preflights do not establish a controlled speedup, 10 GB latency, hard process loss or p95 qualification. The analyzer keeps its three-sample pilot minimum; the single extent trials therefore retain a nonzero analysis exit and false target/qualification flags.

- The first extent preflight refuses its unchanged 32 GiB capacity guard before creating a native trial.
- Verified archival preserves the completed diagnostic worker, 721 inactive test objects and 2,592 stopped-server trash files before retirement.
- The owned RustFS server restarts with the same binary, endpoint, credential files and data directory; all 651 other live object listings retain their lengths, ETags and metadata.
- Capacity refusals and setup attempts remain separate from timed trials.
- The full-size rerun is still unstarted: its unchanged 48,589,930,496-byte guard is unmet at the fresh recorded capacity preflight.
- Durable remote retention/GC, demand loading and broader fault/contention gates remain open.

### Bound materialized replay amplification

Encoded tail bytes alone miss complete-image rewrite amplification. Replay seals bounded private batches; one small retained tail can therefore require several complete output images. The new regression disables size-triggered rotation, sets a one-image replay budget and captures 64 data updates. Before enforcement it advertises all 64 entries, requiring a second image rewrite.

- `ArchiveLimits::max_replay_images` now accepts one through 4,096 and defaults to two.
- The verifier records completed image seals and their summed logical lengths only with a successful physical proof and joined cleanup.
- Its fixed-size memo retains those values for the exact physical key.
- An over-budget candidate must pass every existing intermediate-image and remote-dependency check before the publisher replaces it with a separately confirmed full base.
- Only that final base may advance the conditional head.
- An existing over-budget tail can also be checkpointed at the exact same cut; an older capture cannot rewind a newer head.

- The trace exposes `verified_restore_images`, `verified_restore_image_bytes` and the `replay_budget_checkpoint` decision.
- This is work under the verifier's apply limits, not measured device bytes, a bound for stricter receiver limits or a replay-time guarantee.
- The publisher still pays for the final verification that triggers rotation; subsequent recoveries can use the fresh base.
- Existing valid archives remain readable.
- Measured replay-time rotation, archive-lag readiness, foreground bandwidth, verified warm handoff and demand loading remain open.
- Validation passes 15 archive private tests, the four focused public budget cases and the complete optimized archive/large-image/rooted suites (80, 20 and 32 cases; 14 physical cases remain ignored).
- The budget fixture verifies one versus two actual destination image syncs, same-cut checkpointing without stale-cut rewind, missing/corrupt prior payload refusal and an overwritten false intermediate digest.
- All-feature host/qualification/performance-node Clippy, 246 Python tooling cases (one skipped), formatting/layout over 1,315 Rust files and the dependency/documentation guards pass.
- The frozen runtime input SHA-256 is `33736da88a529956013068d7a0bca2c198cd4c97f7b3f972221ac0a55e8c565d`.
- Physical timing and full-size M1/M2 qualification remain separate gates.

## RT-04: Connect complete extent recovery end to end

- The first implementation uses the existing extent-aware native descriptor, canonical manifest and local `SealedImageRoot` contract.
- `RootSnapshotSource` captures a coherent committed root without a full-image encoded spool, binds its manifest to that exact metadata, and pins the source.
- `RootSnapshotSet` decodes against the existing group or catalog birth and imports only its declared extent IDs.
- Each source object is rehashed before any bytes escape; each destination object retains independent verification and sync.
- Only a complete verified local closure produces the existing seal; native admission stays separate.
- The transfer uses one caller-admitted 4 MiB extent at a time and the existing manifest, not a per-page index.
- Duplicate imports reverify existing objects without overwriting them.

- All ten native rooted cases pass, including four new transfer/binding cases.
- They cover complete/partial closure, repeat import, catalog birth/schema, foreign history and objects, corrupt metadata/data, source corruption after capture and exact closed/foreign engine owners.
- The first refusal fixture correctly hit `WrongIdentity` while trying to reuse an existing group ID for a different cell; assigning the foreign history its own ID lets every refusal assertion run.
- That failed log is retained.
- Validation passes 16 extent-store cases (one existing ignored), all ten rooted cases, 45 ordinary materialized snapshot cases (two existing ignored), all 676 library cases and both affected crates' all-feature/all-target Clippy with warnings denied.
- Formatting and layout cover 1,201 Rust files; native dependency and docs guards pass.
- The tested source input SHA-256 is `2feba3554b2d094b663d3835fcddb642006f16e09d64f056c2674c28c808b259`.
- This is not yet public peer/archive seed integration: transport adapters, physical owners, fresh-learner seed installs, node-wide inventory recovery/GC and backend selection remain open below.
- No new metadata/data value supplies voting or installation authority, and no partially hydrated seal is introduced.

- The next native-install slice connects a complete local root to the existing `PreparedSnapshot`, ticketed completion, pending/post-sync references and rooted image publisher.
- Runtime tickets retain their original runtime-owner token and now also bind the engine that issued them.
- The source/receiver still transfers immutable extents without encoding a complete flat image.
- The publisher requires the exact durable snapshot/image marker and adopts through its own inventory; a same-named root held elsewhere cannot stand in for local bytes.
- This does not add fresh-member authorization or the public peer/RustFS adapters.
- All 13 native rooted cases pass, including three new cases for install/restart and a subsequent committed update, delayed engine sync with premature/foreign-inventory publication refusal, and exact engine identity with otherwise identical replica metadata.
- The original receive path failed the new root-install regression; the first prototype also exposed the distinction between runtime and engine owner tokens.
- Both failures are retained.
- The correction explicitly binds both owners; no refusal assertion or deadline was relaxed.

- The same checked tree passes 76 archive, nine ordinary cold/large-image, 23 operations, 19 runtime, 45 ordinary snapshot and 676 library cases.
- Existing ignored tests remain ignored.
- Both affected crates pass all-feature/all-target Clippy with warnings denied; formatting/layout cover 1,203 Rust files and native dependency/docs guards pass.
- Tested input SHA-256: `58fb0b021198dba47b99b54e0a6ddf38c1d491ef74b0233541acf834c0fc7722`.
- Native executables and source bindings are retained outside the disposable build cache.
- This is functional native-boundary evidence; no new full-size latency result or public extent-backend qualification is claimed.

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

Verification: existing extent tests plus new native rooted peer/archive seed and restart tests pass. Corruption, missing dependencies, truncation/growth, reserved lock-page holes and offsets above 4 GiB must be tested. Repeat both near-quota drills with the public extent path and the same payload/receipt oracle. Backend flags without remote recovery are not completion.

### Canonical root peer stream and physical seed integration

- The next RT-04 slice extends the existing admitted package and joined receiver rather than introducing a second network stack.
- Canonical root metadata binds a required total transfer length in addition to its own length/digest; the body contains that manifest and each unique extent once in sorted identity order.
- A source caches one verified extent across wire windows.
- The receiver validates the complete manifest/closure length before importing objects, holds at most one extent body and releases it before allocating the sealing workspace.
- Exact inventory identity prevents a caller from sealing through a different owner.
- Flat bodies keep their existing encoded-file digest and independent seal.

- Root capture/checkpoint/retained-source operations and receiving/publication now dispatch through the SQL image owner.
- A retained cut can precede newer data or configuration on its source; the target separately refuses regressing membership.
- The existing fresh-learner grant is carried through root physical seeding, its learner/barrier validation, pending references, atomic native fence and post-sync retention.
- No metadata value mints authority.
- RustFS root archives, public root selection, whole-node reconciliation/GC and complete fresh-learner root qualification remain open.

- The descriptor field and changed peer/snapshot/seed authentication domains form a marker-1 hard cutover.
- Old workers/native state are refused; no fallback reader or migration is added.
- Root working admission includes a 4 MiB extent plus a 1 MiB bound for quota-limited manifest/index metadata: Compact with 1 MiB wire windows now reserves 9 MiB; Buffered remains 16.25 MiB.

- Three stream regressions pass, covering repeated extent identities, malformed lengths before import, corrupt/short streams and foreign destination inventories.
- The authenticated two-host TLS/SQL-worker case also passes: it checkpoints a committed root, makes another committed update, transfers the retained older cut, installs and publishes it, catches up through ordinary Raft append/apply, then reopens the target and reads the newer value.
- It creates no materialized image directory.
- This exercises established-member snapshot installation; it does not qualify a fresh learner's quorum grant or public backend selection.

- Initial stream fixtures incorrectly assumed zeroblob overflow pages were identical; their page pointers differ.
- The corrected fixture packs free-list trunks ahead of zeroed free leaf pages, retaining SQLite integrity and creating actual repeated extents.
- The false-total test uses that fixture so its metadata passes coarse geometry bounds and fails the exact closure check before any object import.
- Refusal and deduplication assertions remain intact.

- Input `f4cb1ed4e7ee26fc03a757a66583c996729163364abce6f922a17207e48287dc` passes 679 library, 188 native and 16 extent-store cases.
- Clippy then caught two private getters still named for encoded-file length while returning transfer length.
- Renaming those getters and their private callers changes five files; input `83f7a0299a39547e91f35f28949f96186b2c30d15e8d6ada2238fbc5a37e6b7c` passes all 679 library and 16 native rooted cases again, and both affected crates' all-feature/all-target Clippy with warnings denied.
- Formatting and layout cover 1,205 Rust files; native dependency and documentation guards pass, with unchanged checked source inputs.
- Tested executables, source inventories, log digests and both validation phases are retained outside the disposable build cache.
- No assertion or deadline changed.
- No full-size root latency or public backend result is claimed.

### Immutable root objects in RustFS

- `CellArchive::store_root_snapshot` now confirms a committed source's distinct extent objects before returning a separate immutable `RootArchiveReference`.
- Its bounded metadata object binds the namespace, canonical native snapshot and extent manifest.
- `load_root_snapshot_at` checks that exact reference and reads every declared remote dependency, independently imports/verifies/syncs the destination and retains its complete local seal through native preparation.
- Two object windows include both response and output copies within the existing physical allowance.
- The 4 MiB sealing workspace is allocated after those reads finish.
- A complete seal can be reused only through its exact inventory owner; matching root content elsewhere supplies no local capability.

- The owning native regression closes the source, removes its entire local tree, restores from object storage, installs through a real runtime ticket, publishes the image and reopens the receiver.
- Separate cases refuse closed sources, malformed metadata before import and corrupt/missing remote objects despite a populated local cache.
- A real RustFS run passes the same small fixture.
- Its 24 MiB padding is recycled to create repeated extents; the preserved bucket inventory contains three extent objects totaling 8,588,288 bytes and one 1,420-byte root metadata object.
- No archive head is published.
- This is data-path evidence, not a 10 GB latency or public fresh-learner qualification.

- Resource review found that this new path initially bypassed a configured per-object limit smaller than 4 MiB.
- A new regression first fails with a 64 KiB object cap.
- The correction validates all declared dependencies before upload or import and enforces the bound at individual object operations too.
- The same regression then passes both directions without remote writes or local imports on refusal.
- No assertion or deadline was weakened.

- Before that correction, input `8120d5f192a8de2b774d186900de53513b94a799a48782906821ba249ab31f41` passes 679 library, 76 archive, 45 snapshot and 16 extent-store cases.
- Its validation pipeline was deliberately stopped after those functional checks to add the object-limit regression.
- Corrected input `b811b8350b1ff426b83fc6e61fea1f9414d34f1209cae00acb3448529b169776` passes all 21 native rooted cases and the repeated RustFS drill.
- Both affected crates pass all-feature/all-target Clippy with warnings denied; formatting and layout cover 1,207 Rust files, and native dependency/documentation guards pass.
- The tested corrected binary, source bindings, failing regression and both RustFS observations are retained.
- These results do not close RT-04: selected archive-head publication, live fresh-learner grants, public backend selection and node-wide inventory/GC integration still need implementation and proof.

- PR #136 merged while this slice was being validated.
- The root-object change is replayed onto updated `main` at `784edc1b`, including its newer RPC changes, and tracked separately in PR #140.
- Combined input `942ee9885b7bd24e6b2a1b0edff10c75a96704d2417e7bf4608c815fc589dfcc` passes 711 library, 76 archive, 21 rooted, 45 snapshot and 16 extent-store cases, plus the explicit RustFS fixture.
- Both affected crates pass all-feature/all-target Clippy with warnings denied.
- Formatting/layout cover 1,233 Rust files; native dependency and documentation guards pass.
- Source inputs remain unchanged.
- The repeated RustFS objects, all five tested executables and their exact source and log bindings are retained.
- Public root head/seed integration and full-size latency/failure qualification remain open.

- The following slice makes the physical archive representation explicit behind the existing cut, metadata and recovery-set APIs.
- A root cut conditionally publishes an exact native snapshot and Blob closure.
- Advancing a root head first checks the previously selected closure, then independently confirms the new closure; current source bytes never repair corrupt selected history.
- Exact reference reads do not substitute a newer head.
- Roots have no flat snapshot or LTX replay tail, and incompatible physical installers refuse conversion.

- The joined SQL worker now captures committed roots and their Blob dependencies.
- Root seed preparation preserves the same live grant, birth, learner/barrier and owner checks as materialized seeding and retains the complete inventory-owned seal.
- Phase logs separate root upload, previous-root verification, material reads/sealing and head confirmation.
- This still verifies complete closures: neither demand loading nor an incremental remote proof is implied.

- On updated main at `a9da48ac`, the 712 library and 24 native rooted cases pass.
- The new cases cover old-reference restore after head advancement and source loss, corrupt selected history without repair, lost write/confirmation replies, and SQL capture with committed Blob dependencies.
- A real RustFS run repeats head advancement, source deletion, exact old-reference installation and receiver reopen.
- Its small 9 MiB-padding fixture is functional evidence only.
- Public activation still selects materialized images; shared node inventory/GC, effect-scheduler integration, public fresh-learner root seeding, full-size latency and fault campaigns remain open.
- RT-04 and the M1/M2 gates stay open.

- The broader regression first detects a changed error domain for malformed archive references.
- Removing a duplicate preliminary validation restores the existing refusal path; its assertion remains unchanged.
- Corrected input `69a0de445ff59501ac1736a3d468533d028fb400e64ce40ba61a548d979a77bb` passes all 712 library, 76 archive, 45 snapshot, 24 rooted and 16 extent-store cases, plus the explicit RustFS head drill.
- Both affected crates pass all-feature/all-target Clippy with warnings denied.
- Formatting and layout cover 1,238 Rust files; native dependency and documentation guards pass.
- The five matching tested executables, source inventory, failing regression and successful logs are retained.
- The preserved RustFS prefix contains two root snapshots, two head manifests, four extents totaling 13,707,264 bytes, one dependency and one selected head.
- These small drills do not qualify full-size root recovery, public fresh learners, hard process loss or recovery percentiles.

### Public node extent ownership and backend selection

- The current integration persists `RaftImageStorage` in the immutable namespace contract and requires explicit node-wide `RaftExtentOptions` for local extent participants.
- The `/extent-owners-1` capability is a marker-1 hard cutover; materialized and extent histories cannot reinterpret each other's archive heads.
- The extent-only quota helper accepts decimal 10 GB without full-image scratch spools, while preserving ordinary command/delta bounds and checking shared inventory capacity before activation.

- One inventory supplies every resident, standby and receiver.
- Startup rebuilds current and checkpoint references from every durable log group, including unregistered groups, before a joined blocking GC job can reclaim files.
- Transient receive pins protect the complete manifest's declared closure across import, network waits and cancellation.
- Physical mutations serialize on bounded blocking owners; retention of an already sealed, exact-owner capability uses metadata only.
- Weak resource observers do not retain the inventory lock after shutdown.
- Failed startup also joins physical-owner cleanup.
- Blob capture and alarm/outbox discovery now dispatch through the same image owner as queries.

- The initial concurrent two-cell checkpoint fixture exposed an async retention call waiting behind inventory I/O; a lease renewal took 1.489 seconds and the node fenced itself under its unchanged one-second TTL.
- Moving sealed-capability retention to metadata-only work removes that wait.
- The next failure came from the test dropping its callback notification receiver.
- Retaining that receiver lets the unchanged write, closed-group, GC, restart and receipt assertions pass.
- A separate new quarantine test initially injected a directory-sync error whose successful cleanup correctly left the inventory healthy.
- It now injects failed cleanup to exercise an actually quarantined inventory; sealed retention must refuse it.
- All failed logs remain retained.

- Input `ab1f11dca62a6997fa379ca3c1889212be62ecae9415a583f8abca471a7044df` passes 715 library and 214 native cases: 76 archive, 18 effects, two shared ownership/preflight, 33 host, nine ordinary large-image, 24 rooted and 52 snapshot cases.
- Existing ignored cases remain ignored.
- The unchanged extent store inputs also pass 21 owning cases.
- The explicit real RustFS public-learner drill passes in 4.21 seconds with its tiny counter image; this is whole functional drill duration, not RTO.
- It verifies live grants, cold seeding, catch-up and joined-node restart.
- The preserved prefix contains three extent objects totaling 73,728 bytes, five root snapshots, five head manifests and one selected head.
- Matching tested binaries and source/log inventories are retained.

- Clippy then finds a redundant `Ok(...?)` in the new ownership fixture.
- A focused metrics regression also catches the newly added availability gauge: a live materialized node now has exactly 111 fixed samples rather than 110.
- The final test-only correction removes that wrapper, retains the exact cardinality check with its new gauge, and checks a live inventory's 14 label-free extent samples.
- The operations run then catches the corresponding JSON field-count change; its exact ten-field assertion now also requires `extents: null` when absent.
- The failed lint, series and JSON regression logs remain retained.

- Final input `eb9ab3d133f83ac01a1688bc2114608f5f0e28f02003d47ab3876859fb328602` differs from the broad passing input only in those two fixtures and their metrics documentation.
- All production implementation inputs are unchanged.
- It passes all 23 operations, two shared-ownership and 21 extent-store cases, plus a rebuilt peer recovery check and a repeat real RustFS public-learner drill.
- Both affected crates pass all-feature/all-target Clippy with warnings denied.
- Format/layout check 1,240 Rust files; documentation and native dependency guards pass.
- Source inputs remain unchanged, and matching final binaries and source/log bindings are retained.
- These small drills provide no new full-size root latency result; RT-04, RT-05 and the M1/M2 gates remain open.

### Public extent cold-recovery measurements

- The existing recovery runner now accepts `--storage extents` and selects exact native cases through the same profile/backend mapping.
- A new fixture module preloads either physical representation before the initial replicas start.
- Both paths verify the complete prepared template digest; the extent path imports and seals canonical extents without constructing a materialized generation.
- Initial-member claims, native applied fences and ordinary command limits remain unchanged.
- The fixture joins physical baseline cleanup on its blocking owner.

- The recovery ledger records the selected representation.
- Both modes still require complete local hydration before their first read; the analyzer rejects wrong representations and incomplete ledgers.
- Owner loss, empty replacement startup, archive waits, live learner grants, payload checks, promotion, first durable write, receipt reconciliation and cleanup retain their original oracle.
- No command/read deadline or refusal assertion was relaxed.

At input `e09000ff7cec9cae1ea061eaa5ad18c505cfb9e8591aae08b12a8902859add6f`, all 16 ordinary large-image cases pass, including six new public extent cases and a template-digest refusal covering both representations. The 12 explicitly physical cases remain ignored. The performance-tooling run reports 233 cases, with 232 passing and one existing skip, and the affected native target passes all-feature Clippy with warnings denied.

- The first release preflight build was deliberately interrupted before any recovery trial started.
- An integration target in the host package also builds its CLI and example executables; their fat-LTO links dominated this build.
- The unchanged test entry point now lives in the private `cellar-recovery-tests` workspace package, reusing the original shared fixtures and release profile without building those unrelated executables.
- The runner selects this package; assertions, native deadlines and exact test names remain unchanged.
- Public near-quota extent measurements and M1/M2 qualification remain open.

- At input `c4464d717034d89ca071963c02819ca391cfba01555343202e9d553f8f6d70f6`, the moved target passes the same 16 ordinary cases with 12 physical cases ignored.
- Clippy, format/layout (1,241 files), documentation, the native dependency guard and 232 tooling cases pass; one tooling case remains skipped.
- The release build emits only the recovery executable and retains the ordinary fat-LTO profile.
- Its tested executable digest is `9108418f007fdb740b94200acfa50d52e88d3c0047d101ec5bea58cf8a0fe2de`.

- The real RustFS extent 272 MiB preflight passes all nine correctness checks, with source identity unchanged and all worker directories cleaned.
- Its failure-to-first-verified-write interval is 31.565631 s: admission-to-archive wait 23.328431 s, seed installation 4.151511 s, promotion 0.862785 s and first write 0.327717 s.
- The required archive wait is included.
- The retained prefix is `cold-recovery-qualification/3f53e381b437680293c0970910e06f01/`.
- This is one embedded joined-domain-loss pilot on uncontrolled caches, not a 10 GB result or percentile qualification.

- The same release executable passes the public peer extent pilot at 9,999,998,976 bytes.
- All nine correctness checks pass, including the erased owner, cold local serving, new durable write, exact receipt, single callbacks, full payload oracle and joined cleanup; source identity remains unchanged.
- Failure to first verified write is 233.748164 s (3m54s): seed installation is 228.080934 s, promotion 1.134577 s and first write 0.302345 s.
- The broad payload oracle finishes 273.925285 s after fault injection.
- The complete functional drill takes 1,589.28 s, including fresh fixture generation and initial-voter setup; that whole-drill duration is not the recovery RTO.
- The 120-second target is missed, and the run does not establish a percentile or a controlled speedup over the earlier materialized pilots.

- Its retained prefix is `cold-recovery-qualification/cc05030dbee11908a9fd6585e87a0edc/`.
- After successful joined shutdown, five native root manifests and the inventory metadata were retained before the two synthetic worker directory trees were removed under an explicit ownership audit.
- The raw generated input is retained read-only for the full-size RustFS trial; its preparation and complete native verification still run independently.
- Public full-size RustFS extent recovery, M1/M2 qualification and RT-05/06 remain open.

- The full-size RustFS extent pilot at the same frozen input fails its final payload oracle.
- It installs the complete local root, serves the indexed read, promotes the cold replacement, acknowledges and verifies a new write, and reconciles receipts with single callbacks.
- Failure to verified write reaches 278.132745 s, including 231.332927 s archive wait and 42.896968 s seed install; the new write takes 0.411125 s.
- These are partial-progress intervals, not an accepted recovery sample.
- The 60-second broad payload query remains in flight when its caller expires with `VersionUnavailable`; its receiver snapshot has three SQL queries started and only two completed.
- Query deadline cancellation maps to that failure.
- The query's cache-copy/sync cost and competing archive I/O need an isolated diagnosis; no timeout or assertion was relaxed.

Joined cleanup passes and source identity is unchanged. Both synthetic worker trees remain retained for diagnosis, along with the failed ledger, tested binary and prefix `cold-recovery-qualification/18c401b2b4561c63eebcc70851eceb03/`. The exact raw source is shared with the peer pilot and hashes to `24a5f2ff65789e9bbc66b4fa69b470b2f98fe45cd7a3262f1434526d25778764`. Full-size RustFS remains unqualified despite the successful earlier stages.

- The retained cold replacement supplies an isolated query diagnosis at the exact applied marker (index 10, root `77c8ca4b92aff5a3364361f7b4a588bac3205850097adfd894f607510bdbefc8`).
- The private qualification probe copies its native log/inventory before opening either and re-establishes the complete local closure proof before timing the query.
- Its debug-profile baseline returns counter 9, 9,953,083,392 payload bytes and three sampled payload hashes in 35.963040 s.
- It reads 20,326,170,624 bytes, writes 10,025,164,800 disposable cache bytes and creates/syncs 2,391 temporary files (9.605389 s in sync).
- This isolated success does not reproduce the fleet deadline; competing archive I/O remains a candidate contributor.

- Pinned local inventory reads now cache independently verified read-only file handles, retaining the same conservative scratch byte/file admission and root pin until last close.
- Reopened entries still require file/directory barriers; cache hits still recheck block hashes.
- Generic sources keep the separate anonymous download, destination verification and sync path.
- The no-copy SQLite regression fails before the change and passes afterward, with added admission, corruption, persistence-failure and final-reader/GC lifetime checks.

- The same retained-root probe then returns matching state and sampled hashes in 13.651191 s, reading 10,301,005,824 bytes with no cache writes, temporary files or query syncs.
- Complete closure verification runs separately in 9.642850 s.
- These are individual uncontrolled-cache/shared-host diagnostics, not a native RTO or a qualified speedup.
- The full-size RustFS failure remains open until a fresh native drill passes the unchanged payload and receipt oracle.

- The final probe also takes the original worker's native-log and inventory locks before copying, refusing a live source.
- Its repeat finishes in 14.071599 s with the same marker, state, three payload hashes and zero cache writes/syncs.
- The affected LTX suites pass 68 cases (two physical cases ignored); native recovery passes 16 cases (12 physical cases ignored), and native rooted-image coverage passes 24 cases (two physical cases ignored).
- All-feature/all-target Clippy for LTX and the qualification package, formatting, layout (1,246 Rust files), native dependency and documentation guards pass.
- An initial diagnostic logging-policy lint was corrected using the existing output helper; no lint was suppressed.
- These checks do not replace the pending fresh full-size drill.

- The committed `d36b7791` release executable (`ad93b4f2ec1d8d2d6c22bf3446a2a284ce275961e1fe7925c2a9b746c0d05287`) passes both real RustFS 272 MiB preflights with all nine correctness checks and valid phase observations.
- Failure to verified write is 11.857459 s with the existing workspace-volume object store and 7.548711 s with a separate disposable internal-volume store.
- These are separate uncontrolled single pilots and storage layouts; neither a percentile nor a controlled code speedup follows.

- A full-size launch then refuses before starting a trial because shared workspace free space falls below its fixture high-water guard.
- The retained failed cold worker is losslessly archived, with all 2,397 decoded file lengths and SHA-256 hashes checked against its offline source, before its directory is retired.
- The complete 10,005,223,082-byte worker occupies 29,662,974 archive bytes; all RustFS prefixes remain retained.
- Evidence and retained executables move to the internal volume with old paths preserved, while Cargo targets stay on the workspace volume.

- The fixture now retires only its private prepared template after all initial native markers and physical preload owners have joined, before runtime startup can overlap archive publication.
- Pending/failed preloads retain the input; failed retirement refuses startup.
- The native assertion that the prepared input is absent fails before this change.
- This saves one full temporary image without relaxing any engine proof, operation deadline or post-fault oracle.
- The release target then passes all 16 existing native cases and both new preparation-lifecycle cases; 12 physical cases remain ignored.
- Qualification package all-feature/all-target release Clippy, format/layout (1,248 Rust files), native dependency and documentation guards pass.
- Production Rust matches `d36b7791`; only the fixture, its tests and tooling documentation change.

The fixture repeats a pseudo-random 1 MiB payload across rows, with distinct SQLite pointers and sample tags. Full logical/transfer-byte checks still apply, but the strong lossless compression across files means these pilots do not qualify independently high-entropy contents. That workload remains part of RT-06, alongside controlled hardware/cache and process-failure campaigns.

- At `878100de`, the all-internal 272 MiB preflight passes all nine checks in 7.979558 s through verified write, with valid phase observations.
- Its full-size attempt fails during preparation, before runtime startup or fault injection: the supplied raw input is read-only, and cross-volume `fs::copy` propagates that mode to the private SQLite copy.
- The failure is retained as a failed attempt, not an accepted RTO.
- Its duplicate prepared file matches the retained raw input byte-for-byte before retirement; failure metadata and the raw source remain.

- Private template copying now uses exclusive writable creation and a source-length bounded copy when COW is unavailable.
- It checks the opened source and destination lengths without changing source permissions.
- Read-only-input and existing-output regressions both fail before this change and pass afterward.
- The native target retains these cases; the temporary fast-feedback entry is retained in evidence.
- The final release target passes 20 ordinary cases, with 12 physical cases ignored.
- All-feature/all-target qualification-package release Clippy, format/layout (1,250 Rust files), native dependency and documentation guards pass.
- The failed read-only setup is still recorded; at that revision a fresh full-size pass was still required before accepting native RustFS recovery.

- The all-internal retry at `916e7ff58847bba37ccd0f15cfec0bb06d01efe1` passes the complete 9,999,998,976-byte RustFS native oracle: all nine correctness checks and all six phase-observation windows are valid.
- Failure to first verified durable write is 200.704983 s, including 123.620465 s waiting for the archive and 71.636138 s installing the seed.
- Promotion takes 0.419161 s and the first write 0.164724 s.
- The broad payload oracle passes in 13.967413 s under its unchanged 60-second deadline; failure to payload verification is 214.695243 s.
- The whole 482.89-second fixture includes initial preparation and is not RTO.
- Source input SHA-256 is `7de5605136ba2de4e007c46996762e556410c61d3699d0fcf6d502d12bf1d54c`; the release executable is `c3c46b61741dbee148e512b14a9789cc1c978d9cc33968ef7f29f1b3e0da03c9`.
- The retained prefix is `cold-recovery-qualification/e5e1ef074c4de015d414824848368b5b/`.
- This single embedded joined-domain-loss trial uses an all-internal storage layout, uncontrolled caches and shared-host load.
- Its different layout prevents a controlled comparison with preceding trials.
- The payload repetition caveat still applies; no process-kill or percentile qualification is established.
- The native RustFS functional failure is closed for this tested input, while the 120-second target, a current-source peer campaign and RT-05/06 remain open.

- Phase counters identify amplification before the cold receiver starts: one publisher consumes 50,000,734,901 GET body bytes for the approximately 10 GB image.
- Another replica consumes 10,000,078,296 GET body bytes during receiver installation, alongside the receiver's 10,000,159,919 bytes.
- Requested PUT payload bytes are not actual wire-upload bytes.
- Publication coordination already applies to rooted cuts; these counters do not establish a missing coordination bug.
- Repeated confirmation/materialization and covering reads motivate reusing same-attempt verified material, bounded download/install overlap and foreground I/O scheduling.
- Operation durations overlap and must not be added to RTO.

- The archive loader now bounds the complete GET/verify/install lifetime to two objects, keeping network polling active while one destination install runs.
- An async gate serializes only disk mutation, preserving the ordinary inventory contract that refuses overlapping mutations.
- The memory admission bound remains unchanged.
- Entered physical installs and final sealing retain their own receive pins and memory admission across cancellation.
- After an observed error, the loader admits no more objects and drains its existing window before returning; only a complete independently verified local closure can produce the seal.

- Two native regressions hold an actual extent's destination verification while releasing the second GET.
- Both fail with the original loader and pass with the pipeline.
- They check the two-object bound and drain an injected GET failure before return.
- A third test cancels the receive while its physical install is held, verifies that execution-domain shutdown waits, then checks that GC can reclaim the abandoned closure without a leaked pin or quarantined inventory.
- The complete release qualification package passes 20 ordinary large-image cases and 27 native root-image cases; 12 large physical, two root RustFS and one retained diagnostic case remain explicitly ignored.
- These owning-boundary tests provide no new full-size RTO measurement.
- Peer source inspection confirms that one verified extent is already cached across wire chunks, so repeated full extent reads per 1 MiB peer window are not an established bottleneck.
- All-feature release Clippy passes for the production library and every qualification-package target with warnings denied.
- Format/layout (1,252 Rust files), native dependency and documentation guards also pass.
- Tested executable copies and the complete matching source inventory are retained.

- At `cb1138817241f3d441dbdec357e80862839cbeef`, the matching all-internal 272 MiB RustFS pilot passes all nine correctness checks and all six phase windows.
- Failure to verified durable write is 9.541490 s, including 3.301136 s of archive waiting, 0.958618 s of seed installation, 2.381931 s of promotion and 0.171622 s for the first write.
- Its source inputs are `284aa6d8b2a4787ede81d73710e0cca661b49d14f45b348429133162a20742fa` and tested release executable is `b58a383138d208755421a20697862970562bf929e32864dc8b8efb2fd8908892`.
- The analyzer retains its three-sample pilot minimum and correctly returns nonzero for this one-sample smoke run; native recovery and phase diagnostics pass, while latency qualification remains false.
- These contended small pilots are not a controlled speedup comparison.
- The latest passing 10 GB measurement remains the preceding `916e7ff5` trial; the pipelined source still needs fresh full-size peer/RustFS campaigns.

- Root publication now retains a private confirmation receipt instead of fetching its own complete root again after PUT read-back.
- The receipt binds the exact snapshot reference and sealed source image to its actual node inventory, then runs the ordinary local seal on the physical owner.
- Missing/corrupt local root metadata, geometry failures, quarantine and persistence failures remain refused; the receipt does not supply a native grant or publish an archive head.

- Before uploading a newer cut, publication still independently fetches and verifies the complete selected previous root and Blob closure.
- Only unchanged extent identities from that verification can be reused within the same CAS attempt.
- Previous Blob bodies are dropped before the new dependency set is assembled, and a CAS retry reloads the selected history.
- Standalone storage and independent recovery retain full remote reads.
- No wire format changes.

- The operation-count regressions initially fail: first publication of three extents performs three PUTs and six GETs, while changing one of those extents performs three PUTs and nine GETs.
- After reuse, the first publication performs three PUTs and three GETs; advancement performs one PUT and four GETs, including all three previous-history checks.
- All five focused native publication tests pass, including independent recovery, missing/corrupt selected data before any replacement upload, successful PUT without confirmation, and missing/corrupt local root metadata.
- This proves bounded work reduction, not a latency result; full-size RTO and the remaining M1/M2 gates still need qualification.

- The final formatted source passes 52 ordinary native cases (20 large-image and 32 root-image), with 15 physical/retained diagnostic cases ignored.
- All-feature production-library and all-feature/all-target qualification release Clippy pass with warnings denied.
- Format/layout (1,253 Rust files), native dependency and documentation guards pass.
- The intermediate format failure and test-only image-root encoding correction remain in the retained logs; final source inventories and tested executables match.

- To provision the next all-internal full-size drill, the preceding successful worker trees and object-store prefix are preserved in verified archives on the workspace volume, outside the checkout's Cargo target.
- All 7,188 decoded worker files match their source hashes/lengths under the original ownership locks; archive and metadata persistence precede directory retirement.
- The 2,405 object bodies match source identities/ETags, including 2,387 domain-separated extent identities, and their decoded archive hashes match.
- The exact inactive fixture prefix is retired only after that backup is durable and a fresh listing matches; restore instructions and the original evidence remain retained.
- Other prefixes and live data are untouched.
- This releases capacity for the unchanged 10 GB oracle and its existing disk-headroom guard.

- At `29a1a9b5d7440eace5be1c212d95e40531340015`, the actual 9,999,998,976-byte all-internal RustFS drill passes all nine correctness checks and all six valid phase windows.
- Failure to first verified durable write is **90.867463 s**: archive waiting is 42.528691 s, seed installation 44.874555 s, promotion 0.491075 s and first write 0.234381 s.
- The full payload oracle passes in 14.419650 s under the unchanged 60-second deadline; failure to full payload verification is 105.315401 s.
- Whole-fixture duration is 339.57 s, including initial preparation, and is not RTO.
- This is the first retained full-size RustFS observation below 120 seconds; it does not qualify M1's p95 gate or M2.

Source inputs are `ba1ca8c582f7457b2c62a4bffb06990bd4c0bbfba19a615449028c18ba9b446e`, with release executable `314d43d8344419bfb2911553b6389b8aec24404e4042aba0b9c030a56d545c4c`. The matching 272 MiB preflight passes all nine checks and six valid phase windows in 6.851718 s. Full-size objects were initially retained under `cold-recovery-qualification/e7d5b5c02b0ee89861211edb8f3b5bd2/`, together with the worker trees and complete source-bound evidence. Their later verified archival and retirement are recorded below.

- Before seeding, the active publisher consumes 10,000,176,825 completed GET body bytes and succeeds at one cut; the preceding 200.704983-second trial consumed 50,000,734,901 bytes across two successful cuts in that observation window.
- The different cut/coalescing schedule is part of this comparison.
- It is not a controlled fivefold algorithmic speedup.
- During seeding the cold receiver reads 10,000,159,912 bytes while another replica consumes 9,983,301,094 bytes.
- Requested PUT payload counters are not actual uploaded wire bytes.
- Logs also show a later metadata-only advancement confirming all 2,385 existing extents without new extent PUT attempts.
- The deterministic native regressions establish the per-attempt reuse contract independently of these timing effects.

These remain individual shared-host, uncontrolled-cache, repeated-payload, joined-domain-loss pilots. The analyzer correctly returns nonzero for the insufficient pilot population and leaves qualification false. At this point, current-source full-size peer recovery remained pending; the merged-source peer result below closes that individual pilot gap. Repeated/high-entropy and hard-process-loss campaigns, verified remote demand loading and durable retention/GC remain open.

### Merged-source full-size peer pilot

After PR #142 merged, the follow-up starts from updated `main` at `cd9a968932c81fcfd4a3fe9f03c4ce7c0e57e1b0`. This includes intervening host, storage and dependency changes, so it uses a fresh locked release build rather than rebinding the preceding executable to another commit. Source inputs are `af8f024b48c32f65b491e5c4c7eb4554ef70df9dd83330a1c2a83b0100e6b957`, with release executable `0f28f0cbf54dde339a1658d6fa83d55a35108b0dd9bc4ad34b05871f113dd11b`.

The 12 MiB peer preflight passes all nine correctness checks and all six phase windows in 3.830853 s. The unchanged 9,999,998,976-byte peer oracle then passes with the same checks and six valid windows. Source inputs and executable bytes remain unchanged throughout each trial.

| Full-size peer interval | Seconds |
| --- | ---: |
| Owner loss to first verified durable write | 86.831416 |
| Archive wait | 0.000405 |
| Seed transfer and installation | 81.616753 |
| Promotion after the first read | 0.562870 |
| First write after promotion | 0.257231 |
| Full payload query after receipt verification | 16.992506 |
| Owner loss to full payload verification | 103.858179 |

- The full payload query remains under its unchanged 60-second deadline.
- The 343.22-second whole-fixture duration includes initial source preparation, preloading and proof establishment; it is not the recovery RTO.
- Installation still verifies the entire local closure before serving, and the replacement then verifies new writes, receipts, duplicate callback behavior and joined cleanup.
- Worker state and objects were initially retained under the new fixture prefix `cold-recovery-qualification/7a6e7d1a44b587be6ccdf205e9b25091/`.
- Before the next physical campaign, verified archives preserve all 7,188 worker files and 2,405 object bodies, including 2,387 extent identities.
- The exact inactive prefix, worker trees and stopped-store trash are retired after verification and persistence; restart again preserves all 465 other live object entries.
- Raw campaign evidence, archives and restoration instructions remain retained.

- During the seed window, the replacement records 10,000,075,408 received and appended peer bytes across 9,537 frames.
- Completed append work sums to 55.638312 s, queued-frame time sums to 50.516770 s, and network receive work sums to 30.703258 s.
- These owner-clock observations overlap; they neither add up to the controller interval nor isolate scheduling delay from disk pressure.
- The receiver already uses one registered physical owner and two bounded wire windows.
- This result does not justify adding a duplicate receive pipeline.
- The two surviving hosts also complete 10,000,168,679 and 10,000,366,910 object GET body bytes in that window while normal archive publication stays enabled.
- Foreground I/O contention and destination persistence are therefore concrete next measurement targets; the counters alone do not prove either root cause.
- Requested PUT bytes are not actual uploaded wire bytes.

- This is one embedded joined-domain-loss trial with shared hardware, uncontrolled source/store caches and repeated pseudo-random 1 MiB payloads.
- The analyzer leaves qualification false with its three-sample pilot minimum unchanged.
- It neither establishes p95 nor qualifies hard process loss, M1 or M2.
- The preceding 90.867463-second RustFS pilot uses a different source revision; these observations do not establish a controlled backend comparison.
- The next qualification work still includes repeated trials, independent high-entropy data, hard kills and contention, followed by verified demand loading and durable remote retention/GC.

- Provisioning this drill preserves the preceding RustFS trial before reclaiming its capacity: all 7,188 worker files and all 2,405 object bodies match their decoded archives, including 2,387 domain-separated extent identities.
- Archive and metadata persistence precede retirement of the inactive worker trees and exact object prefix.
- RustFS initially retains deleted bytes in internal trash; the owned test server is stopped before that trash is archived, verified and retired.
- Restart preserves all 465 live object entries and their lengths/ETags.
- The new trial uses the same server binary, endpoint and internal-volume layout, and passes the unchanged four-image-plus-8-GiB capacity guard.
- Prior evidence, restore instructions and backups remain outside the Cargo target.
- No older RustFS instances or unrelated data are reclaimed.

### Borrowed extent installation windows

- The accepted peer pilot identifies installation as a large part of the seed window but does not isolate its disk cost from concurrent archive work.
- One specific implementation issue is independently reproducible: a complete received extent is already resident under admission, yet installation copies it into 64 separately allocated 64 KiB source chunks and issues 64 provider writes for a full 4 MiB extent.
- The regression reproduces 64 calls against an expected one before the optimization, while the three integrity/admission/cleanup guards pass.

- `NodeExtentStore::put_extent_bytes` now borrows that bounded input through `HostFile::write_all_at_window`.
- Native providers accept one window; injected providers retain 64 KiB positional writes.
- Peer and archive installation use this common path, and extent apply reuses its existing admitted working buffer.
- The streaming callback API retains its 64 KiB bound.
- Both input forms share exclusive creation, inventory charging, source integrity, file sync, independent destination verification, directory sync and uncertain-cleanup quarantine.
- No extra whole-extent allocation, deferred durability or incomplete local seal is introduced.
- Focused tests cover both provider modes, restart reuse, corrupt existing/input/destination bytes, wrong lengths, capacity refusal, partial writes, failed file/directory syncs and cleanup failure without premature refunds.
- The final source passes 120 LTX integration cases and 52 ordinary native cases (20 large-image/recovery and 32 root-image/archive).
- Four LTX and 14 native physical cases remain explicitly ignored.
- The no-default-features LTX check, all-feature/all-target LTX and qualification Clippy, all-feature host-library Clippy, format/layout (1,311 Rust files), native-dependency and documentation guards pass.
- The initial Clippy failure identifies an unused `io` import left after replacing the old closure; the final source removes it without a lint suppression and repeats validation with unchanged source inventories.

- A separate component probe compares both input APIs in the same release-built library with matching workspace optimization flags.
- Three alternating pairs import 32 distinct 4 MiB objects per mode.
- Median import time is 0.593364 s for streaming callbacks and 0.541661 s for borrowed input.
- Each run performs 32 file syncs and 32 directory syncs, while provider writes decrease from 2,048 to 32.
- Median borrowed-input write time is 0.202425 s, file sync time 0.140130 s and directory sync time 0.002746 s.
- These observations make directory-sync batching a low-priority hypothesis on this host; they do not isolate costs under the concurrent archive traffic of a full recovery.
- The probe runs after its own LTX test I/O has finished, while native compilation and other shared-host work may continue.
- Caches are uncontrolled; the earlier overlapping probe remains separately recorded.
- These are component observations, not native RTO or percentile qualification.
- Fresh source-bound physical pilots are required before claiming a full-size recovery-time gain.

- At `3be8c9edd6a91e5c9613746c30f4be81018f9017`, fresh peer (12 MiB) and real RustFS (272 MiB) preflights pass all nine checks and six valid observation windows.
- Failure to first verified durable write is 3.827742 s and 6.971304 s, respectively; seed installation is 0.303747 s and 1.158608 s.
- Inputs are `464cf0ca1cc939a43470a7169adc91e457f43c7906f9ac103bf65b08b1ccff2e`, with release executable `f7c617d3632429f136791d44866421b3d18fddf5f0142373365b410baa16999d`.
- Both source inventories remain unchanged throughout their trials.
- Their single observations do not meet the analyzer's unchanged pilot sample requirement.
- The new full-size campaign has not started: its capacity preflight refuses before creating a native trial when the unchanged four-image-plus-8-GiB guard is unmet.
- This unavailable capacity observation is separate from timed recovery samples.
- The earlier full-size timings above are not results for this new code.

- Full-size setup diagnostics distinguish source creation and initial-voter preloading from the fault-to-ready interval.
- A one-second sample of the release process during baseline import records 695 of the active blocking worker's 775 samples inside `fcntl`; stripped Rust callsites prevent identifying that syscall by stack alone.
- A separate eight-file diagnostic using Darwin's exported `F_FULLFSYNC` during setup measures about 380 ms file sync plus 191 ms directory sync on the workspace volume, versus 5 ms plus 1 ms internally.
- These contended diagnostics are not reference-hardware or percentile qualification.
- They motivate measuring persistence-barrier time during the actual receiver phase before changing buffering, concurrency or synchronization.

- `NodeExtentStore` inventories immutable filenames at open; it does not rewrite a full catalog for each imported object.
- New imports independently sync and verify their file and sync the containing directory.
- Reopened entries start unverified, and native marker adoption re-establishes their byte and persistence proof before admission.
- Therefore initial fixture startup includes a second proof-establishment pass after preloading closes its owner.
- Keep that setup cost distinct from timed cold replacement installation.
- Any future reuse or batching must preserve the selected marker's exact owner/history binding, independent byte verification, required persistence barriers and failure cleanup; filenames alone still provide no durability proof.

### Phase-associated API work observations

- The follow-up qualification fixture retains `RaftCells::performance()` snapshots before fault injection, around seed installation, after verified write/payload checks and around joined cleanup.
- Startup source identity keys the captures; service-vector ordinals can shift when the failed voter is removed.
- Retained closed handles keep that startup's final counters visible without retaining its physical inventory.
- Controller-clock bounds record the capture overhead, which is not subtracted from RTO.

- The analyzer reuses the strict performance vocabulary and histogram/delta validator.
- Existing sources use adjacent completion windows; a newly observed host reports its lifetime without assuming a zero baseline.
- Missing sources, source/counter/clock regressions, inconsistent live timing and captures outside their controller phases refuse the diagnostic.
- After domain shutdown, a retained host can correctly have no owner clock and outcomes without duration samples.
- Keep that raw final state explicitly unavailable; do not subtract it or replace it with zeros.
- Other live sources still use the unchanged strict validator.
- Completion durations can overlap and straddle a boundary, so they never replace or add up to the controller RTO.
- Optional diagnostic failures are explicit and do not replace the native oracle.
- The first native peer-extent case passes all nine correctness checks and joined cleanup.
- Its first analysis correctly refuses closed-clock observations as measured deltas; the follow-up reports them explicitly unavailable instead, retaining the rejected analysis and raw capture.
- The tooling run reports 245 cases: 244 pass and one existing case is skipped.
- At input `ceb9ce3338e01044b2162d59f4ed20759f1c0c8f1ce47b08c6837d939819e4a9`, three focused peer-extent, archive-extent and materialized-peer native cases pass with all six adjacent observation windows validated.
- The complete native target passes 16 cases with 12 physical cases ignored.
- All-feature/all-target Clippy for the qualification package, format/layout (1,242 Rust files), native dependency and documentation guards pass.
- The full-size pilots retain the preceding frozen source and executable; neither these small instrumentation cases nor the failed full-size RustFS run qualify M1/M2.

### Three same-artifact full-size pilots after the archive admission fix

At commit `c1bb3a038c361f458b3ddd7baf15316cc1713ebd`, input digest `b6a2a20375b1f9c8c5c0ded0d5d74ff47609c0a73ece2cbdf138bb9392b9e633`, the release executable with SHA-256 `c080c658f643102fda2951135db279190b340c3974d7df569fdff6ca30c7c2c3` passes three actual 9,999,998,976-byte extent restores per backend. Every trial passes all nine correctness checks, all six phase-observation windows and joined cleanup. Capacity preflights pass after development-host disk cleanup.

| Backend | Trial 1 | Trial 2 | Trial 3 | Median |
| --- | ---: | ---: | ---: | ---: |
| Peer | 94.874534 s | 119.430358 s | 95.223625 s | 95.223625 s |
| RustFS | 138.557515 s | 105.314964 s | 106.169031 s | 106.169031 s |

- These intervals include injected owner loss through the verified durable write.
- The RustFS trial above 120 seconds remains in the population: its pilot target is unmet.
- RustFS archive-coverage waits are 64.173601, 54.988260 and 58.703560 seconds; seed installation takes 70.827723, 47.394451 and 43.379604 seconds.
- The peer seed intervals are 91.100102, 114.532020 and 90.580674 seconds.
- First writes after full hydration take 0.199513–0.341893 seconds.
- This does not establish an inexpensive first write against an incompletely hydrated image.

- The evidence controller retains raw campaigns, exact executable provenance, phase ledgers and successful worker-retirement receipts.
- Its combined analyzer requires identical source, profile, backend, image size and trial completeness; all six outcomes are retained.
- These are single-host, uncontrolled-cache, repeated-payload, joined-domain-loss pilots.
- Three samples establish neither production percentiles nor hard process-loss qualification.
- RT-05/06 and both production milestones remain open.

- The subsequent `5c050d37` component change coalesces concurrent reads of an uncached extent under one receive/scratch reservation.
- A gated two-reader regression reproduces `ScratchFull` before the change and passes afterward.
- Failure, corruption, producer panic, cache eviction and independent-extent concurrency cases pass.
- All-feature LTX extent suites pass 62 cases, with three existing physical cases ignored; all-target/all-feature Clippy and repository guards pass.
- These full-size timings use the preceding `c1bb3a03` executable; they do not qualify the newer component or native partial activation.

### Avoid retransmitting already stored extent objects on publication retry

- The next regression reproduces three conditional-create requests for three already stored extents.
- It also shows that a failed confirmation read follows an unnecessary upload.
- Root confirmation now probes and hashes remote bytes before reading the local source.
- Only explicit absence permits an upload; transport failures and corrupt objects stop confirmation.
- Newly uploaded objects still require exact read-back, including when the PUT response is lost.

- The source engine must remain open.
- Existing selected history still verifies before new material can be uploaded, so this optimization cannot repair or hide a missing/corrupt selected dependency.
- Probing precedes the source buffer allocation and stays within the existing two-object/16 MiB read bound.
- The entered work drains on failure.
- Completion traces separately report freshly verified existing objects, new uploads and previous-root reuse.

- The first native rooted suite passes 35 cases with two physical cases ignored, including the new retry, unknown-PUT-response, corruption, read-failure and closed-source cases.
- Publication tests still require exactly one successful object download per new extent: an absence probe adds a request, not another object download.
- Public RustFS smoke/full-size measurements on the resulting artifact are required before attributing an RTO improvement to this change.

- Final validation at input digest `7d6b7419ac19f6978406046000c9b6332f5c9c2ba60aa76154b88fb7e6882ab1` passes 135 native archive, large-image and rooted cases, with 14 physical cases ignored in the ordinary suites.
- The explicit RustFS root restore also passes: the source owner closes and its directory is removed before restoration, native installation and restart verification.
- All-feature host-library and all-target qualification Clippy, formatting/layout (1,354 Rust files), dependency, documentation and diff checks pass.
- The release large-image executable has SHA-256 `989c384cdee5aa42b48f76afda7be176813474a64ed82c5e1d08cc2489faf6c9`.
- Full-size and hard-failure qualification for this artifact remain separate gates.

- The source-bound `2bb3dbfc` extent campaigns subsequently pass both smokes (peer 12 MiB: 4.868629 s; RustFS 272 MiB: 8.712589 s) and the actual 9,999,998,976-byte pair.
- Peer fault-to-verified-write is 94.956199 s; RustFS is 92.472175 s.
- Each full-size run passes nine correctness checks and six phase windows.
- RustFS spends 38.455207 s waiting for archive confirmation and 50.762878 s installing the seed; its first write takes 0.211575 s.
- Publication confirms 1,366 existing extents and uploads 1,019 missing extents.
- Phase-associated requested PUT payload is 4,270,666,801 bytes, versus about 10.0 GB in the earlier pilots; these counters measure API work, not network or device traffic.
- These single shared-host trials retain the existing uncontrolled-cache, repeated-payload and joined-domain-loss limitations.
- The analyzer remains unqualified; neither production percentiles nor hard-kill qualification follow.

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

- Verification: while hydration is intentionally paused below 100%, the authorized replacement reads an uncached extent, performs and verifies a new durable small command, reconciles a duplicate receipt and survives a fresh process restart.
- Kill it before and after each root/extent/marker sync boundary.
- Delete its cache and verify recovery at the same root.
- Loss of a required remote extent fails safely.
- A read-only or learner-only demonstration does not complete M2.
- For the same hot workload, first-use bytes/RAM stay bounded as total database size increases from 256 MiB to 10 GB.
- Full-scan workloads are reported separately.

### Verified private-writer groundwork

- `ExtentWorkspace::from_source` now accepts an authenticated manifest and a source that retains its dependencies, without requiring a complete local image seal.
- Both writable constructors use the existing bounded, independently verified extent cache and destination-copy checks.
- Open SQLite handles retain the source after workspace retirement, including for an empty base.
- Observations reuse the exact base manifest and actual main/WAL changes; they grant no native authority.

- The first runtime check exposed a whole-image read at a TRUNCATE checkpoint: ordinary capture deliberately creates a full boundary snapshot.
- The new `Db::require_incremental_capture` policy refuses that fallback before snapshot collection after an initial or seeded TXID, returning `CaptureRebuildRequired`.
- Initial bootstrap still permits its bounded snapshot.
- Native extent writers opt in alongside their existing WAL recycling and disabled routine checkpoints.
- The execution host sets its rebuild fence before mutation, so a capture refusal cannot settle a command.
- Durable capture retains its existing behavior.

- Validation passes 129 all-feature LTX integration cases and 135 native archive, large-image and rooted cases.
- Clippy then identified two old internal-test type references; the fixture was adapted without changing its corruption/root assertions.
- Its five unit cases and all-feature/all-target LTX Clippy pass.
- Validation confirms that only this `cfg(test)` file differs from the integration test inputs.
- Host and qualification Clippy, format/layout over 1,355 Rust files, dependency, documentation and diff checks also pass.

- The explicit 9,999,998,976-byte writer case passes on input digest `e82a2cabf4c98e9af9a115696da68970d63b851a475f49ed0f9ffcdffdf3095f`.
- Three counter/receipt transactions and their duplicates fetch **two extents, 4,972,544 bytes**, and capture **53,248 decoded LTX bytes**.
- Cold manifest entries remain unchanged, bounded observations survive passive/restart checkpoints, and all scratch charges release after close.
- Smaller fixtures independently replay LTX into SQLite, verify integrity/results/receipts, and refuse missing, corrupt or interrupted source data.
- The release test executable has SHA-256 `ee7a18d8c63b0d47b1b15736bbb2fab6213d43e0ecb1772a7f1805e1ab93b895`.

- This is a data-access test using a file-backed source, not remote-retention, network, native installation or RTO qualification.
- It prepares a private copy of the retained campaign image, frees only its padding rows to install the workload without growing the image, and hashes source extents before measuring writer work.
- Its 25.57-second total includes preparation and is not a recovery latency.
- The original read-only fixture stays unchanged.
- To reproduce, set `CELLAR_REMOTE_WRITER_TEMPLATE` to that campaign fixture and `CELLAR_REMOTE_WRITER_WORK` to an admitted private directory, then run:

```sh
cargo test --locked --release -p cellar-ltx --all-features --test extent_remote_workspace decimal_ten_gb_remote_writer_fetches_only_the_hot_working_set -- --ignored --exact --nocapture --test-threads=1
```

The test requires space for its source copy, private main and 8 GiB headroom; the separate native recovery drill's 48.6 GB admission remains unchanged. `RootPublishedImage` and `RootImageApplier` still require complete local seals. Persistent remote references, GC fencing, hybrid apply/install proofs, restart and activation while hydration is paused remain implementation work for RT-05.

### Persistent ownership registry groundwork

The verification archive now has an isolated remote-root ownership registry. Its immutable random identity and conditional record bind the exact namespace, birth and schema. The record protects selected roots, explicit references, pending upload generations and their retained parents. It refuses missing or corrupt control metadata rather than resetting allocation counters.

- Allocations require a caller-persisted intent before CAS.
- Each intent includes its expected revision, a nonce and complete operation.
- Exact retries reconcile live results without sharing another caller's hold or recreating a released reference.
- Only definite CAS conflicts retry automatically; changed read-back after an uncertain response requires explicit reconciliation.
- A child reservation protects its parent atomically, and abort fences a late bind.
- The 64 KiB record bounds concurrent holds/reservations while allowing released slots to be reused.
- The [component contract](../../../crates/cellar/durability/raft/archive/retention/README.md) details these limits and the required owner reconciliation.

- Validation passes all 50 rooted cases, including 15 registry fault/lifecycle cases, plus one explicitly selected real RustFS registry case.
- The latter checks conditional competing holds, handle recreation, exact intent reconciliation and refusal after release; its descriptors are synthetic metadata, not a cell-data restore.
- Final code-input digest is `8dbc9704754927bfb170d63135e13d3963019bfef116acfe4d04f257b33366f4`.
- Host/qualification Clippy, container-test Clippy, formatting/layout over 1,359 Rust files, dependency, documentation and diff checks pass.
- The 20-case large-image suite passes with identical production inputs; only the registry tests' required fair-selection macro changed afterward, and those tests were rerun.

- CI on the preceding writer commit reported `Overloaded` while requesting the recovered member's leadership transfer.
- The drill now observes destination readiness and refreshes term/revision before retrying only that typed admission failure within its existing 60-second deadline.
- The independent container-expiry CI failure reported a closed execution domain; the exact case passes twice locally.
- Startup/termination lease diagnostics were added without changing its assertions or deadlines.
- That CI failure's root cause remains unconfirmed.

- This registry does not yet store physical root descriptors or generation-addressed objects.
- It supplies a consistent mark input, not a deletion permit.
- Collector integration must verify and mark exact closures, protect pending generations, exclude newer generations and never reuse physical keys after deletion.
- Existing archive-head/object keys remain outside that collector.
- Native marker/source binding, hybrid installation and activation before hydration remain open.

## RT-06: Harden and qualify the targets

The [2026-10-29 process recovery record](process-recovery-qualification.md) establishes one passing near-10-GB RustFS case with an owner SIGKILL, erased owner directory, new empty worker, local reads, quorum-durable writes, receipts, payload verification and joined shutdown. It uses complete local extent hydration on a shared host and does not complete this campaign or its targets.

Use deterministic small fault tests in ordinary CI and source-pinned physical campaigns for actual size, process loss and performance. Runtime tests precede each package's rollout; do not defer all fault coverage to this final gate.

| Matrix axis | Required cases |
| --- | --- |
| Physical recovery | Live peer and real RustFS, new process and empty local disk, then retained-disk restart |
| Crash boundaries | Hard process kill during source preparation, download, extent sync, root sync, engine install, first write and background hydration |
| Authority | Old owner returns, stale epoch/incarnation, expired grant, membership change, lost quorum, source changes mid-transfer |
| Integrity/capacity | Missing/corrupt objects or manifests, partial writes, disk full, growth/truncation, >4 GiB offsets, quota refusal and cleanup |
| Retry/lifecycle | Lost replies, same command ID, canceled waiters, overloaded queues, retries, joined shutdown with no premature refunds |
| Workloads | 256 MiB, 1 GiB, 4 GiB, near decimal 10 GB; compressible/incompressible; fixed hot set and broad/random reads |
| Interference | One/two recoveries and repair burst with foreground small cells; source/store stalls; realistic RTT and bandwidth limits |

- Begin with three pilot trials per backend, then at least 100 timed trials for each claimed backend/storage/reference-profile combination.
- Report nearest-rank p50/p95/p99, sample count, spread, failures and censored deadlines.
- Treat failed or timed-out samples as target misses, never discard them from the denominator.
- The ordinary per-operation deadlines and all correctness assertions stay intact.
- One hundred observations provide an empirical percentile, not a universal tail probability guarantee; retain confidence/variability information with the claim.

- Bind every result to source/tree/executable hashes, exact selected tests, profile, limits and complete phase ledger.
- Keep backend credentials and cell contents out of published artifacts.
- No SQL readers or full payload samplers may warm the replacement before its timed first operations.
- Label source/store OS caches separately; an empty worker is not evidence of a cold source cache.
- Use disposable VMs for cold OS-cache experiments; do not drop the shared development host cache.

- A scoped pass requires zero durability/integrity/ownership violations and the chosen target met in its declared envelope.
- Separately report unavailable or underprovisioned runs.
- Backpressure may refuse excess work safely, but a refused recovery is not a successful RTO sample.
- Measure foreground latency/throughput against a same-source control; initially require <=10% degradation at the declared non-saturating load.
- This interference bound is proposed and must be calibrated.

## Verification commands and expected results

Use a writable Cargo target unique to the implementation checkout on the workspace volume. For example, choose a fresh name before the first build:

```sh
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellar-recovery-time-010"
cargo format --check
cargo check-layout
python3 tools/check_native_dependencies.py
python3 tools/docs/check.py
cargo test --locked -p cellar-ltx --all-features --lib --tests
cargo test --locked -p cellar --lib
cargo test --locked -p cellar-recovery-tests --test native_raft_large_images
cargo test --locked -p cellar --test native_raft_archive
cargo test --locked -p cellar --test native_raft_snapshot
cargo test --locked -p cellar --test native_raft_rooted
cargo test --locked -p cellar --test native_raft_execution
cargo test --locked -p cellar --test native_raft_operations
cargo test --locked -p cellar --test native_raft_daemon
cargo clippy --locked -p cellar -p cellar-ltx -p cellar-recovery-tests --all-targets --all-features -- -D warnings
```

Expected: nonzero selected tests for each named target, all pass, guards and Clippy exit zero; no lint allowances or weaker timeout/proof assertions. Apply owning-crate feature checks from `.github/workflows/native-sdk.yml` and its wider release gates before deployment. Do not enable `cellar_internal_tests` manually.

Existing explicit full-size commands (supply the documented private RustFS/S3 environment and work directories; never print credentials):

```sh
cargo test --locked --release -p cellar-recovery-tests --test native_raft_large_images full_capacity::decimal_ten_gb_rustfs_restores_native_serving_after_owner_and_local_state_loss -- --ignored --exact --nocapture --test-threads=1
cargo test --locked --release -p cellar-recovery-tests --test native_raft_large_images full_capacity::decimal_ten_gb_peer_restores_native_serving_after_owner_and_local_state_loss -- --ignored --exact --nocapture --test-threads=1
```

Expected: exactly one selected test per command, both pass with payloads, quota, new writes, restart, receipts and joined shutdown. These commands alone do not qualify M1/M2. They select the materialized representation. The extent timing campaign commands below select distinct exact cases and bind that representation to their evidence.

The following campaign interface is implemented for complete-local materialized and extent embedded-node pilots. Qualification still requires the remaining fault and hardware gates; see `tools/perf/recovery-README.md` for current behavior:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tools/perf -p 'test_recovery.py' -v
python3 tools/perf/recovery.py --backend rustfs --storage materialized --profile reference-10gb --repetitions 3 --output /tmp/cellar-recovery-010-rustfs
python3 tools/perf/recovery.py --backend peer --storage materialized --profile reference-10gb --repetitions 3 --output /tmp/cellar-recovery-010-peer
python3 tools/perf/recovery.py --backend rustfs --storage extents --profile reference-10gb --repetitions 3 --output /tmp/cellar-recovery-010-rustfs-extents
python3 tools/perf/recovery.py --backend peer --storage extents --profile reference-10gb --repetitions 3 --output /tmp/cellar-recovery-010-peer-extents
python3 tools/perf/analyze_recovery.py /tmp/cellar-recovery-010-rustfs --mode pilot --target-seconds 120 --minimum-samples 3
```

- The pilot analyzer must clearly label its result unqualified.
- For acceptance, use fresh private run directories, `--repetitions 100` and `--mode qualify --minimum-samples 100`; require all phases, the exact target-sized fixture, post-recovery value/receipt checks, hard-kill suite evidence and source/profile identity.
- Repeat independently for peer and RustFS.
- `--storage extents` currently requires a complete local closure.
- RT-05 adds a distinct incremental activation profile/30-second target; a hydrated-first fixture cannot satisfy that profile.
- Qualification mode must exit nonzero with inadequate evidence or an unmet target; pilot mode must never emit a qualified result.

## Completion, stop conditions and maintenance

- Before editing, compare the current implementation with the source map and record `git diff --stat c6338ad45f64425e44ba54a9ad04f2058b947089..HEAD -- crates/cellar/durability/raft crates/cellar-ltx tools/perf`.
- Reconcile drift in these contracts before executing a step.
- Use a `codex/` branch and reuse an active relevant PR; after it merges, start from updated main.
- Keep each package independently reviewable.
- Update this status table and the current qualification ledger with exact evidence; never mark the entire existing multi-Raft delivery complete from a recovery-only pass.

- Stop the affected rollout and report a contract/design decision when a step would require trusting unverified bytes, losing a live root reference, counting a partially hydrated voter without a valid durability proof, changing history or weakening ownership/output/deadline checks.
- Safe work on independent packages can continue.
- Hardware that cannot meet the profile should yield an honest measured envelope and revised design, not a concealed timing exclusion.

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

Review future changes to command envelopes, root formats, archive compaction, log retention, WAL policy, storage providers and execution-domain lifecycle against these gates. Source lag and per-small-write full-image work are recovery costs that must remain visible even after the bulk network path becomes faster.