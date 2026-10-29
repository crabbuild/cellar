<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cellar multi-Raft implementation plan

Implement this package against the pinned source baseline or a reviewed successor.
The first release is MR-01 through MR-15. Each work package is a reviewable change
with public contract tests; split its implementation into smaller commits when
needed, while preserving its acceptance boundary.

The runtime files and test targets below belong to their listed work packages.
Task status and linked evidence identify the implemented outputs. The executable
package tool validates the plan and prints ready work; runtime verification
commands become available as their owning work packages are implemented.

## Execution rules

1. Run the package validator and source preflight before starting. Read the
   [technical design](technical-design.md) and relevant current code/callers/tests.
2. Start only when dependencies are DONE. Keep one owner of each shared code
   boundary; independent prerequisites do not authorize concurrent edits.
3. Preserve Fleet mode and add the new isolated namespace behind a default-off
   runtime capability. Never land an intermediate state that acknowledges with
   weaker proof, exposes speculative images, or bypasses node lifecycle ownership.
4. Add named tests with the behavior stated in the manifest. List tests and
   verify the names before running them; an empty target is not evidence.
5. Record evidence and limitations before marking DONE. Do not replace failed
   safety tests with weaker assertions. Update design and task contracts together
   when a boundary changes.
6. Use ordinary checked-in test support. Do not enable external internal-test
   configuration without its sources. Run independent fleet fixtures serially.
7. Finish each task with relevant formatting/layout checks and existing regression
   targets. MR-15 adds the full repository and release qualification matrix.

Useful commands, runnable now:

~~~sh
python3 plans/multi-raft/plan.py validate
python3 plans/multi-raft/plan.py preflight
python3 plans/multi-raft/plan.py ready
python3 plans/multi-raft/plan.py show MR-01
~~~

After implementing a target, capture its actual list and verify it before running
the full target. Example commands for MR-01:

~~~sh
cargo test -p cellar --test native_raft_contract --locked -- --list > /tmp/cellar-raft-contract-tests.txt
python3 plans/multi-raft/plan.py check-list MR-01 /tmp/cellar-raft-contract-tests.txt
cargo test -p cellar --test native_raft_contract --locked -- --test-threads=1
~~~

The temporary listing is diagnostic; copy the final commands/results into durable
task evidence. Do not use a temporary file as the only release record.

## Feature priorities and delivery milestones

Prioritize complete native features in this order. MR-01 through MR-09 provide
leader commands, follower queries and committed S3 archives; they do not yet
provide the recovery and operating lifecycle required for a release. Component
tests qualify their stated boundary. Mark a feature milestone complete only
after its public Node path and failure cases pass together.

| Priority | User-visible milestone | Completion gate | Packages |
| --- | --- | --- | --- |
| 1 | Recover a lagging or replacement replica | Create a checkpoint, transfer the exact cut over authenticated bounded RPCs or restore from S3, replay committed LTX, then resume follower reads. Interrupted transfers retry safely; lost disks use newly authorized member identities; missing quorum refuses normal activation. | MR-10 |
| 2 | Repair redundancy automatically | Detect an unavailable replica, place and seed a learner, verify catch-up, promote it and remove the old member while commands and queries continue. Resume interrupted membership changes after restart. | MR-11 |
| 3 | Run durable business features | Commit alarms, outbox entries and referenced Blob dependencies with the cell state. Only an authorized leader dispatches effects; retries and recovery preserve delivery contracts. | MR-12 |
| 4 | Operate and maintain nodes | Expose role-aware readiness, resource limits and metrics. Drain, restart and shut down through the public Node/CLI paths; require explicit fencing and a new history for disaster restore. | MR-13 |
| 5 | Qualify the initial release | Complete workload batching, benchmarks, checked operation histories, fault/recovery drills and soak tests. Publish the supported topology and measured limits with reproducible evidence. | MR-14, MR-15 |
| 6 | Expand the supported scale | Qualify measured group density and larger cluster tiers, then independent-cluster federation. Millions of cells and millions of physical nodes are separate qualification targets. | MR-16, MR-17 |

Priority 1 is complete at the qualified native recovery boundary: exact-ticket
abort, native checkpoints, existing-replica catch-up, authorized peer/S3 learner
seeding, public joined-node restart, claim reconciliation and process-kill cuts.
Priority 2 is complete at the qualified six-node native boundary. Priority 3 is
complete at the [MR-12 native boundary](evidence/MR-12.json): committed alarms,
receipt-checked outbox delivery and immutable Blob dependency closure. Priority 4
is in progress, beginning with Node-owned shutdown and archive-obligation reports.
Keep each later package's dependency and acceptance gates intact.

### Current execution priorities

The [density observations](evidence/MR-14-density-progress.json) and subsequent
[bounded control pipelining](evidence/MR-14-pipeline-progress.json) narrow the next
work to queue bursts and repeatable latency within the authorized five-node
Colima/RustFS scope. MR-13 and MR-14 remain in progress. This order reflects the
requested MR-14 focus; it does not waive MR-13's release dependency.

1. **Resolve observation stalls and qualify repeatability.**
   Startup now keeps peer routes private until every local group and the archive
   owner are ready. The sender retains bounded eight-frame control pipelining
   and persistence waiters leave the runnable queue until exact sync receipts.
   The [paired 1,500-cell sequence](evidence/MR-14-repeat-progress.json) uses the
   same immutable executable and identical probe schedules. Both healthy
   controls verify all 4,500 replicas and archive coverage: 1,200/1,200 at
   95.426 ms p99 and 1,199/1,200 at 79.254 ms p99, respectively. The intervening
   slow-follower attempt times out during cycle 91's sixth leader probe and is
   unqualified; final replica and archive verification never run. Keep that
   failure, its 32 rejected arrivals and the incomplete fault schedule intact.
   The next reproducer/profiling work must distinguish host/runtime observation
   delay from transport queues or short CPU pressure without raising the 100 ms
   probe cap, retrying failed probes or ignoring cycles. New failed-probe records
   retain the exact cell, budget, elapsed time and exception type before cleanup.
   The [bounded probe timings](evidence/MR-14-probe-progress.json) subsequently
   reproduce the timeout in a healthy control with no node pauses. Cycle 68's
   cell-25 request has no server-handler record; all other 413 attempted probes
   correlate, and the final source-stable sample still ends at cell 20. This
   narrows the next work to HTTP ingress and executor responsiveness under
   density. [Client HTTP timing](evidence/MR-14-client-progress.json) now records
   bounded connect/send/response phases without retries or changed deadlines;
   the [fresh healthy attempt](evidence/MR-14-client-run-progress.json) completes
   connect/send in 0.127 ms, then times out waiting for headers. Its matching
   server record spends 111.883 ms in the status stage. Create a bounded
   executor/status-admission regression before changing scheduling. Diagnose the
   later sampler socket refusal (`EADDRNOTAVAIL`) separately; neither observation
   establishes the exact host/runtime cause. The separate slow-follower run completes
   116 cycles and verifies 4,500 replicas/archive coverage but still misses the
   declared traffic targets (304.107 ms p99, 24 rejected arrivals). Do not call
   the new diagnostic instrumentation a performance fix or qualify the failed
   healthy control. Matched-contract and sustained S3 work follows this gate.
   The controls narrow the investigation but do not prove a cause. These runs
   use thirty active cells and ten offered operations per second on one shared
   Mac; sustained capacity, overhead and cross-revision speedup remain unproven.
   Earlier startup and traffic misses remain at their original source pins.
2. **Finish MR-14's performance acceptance.** Establish repeatable healthy and
   slow-follower latency before claiming a supported capacity. Complete sustained
   S3 acknowledgement and equivalent-contract Fleet/bucket fault comparisons.
   Shared RustFS pause scheduling and independent acknowledgement-window checks
   now execute through both runners. The eight-run local healthy/outage matrix
   reconciles across Quorum, S3, Fleet and bucket policies. Retain the S3, Fleet
   and bucket outage traffic misses, investigate Fleet's delayed owner queries,
   and qualify fault histories through ambiguous outcomes. The
   [recovery drills](evidence/MR-14-recovery-progress.json) verify four original
   command identities in one Raft S3 run and one bucket run. The bucket repeat
   self-fences on expired ownership and remains unqualified. The separate
   [all-owner restart drills](evidence/MR-14-restart-progress.json) now preserve
   acknowledged state before retry, recover eight original identities and commit
   twelve new writes in each of two runs. Mixed live/fenced owners and cold or
   disaster restore remain separate gates; preserve the original lease and
   deadline bounds. Recovery never improves the original performance score. Define any common
   service contract explicitly before permitting a cross-adapter ratio;
   current guarantees still differ.
   Account for observation overhead and device I/O. Keep Q1's original workload
   and the 4,096-group bound; the six-process gate exceeds the current local scope.
3. **Complete MR-13's remaining operator features.** Qualify receipt maintenance
   through response loss, recurring effects and leadership changes; finish
   authenticated discovery/management; deliver platform-enforced old-deployment
   fencing and exact archive restore into a distinct history. Close the retained
   intermittent findings and run the whole operations matrix.
4. **Qualify the release (MR-15).** Complete concurrent-history checking,
   crash/partition/store failure and compatible-binary rollback drills, then the
   24-hour soak. Publish only the topology, durability and failure envelope that
   actually passes. Real S3 and independent failure domains need separate evidence.
5. **Expand scale and federation (MR-16/MR-17).** After release gates pass, qualify
   density improvements and physical fleet tiers, then home-cluster discovery,
   source-fenced migration and cross-cluster failure behavior. Local containers
   do not establish 30/100/1,000-node or million-node support.

The bounded control-path change, physical comparison and native persistence-wait
improvement are delivered. The
[shared object-store outage runner](evidence/MR-14-store-fault-progress.json) is implemented;
[its physical matrix](evidence/MR-14-store-physical-progress.json) verifies bounded
pause behavior and final state across all four policies. Equivalent-contract
comparison and broader fault qualification remain open. The subsequent
[ambiguous-command recovery evidence](evidence/MR-14-recovery-progress.json)
retains both successful receipt checks and rejected attempts, including the
bucket owner's expired-lease fence. The subsequent all-owner restart evidence
closes that explicitly bounded harness scenario, with unavailable old-process
counters and performance comparison refused. Shared-host observations
do not establish a controlled latency comparison. The physical
queue/latency gate still needs causal evidence and the unchanged workload
comparison. All misses remain in the evidence; one passing short run does not
qualify a supported capacity.
No group-limit increase, automatic seed retry or relaxed SLO substitutes for it.

## Package dependency order

| Task | Result | Dependencies | Release |
| --- | --- | --- | --- |
| MR-01 | Define identities, invariants and operation contracts | None | v1 |
| MR-02 | Persist Raft entries and metadata in the shared log engine | MR-01 | v1 |
| MR-03 | Drive multiple RawNode instances with ordered Ready handling | MR-02 | v1 |
| MR-04 | Add authenticated bounded peer transport | MR-03 | v1 |
| MR-05 | Implement verified LTX apply and immutable generations | MR-01 | v1 |
| MR-06 | Stage commands privately and bind results to committed LTX | MR-03, MR-04, MR-05 | v1 |
| MR-07 | Integrate Raft authority and command registration with the host | MR-04, MR-06 | v1 |
| MR-08 | Serve explicit follower query consistency modes | MR-07 | v1 |
| MR-09 | Archive committed history and enforce durability modes | MR-07 | v1 |
| MR-10 | Install snapshots and recover durable group state | MR-09, MR-05, MR-03, MR-08 | v1 |
| MR-11 | Place replicas and repair membership automatically | MR-10, MR-04, MR-07 | v1 |
| MR-12 | Integrate committed alarms, outbox and capability gates | MR-08, MR-09, MR-11 | v1 |
| MR-13 | Bound resources and complete node lifecycle operations | MR-11, MR-12 | v1 |
| MR-14 | Batch multi-group work and deliver the workload harness | MR-13 | v1 |
| MR-15 | Qualify failure histories and release the isolated mode | MR-14 | v1 |
| MR-16 | Qualify density improvements and larger cluster tiers | MR-15 | scale |
| MR-17 | Design and qualify federation between independent clusters | MR-16 | federation |

MR-05 is independent of the log/transport chain after MR-01. This permits flexible
ordering; it does not require parallel agents or overlapping edits.

## Work packages

### MR-01 Define identities, invariants and operation contracts

Dependencies: none. Phase: v1. Status: DONE. Evidence: [MR-01](evidence/MR-01.json).

Modify: `crates/cellar-logic/types.rs`, `crates/cellar-logic/lib.rs`, `crates/cellar/application/mod.rs`.

Create: `crates/cellar-logic/consensus.rs`, `crates/cellar/application/consensus/mod.rs`, `crates/cellar/application/consensus/protocol.rs`, `crates/cellar/tests/support/raft_model.rs`, `crates/cellar/tests/native_raft_contract.rs`.

1. Implement pure identity, authority, durability and version types, with explicit Fleet/Raft discrimination.
2. Define marker-1 envelope, maximum sizes, canonical digest inputs, token scope and error taxonomy from the design.
3. Add a checked-in deterministic protocol-boundary model and fake clock/disk/transport support usable without cellar_internal_tests.
4. Model speculative execution, commit, publication, archive and stale-completion events; require the core to reject illegal transitions.

Acceptance: All I01-I16 have a defined responsible boundary and observable failure outcome. No raft, async or I/O dependency is added to cellar-logic.

Stop conditions: Stop if authority cannot be expressed without reusing owner epoch as Raft term. Do not implement transport or claim consensus from the boundary model.

| Required test | Observable result |
| --- | --- |
| `mode_isolation` | A Fleet proof cannot settle a Raft operation and vice versa |
| `incarnation_token_scope` | Equal indices in different histories do not satisfy a token |
| `speculative_visibility_model` | Only committed and published state can be observed |
| `late_completion_model` | Old term/process/operation completion cannot release a new request |
| `envelope_bounds` | Duplicate fields, trailing bytes, invalid lengths and decoded-size excess are refused |
| `birth_retry_identity` | Ambiguous birth/bootstrap claims converge on one identity; missing stable state cannot reset a claimed voter |

Verification target: `native_raft_contract`. Run the exact list/test commands
shown by `plan.py show MR-01`; the manifest is authoritative for arguments.

### MR-02 Persist Raft entries and metadata in the shared log engine

Dependencies: MR-01. Phase: v1. Status: DONE. Evidence: [MR-02](evidence/MR-02.json).

Modify: `Cargo.toml`, `Cargo.lock`, `crates/cellar/Cargo.toml`, `crates/cellar/lib.rs`, `crates/cellar-ltx/src/host.rs`, `crates/cellar-ltx/src/lib.rs`, `tools/xtask/src/main.rs`, `crates/cellar/application/consensus/mod.rs`, `Dockerfile`, `NOTICE`, `tools/perf/Dockerfile`, `tools/perf/fleetcare.Dockerfile`, `tools/perf/fleetcare-console.Dockerfile`, `tools/perf/fleetcare.py`, `tools/perf/faults.py`, `tools/perf/test_fleetcare.py`, `tools/perf/test_build_freshness.py`, `crates/cellar/application/consensus/protocol.rs`.

Create: `crates/cellar/durability/raft/mod.rs`, `crates/cellar/durability/raft/storage.rs`, `crates/cellar/durability/raft/options.rs`, `crates/cellar/tests/native_raft_storage.rs`, `crates/cellar/durability/raft/filesystem.rs`, `vendor/raft-engine/Cargo.toml`, `vendor/raft-engine/UPSTREAM.json`, `vendor/raft-proto/Cargo.toml`, `vendor/raft-proto/build.rs`, `vendor/raft-proto/UPSTREAM.json`, `crates/cellar/durability/raft/cache.rs`, `crates/cellar/durability/raft/records.rs`, `crates/cellar/tests/support/raft_storage_fault.rs`, `vendor/.gitattributes`, `vendor/raft/Cargo.toml`, `vendor/raft/UPSTREAM.json`, `vendor/raft/src/raft.rs`, `vendor/raft/src/raw_node.rs`.

1. Pin raft 0.7.0, raft-engine 0.4.2 and required codec/logging dependencies centrally; verify the locked build and license notices.
2. Implement Storage over one node-wide engine with durable numeric-key allocation and group/incarnation mapping.
3. Persist suffix truncation, entries, HardState, ConfState and snapshot boundaries with explicit sync completions and strict corruption handling.
4. Adapt engine filesystem/runtime work through host services, including the narrowly attributed dependency patch in storage-integration.md; expose fault hooks for torn append, lost sync, directory loss and interrupted purge.

Acceptance: The actual pinned crates compile with the repository toolchain and injected lifecycle. Entries and metadata survive process kill/reopen at every persistence cut.

Stop conditions: Stop on incompatible protobuf types, unsupported sync/directory semantics or unowned background tasks. Never substitute MemStorage or a weaker recovery mode to pass tests.

| Required test | Observable result |
| --- | --- |
| `vote_survives_restart` | A restarted voter cannot grant conflicting votes in one term |
| `append_sync_boundary` | No durable completion is emitted before injected sync succeeds |
| `conflicting_suffix_replaced` | Only the uncommitted suffix is replaced; boundary terms remain queryable |
| `engine_group_isolation` | Reused numeric-key allocation cannot cross group/incarnation data |
| `disk_loss_identity` | Missing stable state refuses old voter resume |
| `corrupt_storage_refused` | Corruption is surfaced and closes proof admission |

Verification target: `native_raft_storage`. Run the exact list/test commands
shown by `plan.py show MR-02`; the manifest is authoritative for arguments.

### MR-03 Drive multiple RawNode instances with ordered Ready handling

Dependencies: MR-02. Phase: v1. Status: DONE. Evidence: [MR-03](evidence/MR-03.json).

Modify: `crates/cellar/durability/raft/mod.rs`, `crates/cellar/durability/raft/storage.rs`, `crates/cellar/durability/raft/options.rs`, `crates/cellar/application/consensus/mod.rs`, `vendor/raft/UPSTREAM.json`, `NOTICE`, `vendor/raft/src/raft.rs`, `vendor/raft/src/raw_node.rs`, `crates/cellar/durability/raft/cache.rs`.

Create: `crates/cellar/durability/raft/runtime.rs`, `crates/cellar/durability/raft/completion.rs`, `crates/cellar/tests/native_raft_runtime.rs`, `crates/cellar/durability/raft/replica.rs`, `crates/cellar/durability/raft/budget.rs`, `crates/cellar/tests/support/raft_runtime_fixture.rs`, `crates/cellar/durability/raft/README.md`.

1. Own each RawNode on one serialized worker; use shared bounded scheduling across groups.
2. Implement conservative one-outstanding-Ready processing, persistence batching across groups and LightReady handling.
3. Persist commit advancement before publishing applied progress; apply configuration/no-op entries and advance_apply_to in order.
4. Emit identity-bound observations to the actor and keep slow storage/apply for one group from blocking the entire service.

Acceptance: At least three independent groups elect different leaders and commit concurrently in an in-process deterministic harness. No blocking I/O occurs in actor/consensus loops.

Stop conditions: Stop if a callback lacks enough identity to reject obsolete work. Do not add per-group threads to make the test pass.

| Required test | Observable result |
| --- | --- |
| `ready_persistence_order` | Dependent messages follow successful persistence and Ready lifecycle rules |
| `light_ready_commit_saved` | A commit advance delivered by LightReady survives restart |
| `apply_progress_order` | Completion out of order never skips an unapplied entry |
| `stalled_group_isolation` | Unrelated groups continue during one delayed disk/apply operation |
| `noop_configuration_progress` | Entries without LTX advance protocol state without inventing data changes |
| `current_term_admission_barrier` | New leader admission waits for current-term commit and apply |

Verification target: `native_raft_runtime`. Run the exact list/test commands
shown by `plan.py show MR-03`; the manifest is authoritative for arguments.

### MR-04 Add authenticated bounded peer transport

Dependencies: MR-03. Phase: v1. Status: DONE. Evidence: [MR-04](evidence/MR-04.json).

Modify: `crates/cellar/fleet/peer/auth.rs`, `crates/cellar/node/embedded.rs`, `crates/cellar/node/mod.rs`, `crates/cellar/sdk.rs`, `crates/cellar/tests/native_node.rs`, `crates/cellar/durability/raft/mod.rs`, `crates/cellar/durability/raft/runtime.rs`, `crates/cellar/durability/raft/budget.rs`, `crates/cellar/application/consensus/mod.rs`, `crates/cellar/durability/raft/README.md`.

Create: `crates/cellar/durability/raft/transport.rs`, `crates/cellar/durability/raft/transport_frame.rs`, `crates/cellar/durability/raft/transport_options.rs`, `crates/cellar/durability/raft/transport_frame_tests.rs`, `crates/cellar/durability/raft/transport_tests.rs`, `crates/cellar/tests/support/raft_network.rs`, `crates/cellar/tests/native_raft_transport.rs`.

1. Define separate Raft peer routes/frames with source/target process, group/incarnation, protocol digest and bounded lengths.
2. Authenticate responses as well as requests, use protected transport and verify membership before dispatch.
3. Pool connections and multiplex groups; reserve control-message capacity and report unreachable/snapshot failures to raft-rs.
4. Implement duplicate/reordering/one-way partition and delayed-response fault injection in real process fixtures.

Acceptance: Three real node processes communicate over the production transport. Per-peer byte budgets and cancellation release are observable.

Stop conditions: Stop if only request signatures are checked. A transport timeout must not be translated into an append acknowledgement.

| Required test | Observable result |
| --- | --- |
| `peer_response_authenticated` | Forged or wrong-target responses cannot create durable proof |
| `unknown_group_no_allocation` | Untrusted unknown-group traffic cannot initialize state |
| `stale_incarnation_message` | Delayed old-history messages cannot alter a new group |
| `one_way_partition` | Majority progresses without minority commits |
| `control_traffic_reserved` | Saturated data/snapshot queues do not starve control traffic |
| `transport_byte_limits` | Encoded and decoded limits reject input before unbounded allocation |

Verification target: `native_raft_transport`. Run the exact list/test commands
shown by `plan.py show MR-04`; the manifest is authoritative for arguments.

### MR-05 Implement verified LTX apply and immutable generations

Dependencies: MR-01. Phase: v1. Status: DONE. Evidence: [MR-05](evidence/MR-05.json).

Modify: `crates/cellar-ltx/src/lib.rs`, `crates/cellar-ltx/src/host.rs`, `crates/cellar-ltx/src/ltx.rs`, `crates/cellar/storage/sqlite/mod.rs`, `crates/cellar-ltx/Cargo.toml`, `Cargo.lock`, `crates/cellar-ltx/README.md`.

Create: `crates/cellar-ltx/src/apply.rs`, `crates/cellar-ltx/src/generation.rs`, `crates/cellar/tests/native_raft_ltx.rs`, `crates/cellar-ltx/src/bounded_ltx.rs`, `crates/cellar-ltx/src/generation_vfs.rs`, `crates/cellar/storage/sqlite/immutable.rs`, `crates/cellar/tests/support/raft_ltx_fixture.rs`, `crates/cellar/tests/support/raft_ltx_fault.rs`.

1. Add bounded physical LTX application against a verified base with mandatory envelope image digests, optional LTX database checksums and full file/chain validation.
2. Implement unique generation creation, copy/reflink fallback, page growth/truncation and durable directory publication through FileSystem.
3. Add a query opener that never initializes framework schema or mutates an immutable generation.
4. Make generation pinning and bounded reclamation explicit; exercise corruption and crash cuts through real filesystem tests.

Acceptance: Public library mechanics are tested through ordinary Cellar integration targets. No published image is modified in place; unsupported filesystem operations fail explicitly.

Stop conditions: Stop if exact base/result image digests or LTX file integrity cannot be verified. Do not use a live SQLite copy or immutable=1 over a file that is still being patched.

| Required test | Observable result |
| --- | --- |
| `ltx_exact_base` | Wrong mandatory image digest or TXID gap refuses apply, including NoChecksum LTX |
| `ltx_growth_truncation` | Database contents and page count match the expected resulting image |
| `ltx_checksum_corruption` | Header/page/trailer or envelope image corruption cannot publish a generation |
| `immutable_reader_pinning` | A long query sees one unchanged image during later publication |
| `image_sync_before_marker` | Failed file or directory sync cannot make the image publishable |
| `bounded_image_reclamation` | Pinned generations survive while disk budgets enforce refusal |

Verification target: `native_raft_ltx`. Run the exact list/test commands
shown by `plan.py show MR-05`; the manifest is authoritative for arguments.

### MR-06 Stage commands privately and bind results to committed LTX

Dependencies: MR-03, MR-04, MR-05. Phase: v1. Status: DONE. Evidence: [MR-06](evidence/MR-06.json).

Modify: `crates/cellar/storage/sqlite/mod.rs`, `crates/cellar/durability/raft/mod.rs`, `crates/cellar/durability/raft/cache.rs`, `crates/cellar/application/consensus/mod.rs`, `crates/cellar/tests/native_raft_runtime.rs`, `crates/cellar/tests/support/raft_runtime_fixture.rs`, `crates/cellar/tests/support/raft_network.rs`, `crates/cellar/durability/raft/README.md`, `crates/cellar-ltx/README.md`.

Create: `crates/cellar/durability/raft/execution/mod.rs`, `crates/cellar/durability/raft/apply.rs`, `crates/cellar/tests/native_raft_execution.rs`, `crates/cellar/durability/raft/working.rs`, `crates/cellar/durability/raft/receipts.rs`, `crates/cellar/storage/sqlite/command.rs`.

[MR-06 evidence](evidence/MR-06.json) covers private command/capture production,
replicated receipt/expiry semantics and durable image publication. The earlier
[partial apply record](evidence/MR-06-apply-progress.json) is retained as history.
SDK ingress, live authority revalidation, cancellation/eviction ownership and
output-gate integration remain MR-07 deliverables.

The command adapter uses the existing public `Db` capture API and a separate
owning SQL adapter. Fleet upload and activation transaction paths are unchanged;
no Fleet output ticket is converted into a Raft or LTX version.

The apply marker now carries page geometry explicitly. MR-04 is also a prerequisite
because its transport fixture consumes this shared marker contract.

1. Add private execution images, one command transaction and one pending data proposal per cell.
2. Persist request digest, bounded result and per-client sequence/expiry floors in the business transaction; capture all intervening LTX/control writes and the frozen result image digest.
3. Publish committed image then sync the engine apply marker before returning a covered completion.
4. Rebuild private state after rejected/uncertain proposals and leadership changes; retain uncommitted log entries for normal Raft reconciliation.

Acceptance: Leader kill at every command step preserves the client receipt contract. No numeric cast maps Fleet output tickets into Raft/LTX versions.

Stop conditions: Stop if a second command can execute while its base proposal is uncertain. Do not expose the leader's writable image to query code.

| Required test | Observable result |
| --- | --- |
| `private_commit_not_visible` | A crash before proposal leaves no public read/effect |
| `lost_proposal_rebuild` | New commands never build on an abandoned private delta |
| `duplicate_command_receipt` | A retry after lost reply changes business state once |
| `conflicting_command_digest` | Reused command identity with different payload is refused |
| `apply_marker_crash_cut` | Restart chooses an image and index that were durably published together |
| `oversized_delta_atomic_refusal` | An oversized LTX result never leaks a partial mutation |
| `receipt_expiry_floor` | Gaps are refused and an expired client sequence cannot execute as a new request |

Verification target: `native_raft_execution`. Run the exact list/test commands
shown by `plan.py show MR-06`; the manifest is authoritative for arguments.

### MR-07 Integrate Raft authority and command registration with the host

Dependencies: MR-04, MR-06. Phase: v1. Status: DONE.

Modify: `crates/cellar-logic/cell.rs`, `crates/cellar-logic/output_gate.rs`, `crates/cellar-logic/routing.rs`, `crates/cellar-logic/types.rs`, `crates/cellar/actor/effects.rs`, `crates/cellar/actor/production.rs`, `crates/cellar/actor/output_proofs.rs`, `crates/cellar/node/embedded.rs`, `crates/cellar/fleet/ownership.rs`, `crates/cellar/application/mod.rs`, `crates/cellar/application/cells.rs`, `crates/cellar/sdk.rs`, `crates/cellar/Cargo.toml`, `crates/cellar/application/consensus/mod.rs`, `crates/cellar/durability/raft/runtime.rs`, `crates/cellar/durability/raft/mod.rs`, `crates/cellar/durability/raft/README.md`, `crates/cellar/application/consensus/protocol.rs`, `crates/cellar/node/mod.rs`, `crates/cellar-logic/lib.rs`, `crates/cellar/durability/raft/apply.rs`, `crates/cellar/durability/raft/execution/mod.rs`, `crates/cellar/durability/raft/completion.rs`, `crates/cellar/storage/sqlite/immutable.rs`, `crates/cellar/tests/native_raft_execution.rs`, `crates/cellar/application/README.md`, `crates/cellar-axum/application.rs`, `crates/cellar-axum/builder.rs`, `crates/cellar-axum/tests/builder.rs`, `crates/cellar-axum/README.md`.

Create: `crates/cellar/durability/raft/catalog.rs`, `examples/raft-counter/main.rs`, `crates/cellar/tests/native_raft_host.rs`, `crates/cellar/durability/raft/catalog_wire.rs`, `crates/cellar/tests/support/raft_catalog_fault.rs`, `crates/cellar-logic/raft_command.rs`, `crates/cellar-logic/tests/raft_command.rs`, `crates/cellar/application/consensus/class.rs`, `crates/cellar/application/consensus/client.rs`, `crates/cellar/durability/raft/host.rs`, `crates/cellar/durability/raft/host_sql/mod.rs`, `crates/cellar/tests/support/raft_node.rs`, `examples/raft-counter/counter.rs`, `examples/raft-counter/README.md`, `examples/raft-counter/smoke.py`, `crates/cellar/tests/support/raft_cluster.rs`, `plans/multi-raft/host-audit.md`.

[MR-07 evidence](evidence/MR-07.json) records all six planned host gates and 47
owning cases: 20 host, 16 command/apply and 11 pure lifecycle cases. The
[host audit](host-audit.md) covers mode separation, exact receipt gates, joined
ownership and both submitted-partition histories. Required command contracts
are bound into the application identity; query contracts remain independently
checked. The compiling counter and process drill cover native/Axum ingress,
late startup failure, disk reuse, retained receipts and subsequent writes.
Earlier catalog, registration and serving progress records remain linked from
the task manifest as historical evidence. Strong query consistency and S3
archive/restore remain MR-08/MR-09 rather than claims of this completed boundary.

1. Implement immutable namespace mode, conditional group birth, durable one-time member bootstrap claims and actor-owned lifecycle registration.
2. Route Raft commands through restricted class definitions; reject existing mutable factory registration in that mode.
3. Settle core output gates from exact committed/applied receipts and complete authority identity; keep Fleet proofs separate.
4. Ship a compiling counter example through native and Axum ingress, adding the Cargo target explicitly.

Acceptance: Normal host command admission works end to end through the public SDK. Existing native_storage, native_replication and native_log_protocol targets still pass.

Stop conditions: Stop if the old owner CAS and Raft can independently grant mutation authority. Do not route background writes through Fleet methods as a temporary shortcut.

| Required test | Observable result |
| --- | --- |
| `raft_gate_exact_receipt` | Only a matching committed applied receipt releases success |
| `fleet_gate_unchanged` | Fleet bucket/follower proof behavior remains unchanged with feature off |
| `namespace_mode_conflict` | Conflicting root mode or group birth cannot admit a second writer |
| `retained_handle_fenced` | A handle from an old activation cannot modify private state |
| `native_axum_command_parity` | Both ingress adapters enforce identical registration and receipt behavior |
| `unsupported_factory_refused` | Normal mutable Cell factories cannot enter the Raft namespace |

Verification target: `native_raft_host`. Run the exact list/test commands
shown by `plan.py show MR-07`; the manifest is authoritative for arguments.

### MR-08 Serve explicit follower query consistency modes

Dependencies: MR-07. Phase: v1. Status: DONE.

Modify: `crates/cellar/application/consensus/mod.rs`, `crates/cellar/application/cells.rs`, `crates/cellar/sdk.rs`, `crates/cellar-logic/routing.rs`, `crates/cellar/application/consensus/client.rs`, `crates/cellar/durability/raft/mod.rs`, `crates/cellar/durability/raft/apply.rs`, `crates/cellar/durability/raft/catalog.rs`, `crates/cellar/durability/raft/catalog_wire.rs`, `crates/cellar/durability/raft/completion.rs`, `crates/cellar/durability/raft/execution/mod.rs`, `crates/cellar/durability/raft/host.rs`, `crates/cellar/durability/raft/host_sql/mod.rs`, `crates/cellar/durability/raft/replica.rs`, `crates/cellar/node/embedded.rs`, `crates/cellar/tests/native_raft_host.rs`, `crates/cellar/tests/support/raft_cluster.rs`, `crates/cellar/tests/support/raft_node.rs`, `examples/raft-counter/main.rs`, `crates/cellar/durability/raft/README.md`, `examples/raft-counter/README.md`, `crates/cellar/durability/raft/transport.rs`, `crates/cellar/durability/raft/transport_frame.rs`, `crates/cellar/durability/raft/transport_options.rs`, `crates/cellar/durability/raft/transport_tests.rs`, `crates/cellar/node/mod.rs`, `crates/cellar/tests/native_node.rs`, `crates/cellar/tests/native_raft_transport.rs`, `crates/cellar/tests/support/raft_network.rs`, `crates/cellar/application/consensus/class.rs`, `examples/raft-counter/counter.rs`, `examples/raft-counter/smoke.py`, `crates/cellar-logic/consensus.rs`.

Create: `crates/cellar/durability/raft/query.rs`, `crates/cellar/durability/raft/version.rs`, `crates/cellar/tests/native_raft_reads.rs`, `crates/cellar/durability/raft/query_route.rs`, `crates/cellar/durability/raft/query_wire.rs`, `crates/cellar/durability/raft/query_wire_tests.rs`, `crates/cellar/durability/raft/query_transport.rs`, `plans/multi-raft/read-audit.md`, `crates/cellar/tests/support/raft_read_context.rs`, `crates/cellar/durability/raft/query_identity.rs`.

The [final read evidence](evidence/MR-08.json) pins source, executable digests,
exact commands and raw outputs. All seven required cases and the
[requirement-to-boundary review](read-audit.md) are covered. Qualification includes
89 component owning cases, 88 Node cases, 14 Raft unit cases, native/Axum HTTP and
the external restart drill. Query-only ingress verifies existing metadata and
creates no member/image; namespace grants retain independent live revocation.
Controller-owned durable topology/grant distribution and large directory
lifecycles remain their original later packages; this task does not close the
17-package goal.

1. Implement read-only query registration, snapshot pinning and served-version propagation.
2. Implement Stale, AtLeast and safe ReadIndex barriers with unique request correlation and original deadlines.
3. Authenticate scope-bound version tokens, rotate verification keys and refuse cross-incarnation comparisons.
4. Add bounded replica selection and explicit same-consistency leader fallback.

Acceptance: Run history assertions through follower reads while leaders change. Read result metadata names the pinned image rather than a later mutable watermark.

Stop conditions: Stop if a query shares the private leader execution connection. Do not implement lease-based reads in this task.

| Required test | Observable result |
| --- | --- |
| `stale_is_committed_prefix` | Stale results correspond to a coherent committed generation |
| `read_your_writes_follower` | AtLeast waits for the actual queryable image covering the token |
| `linearizable_read_barrier` | A completed write preceding a query invocation is visible |
| `minority_strong_read_refused` | Isolated replicas cannot turn cached progress into a fresh strong read |
| `query_write_capability_denied` | SQL/PRAGMA/ATTACH and ordinary context paths cannot mutate state |
| `query_token_auth_scope` | Forged/cross-cell/old-incarnation tokens fail validation |
| `fallback_preserves_contract` | Timeout/fallback never downgrades consistency or extends deadline |

Verification target: `native_raft_reads`. Run the exact list/test commands
shown by `plan.py show MR-08`; the manifest is authoritative for arguments.

### MR-09 Archive committed history and enforce durability modes

Dependencies: MR-07. Phase: v1. Status: DONE.

The archive storage component now captures exact durable engine/image cuts,
pins their immutable generations, and publishes bounded snapshot/tail/dependency
sets through conditional heads. Each proposed head is downloaded, checked and
physically replayed in a separate generation store before publication. Membership
changes establish a new full snapshot with the configuration from that exact cut.
The node-owned dispatcher now settles S3 output from these proofs, preserves
minimum durability and stronger retries, and bounds backlog admission and image
retention. Atomic Raft snapshot installation and
authorized member recovery remain MR-10. See the
[archive boundary audit](archive-audit.md) and
[storage integration notes](storage-integration.md#committed-archive-storage-component).
The [completed evidence](evidence/MR-09.json) pins source
`2298fd442f0b867695b564527a66b61684aff2c2`: all seven named requirements and
26 archive cases pass, together with 225 other Rust cases, Axum feature runs,
the executable restart drill and the repository checks. The earlier
[storage progress record](evidence/MR-09-storage-progress.json) remains historical;
it does not supply the DONE gate. This completes MR-09, not the 17-package goal.

Modify: `crates/cellar/durability/raft/apply.rs`, `crates/cellar/durability/raft/catalog.rs`, `crates/cellar/storage/object/bucket.rs`.

Create: `crates/cellar/durability/raft/archive/mod.rs`, `crates/cellar/durability/raft/manifest.rs`, `crates/cellar/tests/native_raft_archive.rs`.

1. Build immutable committed-prefix archive bundles and manifests including entry metadata and external object dependencies.
2. Advance archive heads only after referenced objects are durable; reconcile ambiguous conditional writes.
3. Implement Quorum versus S3 output gates, retry strengthening and contiguous archive coverage.
4. Pin sufficient log/image recovery material during outages; enforce backlog and disk admission limits.

Acceptance: A new isolated restore from S3 reconstructs every S3-acknowledged command. No archive operation becomes a second consensus authority.

Stop conditions: Stop if object existence is credited before complete manifest/dependency verification. Do not promise all-disk-loss survival for unarchived Quorum writes.

| Required test | Observable result |
| --- | --- |
| `archive_never_speculative` | Persisted uncommitted LTX cannot enter a recoverable manifest |
| `archive_head_monotonic` | Delayed worker/CAS retries cannot regress or fork archive history |
| `s3_ack_complete_restore_set` | S3 success covers snapshot/tail and referenced immutable objects |
| `quorum_s3_outage` | Quorum writes continue only within retained-disk budgets |
| `s3_timeout_ambiguous` | A committed command with failed archive wait is never called rolled back |
| `retention_preserves_only_copy` | Disk pressure cannot delete the only acknowledged unarchived recovery path |
| `retry_strengthens_durability` | A stored Quorum result waits before answering a stronger S3 retry |

Verification target: `native_raft_archive`. Run the exact list/test commands
shown by `plan.py show MR-09`; the manifest is authoritative for arguments.

### MR-10 Install snapshots and recover durable group state

Dependencies: MR-09, MR-05, MR-03, MR-08. Phase: v1. Status: DONE.

The [MR-10 completion evidence](evidence/MR-10.json) pins source
`879d13e2608e94cf3ef5c92d4f3e05029f2a678d`: 317 Rust cases across 14 binaries and all 22 commands pass.
Runtime tests pin `1cd373cf7bc994e8ae1890f3934d977ea8b7c7af`; the final source
adds only three shared-test-helper lint annotations. The exact delta, initial
failed lint output and per-command source revisions are retained; production
code is unchanged. Clippy and remaining repository checks pass at the final
source. This includes 42 snapshot cases, 29 archive, all 88 Node cases, all 16 domain
cases, both executor lifecycle cases, six protocol contract and 22 library cases,
plus the owning Raft regressions. Clippy, both cellar-ltx backend modes, format
(640 Rust files), layout, dependency policy, docs, 17 planning tests, all 32 required
snapshot names and the reviewed 71-anchor preflight pass. Eight ignored child
entry points run through owning parents and are excluded from pass totals.

Native peer and exact-reference S3 learner seeding require a fresh safe ReadIndex.
Images seal before atomic group/configuration/binding/applied initialization and
permanent-claim reconciliation. S3 restores cell images and committed LTX tails;
it restores no old votes, process identity, leadership or quorum. Missing selected
payloads refuse initialization even when a newer complete head exists. A busy
private writer does not block S3 metadata selection or expose speculative state.

Public joined-node restart preserves verified state and vote zero, serves retained
causal reads before voters start, and refuses strong reads until quorum returns.
Three claim faults reconcile exact synced state without a blank reset. Sixteen
fresh-learner process kills cover eight cuts each for peer and S3 seeding: image
fsync, rename, directory fsync, engine sync before/after, engine reply, permanent
claim and public attachment. Seven existing-replica snapshot cuts remain covered.
Every fresh-seed restart verifies all-or-absent group state and claim ordering,
then resumes follower reads and subsequent replication without callback replay.

MR-10 is complete at the native recovery boundary. MR-11 through MR-17 and the
full delivery goal remain open. That MR-10 source uses protocol identity
`cellar-raft-ltx-1/members-1/seeds-1/s3-1`, with numeric markers 1; use a fresh
experimental namespace/prefix and local state. Ordinary stores are in-memory;
process-kill fixtures checkpoint successful conditional mutations and rehydrate
them after child exit, then copy exact node file bytes into exclusively owned
restart directories. These checks establish process-crash recovery, without
physical S3, power-loss, zones, production capacity, larger-scale, federation or
complete release qualification.

The physical snapshot component captures exact committed cuts, binds canonical
history/schema/configuration metadata to full LTX bytes, assembles bounded ordered
chunks and seals destination generations before an exact runtime completion.
Runtime checkpoints use a dedicated batched persistence phase and monotonic
prospective/durable image references, including uncertain I/O and joined close.
Empty/configuration-only snapshots carry no image and can precede the first LTX
command. `recovery.rs` validates coherent engine-selected references before GC.

The [native recovery evidence](evidence/MR-10-native-progress.json) pins source
`08b2bca3da32982795994d0240844324437ff0c7`: all 304 Rust cases and 21 commands pass,
including 26 snapshot, 18 read and all 88 public Node cases, owning/integration
regressions and both cellar-ltx backend feature modes. Exact-ticket abort joins
older physical apply; native transfers retry and close through owned lifecycles.
A dropped ReadIndex reply retries within its original invocation and still needs
quorum proof. The [snapshot audit](snapshot-audit.md) records the implemented path
and the complete current native recovery qualification. Authorized peer/S3
seeding, public restart and fresh-seed process kills are covered above.
That historical source did not complete MR-10; the current evidence above does.

The native host now captures periodic checkpoints and pins the exact requested
source before sending snapshot metadata. Member-only pulls use separate bounded
TLS connections, ordered authenticated chunks and fixed deadlines. The SQL owner
seals and publishes only after the exact durable install. Older physical apply
forces abort/join before retry; other groups retain command and query progress.
Native drills cover catch-up, partial-transfer retry and joined shutdown. Snapshot
admission is checked before catalog objects or member storage are created.

The [quorum-loss follow-up](evidence/MR-10-quorum-progress.json) pins
`ad5818a1ba4d68f0cb1ca36890e34f819eadd5a1`: all 27 snapshot cases and 12 commands pass,
including all 17 required inventory names. Only the snapshot integration test
changed since the separate 304-case qualification. A three-voter group archives
an acknowledged command, then loses two consensus owners. Available archive bytes
cannot establish a command acknowledgement or strong-read proof. Physical restore
into three blank replacement directories still refuses an old claimed identity,
a different disk and an unassigned node; every failed attempt leaves no local
group/voting state. Explicit disconnected historical reads remain coherent.


The [member-binding progress](evidence/MR-10-bindings-progress.json) pins source
`88b05883acaba7881fcf30fc0d06e0dc2474725c`: 198 Rust cases and all 21 commands pass,
including 30 snapshot, 27 archive, 18 read, six protocol contract and 22 library
cases, owning Raft regressions and the affected public Node Raft lifecycle.
Clippy, both cellar-ltx backend feature modes, format (633 Rust files), layout,
dependency policy, docs, 17 plan tests and 68-anchor preflight pass. The snapshot
inventory includes all 20 required cases. Joined node/disk bindings and retired-ID
high-water now survive journal sync, snapshot transfer, compaction and restart;
archives verify their immutable birth IDs. Stale/unbound/rebound proposals, direct
fresh voters, empty-learner promotion and mixed member/snapshot markers are refused.

That earlier cut changed the internal namespace protocol identity to
`cellar-raft-ltx-1/members-1` while keeping format markers at 1. Earlier namespace
roots are refused; use a fresh experimental namespace/prefix and local Raft state.
There is no compatibility reader or migration. Binding observations do not grant
fresh-member admission. At that cut, native live-quorum authorization, peer/S3
seeding, claim reconciliation and the native crash matrix were still outstanding.
MR-11 through MR-17 remain open.

Configuration-owned member/node/disk bindings now accompany the durable applied
configuration, peer snapshots and S3 manifests. Canonical revision-bound intent,
learner-first admission, caught-up promotion and a retained ID high-water preserve
identity across joint transitions, compaction and restart. This does not authorize
seeding by itself. Explicit native learner admission, a fresh safe ReadIndex seed
grant, bounded peer transfer, atomic local initialization and permanent claim
reconciliation are now implemented. Native tests cover reads and later replication,
partial-body retry, restart with vote zero, claimed-state loss and missing quorum.
Exact-reference S3 seeding and public restart are implemented. Both eight-cut
fresh-learner crash matrices pass focused tests; the complete source qualification
is recorded above. Automatic repair remains MR-11.

Modify: `crates/cellar/durability/raft/storage.rs`, `crates/cellar/durability/raft/apply.rs`, `crates/cellar/durability/raft/transport.rs`, `crates/cellar/node/embedded.rs`, `crates/cellar/durability/raft/catalog.rs`, `crates/cellar/tests/support/raft_catalog_fault.rs`, `crates/cellar/execution/domain.rs`, `crates/cellar/tests/native_domains.rs`, `docs/10-embedding.md`

Create: `crates/cellar/durability/raft/snapshot.rs`, `crates/cellar/durability/raft/recovery.rs`, `crates/cellar/durability/raft/member_state.rs`, `crates/cellar/durability/raft/member_state_tests.rs`, `crates/cellar/tests/native_raft_snapshot.rs`, `crates/cellar/durability/raft/host_membership.rs`, `crates/cellar/durability/raft/host_seed.rs`, `crates/cellar/durability/raft/seed_transport.rs`, `crates/cellar/durability/raft/snapshot_seed.rs`, `crates/cellar/durability/raft/seed_archive.rs`, `crates/cellar/tests/support/raft_seed_fault.rs`, `crates/cellar/tests/support/raft_seed_crash.rs`

1. Implement bounded snapshot build/download with image, index/term, ConfState and incarnation metadata.
2. Atomically publish engine install markers only after the image is durable; retain old reader pins.
3. Recover markers and replay durable committed prefixes; preserve uncommitted suffixes for Raft.
4. Implement authorized fresh-member seeding from S3 or a peer; explicitly refuse quorumless normal activation.

Acceptance: Crash injection covers every image/marker/snapshot acknowledgement boundary. Repeated installs are idempotent and bounded in disk/memory.

Stop conditions: Stop if restart repairs applied/commit mismatch by inventing progress. No response acknowledges snapshot installation before durability.

| Required test | Observable result |
| --- | --- |
| `snapshot_install_crash_matrix` | Restart never mixes old metadata with a new image |
| `snapshot_wrong_history` | Snapshot identity or base mismatch is refused |
| `restart_committed_replay` | Durable committed commands survive process restart and replay |
| `compacted_boundary_term` | Snapshot boundary term remains usable for log matching |
| `missing_quorum_restore_refused` | Available S3 bytes cannot independently reactivate an existing group |
| `snapshot_old_reader` | A query pinned before install remains coherent through install and GC |
| `maximum_image_retained_snapshot_and_archive_roundtrip` | The largest admitted incompressible SQLite image transfers at the retained cut and its newer archive restores after deleting local node directories |
| `retained_checkpoint_capture_never_substitutes_newer_state` | A replaced checkpoint is refused while an already captured transfer remains coherent through GC |
| `retained_empty_checkpoint_has_no_image_or_payload` | An empty retained checkpoint invents no image or payload |
| `snapshot_limits_require_full_encoded_image_capacity` | Encoded admission covers full-LTX overhead without shrinking the admitted image |
| `snapshot_abort_preserves_vote_and_committed_image` | Cancellation persists required term/vote over the old committed prefix, refuses late install and preserves the old image/checkpoint across restart |
| `snapshot_abort_joins_older_apply_and_allows_retry` | Cancellation joins the existing physical apply before completion; a fresh ticket can install the retried snapshot |
| `snapshot_abort_sync_failure_refuses_progress_and_recovers_prefix` | Failed cancellation sync releases no progress or acknowledgement and restart recovers the coherent old image/prefix |
| `native_snapshot_catchup_resumes_follower_reads` | An isolated native follower installs the compacted checkpoint through bounded authenticated chunks, catches up and serves the retained token without business callbacks |
| `native_snapshot_interrupted_transfer_retries_without_blocking_other_groups` | A partial transfer supplies no read result; retry restores the follower while a second group keeps writing |
| `native_snapshot_shutdown_joins_partial_transfer` | Native close joins the transfer, sockets, blocking owners and reservations while responses remain held |
| `native_snapshot_admission_refuses_insufficient_restore_capacity` | Startup rejects encoded/image-slot/copy budgets before creating namespace objects or permanent member storage |
| `snapshot_member_binding_and_retirement_survive_restart` | Joined member/node/disk bindings and retired-ID high-water survive compaction, transfer and restart |
| `snapshot_refuses_rebound_binding_and_lost_id_watermark` | Canonical higher snapshots cannot rebind an existing member or regress its retired-ID watermark |
| `snapshot_same_cut_requires_member_binding_marker` | Recovery refuses a snapshot whose same-cut member marker was not atomically installed |
| `native_fresh_learner_seed_requires_committed_binding_and_resumes_reads` | A committed exact learner binding obtains a fresh follower ReadIndex grant, seals and atomically installs its image, reconciles a permanent claim, resumes reads and later replication, preserves vote zero on restart, and refuses a claimed blank replacement |
| `native_fresh_learner_seed_refuses_missing_quorum` | Available S3 data and a committed learner binding cannot seed a blank node without a fresh quorum barrier; refusal leaves no group or permanent claim |
| `native_fresh_learner_seed_retries_partial_body_with_fresh_grant` | A held partial learner body expires its stream and retries with a fresh authenticated quorum grant under the original deadline, then resumes reads and subsequent replication without invoking seed-time business callbacks |
| `native_fresh_learner_s3_seed_replays_tail_and_resumes_reads` | A fresh follower barrier selects an immutable S3 cut; full image/tail replay resumes reads and replication, then full public Node restart with fresh process identities preserves data/votes and refuses strong reads until quorum returns |
| `native_fresh_learner_s3_seed_refuses_missing_quorum` | A committed learner binding and available S3 snapshot/tail cannot seed without a fresh quorum barrier; no local group or new claim is created |
| `native_s3_seed_does_not_wait_for_private_writer` | S3 metadata selection and follower reads proceed while a leader callback holds an unpublished write; only its committed predecessor is visible until release |
| `native_s3_seed_missing_selected_payload_refuses_newer_head_fallback` | After grant selection a missing old payload refuses initialization even with a newer complete head; no group or member claim appears |
| `native_s3_seed_reconciles_lost_member_claim_response` | A lost conditional claim response reconciles by exact readback and attaches the same synced learner without resetting state |
| `native_s3_seed_resumes_synced_state_before_member_claim` | A failed pre-claim PUT leaves synced nonvoting state; public restart first resumes that exact state and creates its claim without a new blank seed or quorum proof |
| `native_s3_seed_resumes_after_lost_claim_confirmation` | A committed claim with a lost confirmation refuses initial attachment, then public restart reconciles that same local state/claim and preserves vote zero |
| `native_peer_seed_crash_matrix` | Eight fresh-learner process-kill cuts preserve all-or-absent atomic state and claim ordering; public Node restart resumes verified state, keeps vote zero and later replicates without callback replay |
| `native_s3_seed_crash_matrix` | The same eight cuts for exact-reference S3 seeding preserve history and quorum/claim requirements across public Node restart |

Verification target: `native_raft_snapshot`. Run the exact list/test commands
shown by `plan.py show MR-10`; the manifest is authoritative for arguments.

### MR-11 Place replicas and repair membership automatically

Dependencies: MR-10, MR-04, MR-07. Phase: v1. Status: DONE.

The [MR-11 completion evidence](evidence/MR-11.json) pins source
`cc93ccf96bda3c48caf57d1f59d7d3bcd1a4a355`: all 248 unique Rust cases across
12 binaries and all 26 commands pass. This includes all seven original MR-11
requirements, 25 membership cases, seven pure leadership cases, 48 Raft library,
42 snapshot, 29 archive, 27 host, 16 directory and the affected public Node
lifecycle case. Five ignored child entry points execute through owning parents
and are excluded. Clippy, both LTX modes, format (678 Rust files), layout,
dependency policy, docs, 17 planning tests and 71-anchor preflight pass.

An opt-in owned controller inventories committed plans, probes bounded rotating
candidate sets and repairs without incoming business traffic. A six-Node fixture
covers failed-member replacement, canonical empty-state repair before the first
command, pause/resume, minority refusal with reachable object storage, and loss
of controller/leader while resuming the exact Seeding plan. Eight groups exercise
automatic leadership movement to caught-up voters while a control-reachable,
data-lagging voter remains ineligible; node-wide attempt limits bound movement.
Fresh-quorum seed grants, actual matched/published progress and explicit joint
consensus retain the existing authority boundaries. Declared domains/capacities
are placement inputs, not measured infrastructure qualification.

Public Node shutdown joins the controller's domain and closes retained Raft
handles. Its existing Fleet stop report does not yet supply a graceful Raft
close receipt or report Raft archive obligations; MR-13 retains that semantic
drain work. Existing explicit `RaftCells.close()` tests retain their separate
receipt/join contract. No physical S3, power-loss, real-zone resilience,
production capacity, larger tiers, federation or complete release is claimed.
MR-12 through MR-17 remain open.

Earlier component evidence remains linked from the completion record with its
original source, failures and narrower qualification scope. The
[native membership contract](membership-design.md) and
[node-directory contract](node-directory-design.md) describe the implementation.

Modify: `crates/cellar/durability/raft/catalog.rs`, `crates/cellar/durability/raft/runtime.rs`, `crates/cellar-logic/rebalance.rs`, `crates/cellar-logic/lib.rs`, `crates/cellar/durability/raft/member_state.rs`, `crates/cellar/application/consensus/mod.rs`, `crates/cellar/application/consensus/protocol.rs`, `crates/cellar/durability/raft/catalog_wire.rs`, `crates/cellar/application/consensus/client.rs`, `crates/cellar/durability/raft/host.rs`, `crates/cellar/durability/raft/host_membership.rs`, `crates/cellar/durability/raft/completion.rs`, `crates/cellar/durability/raft/replica.rs`, `crates/cellar/durability/raft/transport_frame.rs`, `crates/cellar/tests/native_raft_runtime.rs`, `crates/cellar/tests/support/raft_cluster.rs`, `crates/cellar/tests/support/raft_seed_crash.rs`, `crates/cellar/durability/raft/README.md`, `crates/cellar/durability/raft/host_sql/mod.rs`, `crates/cellar/durability/raft/seed_transport.rs`, `crates/cellar/durability/raft/seed_archive.rs`, `crates/cellar/node/embedded.rs`, `crates/cellar-logic/consensus.rs`, `crates/cellar/durability/raft/storage.rs`, `crates/cellar/durability/raft/records.rs`, `crates/cellar/tests/native_raft_storage.rs`, `crates/cellar/tests/support/raft_storage_fault.rs`, `crates/cellar/tests/native_raft_host.rs`, `crates/cellar/tests/support/raft_catalog_fault.rs`, `crates/cellar/tests/native_raft_snapshot.rs`, `crates/cellar/durability/raft/transport.rs`, `crates/cellar/durability/raft/transport_tests.rs`, `crates/cellar/durability/raft/query_transport.rs`, `crates/cellar/durability/raft/snapshot_transport.rs`, `crates/cellar/durability/raft/filesystem.rs`, `crates/cellar/durability/raft/mod.rs`, `crates/cellar/tests/native_raft_archive.rs`, `crates/cellar/durability/raft/transport_options.rs`.

Create: `crates/cellar/durability/raft/membership.rs`, `crates/cellar/durability/raft/placement.rs`, `crates/cellar/tests/native_raft_membership.rs`, `plans/multi-raft/membership-design.md`, `crates/cellar-logic/raft_placement.rs`, `crates/cellar/durability/raft/member_plan_tests.rs`, `crates/cellar/tests/support/raft_repair_native.rs`, `crates/cellar/tests/support/raft_repair_crash.rs`, `crates/cellar/durability/raft/replica_leadership.rs`, `crates/cellar-logic/raft_leadership.rs`, `crates/cellar-logic/raft_leadership_tests.rs`, `crates/cellar/durability/raft/host_standby.rs`, `crates/cellar/tests/support/raft_standby_native.rs`, `plans/multi-raft/node-directory-design.md`, `crates/cellar/tests/support/raft_node_registration_storage.rs`, `crates/cellar/durability/raft/node_directory.rs`, `crates/cellar/tests/native_raft_directory.rs`, `crates/cellar/durability/raft/host_directory.rs`, `crates/cellar/durability/raft/transport_directory.rs`, `crates/cellar/durability/raft/transport_directory_tests.rs`, `crates/cellar/tests/support/raft_directory_native.rs`, `crates/cellar/durability/raft/host_startup_tests.rs`, `crates/cellar/durability/raft/mailbox.rs`, `crates/cellar/durability/raft/mailbox_tests.rs`, `crates/cellar/durability/raft/inspection.rs`, `crates/cellar/durability/raft/host_inspection.rs`, `crates/cellar/tests/support/raft_inspection_native.rs`, `crates/cellar/durability/raft/inspection_auth_tests.rs`, `crates/cellar/durability/raft/inspection_identity.rs`, `crates/cellar/durability/raft/inspection_transport.rs`, `crates/cellar/durability/raft/inspection_wire.rs`, `crates/cellar/durability/raft/inspection_wire_tests.rs`, `crates/cellar/durability/raft/transport_inspection_tests.rs`, `crates/cellar/tests/support/raft_peer_inspection_native.rs`, `crates/cellar/durability/raft/host_leader_load.rs`, `crates/cellar/durability/raft/host_leader_load_tests.rs`, `crates/cellar/durability/raft/host_controller.rs`, `crates/cellar/durability/raft/controller_probe.rs`, `crates/cellar/durability/raft/controller_action.rs`, `crates/cellar/tests/support/raft_controller_native.rs`.

The [native membership implementation contract](membership-design.md) defines
committed plan recovery, learner seeding, joint consensus, dynamic standby
attachment and bounded controller work. It establishes no implementation or
capacity qualification by itself.

1. Implement domain-aware desired placement and versioned idempotent controller plans.
2. Add learners, install/catch up, promote through ConfChangeV2 and retire obsolete replicas in safe order.
3. Resume joint configurations correctly after crashes and throttle concurrent repairs/snapshots.
4. Balance eligible leaders and repair missing replicas without incoming application traffic.

Acceptance: A six-node fixture replaces failed members and restores the configured replica count. Losing the controller does not invalidate ongoing data-plane authority.

Stop conditions: Stop if membership comes from directory values instead of the committed configuration. Do not remove an old voter merely because its lease expired.

| Required test | Observable result |
| --- | --- |
| `learner_before_promotion` | An empty replacement cannot count as a voter before authorized catch-up |
| `joint_configuration_restart` | Both-majority transition rules survive leader/process loss |
| `stale_plan_idempotence` | Concurrent/stale controller plans cannot duplicate or force membership |
| `automatic_no_traffic_repair` | A failed node triggers repair without requests or manual assignment |
| `insufficient_domain_capacity` | Placement refuses unsafe domain concentration when policy requires separation |
| `minority_cannot_reconfigure` | Missing quorum cannot be bypassed by S3 plan edits |
| `leader_balance_with_lag` | Transfers select compatible caught-up voters and bound movement |

Verification target: `native_raft_membership`. Run the exact list/test commands
shown by `plan.py show MR-11`; the manifest is authoritative for arguments.

### MR-12 Integrate committed alarms, outbox and capability gates

Dependencies: MR-08, MR-09, MR-11. Phase: v1. Status: DONE at the recorded native boundary.

The [complete business-feature evidence](evidence/MR-12.json) pins source
`a28babe1904c1287cb4d1877674bd98a87313700`: all 782 unique unit/integration cases
identified by target/name across 21 binaries, four compile-fail SDK doctests,
two producer/receiver process runs and all 34 checks pass. This includes all 413
library cases, all 88 public Node cases, 16 native effects cases, all 42 snapshot
cases and owning Raft/Fleet outbox/event-stream regressions. Nine child entry
points execute through owning parents and are excluded from unique totals.

All six original MR-12 requirements are present and pass. A held private emission
produces no sends. An old source request commits at the receiver while its S3
response is held; source primary replacement retries identical bytes/ID and
produces one receiver mutation. Lost source settlement output leaves an actual
ambiguous close report, while survivors recover the replicated prefix without
another send. Authenticated retries preserve one client per lane; wrong bytes,
policy and unrelated operations cannot consume that identity. Actual S3-gated
self-delivery qualifies stable definition identity without recursive manifests;
full namespace compatibility still checks the complete compiled schema.

Alarms/outbox share one bounded owned scheduler and committed internal allocator.
Native mutable-factory refusal precedes catalog/engine creation; static SDK
checks exclude Workflow/Queue/container/socket capabilities. Immutable Blob
bytes remain in the complete verified archive closure. The
[executable recovery example](https://github.com/crabbuild/cellar/blob/a28babe1904c1287cb4d1877674bd98a87313700/examples/raft-effects/README.md)
replaces a primary and resumes the same retained disks in another process.
The [business-feature contract](effects-design.md) records bounds, proof and retry
semantics. Earlier [Blob](evidence/MR-12-blob-progress.json) and
[alarm](evidence/MR-12-alarm-progress.json) component records retain their original
source pins and limits; counts from different sources are not added.

MR-12 is DONE at this stated native boundary. Generic receipt limits still apply
without automatic pruning; long outages/permanent refusals retain and can block
lanes. Physical SQL can delay the single scheduler. There is no arbitrary external
exactly-once, production latency/capacity, physical-zone or complete-release claim.
Public Node-first closure still supplies joined cancellation rather than a
semantic Raft/archive report. Unified shutdown, readiness, resources, safe receipt
maintenance and fenced disaster recovery remain MR-13. MR-14 through MR-17 retain
their workload, release/24-hour soak, physical-scale and federation gates.

Modify: `crates/cellar/application/outbox.rs`, `crates/cellar/application/storage/outbox.rs`, `crates/cellar/fleet/wake/mod.rs`, `crates/cellar-logic/alarm.rs`, `crates/cellar/runtime/workers/native/mod.rs`, `crates/cellar/application/consensus/mod.rs`, `crates/cellar/application/consensus/class.rs`, `crates/cellar/durability/raft/mod.rs`, `crates/cellar/durability/raft/host.rs`, `crates/cellar/durability/raft/host_sql/mod.rs`, `crates/cellar/durability/raft/archive/mod.rs`, `crates/cellar/node/embedded.rs`, `crates/cellar/storage/sqlite/command.rs`, `crates/cellar/tests/support/raft_catalog_fault.rs`, `crates/cellar/durability/raft/README.md`, `crates/cellar/application/consensus/client.rs`, `crates/cellar/durability/raft/execution/mod.rs`, `crates/cellar/Cargo.toml`, `crates/cellar/application/mod.rs`, `crates/cellar/durability/raft/host_standby.rs`.

Create: `crates/cellar/durability/raft/effects.rs`, `crates/cellar/tests/native_raft_effects.rs`, `crates/cellar/durability/raft/effect_blob.rs`, `crates/cellar/durability/raft/effect_blob_tests.rs`, `plans/multi-raft/effects-design.md`, `crates/cellar/durability/raft/effect_alarm.rs`, `crates/cellar/durability/raft/host_effects.rs`, `crates/cellar/tests/support/raft_alarm_native.rs`, `crates/cellar/durability/raft/effect_delivery.rs`, `crates/cellar/durability/raft/effect_delivery_tests.rs`, `crates/cellar/durability/raft/effect_inbox.rs`, `crates/cellar/durability/raft/effect_outbox.rs`, `crates/cellar/durability/raft/effect_options.rs`, `crates/cellar/durability/raft/effect_scan.rs`, `crates/cellar/tests/support/raft_outbox_native.rs`, `crates/cellar/tests/support/raft_outbox_loopback.rs`, `crates/cellar/tests/support/raft_binding_gate.rs`, `examples/raft-effects/main.rs`, `examples/raft-effects/effects.rs`, `examples/raft-effects/README.md`, `examples/raft-effects/smoke.py`.

1. Add explicit Raft-compatible alarm and outbox adapters using committed intents and command-based progress.
2. Recover wake scheduling after election without ordinary cell constructors or user traffic.
3. Bind dispatch attempts to current activation while retaining stable receiver idempotency keys across races.
4. Validate unsupported existing Workflow/Queue/container/socket capabilities at registration rather than running them under the wrong authority.

Acceptance: Producer/receiver examples demonstrate recovery using durable receipts. Follower nodes never run business timers or effect callbacks for followed groups.

Stop conditions: Stop if external exactly-once behavior is inferred from Raft alone. Do not acknowledge a missing object merely because its reference is replicated.

| Required test | Observable result |
| --- | --- |
| `outbox_never_speculative` | Uncommitted private intents produce no outbound calls |
| `effect_leadership_race` | Destination receipts suppress duplicate effects after an old request races failover |
| `alarm_no_traffic_recovery` | Committed alarms resume after leader loss without user requests |
| `delivery_progress_replicated` | Lost completion retries preserve intent and stable delivery ID |
| `unsupported_binding_gate` | Incompatible mutable SDK capability fails before activation |
| `blob_archive_dependency` | Referenced immutable objects exist before the promised acknowledgement |

Verification target: `native_raft_effects`. Run the exact list/test commands
shown by `plan.py show MR-12`; the manifest is authoritative for arguments.

### MR-13 Bound resources and complete node lifecycle operations

Dependencies: MR-11, MR-12. Phase: v1. Status: IN_PROGRESS.

The [node operations contract](operations-design.md) describes admission closure,
owned application drain, bounded leadership handoff and final local observations.
The [partial qualification record](evidence/MR-13-shutdown-progress.json) pins
810 unit/integration cases across 24 binaries to library source `6784512`, with
the example-only status-poll fix and two recovery process runs at `275c45d`.
Seven native operations cases cover resource pressure and shutdown plus the four
additional report/handoff requirements below.

The [durable management receipt component](evidence/MR-13-management-progress.json)
records 839 unit/integration cases across 26 binaries, four SDK doctests and two
recovery process runs at `ec312e8`. Shared caller/operator/signal shutdown closes
the actual Raft host before publishing `joining_domain`; the final durable
receipt follows whole-domain joining. Held startup/closing fsync, close failure,
the original deadline, private SQL, archive lag and exact submitted ambiguity
have native coverage. A prior unlocalized transfer-test `Overloaded` refusal
remains an open qualification finding despite bounded successful replays and
the current passing matrix. No transfer oracle or retry rule was weakened.

The [retained-state mode guard](evidence/MR-13-mode-guard-progress.json) qualifies
all 15 native operations cases at the test-only successor `33f3c86`; 790 other
frozen parent files match `ec312e8`. Incompatible mode/factory admission leaves
the retained Raft trees and namespace root unchanged. Re-enabling Raft resumes
the same engine with a fresh process, preserves the old scoped token/value and
does not rerun the original callback. This is a native retained-state drill;
production Raft CLI and compatible-binary rollback remain separate work.

The [local readiness and reader-resource component](evidence/MR-13-readiness-pins-progress.json)
qualifies 847 unique unit/integration cases across 26 binaries, four SDK doctests
and two recovery processes at `e55e6cb`. Native checks distinguish voter/learner
roles, absent/coherent images, historical/recent-source queries and fresh learner
ReadIndex reads. A node-wide physical reader limit survives waiter cancellation,
refuses extra query callbacks while another worker continues commands, and stays
owned through Node joining. Fixed-size SDK usage reports actual host budgets.
The record retains a separate concurrent fallback `VersionUnavailable` finding;
its cause and the earlier transfer refusal remain unresolved. No retry, deadline
or production readiness check was weakened.

The [authenticated host reservation exporter](evidence/MR-13-metrics-progress.json)
qualifies 551 unique cases across six affected binaries, four SDK doctests and
two recovery processes at `4e25ffa`. The exact Node process generation and peer
authentication bind each scrape. One and 32 committed groups expose the same
14 series without user callbacks; cancelled readers retain their visible pin,
and held cancelled startup refuses an unavailable sample. This scoped result
is separate from the broader parent record; counts are not pooled.

The [SQL and transport resource component](evidence/MR-13-resources-progress.json)
qualifies 850 unique unit/integration cases across 26 binaries, four SDK doctests
and two recovery processes at `f89c7b4`. Physical SQL stacks own worker counts;
actual semaphores report queued/executing work. Weak transport observations expose
directional node and auxiliary banks without retaining the engine. A held real
RPC demonstrates reservations at both ends; joined shutdown releases physical
work. One and 32 committed groups expose the same 68 samples. A strengthened HTTP
oracle rejects the prior encoder's interleaved metric families; the grouped
replacement passes. The prior transfer/fallback findings remain open.

The [resource-view component](evidence/MR-13-resource-views-progress.json) extends passive resources with physical image
retention accounting, incoming/outgoing connection owners, complete bounded
role/readiness/lag scans, and age-bearing controller activity. See the
[resource contract](operations-design.md#physical-query-reader-admission).
At `37aed98`, 610 cases across eight binaries pass with serial fixtures, alongside
15 doctests, workspace Clippy and repository checks. The scoped record retains
an unexplained parallel outbox callback-count failure and passing isolated
replays. This does not close the finding, enlarge the frozen parent results or
complete this milestone.

Production Raft CLI startup now shares the embedded startup implementation.
`Host::raft_cells` binds the active compiled application before public ingress,
requires an explicit persistent `--data-dir`, pins the initial deployment and
joins failed/cancelled startup. The process qualification includes retained
state, two groups and three separate TLS-connected daemons; see the
[CLI contract](operations-design.md#production-cli-startup) and
[frozen qualification](evidence/MR-13-cli-startup-progress.json): 984 standard
cases, 15 doctests, the custom process runner, a focused Python watchdog and
workspace Clippy at `0fa09f1`. This is a partial
component and does not complete operating qualification.

The [receipt-maintenance component](receipt-maintenance-design.md) now couples
bounded expired-prefix cleanup to a new command's private transaction and LTX.
It preserves inclusive retry deadlines, every client floor and latest receipt,
and refuses live-capacity saturation before business callbacks. The
[frozen record](evidence/MR-13-receipt-maintenance-progress.json) pins 551 cases,
15 doctests, Clippy and the integrated daemon process runner to `817a4ed`.
It also repairs the S3 seed fixture's setup admission race within its original
deadline. These results do not close the independent CI findings.

Remaining receipt qualification, management surfaces, fenced new-history
disaster restore and complete operating qualification remain required before DONE. The remaining original case is
`disaster_old_quorum_fenced`.

Continue the remaining features in this order:

Current execution scope: local Colima containers, up to five Cellar nodes with
one CPU and 3 GiB memory each, and RustFS as the object store. Use the separate
`cellar-multi-raft` profile; see `tools/multi-raft/README.md` in the repository.
The [backend record](evidence/MR-13-local-backend-progress.json) covers seven
real object-store protocol operations, with zero Cellar nodes launched by that
component. It does not qualify the later physical fleet or release gates.
The subsequent [native container smoke](evidence/MR-13-colima-native-progress.json)
ran three Cellar nodes with enforced one-CPU/3-GiB/no-swap limits: six groups,
10 acknowledged writes, 18 exact minimum-version replica checks and verified
RustFS archive coverage for all groups. The initial optional Docker `Tmpfs`
metadata failure and same-cluster setup continuation remain in its evidence.
This establishes functional integration, with failure/recovery and five-node
qualification still required. The [merged-main record](evidence/MR-13-main-integration-progress.json)
separately qualifies 664 cases, 15 doctests, the daemon process runner, Python
watchdog and workspace Clippy at `f214ddf`.

1. Finish receipt-maintenance qualification: signed receiver acknowledgement
   recovery after response loss, alarm/outbox recurrence past the receipt ceiling,
   follower/archive floor recovery, and cleanup during leadership loss. Preserve
   promised retry windows, non-reusable floors, latest receiver receipts and
   unresolved delivery state. The new opportunistic path uses replicated command
   updates; the old private pruning primitive remains verification-only.
2. Complete operator discovery and management over the existing authenticated
   resource/inspection contracts. Carry exact namespace, group, incarnation,
   process and revision guards into mutations; preserve partial/unknown coverage
   and original source age. Production startup pins deployments until a separate
   live rebinding protocol is qualified.
3. Implement and qualify a real old-deployment fence for the selected deployment
   platform, then exact per-cell archive selection and restore into a distinct
   namespace/group/incarnation. Reconcile pending old-origin effects and prove
   old tokens and a returning partitioned quorum cannot operate on the new
   history. A stop receipt or caller assertion cannot replace that fence.
4. Close the outstanding transfer, concurrent fallback and effect callback-count
   findings, then run the complete operating matrix and disaster drill. Keep
   physical S3, power-loss, failure-zone and scale evidence separate from local
   fixture results. MR-14 through MR-17 retain their own release gates.

Modify: `crates/cellar/node/embedded.rs`, `crates/cellar/actor/output_proofs.rs`, `crates/cellar/fleet/management/nodes.rs`, `crates/cellar/fleet/management/routing.rs`, `crates/cellar/cli/cli_options.rs`, `crates/cellar/durability/raft/options.rs`, `crates/cellar/application/consensus/client.rs`, `crates/cellar/application/consensus/mod.rs`, `crates/cellar/durability/raft/effect_delivery.rs`, `crates/cellar/durability/raft/host.rs`, `crates/cellar/durability/raft/host_controller.rs`, `crates/cellar/durability/raft/host_effects.rs`, `crates/cellar/durability/raft/host_inspection.rs`, `crates/cellar/durability/raft/mod.rs`, `crates/cellar/execution/services.rs`, `crates/cellar/node/driver.rs`, `crates/cellar/node/lifecycle.rs`, `crates/cellar/tests/native_raft_membership.rs`, `crates/cellar/tests/support/raft_alarm_native.rs`, `crates/cellar/tests/support/raft_controller_native.rs`, `crates/cellar/tests/support/raft_node.rs`, `crates/cellar/tests/support/raft_outbox_loopback.rs`, `crates/cellar/tests/support/raft_outbox_native.rs`, `crates/cellar/durability/raft/transport_inspection_tests.rs`, `crates/cellar/tests/support/raft_repair_native.rs`, `crates/cellar/fleet/management/mod.rs`, `crates/cellar/fleet/management/operations.rs`, `crates/cellar-admin/tests/console.rs`, `crates/cellar/tests/support/raft_storage_fault.rs`, `crates/cellar/application/consensus/class.rs`, `crates/cellar/durability/raft/query.rs`, `crates/cellar/tests/native_raft_reads.rs`, `crates/cellar/tests/support/raft_cluster.rs`, `crates/cellar/durability/raft/budget.rs`, `crates/cellar/durability/raft/host_sql/mod.rs`, `crates/cellar/node/mod.rs`.

Create: `crates/cellar/durability/raft/operations.rs`, `crates/cellar/tests/native_raft_operations.rs`, `crates/cellar/durability/raft/host_drain.rs`, `plans/multi-raft/operations-design.md`, `crates/cellar/application/consensus/admission.rs`, `crates/cellar/application/consensus/admission_tests.rs`, `crates/cellar/fleet/management/raft_stop.rs`, `crates/cellar/fleet/management/raft_stop_tests.rs`, `crates/cellar/tests/support/raft_management_native.rs`, `crates/cellar/tests/support/raft_mode_native.rs`, `crates/cellar/application/consensus/readiness.rs`, `crates/cellar/durability/raft/host_readiness.rs`, `crates/cellar/durability/raft/host_readiness_tests.rs`, `crates/cellar/tests/support/raft_readiness_native.rs`, `crates/cellar/application/consensus/resources.rs`, `crates/cellar/tests/support/raft_resources_native.rs`, `crates/cellar/node/raft_metrics.rs`, `crates/cellar/tests/support/raft_metrics_native.rs`, `crates/cellar/application/consensus/resource_metrics.rs`, `crates/cellar/application/consensus/resources_tests.rs`, `crates/cellar/durability/raft/transport_resources.rs`.

1. Expose explicit limits and metrics for groups, byte queues, apply/archive lag, query pins, snapshot and repair budgets.
2. Implement readiness by group role and compatible applied state; avoid aggregate health that conceals unavailable cells.
3. Close admission, drain proposals, transfer leadership where possible, persist, cancel and join all tasks on shutdown.
4. Add isolated disaster restore/runbook paths with old-deployment fencing, a new namespace/group incarnation, and explicit per-cell recovery-point reporting.

Acceptance: CLI and embedded-node paths use the same explicit policies. Operational runbooks describe retained disks, archive lag and incomplete shutdown.

Stop conditions: Stop if cancellation drops owned blocking work without a truthful join result. No force-new-group command may silently overwrite an old incarnation.

| Required test | Observable result |
| --- | --- |
| `bounded_resource_pressure` | Saturation yields controlled refusal while control traffic remains live |
| `shutdown_pending_proposal` | Returned stop proof distinguishes committed/ambiguous work and joins tasks |
| `shutdown_archive_lag` | Quorum stop cannot authorize disk deletion while S3 coverage is incomplete |
| `disaster_old_quorum_fenced` | Restore refuses cutover without the required old-deployment fence evidence |
| `rollback_mode_guard` | Feature disable never opens Raft data with Fleet ownership |
| `metrics_group_cardinality` | Fleet metrics remain bounded while sampled group inspection is available |
| `node_shutdown_hands_off_to_published_voter` | An authenticated caught-up published voter receives leadership before joined Node shutdown |
| `node_shutdown_handoff_waits_for_physical_sql` | A held archive capture keeps its turn until the worker finishes, then transfers to a verified published voter |
| `node_shutdown_handoff_sql_wait_is_bounded` | A stalled archive capture exhausts the original one-second turn without inventing a transfer; closure still joins its physical worker |
| `node_shutdown_reports_submitted_ambiguity` | A committed command held below its requested S3 proof remains ambiguous in a bounded Node stop report |
| `node_shutdown_report_bounds_preserve_complete_lag_count` | Omitted bounded group details do not hide archive lag in complete aggregate counts |
| `node_shutdown_reports_parked_archive_unknown` | A resumed subset reports every durable engine history and explicitly unknown archive coverage for parked histories without executing their callbacks |
| `management_stop_seals_raft_before_joined_receipt` | An authenticated stop seals Raft work and retains an unjoined receipt while private SQL is held |
| `management_stop_reports_raft_archive_lag` | A durable receipt binds the actual engine and preserves archive obligations; malformed Raft evidence is refused |
| `management_stop_keeps_raft_ambiguity_incomplete` | A committed command held below S3 proof keeps the durable receipt incomplete |
| `management_stop_raft_cancel_has_no_graceful_proof` | Forced cancellation joins held SQL and records failure without graceful proof |
| `management_stop_raft_close_error_is_incomplete` | A physical fsync failure survives in the durable stop receipt |
| `management_stop_raft_physical_join_preserves_deadline` | A held fsync remains owned beyond the original deadline and the final receipt is incomplete |
| `management_stop_raft_cancelled_start_is_incomplete` | Interrupted startup joins its opening fsync and retains explicit incomplete startup evidence |
| `query_pin_pressure_survives_waiter_cancellation` | A held cancelled reader retains its pin while another worker refuses queries and continues commands; release resumes exact reads |
| `query_pin_shutdown_waits_for_physical_reader` | Node shutdown joins the cancelled physical reader before reporting zero pin usage |

Verification target: `native_raft_operations`. Run the exact list/test commands
shown by `plan.py show MR-13`; the manifest is authoritative for arguments.

### MR-14 Batch multi-group work and deliver the workload harness

Dependencies: MR-13. Phase: v1. Status: IN_PROGRESS.

The October 22 performance work adds a FIFO queue for shared durable batches,
a native conditional-update/transfer workload, fixed open-loop arrivals,
stable per-cell clients, durable invocation/completion ledgers and an independent
serialized-history/replica/archive checker. The runner verifies compiled source
and deployed executable hashes before admitting traffic. Profiles declare
proposed numeric objectives and preserve the Q1 capacity gap.

The first three-node Colima/RustFS experiment reconciled 138 commands including
initialization, 113 queries and all 36 replicas across 12 groups. Its measurement
completed 190 of 200 scheduled arrivals, with 10 generator rejections and about
535 ms p99 arrival-to-response latency. This misses the proposed local traffic
objectives and does not establish Q0 release qualification or Q1 capacity.
After fixing an arrival-sequence regression, the final Quorum and S3 runs
reconciled all submitted operations and all replicas, while completing 168/200
and 133/200 offered arrivals respectively. Both missed their proposed traffic
objectives. Earlier diagnostics remain in the evidence; no capacity or equivalent
Fleet/bucket speedup is claimed. `tools/perf/raft-performance.md` documents
execution, accounting, measured results and limits.

The native `repair_budget_under_load` case exercises two compacted, lagging cells
competing for the receive budget while a third cell completes 24 commands and 24
AtLeast follower queries. It holds real TLS snapshot responses, checks node-wide
reservation bounds, restores distinct per-cell histories and requires physical
reservation release. Its [source-pinned evidence](evidence/MR-14-repair-budget-progress.json)
qualifies bounded native catch-up under that workload,
not physical-node density, new-member replacement throughput or an SLO.

The native archive producer gathers at most two due cuts from one namespace and
coalesces small immutable snapshot/tail payloads into a bounded, indexed object.
Each cell still physically verifies its own recovery set and conditionally
publishes its own head. `archive_bundle_group_index` checks two exact histories,
a lost bundle PUT response, independent post-shutdown restores and corruption
refusal. The [archive-bundle evidence](evidence/MR-14-archive-bundle-progress.json)
also covers independent proofs after one member's head publication fails,
bounded direct fallback and adversarial index validation. All six required
MR-14 integration cases now execute; performance acceptance remains open.
This changes the internal protocol identity to include `/bundles-1`;
numeric markers remain 1 and earlier experimental roots are refused.

The SDK and authenticated peer endpoint now expose fixed-size engine/SQL physical
operation outcomes, microsecond histograms and byte counters. Fsync wraps the
actual engine filesystem; canceled waiters do not finish physical SQL timers.
The workload runner captures raw phase boundaries and periodic node observations.
Its analyzer rejects missing data, changed sources, counter resets, saturation and
impossible histograms; reported internal quantiles are bucket upper bounds.
Engine API bytes exclude SQLite images, node metadata and physical-device effects.
The [measurement evidence](evidence/MR-14-performance-metrics-progress.json)
records 596 Rust cases, 128 Python tooling cases, native snapshot/analyzer
interoperability and repository checks. This instrumentation has not established
a measured overhead or speedup.

Current instrumentation also observes streamed object API reads, conditional PUT
outcomes and requested/confirmed payload bytes, socket bytes below TLS on all
three Raft lanes, and controller probe/action attempts. The fixed vocabulary is
20 counters and 17 operations (413 Prometheus samples). These observations do not
establish physical device usage, network delivery or completed repair.
The [I/O measurement evidence](evidence/MR-14-io-metrics-progress.json) records
683 passing Rust cases and one failing membership case (`learner_before_promotion`
returned `Overloaded`). An earlier shutdown-handoff failure is also retained.
At that revision, isolated repetitions passed without explaining either failure.
Clippy, native-only compilation, repository checks and 128 Python cases pass.

The [background-admission evidence](evidence/MR-14-background-admission-progress.json)
subsequently reproduces the learner/abort failure: background SQL occupied the
physical worker while the business-command slot was free. One already-budgeted
command now waits in that slot until SQL completes, retaining its original
deadline and authority checks. A second producer is still refused. Deterministic
native tests verify waiting, cancellation before execution and an exactly-once
retry; the original scenario passes 60 repetitions in one process. Final source
validation passes 671 Rust cases, Clippy, native-only compilation, repository
checks, 128 Python cases and 17 plan-tool cases. This resolves the learner
admission finding; the independent shutdown-handoff finding remained open at
that revision.
No throughput or capacity claim follows from this change.

The [handoff evidence](evidence/MR-14-handoff-progress.json) subsequently reproduces
the shutdown failure: a healthy leader was skipped solely because an archive
capture occupied its SQL worker. The current group now retains its original
one-second handoff turn while SQL finishes. Waiting, inspection and transfer share
that budget, capped by the original Node deadline. Native regressions hold the
actual archive image read, verify handoff after release and bounded refusal under
a continued stall, then verify successor writes. The original scenario passes
80 repetitions. The broad matrix passes 690 Rust cases at `9e51538`; after a
test-module import cleanup, 47 affected cases and all-target/all-feature Clippy
pass at `b6e6f79`. Source pins, initial failures and full logs remain separate.
This resolves the recorded handoff finding; earlier MR-13 findings and the
performance acceptance gates retain their separate status.

[Fresh Colima/RustFS evidence](evidence/MR-14-io-local-progress.json) uses the
instrumented `/bundles-1` binary on three 1-vCPU/3-GiB nodes and 12 cells. All
submitted operations, 36 replicas and final archive cuts reconcile in each run.
Quorum completed 172/200 offered operations (p99 arrival latency 579 ms); S3
completed 126/200 (p99 1,204 ms). Both miss the proposed traffic objectives.
Owner metrics include real engine sync, SQL stage/prepare, object GET/PUT and
socket costs. They do not isolate a critical path or prove a batching speedup.
The shared Mac had about 10 GiB of swap in use before measurement. Raw logs,
ledgers and observations retain this confounder and the separate durability
contracts. The owned profile is stopped; volumes and evidence remain.

The [reference-baseline evidence](evidence/MR-14-baseline-progress.json) adds
executable Fleet and bucket adapters sharing the Raft workload's SQL statements,
bindings, conditional transitions and open-loop scheduler. Application receipts
commit with each baseline mutation; exact retries, conditional no-ops and state
survive a native fresh-owner/empty-directory restore. Six affected Rust cases,
all-target/all-feature Clippy, repository checks, 137 Python cases with one
opt-in skip, and 17 plan-tool cases pass at source `efb8328`.

Four sequential runs from one image use three 1-vCPU/3-GiB/no-swap containers,
12 cells and 200 offered measurement operations. The baseline checkers reconcile
owner-route histories and the Raft checker verifies all 36 actual replicas and
archive coverage. These paths have different acknowledgement and read contracts:

| Path | Completed / offered | Arrival p99 | Observation |
| --- | --- | --- | --- |
| Fleet | 200 / 200 | 35 ms | 97 Fleet output proofs, no proof errors |
| Bucket | 200 / 200 | 37 ms | 97 bucket output proofs, no proof errors |
| Raft Quorum | 199 / 200 | 28 ms | One non-ambiguous `VersionUnavailable` follower query |
| Raft S3 | 139 / 200 | 859 ms | 61 generator rejections; traffic objectives missed |

The comparison command reruns each analyzer and refuses a throughput ratio for
these unequal contracts. Fleet races all current follower fsyncs against a bucket
proof; owner reads do not prove a separate replica. Raft S3 also requires quorum
commitment and image publication. The shared Mac had about 9.5 GiB swap in use;
these observations establish neither steady state nor causal improvement over
older runs. Exact owned containers exited zero and the dedicated profile is
stopped; volumes and raw evidence remain. Other profiles were not changed.

The [bounded replay evidence](evidence/MR-14-replay-progress.json) replaces one
sealed image per archive entry with one private image per bounded batch. Every
intermediate SQLite image, SHA-256 and optional rolling checksum is still checked;
existing aggregate apply limits and peak disk reservations remain enforced. The
32-entry regression reduces image publication from 33 sync/rename sequences to
one with identical restored bytes. Long-tail partitioning, intermediate corruption,
growth/truncation and process-kill cases pass. Source `1093fde` passes 298 Raft
integration cases, the daemon qualification, Clippy, feature-mode and repository
checks; raw red/green logs and the original failing patch are retained.

A fresh three-node RustFS S3 run completed 199/200 offered operations at 169 ms
p99 arrival latency, with one generator rejection. All submitted histories,
36 replicas and final archive cuts reconcile. This short run meets its proposed
traffic objectives. The preceding repeat of the original binary completed
183/200 at 264 ms p99; both raw runs remain in evidence. Shared-host work and
swap prevent a controlled causal speedup or steady-state capacity claim. All
exact containers exited zero, the owned profile is stopped, and volumes remain.

The [five-node evidence](evidence/MR-14-five-node-progress.json) extends the
runner to three-voter placement across three to five physical nodes. Unassigned
nodes do not register a cell. Complete before/after scans verify actual voter
membership, roles and registration; owner metrics verify local group counts,
and final reads verify every assigned replica. Raw receipts and query invocations
must match the declared durability and follower-read contract. Resource checks
require distinct CPU sets and all five 1-vCPU/3-GiB/no-swap containers.

At source `bd7868f`, both five-node profiles use 150 cells, 30 active cells,
90 replicas and 30 leaders per node, 30 seconds of warmup and 120 seconds of
measurement at 10 offered operations/second. Quorum completed 1,200/1,200
measured operations at 65 ms overall p99 arrival latency. S3 completed
1,158/1,200 at 233 ms p99, with 42 `cell_busy` generator rejections, missing
the declared traffic objectives. Every submitted history, all 450 replicas and
final archive coverage reconcile in both runs. These shared-host observations
do not qualify steady state or Q1 capacity. The comparison refuses a speedup
ratio between the different durability contracts. All six containers per run
exited zero; the dedicated profile is stopped and volumes remain.

The same change retains a separate CI finding: the preceding Python 3.14
baseline job failed a counter acknowledgement without logging its response.
Three local repetitions passed, so the failure is unresolved. CI now retains
bounded response details and private fixture failure artifacts; assertions
remain unchanged. This diagnostic improvement is not a CI correctness fix.

The [archive wake-up evidence](evidence/MR-14-wakeup-progress.json) removes two
maintenance-timer waits: new committed images wake idle archive scanning, and
accepted archive coverage wakes pending command completion. Fresh authority,
receipt and output-proof checks still determine the response. Fair scheduling,
capture budgets and per-cell failure backoff are unchanged. The amplified
one-second-maintenance regression fails three times on the original code;
invalidating only the idle scan still fails. Both wakeups pass the same test
and 20 repetitions. Source `8424f02` passes all 299 Raft integration cases,
the daemon qualification, Clippy, feature-mode and repository checks.

Fresh runs keep the same five-node profiles, resources and offered load. They
retain the new image, subsequent repeats of the original cached image, and a
final repeat of the fixed image:

| Source | Acknowledgement | Completed / offered | Overall p99 | Command p99 | Query p99 | Rejections |
| --- | --- | --- | --- | --- | --- | --- |
| Wake-up fix `8424f02` | Quorum | 1,170 / 1,200 | 432.9 ms | 651.2 ms | 219.5 ms | 30 |
| Wake-up fix `8424f02` | S3 | 1,142 / 1,200 | 662.6 ms | 721.5 ms | 113.2 ms | 58 |
| Original repeat `bd7868f` | Quorum | 1,200 / 1,200 | 86.4 ms | 145.5 ms | 13.7 ms | 0 |
| Original repeat `bd7868f` | S3 | 1,158 / 1,200 | 367.6 ms | 427.6 ms | 223.6 ms | 42 |
| Fix repeat `8424f02` | Quorum | 1,189 / 1,200 | 190.5 ms | 250.0 ms | 20.1 ms | 11 |
| Fix repeat `8424f02` | S3 | 1,194 / 1,200 | 160.8 ms | 205.6 ms | 15.0 ms | 6 |

The final fixed-image repeats meet both short-run traffic targets; the first
fixed-image pair misses both. All submitted histories, all 450 replicas and final
archive coverage reconcile in each run. These observations do not establish a
causal speedup or steady-state capacity: the shared Mac has varying load and
swap, and the comparison refuses
ratios between different binaries. Host observations and complete raw outputs,
including missed traffic objectives, remain in the evidence. All exact containers
exited zero; the owned profile is stopped and volumes remain.

The [controlled slow-follower evidence](evidence/MR-14-slow-follower-progress.json)
adds a joined fault owner, physical pause observations and exact healthy-majority
command witnesses. The target also leads some cells and serves follower queries;
separate role cohorts make that whole-process effect visible. Healthy controls
use the same probes without node pauses. Final replica and archive checks remain
mandatory. Source `97d00b4` retains four runs using the unchanged five-node
profiles and 116 observation cycles per run:

| Acknowledgement | Scenario | Completed / offered | Overall p99 | Traffic targets |
| --- | --- | --- | --- | --- |
| Quorum | Healthy control | 1,194 / 1,200 | 169.8 ms | Met |
| Quorum | Slow follower | 1,165 / 1,200 | 311.9 ms | Missed |
| S3 | Healthy control | 1,198 / 1,200 | 146.8 ms | Met |
| S3 | Slow follower | 1,176 / 1,200 | 332.5 ms | Missed |

All four histories, 450 replicas per run and archive coverage independently
reconcile. The injected Quorum/S3 runs retain 44/45 exact healthy-majority
command witnesses and recovery upper bounds of 3.16/3.21 seconds. Every observed
pause stays within the unchanged 750 ms bound. Where the target is only the third
voter, Quorum completes 403/406 arrivals at 112.7 ms p99; S3 completes 406/406 at
79.5 ms p99. These are short shared-host observations, not steady-state capacity
or a causal comparison between durability modes.

The first five-node fault run at `8beeee3` remains rejected because eight pause
cycles exceeded 750 ms. Stage timings and an alternating isolated control probe
support replacing repeated CLI startup with a persistent local Engine connection
bound to the owned container's immutable ID. The original scenario passes at the
new source without relaxed gates. A separate S3 attempt failed before workload
creation because retained empty networks exhausted Docker address pools. Its
diagnostics and fresh-directory retry remain in the evidence. Seven exact owned
empty networks were recorded and removed; stopped containers and data volumes
remain. The dedicated profile is stopped; other profiles were not mutated.

Validation retains 154 passing Python cases and one existing opt-in Docker skip,
two Rust fixture cases and affected all-feature Clippy at `8beeee3`, plus
formatting/layout, dependency and documentation checks. The source-pinned raw
capsules independently reanalyze after extraction; failed attempts stay failed.

The [kernel and density evidence](evidence/MR-14-density-progress.json) adds
cgroup v2 CPU, memory/OOM, per-device I/O and PID 1 RSS/thread/FD observations for
every Cellar node and RustFS. Independent analysis checks exact container/process
identity, limits, monotone counters and phase-enclosing time windows. Memory
headroom is a separate result; boundary gauges are not peaks, and cgroup I/O is
not whole-device attribution. The 500/1,500-cell profiles vary only total seeded
cells from the 150-cell profile: 30 active cells, 100 ms ticks, 10 offered
operations/second and Quorum acknowledgements remain fixed.

At source `be5955e`, the 150-cell run completes 1,200/1,200 arrivals at 93 ms p99;
500 cells complete 1,161/1,200 at 339 ms and miss traffic targets. Both reconcile
all histories, assigned replicas and archive cuts. The first 1,500-cell attempt
fails while seeding cell 781 with `NotLeader { leader: None }`; it remains failed.
Source `4bbab55` adds startup observations without changing native code or retry
semantics. Its 1,500-cell run initializes every cell, but completes only 904/1,200
measured arrivals at 2,314 ms p99: 144 generator rejections and 152 server refusals.
Its final topology scan fails because 355 cells changed leader. Diagnostic
reconciliation verifies the acknowledged history and all 4,500 replica values;
this does not convert the failed benchmark into qualification.

Before leadership churn, periodic observations show approximately 4,000 outgoing
control frames queued per node. Average CPU is about 0.51 core per Cellar node
with 226–228 MiB lifetime cgroup memory peaks; no OOM occurs. These observations
motivate a transport experiment, not a proven bottleneck. A subsequent 150-cell
control completes 1,172/1,200 at 311 ms p99 and misses traffic targets, while its
history and all 450 replicas reconcile. Shared-host variability and observation
overhead prevent a sustained-capacity or causal speedup claim.

The final tooling has 161 passing Python cases plus one existing opt-in Docker
skip; all six physical attempts, including both failed 1,500-cell runs, are
retained. Extracted capsules independently reproduce analysis and diagnostic
results. The native executable hash is unchanged from `97d00b4`; earlier Rust
checks retain their own pins. All owned containers are stopped, recorded empty
networks removed, volumes preserved and the dedicated Colima profile stopped.
Other profiles were not mutated.

The [bounded control-pipeline evidence](evidence/MR-14-pipeline-progress.json)
pins source `e788e35`. Up to eight already-admitted FIFO control frames share the
existing authenticated connection, without waiting to fill a window. Reads and
writes progress concurrently under one existing I/O deadline; bulk data stays
one frame at a time. Each reply retains its exact request, channel and route
binding. A connection failure preserves only verified earlier admissions and
fails unresolved frames without replay. Joined shutdown retains physical
reservations, and admission remains separate from durable Raft responses.

The original sender reproducer times out waiting for a second frame while the
first reply is withheld. Seven final regression tests cover the bounded window,
lost and reordered replies, route revocation, timeout, cancellation and one-byte
duplex buffers. Initial test compilation and bootstrap-index fixture repairs are
retained with the failing outputs. All 31 transport unit tests, 299 native Raft
cases across 15 standard targets and the separate daemon process harness pass.
All-target/all-feature Cellar Clippy, formatting/layout, native dependencies,
documentation and 161 Python tests pass; one existing opt-in Docker test skips.

The same five-node density profiles run from one new source-pinned image:

| Cells | Attempt | Completed / offered | Overall p99 | Traffic targets |
| --- | --- | --- | --- | --- |
| 150 | Initial control | 1,200 / 1,200 | 54 ms | Met |
| 500 | Density | 1,166 / 1,200 | 332 ms | Missed |
| 1,500 | Density | 1,071 / 1,200 | 1,774 ms | Missed |
| 150 | Repeated control | 1,104 / 1,200 | 400 ms | Missed |

Every submitted history, all assigned replicas and archive coverage reconcile;
memory headroom and exact before/after placement checks pass. Missing completions
are generator rejections, with no observed server refusals. The 1,500-cell run
verifies all 4,500 replicas without changed leaders, but measured control queues
still reach 3,896–4,049 frames per node. Coarse Docker samples show a shorter CPU
pressure burst that average kernel CPU rates hide. SQL-stage histogram bounds
identify a delayed phase, not a proven sub-operation or cause. The repeated
150-cell miss prevents a repeatable latency claim, and these shared-host runs do
not establish a causal speedup. All four runs independently reanalyze after
capsule extraction. Exact owned containers are stopped and empty networks
removed; volumes remain and the dedicated profile is stopped. Other profiles
were not mutated.

The [persistence-wait evidence](evidence/MR-14-persistence-progress.json) pins
source `729dd8e`. Groups waiting for a queued or in-flight log write now leave the
consensus runnable queue. Their independent persistence FIFO, deferred requests,
reservations, coalesced ticks and hints remain intact. Exact sync receipts wake
their groups; overload preserves order and backoff, and permanent rejection
wakes the failure report. Snapshot installation remains distinct from its
subsequent log-persistence wait.

The original scheduler fails a 32-waiter regression. With the change, an unrelated
group answers 32 heartbeats while persistence futures are held, and each batch
releases only its own responses after real engine persistence. Five regressions
also cover bounded deferred admission, retained hints, overload retry, actual
engine rejection and mismatched receipts. All 96 Raft unit cases, 299 native Raft
cases across 15 standard targets, the custom daemon harness, all-target/all-feature
Cellar Clippy and repository checks pass. The final unit run includes a backoff
assertion that does not race a ten-millisecond wall-clock window. Initial zero-test
selection and failing-before/passing-after outputs remain in the raw capsule.

No physical benchmark runs in this slice. The prior repeated 150-cell control
ends with a host load average near 23 and about 12.2 GiB swap in use. At inspection,
the other Colima profiles have 18 GiB configured and the stopped dedicated profile
has 22 GiB, against 32 GiB host RAM; configured limits are not resident usage.
These observations restrict attribution, not the numeric acceptance gates. The
dedicated profile remains stopped and other profiles were not mutated. This
change proves removal of redundant scheduler work during persistence waits; it
does not prove the cause or resolution of the physical queue/latency bursts.

The [phase-timed density evidence](evidence/MR-14-phase-progress.json) pins
source `81f29b6` and the same image for four fresh attempts. All five Cellar
containers retain one CPU, 3 GiB, no swap and the unchanged workload. The initial
150-cell control passes at 1,200/1,200 and 70.981 ms p99. The 500-cell run verifies
all 1,500 replicas but misses the 200 ms target at 201.343 ms, with three generator
rejections. Its aggregate sender queue means are 2.6–3.3 ms, receiver admission
means 32–39 microseconds, and SQL staging means 11–24 ms. These independent
observations do not establish per-command causality.

The 1,500-cell attempt fails its original startup deadline before measurement:
node 1 exits with catalog `Runtime(Overloaded)`, exit code 1 and no OOM; four
other nodes report 900 groups ready. The repeated 150-cell control completes
1,165/1,200 at 247.441 ms p99 with 35 generator rejections. Startup and traffic
failures remain failed, including after capsule extraction and independent
analysis. No repeatable capacity or cross-revision speedup is claimed. Exact
owned containers are stopped, empty owned networks removed, and volumes retained;
only the dedicated profile was controlled. Startup admission ordering is the
next bounded regression at that source pin; the following correction retains this failed attempt.

The [startup admission correction](evidence/MR-14-startup-progress.json) pins
source `f96a94b`. Host initialization validates routes as before but publishes
none until every local replica has attached and the archive owner is ready.
Otherwise peers can fill the ordinary runtime request budget while the host has
not yet started consuming events, causing later attachments to fail. A real-TLS
test holds the second member claim with a one-request/event budget. Before the
fix, three heartbeats enter and thirteen overload; afterward all sixteen receive
`UnknownGroup`, both groups attach, and a subsequent heartbeat is accepted.
No queue limit, timeout, durability proof or resource allocation changes.

All 97 Raft unit cases and 125 native host, membership, multigroup, operations
and snapshot cases pass; four child-process fixtures are invoked by their parent
crash tests. All-target/all-feature Clippy, formatting/layout, dependency/docs
checks, plan validation and all 17 plan tests pass. Initial compiler diagnostics
and the failing-before regression remain in the validation capsule.

The fresh five-node 1,500-cell run now passes startup and independently verifies
all 4,500 replicas and archive coverage. It completes 1,200/1,200 operations with
no errors or rejections: overall p99 123.939 ms, command p99 149.962 ms and query
p99 38.711 ms. Traffic and memory objectives pass. The workload still has thirty
active cells and ten offered operations per second. Raw capsules are hashed,
extracted and re-analyzed; exact containers are stopped, empty owned networks
removed, volumes preserved and the dedicated profile stopped. Earlier failures
remain at their source pins. This one successful run validates the startup fix
locally; repeatable latency, overhead, matched-contract benchmarks, sustained S3
and Q1 capacity remain open.

The [paired density sequence](evidence/MR-14-repeat-progress.json) uses checkout
`70c5695` and the same immutable image built at `f96a94b`; only documentation
changed and every compiled input and executable digest is independently checked.
Two healthy 1,500-cell controls with the identical 116-cycle probe schedule pass
all traffic, memory, replica and archive checks at 95.426/79.254 ms p99. The
intervening pause attempt fails during cycle 91 after five leader probes. It
confirms unpause and all containers exit normally, but final replica/archive
checks never run. Its 1,168 successful replies, 32 rejected arrivals and diagnostic
341.527 ms p99 do not qualify the declared fault profile: fault scheduling stops
before the 120-second measurement ends. Raw observations and independent capsule
extraction preserve this rejection.

Source `f516e4f` subsequently records failed probe context before cleanup, with
no retry or changed deadline. A failing-before/passing-after regression and a
verifier refusal case pass; the performance-tool suite has 194 passes and one
existing opt-in Docker skip. Documentation, dependency, plan validation and all
17 plan tests pass; the initial missing-Cargo environment failure remains in the
validation capsule. No new physical run is attributed to this diagnostic change.
Only the dedicated profile was controlled and it is stopped; volumes remain.

The [probe-stage evidence](evidence/MR-14-probe-progress.json) pins source
`17fd0b1` and a fresh immutable image. Explicitly tagged reads retain at most 64
server observations with separate status and replication boundaries, source and
sequence identities, eviction counts and truthful cancellation. Diagnostic
completion never replaces an HTTP result or a quorum witness. Six fixture tests,
194 performance-tool tests (one opt-in skip), all-feature fixture Clippy and the
repository checks pass. The raw capsules are extracted and independently checked.

The unchanged 1,500-cell slow-follower run completes all 116 cycles and correlates
696 probes. It verifies all 4,500 replicas and archive coverage, but 1,176/1,200
operations, 304.107 ms p99 and 24 rejections miss the declared traffic targets.
The matching healthy control fails cycle 68's cell-25 probe after 102.390 ms
with the same requested 100 ms timeout. There are 413 server observations for
414 client attempts, no diagnostic sampling failures, and the final sample at
121.516 seconds still ends at probe `68:20`. The missing request cannot be
explained by the 64-entry retention bound: no later probes were issued.
All observed server probes take at most 1.584 ms in that control. This points
before recorded handler entry; it does not distinguish network/HTTP ingress,
executor scheduling or shared-host interference. The control's 1,108 successful
replies, 92 rejected arrivals and 638.467 ms diagnostic p99 remain unqualified:
final replica/archive verification never runs. Both experiments retain host/VM
pressure samples; these are periodic observations, not per-request causal traces.
Exact containers are stopped, empty owned networks removed, volumes retained and
the dedicated profile stopped. The prior failed attempt remains unqualified.

The [client timing and fixture evidence](evidence/MR-14-client-progress.json)
records the diagnostic change at `9c9ef02` and the deterministic snapshot
qualification reproducer at `0638cf5`. Client spans preserve HTTP error behavior,
close response bodies on read failure and exclude credentials and payloads.
There is no physical workload result at either revision. The later merge from
`origin/main` (`64a4562`) adopts stronger snapshot coverage: callback counts are
separate for each cell, and the recovering node deliberately leads an independent
healthy cell during both retry and shutdown. This replaces the narrower local
fixture placement; its original failure and validation remain at their source
pins. The [merged-source validation](evidence/MR-14-main-merge-progress.json)
records 127 passing native cases at `1209e0d` and six focused snapshot cases plus
five-target Clippy after the shared-helper correction. Formatting, layout,
dependency/docs checks, 201 performance-tool cases (one opt-in Docker skip),
Python qualification guards and 17 plan tests pass. Fresh-head CI remains a
separate gate. Snapshot qualification is distinct from the healthy density timeout.

The [client-timed healthy attempt](evidence/MR-14-client-run-progress.json) at
`d0eb310` retains another failure under the unchanged five-node, 1,500-cell
profile. Cycle 1's cell-20 request connects/sends in 0.127 ms and times out during
102.226 ms of header waiting. Its server status stage takes 111.883 ms and
replication inspection 0.550 ms; late server completion does not satisfy the
failed HTTP probe. All eleven early probes correlate. Later, the sampler stops
with macOS `EADDRNOTAVAIL`; socket counts collected after cleanup cannot establish
port exhaustion at the failure. The raw measurement retains 235 completions,
15 ambiguous commands, 11 failures and 939 rejected arrivals. Final replica and
archive checks never run. The extracted capsule is independently rejected as
unfinished. Containers exit normally, volumes remain and the dedicated profile
is stopped. Executor responsiveness and client socket admission need separate
regressions; no production performance fix or capacity is claimed from this run.

Fresh-head CI also exposed a [read-accounting fixture race](evidence/MR-14-read-accounting-progress.json):
completion fsync can legitimately keep the cell busy at the next 10 ms arrival.
An injected fsync delay reproduces one timed-out read where the test assumed
three. The accounting case now controls clock and executor completion and still
requires three timeouts, zero ambiguity and unchanged cell state. Separate real
thread tests retain slow-server admission coverage. The delayed-fsync reproducer,
201 performance cases (one opt-in Docker skip), docs/plan checks and all 17 plan
tests pass at `4dcaf1d`. No production scheduling or timeout contract changes.

The [all-owner restart evidence](evidence/MR-14-restart-progress.json), source
`aaa83f8`, records two three-node bucket runs under an explicit twelve-second
RustFS pause. All owners join an expired-lease fence before the exact containers
restart on the same volumes with fresh lease generations. Each run verifies 36
owner-route reads before any retry, preserves every acknowledged prefix, recovers
and confirms eight original command identities, commits twelve new writes and
passes 36 final reads. Recovery is bounded at 8.22 and 7.69 seconds, within the
unchanged thirty-second objective. No fresh held-window write acknowledges.
Both measured histories retain only 32 of 200 completions, eight ambiguous
commands and ten failed queries; recovery never improves those scores.

Old-process final counters are unavailable, so performance comparison is refused.
Both raw capsules reanalyze independently. All eight containers stop cleanly,
their exact empty networks are removed, volumes remain, and only the dedicated
profile is stopped. The earlier mixed-live/fenced-owner attempt remains rejected;
these runs do not establish cold or disaster restore. At the source pin, 192
Python cases pass with one existing opt-in Docker skip, including eight restart
cases; all five native baseline cases, focused Clippy and repository checks pass.

Remaining MR-14 gates include the observed control-queue and latency failures,
repeatable leadership stability under density, sustained S3 acknowledgement latency, controlled
Fleet/bucket fault baselines and equivalent-contract analysis, instrumentation overhead
and whole-device attribution, steady-state/density evidence and preliminary Q1
execution.
The current local scope is at most five nodes; the original six-process Q1 gate
remains unqualified. MR-13's remaining management/restore work stays open.

Modify: `crates/cellar/durability/raft/runtime.rs`, `crates/cellar/durability/raft/storage.rs`, `crates/cellar/durability/raft/transport.rs`, `crates/cellar/Cargo.toml`, `tools/perf/README.md`, `crates/cellar/durability/raft/host.rs`.

Create: `tools/perf/raft_run.py`, `tools/perf/raft_workload.rs`, `tools/perf/test_raft_run.py`, `tools/perf/raft_profiles.json`, `crates/cellar/tests/native_raft_multigroup.rs`.

1. Optimize shared log batches, pooled transport and fair scheduling while retaining per-group Ready ordering.
2. Deliver the runner interface and reference workload from qualification.md, with executable/image freshness checks.
3. Measure copy/apply cost, group density and failure recovery against current fleet and bucket baselines under equivalent contracts.
4. Record numeric deployment SLO profiles and offered-load methodology before performance qualification.

The current host's 4,096-group limit cannot admit Q1's average 5,000 voter
replicas per process (10,000 cells, three voters, six processes), before reserve.
Reconcile the bound through bounded implementation and density measurements;
changing a numeric limit alone does not qualify it. Keep the Q1 workload gate.

Acceptance: Q0 and preliminary Q1 runs emit raw ledgers, all required metrics and reconciled results. Performance claims compare the same durability contract and disclose hardware.

Stop conditions: Stop if full-image publication dominates the declared SLO; reduce supported size or enter MR-16 design review. Do not silently implement unqualified hibernation to meet group-count targets.

| Required test | Observable result |
| --- | --- |
| `many_groups_mixed_roles` | Nodes concurrently lead and follow independent groups with correct results |
| `hot_group_fairness` | One saturated cell does not starve other groups or elections |
| `cross_group_batch_crash` | Batch crash/reopen preserves each group's committed history independently |
| `repair_budget_under_load` | Snapshot/repair work remains within configured budgets during traffic |
| `archive_bundle_group_index` | Mixed-group bundles restore only the named exact histories |
| `harness_rejects_stale_binary` | Benchmark cannot attribute results to a different executable |
| `archived_progress_wakes_commands_without_maintenance_poll` | Committed images and accepted archive coverage wake eight sequential S3 commands while preserving normal proof checks |

Verification target: `native_raft_multigroup`. Run the exact list/test commands
shown by `plan.py show MR-14`; the manifest is authoritative for arguments.

### MR-15 Qualify failure histories and release the isolated mode

Dependencies: MR-14. Phase: v1. Initial status: TODO.

Modify: `.github/workflows/native-sdk.yml`, `docs/03-architecture.md`, `docs/06-storage-transactions.md`, `docs/13-fleet-topology.md`, `docs/18-recovery-backup.md`, `docs/21-production-readiness.md`.

Create: `tools/perf/raft_history.py`, `tools/perf/test_raft_history.py`, `plans/multi-raft/release-evidence.md`, `crates/cellar/tests/native_raft_qualification.rs`.

1. Implement offline reference-model history checking and negative tests that detect known-bad histories.
2. Run all named cases and the process/disk/S3 failure matrix, including Q1 24-hour mixed-workload soak.
3. Complete repository checks, native/Axum examples, compatible-binary rollback and disaster restore drills.
4. Publish source/image/SLO/failure-domain evidence and an explicit supported release envelope; enable only a fresh canary namespace.

Acceptance: All MR-01..MR-14 cases and repository checks pass with evidence; Q0/Q1 safety gates are met. Numeric SLOs are met for the advertised topology; limitations and unsupported scale tiers remain explicit.

Stop conditions: Any hard safety failure blocks release regardless of throughput. Missing physical-node evidence prevents corresponding Q2-Q4 claims.

| Required test | Observable result |
| --- | --- |
| `history_detects_lost_ack` | Oracle rejects a missing acknowledged effect or receipt |
| `history_detects_stale_strong_read` | Oracle rejects a strong read violating real-time order |
| `history_accepts_ambiguous_commit` | Oracle permits either outcome while enforcing receipt idempotency |
| `all_disk_s3_recovery` | New-incarnation archive restore contains every S3-acknowledged effect |
| `no_bucket_fallback_after_partition` | A minority cannot resume writes when S3 remains available |
| `fleet_regression_matrix` | Feature-off existing applications and lifecycle contracts remain valid |

Verification target: `native_raft_qualification`. Run the exact list/test commands
shown by `plan.py show MR-15`; the manifest is authoritative for arguments.

### MR-16 Qualify density improvements and larger cluster tiers

Dependencies: MR-15. Phase: scale. Initial status: TODO.

The [density decision](density-decision.md) selects shared cell groups as the
replacement for one-cell-per-group consensus and defines the dependent runtime,
manifest, archive, recovery and SDK conversion slices. The decision is recorded;
implementation and qualification are pending. This does not complete MR-16 or
relax its MR-15 dependency and physical-tier release gates.

Modify: `crates/cellar/durability/raft/runtime.rs`, `crates/cellar/durability/raft/placement.rs`, `crates/cellar/durability/raft/apply.rs`, `tools/perf/raft_profiles.json`.

Create: `plans/multi-raft/density-decision.md`, `crates/cellar/tests/native_raft_density.rs`.

1. Measure Q2/Q3 bottlenecks and select page-versioned images, hibernation or shared-cell groups through a reviewed protocol update.
2. Specify crash recovery, wakeup, ownership and read semantics for the selected optimization before coding it.
3. If shared groups are selected, define immutable assignment first and separately qualify source-fenced online transfer.
4. Run real-infrastructure Q2/Q3 and then Q4 with bounded control-plane fan-out and domain-loss recovery.

Acceptance: A decision record identifies measured cause and exact optimization; no blanket scale promise. Only actually executed physical tiers become supported.

Stop conditions: Stop if an optimization changes proof boundaries without new fault tests. Do not count metadata-only dormant names as active group capacity.

| Required test | Observable result |
| --- | --- |
| `density_protocol_restart` | Selected optimization retains committed history across its new crash cuts |
| `idle_wakeup_authority` | Idle-state restoration cannot bypass quorum or current-term read barriers |
| `group_transfer_single_writer` | Any implemented transfer admits one writer across the transition |
| `cold_catalog_bounded` | Catalog/startup scans and metadata caches remain bounded |
| `domain_loss_scale` | Recovery meets the declared tier SLO with reserved resources |

Verification target: `native_raft_density`. Run the exact list/test commands
shown by `plan.py show MR-16`; the manifest is authoritative for arguments.

### MR-17 Design and qualify federation between independent clusters

Dependencies: MR-16. Phase: federation. Initial status: TODO.

Modify: `tools/perf/raft_profiles.json`.

Create: `plans/multi-raft/federation-design.md`, `crates/cellar/fleet/federation.rs`, `crates/cellar/tests/native_raft_federation.rs`.

1. Define partitioned global cell-to-home-cluster directory with versioned ownership and cached routing.
2. Specify source-fenced migration, target activation, token scope and rollback through failure at every transfer step.
3. Keep regional steady-state consensus and placement independent; define separate contracts for any cross-region synchronous writes.
4. Qualify multi-cluster isolation, directory outage, migration and disaster restore before advertising aggregate scale.

Acceptance: Federation has a standalone reviewed protocol and multi-cluster evidence. Millions of physical nodes remain an unqualified capacity target unless directly substantiated.

Stop conditions: Stop if global directory edits can override a live home-cluster quorum. Do not multiply a smaller benchmark to claim million-node qualification.

| Required test | Observable result |
| --- | --- |
| `home_cluster_fencing` | Conflicting directory observations cannot activate two homes |
| `migration_failure_cut` | Every interrupted transfer preserves one authoritative recoverable state |
| `directory_outage_cached_route` | Existing authorized groups progress without a global metadata write per command |
| `federated_token_scope` | Tokens cannot be misapplied to unrelated cluster/incarnation history |
| `cluster_failure_isolation` | One cluster failure does not invalidate unrelated clusters |

Verification target: `native_raft_federation`. Run the exact list/test commands
shown by `plan.py show MR-17`; the manifest is authoritative for arguments.

## Integration regressions

After storage/execution changes, rerun existing `native_storage`, `native_replication`
and `native_log_protocol`. Host/lifecycle changes additionally require `native_node`,
`native_domains`, `native_cells` and `native_actor`. Effect changes require
`native_outbox` and `native_event_stream`. Axum ingress must pass the crate's
default and no-default-feature modes. These existing tests supplement the new
Raft targets; they do not prove the new protocol by themselves.

Before enabling the first namespace, complete the commands and process/fault
matrix in [qualification.md](qualification.md). Later MR-16/MR-17 work must
update its own design and evidence instead of retroactively widening the v1 claim.
