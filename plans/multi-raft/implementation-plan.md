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

## Dependency order


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
| MR-10 | Install snapshots and recover durable group state | MR-09, MR-05, MR-03 | v1 |
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

Create: `crates/cellar/durability/raft/execution.rs`, `crates/cellar/durability/raft/apply.rs`, `crates/cellar/tests/native_raft_execution.rs`, `crates/cellar/durability/raft/working.rs`, `crates/cellar/durability/raft/receipts.rs`, `crates/cellar/storage/sqlite/command.rs`.

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

Modify: `crates/cellar-logic/cell.rs`, `crates/cellar-logic/output_gate.rs`, `crates/cellar-logic/routing.rs`, `crates/cellar-logic/types.rs`, `crates/cellar/actor/effects.rs`, `crates/cellar/actor/production.rs`, `crates/cellar/actor/output_proofs.rs`, `crates/cellar/node/embedded.rs`, `crates/cellar/fleet/ownership.rs`, `crates/cellar/application/mod.rs`, `crates/cellar/application/cells.rs`, `crates/cellar/sdk.rs`, `crates/cellar/Cargo.toml`, `crates/cellar/application/consensus/mod.rs`, `crates/cellar/durability/raft/runtime.rs`, `crates/cellar/durability/raft/mod.rs`, `crates/cellar/durability/raft/README.md`, `crates/cellar/application/consensus/protocol.rs`, `crates/cellar/node/mod.rs`, `crates/cellar-logic/lib.rs`, `crates/cellar/durability/raft/apply.rs`, `crates/cellar/durability/raft/execution.rs`, `crates/cellar/durability/raft/completion.rs`, `crates/cellar/storage/sqlite/immutable.rs`, `crates/cellar/tests/native_raft_execution.rs`, `crates/cellar/application/README.md`, `crates/cellar-axum/application.rs`, `crates/cellar-axum/builder.rs`, `crates/cellar-axum/tests/builder.rs`, `crates/cellar-axum/README.md`.

Create: `crates/cellar/durability/raft/catalog.rs`, `examples/raft-counter/main.rs`, `crates/cellar/tests/native_raft_host.rs`, `crates/cellar/durability/raft/catalog_wire.rs`, `crates/cellar/tests/support/raft_catalog_fault.rs`, `crates/cellar-logic/raft_command.rs`, `crates/cellar-logic/tests/raft_command.rs`, `crates/cellar/application/consensus/class.rs`, `crates/cellar/application/consensus/client.rs`, `crates/cellar/durability/raft/host.rs`, `crates/cellar/durability/raft/host_sql.rs`, `crates/cellar/tests/support/raft_node.rs`, `examples/raft-counter/counter.rs`, `examples/raft-counter/README.md`, `examples/raft-counter/smoke.py`, `crates/cellar/tests/support/raft_cluster.rs`, `plans/multi-raft/host-audit.md`.

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

Modify: `crates/cellar/application/consensus/mod.rs`, `crates/cellar/application/cells.rs`, `crates/cellar/sdk.rs`, `crates/cellar-logic/routing.rs`, `crates/cellar/application/consensus/client.rs`, `crates/cellar/durability/raft/mod.rs`, `crates/cellar/durability/raft/apply.rs`, `crates/cellar/durability/raft/catalog.rs`, `crates/cellar/durability/raft/catalog_wire.rs`, `crates/cellar/durability/raft/completion.rs`, `crates/cellar/durability/raft/execution.rs`, `crates/cellar/durability/raft/host.rs`, `crates/cellar/durability/raft/host_sql.rs`, `crates/cellar/durability/raft/replica.rs`, `crates/cellar/node/embedded.rs`, `crates/cellar/tests/native_raft_host.rs`, `crates/cellar/tests/support/raft_cluster.rs`, `crates/cellar/tests/support/raft_node.rs`, `examples/raft-counter/main.rs`, `crates/cellar/durability/raft/README.md`, `examples/raft-counter/README.md`, `crates/cellar/durability/raft/transport.rs`, `crates/cellar/durability/raft/transport_frame.rs`, `crates/cellar/durability/raft/transport_options.rs`, `crates/cellar/durability/raft/transport_tests.rs`, `crates/cellar/node/mod.rs`, `crates/cellar/tests/native_node.rs`, `crates/cellar/tests/native_raft_transport.rs`, `crates/cellar/tests/support/raft_network.rs`, `crates/cellar/application/consensus/class.rs`, `examples/raft-counter/counter.rs`, `examples/raft-counter/smoke.py`, `crates/cellar-logic/consensus.rs`.

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

Dependencies: MR-07. Phase: v1. Status: IN_PROGRESS.

The archive storage component now captures exact durable engine/image cuts,
pins their immutable generations, and publishes bounded snapshot/tail/dependency
sets through conditional heads. Each proposed head is downloaded, checked and
physically replayed in a separate generation store before publication. Membership
changes establish a new full snapshot with the configuration from that exact cut.
The node-owned dispatcher now settles S3 output from these proofs, preserves
minimum durability and stronger retries, and bounds backlog admission and image
retention. Final qualification is pending. Atomic Raft snapshot installation and
authorized member recovery remain MR-10. See the
[archive boundary audit](archive-audit.md) and
[storage integration notes](storage-integration.md#committed-archive-storage-component).
The [storage progress evidence](evidence/MR-09-storage-progress.json) records
91 passing Rust cases and the exact checks at source
`122a1c7b9a50af74bf9a8c6ab1efa781986661e3`. It predates the host integration
and does not qualify the newer SDK durability/admission boundary.

Modify: `crates/cellar/durability/raft/apply.rs`, `crates/cellar/durability/raft/catalog.rs`, `crates/cellar/storage/object/bucket.rs`.

Create: `crates/cellar/durability/raft/archive.rs`, `crates/cellar/durability/raft/manifest.rs`, `crates/cellar/tests/native_raft_archive.rs`.

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

Dependencies: MR-09, MR-05, MR-03. Phase: v1. Initial status: TODO.

Modify: `crates/cellar/durability/raft/storage.rs`, `crates/cellar/durability/raft/apply.rs`, `crates/cellar/durability/raft/transport.rs`, `crates/cellar/node/embedded.rs`.

Create: `crates/cellar/durability/raft/snapshot.rs`, `crates/cellar/durability/raft/recovery.rs`, `crates/cellar/tests/native_raft_snapshot.rs`.

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

Verification target: `native_raft_snapshot`. Run the exact list/test commands
shown by `plan.py show MR-10`; the manifest is authoritative for arguments.

### MR-11 Place replicas and repair membership automatically

Dependencies: MR-10, MR-04, MR-07. Phase: v1. Initial status: TODO.

Modify: `crates/cellar/durability/raft/catalog.rs`, `crates/cellar/durability/raft/runtime.rs`, `crates/cellar-logic/rebalance.rs`.

Create: `crates/cellar/durability/raft/membership.rs`, `crates/cellar/durability/raft/placement.rs`, `crates/cellar/tests/native_raft_membership.rs`.

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

Dependencies: MR-08, MR-09, MR-11. Phase: v1. Initial status: TODO.

Modify: `crates/cellar/application/outbox.rs`, `crates/cellar/application/storage/outbox.rs`, `crates/cellar/fleet/wake/mod.rs`, `crates/cellar-logic/alarm.rs`, `crates/cellar/runtime/workers/native/mod.rs`, `crates/cellar/application/consensus/mod.rs`.

Create: `crates/cellar/durability/raft/effects.rs`, `crates/cellar/tests/native_raft_effects.rs`.

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

Dependencies: MR-11, MR-12. Phase: v1. Initial status: TODO.

Modify: `crates/cellar/node/embedded.rs`, `crates/cellar/actor/output_proofs.rs`, `crates/cellar/fleet/management/nodes.rs`, `crates/cellar/fleet/management/routing.rs`, `crates/cellar/cli/cli_options.rs`, `crates/cellar/durability/raft/options.rs`.

Create: `crates/cellar/durability/raft/operations.rs`, `crates/cellar/tests/native_raft_operations.rs`.

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

Verification target: `native_raft_operations`. Run the exact list/test commands
shown by `plan.py show MR-13`; the manifest is authoritative for arguments.

### MR-14 Batch multi-group work and deliver the workload harness

Dependencies: MR-13. Phase: v1. Initial status: TODO.

Modify: `crates/cellar/durability/raft/runtime.rs`, `crates/cellar/durability/raft/storage.rs`, `crates/cellar/durability/raft/transport.rs`, `crates/cellar/Cargo.toml`, `tools/perf/README.md`.

Create: `tools/perf/raft_run.py`, `tools/perf/raft_workload.rs`, `tools/perf/test_raft_run.py`, `tools/perf/raft_profiles.json`, `crates/cellar/tests/native_raft_multigroup.rs`.

1. Optimize shared log batches, pooled transport and fair scheduling while retaining per-group Ready ordering.
2. Deliver the runner interface and reference workload from qualification.md, with executable/image freshness checks.
3. Measure copy/apply cost, group density and failure recovery against current fleet and bucket baselines under equivalent contracts.
4. Record numeric deployment SLO profiles and offered-load methodology before performance qualification.

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
