<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# CG-17 full primitive acceptance cases

Purpose: Explain the 32 required primitive compatibility cases and how their acceptance differs from focused test results.

Each case must satisfy its complete oracle and required API, configuration and lifecycle coverage. The executable definitions in [tasks.json](tasks.json) and the [qualification contract](qualification.md#full-primitive-compatibility-cg-17) are canonical. This reference reproduces the current case definitions.

The [implementation index](implementation-plan.md) credits cases through completed tasks. Passing focused tests are reported separately in [progress](progress.md#current-integration-checkpoint); they do not establish full-case acceptance. Remaining feature and qualification work is in [priorities](remaining-work.md#primitive-delivery-priority).

## 1. primitives_capability_inventory_and_deployment_bounds

Inventory every cellar::sdk primitive and binding with its owning public APIs, adapter, durability boundary, feature modes and required scenarios. Compile representative applications and validate their declared command/envelope, receipt, scheduler, image and archive budgets before activation. Cover the 1 KiB SQL profile versus 60 KiB signed inbox envelope: insufficient explicit budgets refuse deployment; a valid bounded profile serves the same application without a legacy backend fallback. Inventory fully qualified public operations, built-in resource classes, schema owners, transitive binding/provider permissions and supported Rust/Python/JavaScript feature contracts. Intersect application limits with Raft envelope, result/receipt, image, staging and scheduler bounds before activation; authorization comes from host-minted provenance, never caller-supplied internal fields.

Invariants: I01, I12, I14.

## 2. primitives_storage_sql_kv_atomicity

Public Storage, SQL and KV operations preserve their documented transaction, rollback, conditional update, list and isolation behavior through real shared-group commits. Reads use the selected consistency policy and exact cell versions; retry receipts cannot be borrowed across cells. State-backed bindings and external providers have separately asserted acknowledgement boundaries.

Invariants: I02, I03, I04, I05.

## 3. primitives_async_callbacks_commit_and_retry

Existing mutable cell and asynchronous callback APIs run on an owned leader-side orchestration adapter; each durable transaction commits through shared consensus before success is exposed. Awaiting I/O never holds a synchronous lock or runs network work inside the replicated SQL callback. Cancellation, reentrancy, lost responses and leader replacement preserve documented transaction boundaries and uncertain outcomes without replaying a committed mutation.

Invariants: I02, I03, I12, I13.

## 4. primitives_queue_leases_ack_retry_and_dead_letter

Public Queue producers and consumer callbacks exercise enqueue, batch delivery, delayed visibility, retry, acknowledgement, lease expiry, purge and dead-letter behavior. Lease generations and terminal settlement survive actual failover; stale consumers cannot acknowledge a newer delivery. Delivery remains at least once and application effects deduplicate only at their documented transactional inbox boundary. Include partial batch acknowledgements, lost DLQ transfer replies across different groups, delayed old lease settlements, purge during delivery and retention-boundary replay; an enqueue receipt does not prove the consumer business transaction committed.

Invariants: I02, I03, I09, I13.

## 5. primitives_event_stream_outbox_inbox_replay

Public EventStream and Storage emission preserve per-lane order, sealed prefixes, replay cursors, subscription leases/checkpoints and receiver receipts. Business changes and intent commit atomically; lose the destination reply after commit and require byte-identical delivery identity, one receiver mutation and eventual source settlement after restart or transfer. Exercise stale subscription generations, checkpoint/receipt pruning, resource deletion/recreation, slow consumers and retained-reader GC. Separate durable EventStream and Workflow output chunks from ephemeral HTTP/socket byte pipes.

Invariants: I02, I03, I07, I13.

## 6. primitives_workflow_steps_waits_retries_and_cancel

Public Workflow creation, durable steps/results, timers, event waits, retries, streams, cancellation, retention and rollback use replicated journals. Kill the leader before and after step result persistence: completed durable steps stay completed, pending work resumes with the original identity, and an unknown external step is reconciled or reported as unresolved rather than rerun under a new identity. Preserve owned callbacks that continue after timeout, generation/leadership settlement fences, partial stream output, compensation/rollback identity and pending work across definition/deployment changes. Do not suppress an ordinary API error or fabricate a completed step when result settlement is stale.

Invariants: I02, I03, I12, I13.

## 7. primitives_alarms_cron_occurrence_identity

Public alarms and cron persist schedule generations, due times, occurrence IDs and execution history. Leader loss, parked wake and retained restart discover due work without client traffic; rearming/cancellation fence old generations. Retries preserve occurrence identity and obey documented delivery behavior without duplicate committed application effects.

Invariants: I02, I10, I12, I13.

## 8. primitives_blob_filesystem_recovery_closure

Public Blob and Filesystem APIs retain their condition, range, checksum, metadata and durability contracts. Every acknowledged replicated reference has a verified recoverable payload closure at the requested durability; mutable filesystem data is covered by its declared storage contract. Independent restoration checks actual bytes and metadata after reclamation; missing payloads refuse admission. Provider durability is never inferred from quorum metadata alone.

Invariants: I02, I04, I06, I07.

## 9. primitives_http_rpc_and_cross_cell_effects

Public Fetch, services, cell calls and RPC preserve authorization, request deadlines, streaming, cancellation and typed lost-response ambiguity. Cross-cell calls use independent transactions with stable intent/receipt identities where retries are supported, including across different groups. Raw external calls retain documented at-least-once or unknown outcomes; the adapter never advertises cross-cell atomicity or universal exactly-once I/O. Cover per-binding publish/read/consume/admin grants, revocation, foreign namespace and host-minted internal-operation provenance. Include same-group and different-group calls; co-location does not join transactions or convert provider outcomes into quorum receipts.

Invariants: I01, I02, I03, I12, I13.

## 10. primitives_websocket_reconnect_and_stale_owner

Public WebSocket upgrades, message/close callbacks, attachments, tags, auto-responses and hibernation run through explicit session ownership. Kill or move the group leader and reconnect using the documented resume contract: durable attachments/state survive, stale executors cannot mutate or send through a revoked session, and transport loss is reported. Connection continuity and durable message replay are asserted only where the public API promises them.

Invariants: I01, I03, I09, I10, I13.

## 11. primitives_container_restart_and_fencing

Public Container, supervision and sandbox bindings persist desired state and operation identity while the provider enforces current execution fencing. Block an external operation across leader loss, then attempt old-owner execution and restart: revoked execution is refused, the successor reconciles the original operation, and persistent volumes restore according to their declared contract. Process memory and untracked external I/O are not reported as quorum-replicated state.

Invariants: I01, I09, I11, I12, I13.

## 12. primitives_stateless_bindings_and_ephemeral_resources

The SDK inventory accounts for assets, cache, facets, loader, streams, sockets, HTML rewriting, codecs and other utility exports. Exercise their public adapters and authorization after cutover; caches, byte pipes and other ephemeral resources keep explicit disposable/reconnect semantics and never supply a durability proof. Every exported family maps to a passing behavioral case or a documented stateless/ephemeral regression, with no unsupported stateful family omitted.

Invariants: I01, I12, I14.

## 13. primitives_public_sdk_native_axum_cli_admin

One representative application uses the normal public SDK and daemon registration for each primitive family; native and Axum ingress, CLI onboarding and admin lifecycle reach the same shared runtime. More than one cell shares each group and at least two groups serve independently; no per-cell Raft host, custom fixture-only primitive dispatcher or weaker backend is selected. Exercise each entry point where the primitive is publicly exposed.

Invariants: I01, I02, I03, I05, I14.

## 14. primitives_leader_loss_quorum_refusal_and_restart

For every stateful primitive, constrained RF3 processes execute pending work across actual leader SIGKILL, partition, follower catch-up, joined-member replacement and retained restart. A minority cannot acknowledge mutations or strong reads; same-ID retries preserve original results and stale executor/lease generations cannot mutate. Compare independent responder state and complete invocation/effect ledgers.

Invariants: I02, I03, I05, I08, I09, I13.

## 15. primitives_split_merge_placement_and_wakeup

For every stateful primitive, move cells with pending work through split, merge, placement changes and fenced park/wake. Interrupt the transition controller at durable phases, then resume the original journal. Preserve logical cell history, receipts, workflow positions, queue leases, stream checkpoints and effect identities; at most one owner is writable and pending work is neither stranded nor silently discarded.

Invariants: I01, I03, I09, I10, I12, I13.

## 16. primitives_archive_outage_reclaim_and_recovery

For every stateful primitive, real RustFS outage lets Quorum work proceed only within unchanged archive-debt bounds and refuses or leaves explicit S3 requests uncertain until exact coverage exists. Recovery of the backend drains pending work. Reclaim real local log files only after verified dependency coverage, then restore actual primitive data, internal tables, results and referenced payloads independently.

Invariants: I02, I04, I06, I07, I12, I13.

## 17. primitives_zero_quorum_fresh_incarnation

Permanently fence every old voter/executor with the Docker qualification authority, delete all owned voter disks and restore every stateful primitive from real RustFS at one verified cut. Fresh histories and quorum reject old hosts, tokens and execution grants; S3-protected state, results and receipt floors survive, while the Quorum-only suffix/RPO is measured separately. Pending old-origin external effects require authenticated reconciliation or explicit unresolved admission refusal and never receive a fresh delivery ID to hide ambiguity.

Invariants: I01, I03, I06, I11, I13.

## 18. primitives_cancellation_capacity_and_joined_shutdown

Saturate declared work, byte, receipt, scheduler, reader and archive budgets while primitive callbacks and external operations are held. Cancel ingress and stop the node: entered work keeps its owner until joined completion or durable recovery obligation, capacity is accounted, and restart discovers unfinished work. Shared services stay bounded without a worker, timer, election loop or open SQLite connection per idle cell.

Invariants: I02, I03, I12, I13.

## 19. primitives_feature_modes_and_cutover

Representative applications compile and execute through the normal shared backend in materialized and extent modes, managed and unmanaged namespaces, and the supported feature matrix. Feature-off applications preserve their contract. Fresh-root cutover rejects old per-cell layouts instead of reinterpreting user data; no automatic fallback, compatibility reader or legacy serving mode is needed to keep a primitive working. Preserve ordinary Fleet and bucket durability as separately supported backends and their feature/mixed-mode regressions; the prohibited fallback is silently serving a requested Raft mutation with a weaker backend. Existing documented unsupported methods retain their prior behavior and are distinguished from newly missing adapters.

Invariants: I01, I04, I08, I12, I14.

## 20. primitives_complete_coverage_evidence

Retained source-pinned evidence enumerates every SDK family, public entry point and mandatory lifecycle scenario with actual selected tests, process IDs, image modes, durability minima and result/effect ledgers. The oracle rejects missing, ignored, zero-selected or silently downgraded families and mixed-source passes. All required cells of the primitive/scenario matrix pass on one source before CG-17 acceptance; CG-15 and CG-16 repeat the integrated matrix and cannot waive coverage by narrowing product scope. This requires the independent report validator and every proposed inventory/requirements/schema output, not an assertion that only counts top-level family tests. CI and the outer acceptance verifier must reject incomplete reports; missing checker/report wiring keeps CG-17 open.

Invariants: I02, I03, I06, I12, I13, I14.

## 21. primitives_activation_storage_backend_bridge

Run ordinary public Context/Storage and built-in primitive factories through a shared replicated storage adapter, preserving synchronous local preparation, private transaction state, initializer/system-table authority and activation identity. More than one cell shares a group and at least two groups share node workers. Only exact applied quorum receipts can release protected output; stale activation, foreign cell and mixed-domain handles refuse. Inspect actual group histories and local storage owners to exclude an ordinary Fleet implementation hidden behind the SDK.

Invariants: I01, I02, I03, I04, I09, I12.

## 22. primitives_async_nested_transactions_and_output_gates

Exercise synchronous and asynchronous transactions, nested children, read-your-writes, rollback/panic, reentrancy and dropped futures through ordinary public APIs. Hold a callback after its first private write, await external work, replace the leader and release the old continuation: no partial atomic transaction becomes visible and stale continuation settlement refuses. Distinguish local mutation return, quorum commitment, output release, sync/fsync and ambiguous submitted outcomes. A later proven rejection cannot erase an earlier uncertain transaction; retain original invocation, transaction and command identities rather than replaying an entire multi-transaction callback.

Invariants: I02, I03, I04, I05, I09, I12, I13.

## 23. primitives_external_execution_fence_and_reconciliation

Commit stable external-operation intent before dispatch and preserve the same identity across retries, timeout, leader loss, split/merge and recovery. Hold a Workflow/provider callback across its timeout and leadership replacement; actual work remains owned, stale result writes refuse, and a fencing-capable real provider rejects superseded execution generations. An arbitrary HTTP target retains its documented at-least-once/unknown contract; an adapter requiring provider fencing or reconciliation refuses when unavailable. Test provider lookup, idempotency retention and lost replies without claiming that a Raft term or metadata receipt stops external execution.

Invariants: I01, I02, I03, I09, I11, I12, I13.

## 24. primitives_due_work_recovery_without_requests

Persist and rediscover application alarms plus every framework work reason: outbox, stream maintenance, resource reconciliation, filesystem maintenance and sandbox cleanup, with Queue/Workflow/cron obligations mapped explicitly. Kill or park/wake leaders without client traffic and require bounded shared schedulers to resume pending work. Completing one installation cannot clear another reason or generation; undeploying application code retains its existing recovery-only obligations without admitting fresh business work. No per-idle-cell timer/task/connection is created.

Invariants: I02, I03, I07, I09, I10, I12, I13.

## 25. primitives_payload_reference_publication_and_gc

Distinguish ordinary external KV/Blob payloads, immutable in-SQL Raft Blob bytes, transactional SQLite filesystem chunks and provider volumes. Fault before/after payload upload, reference commit and archive publication; lost replies reconcile the exact content/operation and never acknowledge a missing required payload. Collect against all live images, retained checkpoints/archive cuts, readers, transfers and pending uploads, not only current KV rows. Restore actual bytes from an older retained valid cut after newer values are deleted or replaced; missing/corrupt content fails closed. Preserve original quotas and declare provider acknowledgement boundaries independently of metadata quorum.

Invariants: I01, I02, I04, I06, I07, I12.

## 26. primitives_sql_sessions_kv_ttl_and_revision_conditions

Exercise the ordinary public Database/Session/KV binding APIs rather than substituting only CommandContext.sql calls. Preserve SQL batch/migration/rollback semantics, session/bookmark behavior and the binding-specific read policy; authenticate and reject foreign or stale minimum-version tokens where applicable. Verify KV revision conditions, metadata, expiry/TTL, bulk operations, list cursors and streamed/large values before and after movement/restart. Cross-cell/provider bindings retain independent transaction and proof boundaries; one cell/group index cannot stand in for another cell or database token.

Invariants: I01, I02, I03, I04, I05, I12.

## 27. primitives_filesystem_descriptors_transactions_and_gc

Run the existing filesystem conformance profile through the shared storage backend: atomic file plus business-state changes, nested rollback, inode/version edits, rename/unlink with open handles, shared versus independent offsets, directory cookies, sparse chunks, quotas and bounded revision-aware GC. Descriptors expire at their documented invocation/transaction/activation boundaries and cannot be persisted or borrowed across leaders/cells. Inject cancellation, sync failure and lost replies around file mutation and sync/fsync; preserve NotApplied/LocallyCommitted/Unknown semantics and exact operation-receipt reconciliation. delete_all refuses live entries, pins and unfinished reclamation instead of erasing framework state.

Invariants: I01, I02, I03, I04, I07, I09, I12.

## 28. primitives_receipt_retention_pruning_and_recreation

Drive command, inbox, queue-transfer, stream-ack and workflow receipt horizons through explicit pruning, retention expiry, purge, delete/recreate, movement and archived-cut recovery. Before expiry same-ID retries retain exact outcomes; after expiry preserve each API documented stale/unknown/ended-deduplication behavior rather than inventing indefinite exactly-once delivery. Keep identity/generation high-water marks needed to reject old requests; partial batch/DLQ transfers settle per original identities without double application. Collection cannot strand an outstanding delivery or erase another family business/framework tables.

Invariants: I01, I02, I03, I07, I09, I13.

## 29. primitives_schema_definition_and_deployment_changes

Inventory every primitive framework schema and bind its system operations, capabilities and application/Workflow definitions to the deployed contract. Qualify the explicitly supported deployment/schema change path with pending queues, workflow steps, streams and cleanup work; if a change requires a controlled fresh-root cutover or is unsupported, refuse it before activation and record that existing contract. Mixed incompatible definitions cannot vote/execute together; stale executors and client contracts refuse, and undeploy/redeploy cannot silently delete durable obligations. Do not reinterpret existing Fleet data or add legacy format readers.

Invariants: I01, I03, I04, I08, I09, I12, I13, I14.

## 30. primitives_clock_discontinuities_and_timer_generations

Inject forward/backward wall-clock changes, process suspension and leaders with different observed clocks around KV TTLs, Queue leases/delay, Workflow sleeps/retries and cron occurrence boundaries. Preserve the documented wall-clock schedule behavior and monotonic invocation deadlines; sample/capture operation timestamps and random identities consistently for retries rather than recomputing committed effects on followers. Stale timer/lease completions cannot settle newer generations, missed deadlines are rediscovered without traffic, and no test hides a failure by extending operation or enrollment budgets.

Invariants: I02, I03, I09, I10, I12, I13.

## 31. primitives_coverage_manifest_and_rejection

Implement a machine-readable public-symbol inventory, explicit required coverage cells and report schema, plus an independent validator invoked by plan.py check_evidence for CG-17 and the CG-15/16 release reruns. Negative fixtures remove one API/family/scenario/image/namespace/durability/feature/provider/public path, substitute an ordinary backend, mark ignored or zero-selected cases, duplicate cells, corrupt artifact hashes or mix binary/source revisions; every missing mandatory proof is rejected even when all named top-level tests print success. Derive applicability from reviewed API/feature contracts, never from unavailable providers. Classify pre-existing documented unsupported methods explicitly without using them to omit supported stateful families.

Invariants: I01, I02, I12, I14.

## 32. primitives_fleet_bucket_regression_and_no_fallback

Reuse public conformance workloads with explicit backend factories to run ordinary Fleet mode under both fleet and bucket durability, feature-off configurations and supported mixed Fleet/Raft deployments. Preserve existing ownership, output-gate, rollback, API and provider semantics. The shared-quorum run proves its selected group histories and required durability at every acknowledged primitive mutation; missing quorum or an absent adapter cannot silently route that mutation through an ordinary Fleet/bucket backend. Retain independent binding/provider boundaries instead of claiming cross-cell atomicity.

Invariants: I01, I02, I03, I05, I09, I12, I14.
