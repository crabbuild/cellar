#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Qualify shared-group archives, effects and managed requests against local RustFS.

Uses only the recorded owned backend. Each invocation owns a new evidence
directory and remote prefix. Credentials are passed through the child environment,
never arguments or evidence. Positive acknowledgements require the explicit controller binding.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import time

from local import command
from store_probe import probe
from backend_control import BackendControl
from routing_endpoint_evidence import endpoint_observations
from routing_advice_evidence import advice_observations
from archive_owner_evidence import CASE as ARCHIVE_OWNER_CASE, observations as archive_owner_observations
from read_cohort_evidence import CASE as READ_COHORT_CASE, observations as read_cohort_observations

ROOT = Path(__file__).resolve().parents[2]
TARGET = "native_cell_groups_archive_controller"
ARCHIVE_FAIRNESS_CASE = "rustfs_short_archived_groups_checkpoint_and_recover_independently"
NAMED_BINDING_CASES = {
    "rustfs_ordinary_named_cell_bindings_use_shared_group_quorum": False,
    "rustfs_interrupted_named_bindings_recover_after_retained_restart": True,
}
CASES = (
    "rustfs_quorum_continues_bounded_s3_outage",
    "rustfs_group_archive_requires_all_dependencies",
    "rustfs_group_reclaim_restart_keeps_history",
    "rustfs_archive_coverage_gates_local_compaction",
    ARCHIVE_FAIRNESS_CASE,
    "rustfs_s3_remote_outputs_preserve_durability_and_creation",
    "rustfs_s3_remote_deadlines_preserve_ambiguity_and_callbacks",
    "rustfs_s3_remote_slow_delivery_retains_proofs_and_bounds",
    "rustfs_controller_archive_publishes_stable_cuts",
    "rustfs_controller_archive_reconciles_exact_intent_and_joins_close",
    "rustfs_controller_archive_reservation_recovers_after_immutable_response_loss",
    "rustfs_unproved_s3_responses_refuse",
    "rustfs_s3_commands_require_exact_archived_results",
    "rustfs_s3_replays_from_peer_archive_without_new_upload",
    "rustfs_follower_archiver_verifies_a_published_prefix_without_upload",
    "rustfs_follower_archiver_bootstraps_the_first_prefix",
    "rustfs_s3_command_deadlines_do_not_quarantine_committed_sql",
    "rustfs_s3_command_requires_the_sibling_archive_dependencies",
    "rustfs_s3_namespace_minimum_cannot_be_downgraded",
    "rustfs_s3_enrollment_requires_the_original_archived_creation",
    "rustfs_s3_enrollment_deadlines_preserve_resolved_ownership",
    "rustfs_s3_receipt_lookup_tracks_archived_observations",
    "rustfs_late_s3_receipt_reuses_completed_background_verification",
    "rustfs_s3_receipt_lookup_honors_namespace_and_stored_minimums",
    "rustfs_s3_receipt_lookup_preserves_typed_failures_and_result_admission",
    "rustfs_s3_receipt_lookup_deadlines_refund_read_admission",
)

SUITES = {
    "routing-read-cohorts": (
        "native_cell_groups_routing_optimization",
        (READ_COHORT_CASE,),
        "public fresh ReadIndex cohorts, late arrivals and physical image ownership",
        "Both image modes use separate complete and configuration fixtures with real RustFS. "
        "SDK/native HTTP/Axum reads retain original identities, five-second deadlines and "
        "the two-request bound. Actual persistence/apply holds distinguish prequeued, late "
        "and pending follower barriers; higher term and same-term membership cuts refuse "
        "without query callbacks. Cancellation retains physical image owners until joined. "
        "All four fixtures and their exact request, receipt and owner histories must complete. "
        "Constrained node-container cuts and complete RO/CG/OPT acceptance remain separate gates.",
    ),
    "archive-owners": (
        "native_cell_groups_archive",
        (ARCHIVE_OWNER_CASE,),
        "cancelled archive waiters retain actual physical checkpoint readers",
        "Both image modes hold a real filesystem image/extent read, drop the archive caller, "
        "advance all populated cells through Raft/apply and collect obsolete payloads. The physical "
        "reader retains source/archive admission and the old page/image bytes until it exits. "
        "After release, collection removes the old closure and a fresh upload independently verifies "
        "the live checkpoint. This is a host-native component boundary with real RustFS, not full "
        "RL-02/Fleet capacity settlement, constrained-voter or CG/OPT acceptance.",
    ),
    "routing-removed": (
        "native_cell_groups_routing_optimization",
        ("rustfs_removed_member_advice_cannot_grant_authority",),
        "current authenticated advice cannot restore an actually removed voter",
        "Six fresh SDK/nativeHTTP/Axum image configurations perform a real voter replacement using "
        "the existing eight-turn admission, settlement and physical-retirement proof. Handoff, receipt "
        "replay and a query on the new voter establish current client membership before a different "
        "still-current issuer then sends authenticated advice naming the removed member under the "
        "current configuration. One targeted membership repair preserves the range and the complete "
        "pre-admission non-commit proof, with no SQL/callback entry. Restored permission replays the "
        "same command once with exact S3 receipts. Original five-second windows, resource limits "
        "and native host ownership remain. Constrained Linux and full RO-03/CG/OPT acceptance stay open.",
    ),
    "routing-boot": (
        "native_cell_groups_routing_optimization",
        ("rustfs_superseded_boot_advice_cannot_change_route",),
        "a real issuer restart fences a held old-boot reply",
        "Fresh SDK, native HTTP and Axum fixtures in both image modes hold the final byte of an "
        "authenticated command reply while the issuer resumes retained storage under a new physical "
        "boot. Remote writes remain disabled during startup. Public inspection installs the current "
        "boot and a genuine owner query warms routing before release. The ledger records whether "
        "the old socket writes its tail or is fenced, then requires unchanged refusal/SQL/callback "
        "counts, original ambiguous identity, exact S3 recovery and one cached RPC for both the "
        "first recovery and replay. Original five-second windows remain; removed-member wire cuts, "
        "constrained voters and full RO-03/CG/OPT qualification remain open.",
    ),
    "routing-advice": (
        "native_cell_groups_routing_optimization",
        ("rustfs_wire_advice_preserves_admission", "rustfs_untrusted_tls_advice_cannot_grant_authority"),
        "faulty wire advice and untrusted TLS cannot authorize a command",
        "SDK, native HTTP and Axum in both image modes. Ten independently retained command/reply "
        "faults preserve original identities and inputs, no SQL/callback entry under revoked "
        "permission and once-only S3 replay. A separately rejected certificate cannot reach RPC "
        "decoding or become pre-admission proof; its cached trusted socket is joined first. "
        "The ledger requires all 60 wire observations and all six certificate observations. "
        "Keys and TLS exporters are excluded from evidence; retained authenticators are message tags. Original "
        "five-second windows and parent limits remain. Removed-member and superseded-boot wire "
        "cuts, constrained voters and full RO-03/CG/OPT acceptance remain separate gates.",
    ),
    "routing-endpoint": (
        "native_cell_groups_routing_optimization",
        ("rustfs_endpoint_refresh_keeps_membership", "rustfs_unrelated_follower_keeps_leader",
         "rustfs_read_first_refreshes_endpoint", "rustfs_unrelated_endpoint_keeps_inflight_reply"),
        "authenticated boot changes repair physical endpoints without membership or range repair",
        "Four public cases cover SDK, native HTTP and Axum in both image modes. "
        "A retained-disk voter restart keeps ingress warm. Endpoint metadata is denied until "
        "the original command selects bounded endpoint discovery, with no SQL/callback entry. "
        "Verified new boot metadata restores one application RPC, exact S3 receipts and replay, "
        "unchanged directory/membership and no ingress replicas. An unrelated restarted follower "
        "keeps its endpoint metadata unavailable while the healthy leader serves all three callers. "
        "Read-first Strong/AtLeast queries repair only their selected endpoint. A held real "
        "group-archive PUT keeps an S3 command entered while unrelated endpoint metadata changes; "
        "the unchanged leader's reply and exact receipt replay remain valid. Original deadlines "
        "and parent budgets apply. Forged advice and full RO-03 remain separate gates.",
    ),
    "routing-authority": (
        "native_cell_groups_routing_optimization",
        ("rustfs_cached_advice_respects_origin_revocation",),
        "cached leader advice respects current namespace command-origin permission",
        "Two fresh RF3/ingress fixtures cover SDK, native HTTP and Axum in both image modes. "
        "A warm direct route cannot bypass receiver-side origin revocation; denied calls enter "
        "neither SQL staging nor application callbacks. Independent read permission remains usable. "
        "Stale grant revisions and an unverified process refuse; a newer valid grant restores the "
        "same original command ID with exact S3 receipts and no duplicate callback. Original "
        "deadlines and resource limits apply. Forged/lower/conflicting-term advice, restarted "
        "endpoint wire cuts, constrained voter processes and full RO-03 remain separate gates.",
    ),
    "routing-range-repairs": (
        "native_cell_groups_routing_optimization",
        (
            "rustfs_membership_refresh_keeps_range",
            "rustfs_split_repairs_only_overlap",
            "rustfs_old_lookup_cannot_resurrect_parent",
        ),
        "public membership and split repair with original identities and S3 receipts",
        "Both image modes, seven host-native TLS nodes and the original two-history/byte limits. "
        "Membership replacement and delayed split lookup each run through SDK, native HTTP and Axum "
        "with separate object prefixes. A new-voter leader forces demand membership repair while "
        "the cell range stays unchanged. Removed replicas refuse local SQL and explicit targeting; "
        "their independently granted ingress may still forward remotely. Split repair preserves "
        "unrelated cached ranges, historical witnesses and exact S3 command receipts. Real physical "
        "metadata gates use the original five-second call windows. Peer/boot wire authority cuts, "
        "recovery publication, process crashes, measured load and complete optimization acceptance "
        "remain separate gates.",
    ),
    "named-bindings": (
        "native_cell_groups_primitives",
        tuple(NAMED_BINDING_CASES),
        "ordinary named calls and autonomous interrupted-call recovery on shared RF3",
        "Both image modes and Quorum/S3 source durability with a Quorum target minimum. "
        "The ordinary case checks exact calls, structured values, cancellation, recursive "
        "cursors and retained restart. Recovery joins the interrupted producer, restarts "
        "retained RF3, waits for autonomous settlement, preserves the parent's unknown "
        "result and target value, and advances the sealed target sequence. Each case "
        "must emit all four completed configurations. Target S3 minimum, cross-class "
        "selection, concurrent creation, movement/disaster and full CG-17 remain open.",
    ),
    "ordinary-alarms": (
        "native_cell_groups_primitives",
        ("rustfs_ordinary_alarms_recover_through_shared_quorum",
         "rustfs_axum_alarms_recover_through_shared_quorum"),
        "ordinary alarm SDK/native/Axum paths and autonomous RF3 retained restart",
        "Both image forms and Quorum/S3 namespace minima, with one SQL worker per node. "
        "Public named SDK enrollment and alarm APIs, pinned-image reads, transactional "
        "rollback/cancellation, same-deadline rearming, stale Context refusal, "
        "interrupted callbacks, autonomous retained restart and quorum refusal are checked. "
        "RF3 voters share one native process. No operator CLI is exercised. "
        "Application failure/backoff exhaustion, archive outage, movement/wake, "
        "independent voter limits, load and all-voter disk loss remain separate gates.",
    ),
    "immutable-payloads": (
        "raft_cell_group_payloads",
        ("rustfs_immutable_payloads_confirm_bytes_and_preserve_old_recovery_cuts",),
        "namespace-bound immutable external payload publication and verified reads",
        "Data protocol only; no Raft voters or ordinary KV bindings are launched. "
        "Real conditional creation, lost PUT replies, concurrent publishers, cold reopen, "
        "old/new cell incarnations, corruption, body deadlines, cancellation and "
        "shared operation/retained-byte admission are checked. "
        "Objects are retained without deletion. Root-aware reclamation, fenced KV row "
        "publication and combined lifecycle/disaster recovery remain separate gates.",
    ),
    "ordinary-kv-bindings": (
        "native_cell_groups_primitives",
        ("rustfs_ordinary_kv_bindings_use_managed_shared_groups",
         "rustfs_axum_kv_bindings_use_managed_shared_groups"),
        "ordinary KV SDK and authenticated operator/CLI on shared RF3 with real RustFS",
        "Both image modes, separate source/KV groups, one SQL worker per node, "
        "16-KiB command inputs and unchanged result limits. Native and Axum ingress, "
        "the shared Fleet/Quorum API contract (five separately admitted source phases "
        "with 20-second parents), 1-MiB and 25-MiB values, immutable "
        "wire transport, conditional revisions, autonomous expiration, retained restart, "
        "receipt replay, archive-write outage and quorum refusal are checked. "
        "Large values require the payload provider. Voters share one host process; "
        "interrupted large calls, external-object recovery closure, root-aware reclamation, "
        "independent voter limits, load and disk-loss recovery remain separate gates.",
    ),
    "ordinary-sql-bindings": (
        "native_cell_groups_primitives",
        (
            "rustfs_ordinary_sql_bindings_use_managed_shared_groups",
            "rustfs_axum_sql_bindings_and_operators_use_managed_shared_groups",
            "rustfs_interrupted_sql_bindings_recover_without_replaying_business_calls",
        ),
        "ordinary SQL SDK, authenticated operator/CLI and interrupted parent recovery on shared RF3",
        "Both image modes, separate source/database groups and one SQL worker per node. "
        "Native and Axum-owned listeners use the public SDK and actual frozen CLI. "
        "Rollback, private-contract refusal, grant revocation/regrant, lost committed replies, "
        "retained restart, receipt replay and quorum refusal are checked. "
        "Recovery covers SQL successes and rolled-back errors after child reply but before "
        "parent completion, with Quorum/S3 sources and a Quorum destination minimum. "
        "Original deadlines and quotas remain unchanged. Target-S3, before-child-completion, "
        "movement/wake/archive/disaster, independent voters and sustained load remain separate gates.",
    ),
    "queue-primitives": (
        "native_cell_groups_primitives",
        (
            "rustfs_queue_load_preserves_acknowledged_messages",
            "rustfs_queue_four_in_flight_preserves_acknowledged_messages",
            "rustfs_queue_cross_deployment_faults_preserve_quorum",
            "rustfs_queue_axum_cross_deployment_faults_preserve_quorum",
            "rustfs_queue_long_callbacks_preserve_progress",
            "rustfs_ordinary_queue_callbacks_use_shared_quorum",
            "rustfs_ordinary_queue_axum_callbacks_use_shared_quorum",
        ),
        "ordinary Queue producer/callback/settlement through shared RF3 with real RustFS",
        "Both image modes, one SQL worker per node. Expanded workloads observe an initial archived "
        "group cut before setup; this does not qualify cold first-use S3 latency. Native public ingress, declared bindings, "
        "ack/retry/dead-letter delivery, authenticated Queue operator controls, exact producer replay, "
        "archive-write refusal, joined retained restart and autonomous interrupted-consumer recovery, "
        "leader transfer and quorum-loss refusal. Separate producer, consumer and DLQ deployments, "
        "partial retry, stale handlers, active purge, expired enqueue replay, cross-group S3 DLQ "
        "reply loss, 65-second callbacks and two Queues with two or four requests in flight are checked. "
        "Voters share the host process; complete CG-17 conformance, deployment removal, "
        "clock discontinuities, independent Linux voter limits and disk-loss recovery remain separate gates.",
    ),
    "ordinary-primitives": (
        "native_cell_groups_primitives",
        (
            "rustfs_ordinary_json_rpc_uses_raft_owned_context",
            "rustfs_ordinary_cell_activation_cache_is_bounded_and_fenced",
            "rustfs_ordinary_cell_rpcs_interleave_between_proven_roots",
            "rustfs_ordinary_cell_sdk_dispatch_requires_source_proof",
            "rustfs_ordinary_cell_sdk_resolves_current_histories",
            "rustfs_storage_reads_and_writes_exact_cell_versions",
        ),
        "ordinary JSON cell RPC, Storage checkpoints and bounded native activation on shared RF3",
        "Both image modes. Checkpoints use two cells sharing a group and one SQL worker per node. "
        "Exact retries, nested rollback, bounded owned transactions/syncs, independent commits, "
        "pending receipt lookup, dropped responses, explicit S3 proof and live leadership "
        "transfer are checked. Basic request and cancellation budgets remain unchanged; "
        "multi-stage conformance requests declare twenty seconds before admission. "
        "A second case checks persistent native instances across RPCs, expired invocation handles, "
        "global eviction across two SQL workers, real sixty-second idle expiry, leader fencing, "
        "retained RF3 restart and receipt replay without constructing another instance. "
        "A third case interleaves same-client invocations on one native instance between proven "
        "roots, reopens the latest image, holds private DML/DDL and atomic transactions exclusive, "
        "and fences a parked continuation after leader transfer. Joined stop cancels parked work; "
        "retained RF3 restart restores its first root and unfinished receipt without replay. "
        "It runs Quorum and explicit S3. "
        "A fourth case gates dependent SDK command, query, enrollment and receipt calls on "
        "source proof with independent target durability. It checks dropped child drainage, "
        "ordered-root deadline accounting, callback refusal, recursive calls and leader fencing "
        "with the original child identity and uncertainty. Proven SDK waits release the writer. "
        "A fifth case resolves current cell histories through authenticated read-only quorum discovery, "
        "then constructs and invokes a target handle inside an ordinary RPC after source proof. "
        "It checks absent names, original deadlines, callback refusal, leader transfer and quorum loss. "
        "A sixth case checks committed SQL/KV and keyset cursors, exact cell/version isolation, "
        "range bounds, structured values, mutation, invalidation, rollback and expired views. "
        "Declared bindings, automatic dispatch gates, other primitive families, process "
        "faults, disk-loss recovery and full CG-17 acceptance remain separate gates.",
    ),
    "managed-scheduler-fanout": (
        "native_cell_groups_scheduler",
        ("rustfs_control_plane_fanout_bounded",),
        "bounded scheduler inventory and targeted observations across actual shared groups",
        "Both image modes, 64 enrolled SQL cells and at least four RF3 groups on fifteen host-native TLS nodes. "
        "The standard planner splits during batched enrollment and writes, preserving the original two engine slots, "
        "per-node physical/byte limits and three recovery slots. Recovery retains the original sixteen-cell "
        "source/target count and byte bounds; batched sources fit 32-cell checkpoints. Directory pages contain at most two routes; stable local "
        "scheduler cuts are reused, every live group is sampled, overflow refuses before sampling, and metadata "
        "polls cannot mutate the topology journal. Creation/command replay, linearizable SQL and joined stop are checked. "
        "Independent processes/domains, sustained load and physical peaks remain separate qualification.",
    ),
    "managed-disjoint-partitions": (
        "native_cell_groups_transition_service",
        ("automatic::rustfs_partition_sampling_interval_bounds_metadata",
         "automatic::rustfs_automatic_disjoint_partitions_merge_and_resume"),
        "configured split and automatic merge across disjoint RF3 source sets",
        "Both image modes, nine host-native TLS nodes and original per-node physical limits. "
        "The standard planner selects two disjoint RF3 children under real SQL load. "
        "Controller replacement resumes the selected split; no controller hosts both merge parents. "
        "Bounded adjacent-range discovery and authenticated remote leader telemetry select the quiet "
        "merge through the existing archive-backed transition. SQL, retry receipts, fresh quorum "
        "writes and joined shutdown are checked. A stable-group sampling case checks that early "
        "local and remote repeats perform no catalog or peer work. Independent processes/domains, sustained load "
        "and physical peak measurement remain separate gates.",
    ),
    "managed-automatic-repair": (
        "native_cell_groups_scheduler",
        ("rustfs_repair_preserves_failure_domains_and_reserve",),
        "configured exact-boot failure repair restores RF3 with declared separation and reserve",
        "Both image modes and original fixture resource/deadline bounds. Six host-native TLS nodes "
        "declare four regions; two spares share surviving voter domains and must remain unused. "
        "An unfinished automatic balance yields through durable cancellation. Joined fixture fencing "
        "resumes the same operation after controller replacement; a second domain loss before repair "
        "refuses quorum writes and linearizable reads. Retained return reconciles the ambiguous command, "
        "then repair preserves cells/receipts/SQL, restores RF3 and retains the old disk and recovery room. "
        "Declared labels and joined fixture fencing do not qualify independent hosts, Docker launch "
        "revocation, sustained load or measured physical peaks.",
    ),
    "managed-automatic-topology": (
        "native_cell_groups_transition_service",
        ("automatic_topology::rustfs_configured_topology_balances_groups_and_resumes",
         "automatic_topology::rustfs_configured_topology_preserves_recovery_room",
         "automatic_topology::failure::rustfs_admitted_balance_yields_to_repair_across_controller_restart"),
        "configured whole-group balancing resumes its durable intent after controller replacement",
        "Both image modes, six host-native TLS voters and two engine slots per node. "
        "The positive case declares 1-GiB logical admission within the unchanged 1-GiB disk/3-GiB memory fixture attestation; "
        "the original 512-MiB logical profile must refuse all moves and continue quorum SQL. "
        "Startup waits for its exact boot capacity activation. Two occupied shared groups rebalance "
        "with unchanged cell routing, receipt replay, linearizable SQL reads and fresh quorum writes. "
        "Explicit operator policy takes precedence and hands back through a canonical request. "
        "An actual admitted move also survives old-voter loss and retained controller restart with its "
        "original learner/debit, exact-boot fixture fence replay, mixed quorum SQL and terminal RF3 repair. "
        "Independent failure domains, voter processes, sustained load, larger inventories and measured peaks remain separate gates.",
    ),
    "managed-retained-cleanup": (
        "native_cell_groups_scheduler",
        ("rustfs_retained_member_cleanup_resumes_on_qualified_boot",),
        "retained-engine cleanup resumes across qualified boots with node-owned startup",
        "Both image modes, original fixture bounds. Restart before GC and after GC/before settlement. "
        "Fresh capacity and original boot witness are mandatory; stale process and missing-witness attempts refuse. "
        "Configured startup owns automatic cleanup without an external handle, duplicate workers refuse, "
        "and a later boot replays the committed receipt without another refund. SQL/receipts/versions/S3 persist. "
        "Independent voter processes, domain-loss, mixed load and measured resource peaks remain separate gates.",
    ),
    "managed-member-cleanup": (
        "native_cell_groups_scheduler",
        ("rustfs_managed_member_cleanup_resumes_after_worker_restart",),
        "node-owned targeted cleanup resumes durable work after worker replacement",
        "Both image modes and unchanged fixture capacity. Two completed moves leave the first removal historical. "
        "A real settlement CAS is paused after local GC and cancelled by joined worker shutdown. "
        "The replacement discovers the node queue without caller-supplied reservations and settles once. "
        "Generic queue deletion refuses, empty restart makes no root mutation, and SQL/receipt/version/S3 checks persist. "
        "Successor boots, managed daemon startup, independent failure domains, load and measured peaks remain separate gates.",
    ),
    "managed-member-reclamation": (
        "native_cell_groups_scheduler",
        ("rustfs_removed_member_reclaimed_and_returns_with_fresh_id",),
        "completed live-member removal drains and collects the old copy before return with a fresh member ID",
        "Both image modes, host-native RF3 voters and unchanged fixture capacity. "
        "A surviving member cannot spend another member's completion. Local GC is joined, "
        "bounded and exactly replayable; an actual slot/disk-settlement CAS loses confirmation "
        "and replays once. Generic cleanup mutation refuses, old genesis refuses, and the original node returns "
        "to the same shared group with a higher member ID. Receipts, token reads and S3 writes "
        "remain intact. Known group disk reservations settle once; unknown disk debt and installed memory remain charged. Automatic cleanup discovery, retained "
        "successor cleanup, crash injection, failure domains/reserve/fanout, independent "
        "voter processes and measured peaks remain separate gates.",
    ),
    "managed-group-placement": (
        "native_cell_groups_scheduler",
        ("rustfs_whole_group_scale_out_in",),
        "rooted desired placement moves live shared-group replicas with their logical histories intact",
        "Both image modes with host-native RF3 voters and unchanged two-slot fixture capacity. "
        "Public operation retries recover their exact rooted intent. The node-owned movement "
        "worker is replaced during Joint consensus and converges the original operation. "
        "Shrinking the keep set moves replicas of the same group, preserving cell identities, "
        "receipts, minimum-version reads and S3 writes. Obsolete copies remain retained and "
        "charged. Their physical cleanup, node deletion, failure-domain loss/reserve, larger "
        "inventories, load and measured peaks remain separate gates.",
    ),
    "managed-source-settlement": (
        "native_cell_groups_transition_service",
        ("rustfs_consented_split_settles_source_capacity",),
        "rooted source occupancy is captured, collected and refunded once before transition completion",
        "Both image modes with host-native RF3 voters and unchanged two-slot fixture capacity. "
        "A pending participant blocks completion and generic intent mutation refuses. The last "
        "settlement loses its actual root CAS confirmation; replaced services replay completion. "
        "A split followed by merge reuses source capacity while preserving receipts and S3 writes. "
        "Byte/memory refunds, independent voter processes, measured peaks, automatic placement "
        "and the complete balancing/reserve/fanout matrix remain separate gates.",
    ),
    "managed-source-reclamation": (
        "native_cell_groups_transition_service",
        ("rustfs_consented_split_reclaims_retired_source",),
        "selected retired sources release engine and private page owners before local slot reuse",
        "Both image modes, host-native RF3 voters and unchanged fixture capacity. "
        "An atomic engine tombstone permanently refuses the source group ID. "
        "Collection preserves colocated child reads, receipt retries and S3 writes; "
        "retained restart reopens the refusal and exact cleanup replay. Fleet resource "
        "floor refunds, repeated placement cycles, crash injection, process RSS, disk peaks "
        "and constrained independent voter processes remain separate gates.",
    ),
    "managed-transition-capacity": (
        "native_cell_groups_transition_service",
        ("capacity::rustfs_split_capacity_keeps_parent_writable",
         "capacity::rustfs_split_capacity_counts_unmaterialized_births",
         "capacity::rustfs_split_capacity_respects_platform_storage"),
        "pre-seal capacity refusal preserves writable parents and rooted reservation debt",
        "Both image modes, host-native RF3 voters and fixture capacity. Colocated parents "
        "remain charged; a prior controller's unmaterialized birth cannot impersonate an "
        "empty destination. A two-slot placement with an oversized storage reservation also refuses. "
        "Exact retries after coordinator replacement refuse before "
        "sealing, parents serve quorum reads/writes, and cancellation retains unrelated "
        "reservations. No process RSS, temporary disk peak, independent process, load, "
        "automatic placement or physical reclamation qualification is claimed.",
    ),
    "managed-live-retirement": (
        "native_cell_groups_transition_service",
        ("rustfs_consented_split_retires_live_without_host_restart",
         "rustfs_consented_split_keeps_colocated_children_running"),
        "consented split drains and detaches every parent owner without restarting any voter host",
        "Both image modes, host-native RF3 voters and fixture capacity. All final source "
        "members must durably apply retirement before any owner detaches. Original parent "
        "processes stay live, engine histories remain charged, one child stays colocated with each "
        "parent within the unchanged two-history bound, and ingress "
        "reads/S3 writes continue. Physical disk deletion/reclamation, automatic plan selection, "
        "peak-capacity admission, independent processes, permanent fencing and load remain separate gates.",
    ),
    "managed-transition-service": (
        "native_cell_groups_transition_service",
        ("rustfs_consented_split_resumes_with_owned_workers",),
        "consented node-owned split execution resumes after controller replacement and retained voter return",
        "Both image modes, host-native RF3 voters and fixture capacity. A manual plan stays "
        "cancellable until durable execution consent. The coordinator resumes preparation "
        "after a registry write refusal. Retained parent restart supplies the existing physical "
        "quiescence proof before holds release. Live per-group quiescence, automatic plan "
        "selection, independent processes, permanent fencing and load are separate gates.",
    ),
    "managed-automatic-partitions": (
        "native_cell_groups_transition_service",
        ("automatic::rustfs_automatic_partitions_split_merge_and_resume",
         "automatic::rustfs_automatic_placement_preserves_recovery_reserve"),
        "configured RF3 placement, automatic split, cold controller resume, quiet merge and recovery reserve refusal",
        "Both image modes, host-native TLS voters and fixture capacity. Actual SQL writes trigger sizing. "
        "Deployment configuration supplies no placement callback or child identities. Native service suspension "
        "joins controllers while voters stay live; it is not an OS process crash. Independent hosts and sustained load remain separate gates.",
    ),
    "managed-partition-cancellation": (
        "native_cell_groups_transition_service",
        ("cancellation::rustfs_staged_partition_cancellation_fences_delayed_admission",
         "cancellation::rustfs_partial_partition_cancellation_recovers_birth_and_settlement",
         "cancellation::rustfs_staged_cancellation_fences_transition_slot_admission",
         "selection::rustfs_automatic_selection_resumes_partial_admission_and_executes"),
        "staged cancellation fences actual delayed child CAS writes and recovers permanent birth decisions",
        "Both image modes, host-native RF3 TLS voters and unchanged fixture bounds. Cancellation "
        "wins with zero, one or two reserved children, races transition-slot admission and loses root/birth/settlement "
        "confirmations and resumes on another controller without replacement identities. Old "
        "authorization cannot materialize a cancelled birth; reserved debt remains. Foreign "
        "principals, payload changes and generic deletion refuse. Historical completion cannot "
        "erase newer work or cancel another operation's child. Original SQL receipts, fresh quorum "
        "writes and reads survive. An admitted transition refuses staged cancellation and proceeds "
        "through its existing execution owner. Already materialized births, competing automatic "
        "selection races, independent voter processes and sustained load remain separate gates.",
    ),
    "managed-partition-service": (
        "native_cell_groups_transition_service",
        ("partition_service::rustfs_owned_partition_service_recovers_and_executes",),
        "node-owned automatic partition selection and cold recovery execute an explicitly consented split",
        "Both image modes, host-native RF3 TLS voters and unchanged fixture bounds. A blocked "
        "planner is cancelled and joined; duplicate owners refuse. A lost actual selection CAS "
        "reply precedes both child births. Another node recovers without placement callbacks or "
        "caller-held IDs. AdmissionOnly stays Planned; a foreign principal cannot consent. Explicit "
        "Complete policy drives the original plan through live retirement and child archives, "
        "preserving receipts, minimum-version reads and fresh S3 writes. Independent processes, "
        "competing selection races, stale-work cancellation, adaptive placement, reclamation peaks "
        "and sustained load remain separate gates.",
    ),
    "managed-automatic-selection": (
        "native_cell_groups_transition_service",
        ("selection::rustfs_automatic_selection_resumes_partial_admission_and_executes",),
        "automatic local group discovery resumes partial admission and executes a consented split",
        "Both image modes, host-native RF3 TLS voters and fixture capacity. The sampler discovers "
        "the group, waits for hysteresis and chooses a whole-cell split. A rooted cursor persists "
        "the full request before child reservations. Lost staging/reservation replies, recovery on "
        "another coordinator without caller-supplied IDs, missing payload/plan and changed-plan "
        "refusal, generic deletion refusal and frozen pending discovery are checked. Terminal cursor "
        "settlement loses its real CAS reply and replays once without re-enqueueing the operation. "
        "Both live local children are discovered in turn; a one-tracker profile refuses and "
        "retained retired parents are excluded. "
        "The existing node-owned worker then runs through coordinator replacement, live source "
        "retirement and original receipts/versions plus child S3 writes. Deployment placement and "
        "identities and execution consent remain explicit. Independent processes, physical source "
        "reclamation, large-fleet fanout and sustained load remain separate gates.",
    ),
    "managed-partition-scheduler": (
        "native_cell_groups_scheduler",
        ("rustfs_scheduler_hysteresis_and_hot_isolation",
         "rustfs_scheduler_cannot_grant_authority"),
        "adaptive partition admission retains partial reservations and refuses missing quorum",
        "Both image modes, host-native RF3 voters and fixture capacity. Sizing uses real "
        "SQLite geometry and owner counters. Lost reservation confirmation and joined "
        "shutdown and retained return of both followers are exercised. The minority write is "
        "reconciled with its original command identity before joined shutdown. Physical split execution, whole-group "
        "balancing, autonomous inventory selection and permanent fencing are separate gates.",
    ),
    "archive": (
        TARGET, CASES,
        "shared-group RustFS archive/setup and local/remote command/enrollment/receipt S3 settlement",
        "The bounded full-backend outage case controls only this owned container. "
        "Logical retained-entry admission and controlled-geometry Raft file purge are tested. "
        "Global physical disk bounds, process-kill restart, fencing/DR and sustained performance "
        "remain separate gates.",
    ),
    "archive-reservation": (
        TARGET,
        ("rustfs_controller_archive_reservation_recovers_after_immutable_response_loss",),
        "durable reservation intent recovers an upload after an accepted immutable PUT loses its response",
        "Both image modes use real host-native RF3 engines and conditional RustFS I/O. "
        "The original controllers, SQL workers and Raft runtimes join before an independent "
        "archive domain reopens the registry. The persisted selected root is independently "
        "recovered; exact ticket replay crosses an unrelated registry revision without another "
        "allocation. Collection preserves partial uploaded data and complete SQLite verification "
        "precedes publication. This archive-owner handoff is not a full node/process restart, "
        "power-loss or constrained-voter performance qualification.",
    ),
    "archive-publication-race": (
        "native_cell_groups_archive",
        ("rustfs_group_archive_publication_survives_competing_registry_cas",),
        "verified publication keeps its exact upload through a competing registry CAS",
        "The fixture gates the conditional write and drops the successful provider reply. "
        "Managed process failover and sustained load remain separate gates.",
    ),
    "effects": (
        "native_cell_groups_effects",
        ("rustfs_claim_receiver_and_completion_use_verified_coverage",),
        "shared-group RustFS outbox claim, receiver receipt/deduplication and complete S3 recovery closure",
        "Live effect S3 settlement and independent complete archive replay. Backend outage, "
        "live group transfer, external-effects disaster recovery and sustained load remain separate gates.",
    ),
    "sealed-sources": (
        "native_cell_groups_host",
        ("rustfs_sealed_source_archives",),
        "exact transition source seals, independently replayed RustFS holds and retained source-set CAS",
        "Three host-native voters and the unmanaged transition boundary. Managed capacity, child "
        "install, range publication and independent voter processes remain separate gates.",
    ),
    "managed-ingress": (
        "native_cell_groups_managed",
        ("bootstrap::requests::rustfs_managed_native_axum_read_write_and_retained_restart",
         "bootstrap::lifecycle::cold_router::rustfs_managed_cold_router_returns_after_all_birth_voters_retire"),
        "managed operator provisioning, native/Axum authority, quorum requests, retained restart and replacement-voter discovery",
        "Managed boot and capacity attestations use fixture inputs. Separate voter processes, "
        "production peer authentication and sustained load remain separate gates.",
    ),
    "managed-transition": (
        "native_cell_groups_transitions",
        ("bootstrap::transition::rustfs_managed_split_plan",
         "bootstrap::transition::rustfs_managed_split_authorization"),
        "managed split cancellation, sealed archive replay, quorum child installation, publication and retained source tokens",
        "Host-native voters and fixture capacity. Phase-crash schedules, full effect continuity "
        "and permanent platform fencing remain separate gates.",
    ),
    "managed-disaster-retry": (
        "native_cell_groups_disaster",
        ("rustfs_disaster_retry_and_old_token_refusal",),
        "recovery-seeded SDK command retries retain results and fresh-history Quorum/S3 proofs",
        "Both image modes and host-native RF3 TLS voters with real RustFS. The fixture erases "
        "all source disks, restores the selected child tail, confirms archived receipts, then "
        "replays their original command IDs and inputs through the ordinary SDK. Old tokens "
        "refuse; unchanged cell versions and subsequent SQL values detect callback replay. "
        "Cold operator verification is retained. Permanent Docker launch revocation and the "
        "complete CG-13 profile remain separate gates.",
    ),
    "managed-disaster-outbox": (
        "native_cell_groups_disaster",
        ("rustfs_disaster_outbox_ambiguity_refuses",),
        "uncertain old-origin delivery remains blocked after managed fresh-history recovery",
        "Both image modes, host-native RF3 TLS voters and real RustFS. An external adapter holds "
        "an uncertain reply after the source claim reaches S3. All source disks are erased and "
        "callbacks joined. Recovery preserves the pending envelope, exposes reconciliation status, "
        "and dispatches an independent fresh lane without reissuing the old request. A fresh "
        "operator executable reopens the signed fence from S3 using serialized selectors and a "
        "pinned public key; foreign keys and changed selectors refuse. "
        "The external adapter is in process; no external exactly-once guarantee or permanent Docker "
        "launch revocation is claimed. Independent failure domains and load remain separate gates.",
    ),
    "managed-transition-disaster-closure": (
        "native_cell_groups_disaster",
        ("rustfs_transition_disaster_selects_complete_closure",),
        "managed recovery restores unpublished parent domains and published child S3 tails after voter disk erasure",
        "Both image modes and host-native RF3 TLS voters. The fixture joins all source owners "
        "before erasing their disks, independently replays the selected RustFS cuts, and requires "
        "fresh identities, quorum activation, receipt inspection, old-token refusal, retained restart "
        "and subsequent Quorum/S3 writes. A fresh operator executable reopens only from S3, "
        "serialized selectors and a pinned public key; foreign keys and changed selectors refuse. "
        "Platform fencing is a contract fixture; permanent Docker "
        "launch revocation, external-effect ambiguity and sustained-load qualification remain separate gates.",
    ),
    "managed-publication-loss": (
        "native_cell_groups_transitions",
        ("rustfs_publication_requires_remote_transfer_closure",),
        "published transfer baseline survives erasure of every voter disk and verifier inventory",
        "Both image modes and host-native RF3 voters. Every voter joins before its owned disk is erased, "
        "before child activation or child archives. Fresh clients and private inventories replay only the "
        "pinned S3 closure, including pending outbox, original inbox proof and prior S3 receipts. "
        "Live disaster activation, OS process-kill and permanent platform fencing remain separate gates.",
    ),
    "managed-effect-transfer": (
        "native_cell_groups_transitions",
        ("rustfs_transfer_preserves_tokens_receipts_effects",),
        "S3 outbox and receiver deduplication survive a real split, retained restart and source cleanup",
        "Both image modes and host-native voters. The receiver commits before its first reply is lost. "
        "A child retries the original envelope, then both child archives are independently replayed. "
        "OS process-kill, permanent platform fencing and sustained load remain separate gates.",
    ),
    "managed-phase-restart": (
        "native_cell_groups_transitions",
        ("rustfs_transition_restart_retains_single_writer",),
        "controller loss and exact operation resume through all nine forward transition phases",
        "Both image modes and host-native voters. Each phase cancels an observed provider read, "
        "reopens fresh metadata clients and verifies serving history. Joined voter restarts and lost "
        "CAS replies are included. OS process-kill and independent-host schedules remain separate gates.",
    ),
    "managed-retirement": (
        "native_cell_groups_transitions",
        ("rustfs_retirement_keeps_required_archive_pins",),
        "managed retirement retains source pins on child archive outage and independently restores children after cleanup",
        "Both image modes, host-native voters and fixture capacity. The retired source archive is denied "
        "during fresh child replay. All-voter disk loss, permanent platform fencing and sustained load "
        "remain separate gates.",
    ),
    "managed-split-merge": (
        "native_cell_groups_transitions",
        ("bootstrap::transition::merge::rustfs_managed_recovery_barrier_fences_transfer_and_reopens",
         "bootstrap::transition::merge::rustfs_managed_split_merge"),
        "managed split and merge through retirement, with restored SQL after source disk erasure",
        "Host-native voters, joined source shutdown and erasure of test-owned disks. "
        "Fixture capacity and fencing authority. Permanent Docker launch revocation "
        "and sustained load remain separate gates.",
    ),
    "managed-partial-merge": (
        "native_cell_groups_split_merge",
        ("rustfs_partial_merge_failure_no_double_writer",),
        "partial merge refuses every child setup step until all sealed sources verify",
        "Both image modes; one parent remains durably sealed while the other lacks quorum. "
        "Fresh control clients probe all three reserved child voters, then retry the original "
        "install after quorum returns and finish publication, activation and retirement. "
        "Voter loss uses joined shutdown; capacity is a fixture attestation. "
        "Independent process crashes and permanent launch revocation remain separate gates.",
    ),
    "managed-precopy-resources": (
        "native_cell_groups_split_merge",
        ("rustfs_transition_precopy_resource_bounds",),
        "bounded managed pre-copy keeps entered worker debt while live quorum requests progress",
        "Both image modes with a real child-page directory-sync barrier. Logical restore operation "
        "and byte admission stay charged through cancellation and physical worker completion. "
        "A live source quorum write and read progress while copy is paused. "
        "Host-native voters and fixture capacity; hard RSS, sustained load and process crashes "
        "remain separate gates.",
    ),
    "managed-movement-ledger": (
        "native_cell_groups_split_merge",
        ("rustfs_split_merge_under_writes",),
        "reconcile acknowledged writes, signed delivery and every receipt through split and merge",
        "Both image modes and host-native voters. The signed delivery loses its first S3 reply. "
        "Cached SDK handles follow merged routes before joined host restart, then refuse Closed. "
        "Fresh former-parent ingress verifies the complete command ledger, receiver deduplication, "
        "source version floors and independent merged S3 image replay. "
        "Sustained load, additional split/merge cycles and process-crash schedules remain separate gates.",
    ),
    "managed-minority": (
        "native_cell_groups_managed",
        ("bootstrap::lifecycle::s3::rustfs_managed_minority_cannot_reconfigure_from_s3",),
        "a committed archive and rooted reservation cannot authorize a minority replacement",
        "Fixture capacity and joined closure of two voter services. "
        "Separate process faults and sustained load remain separate gates.",
    ),
    "managed-standby": (
        "native_cell_groups_managed",
        ("bootstrap::lifecycle::s3::rustfs_registered_standby_refuses_quorumless_s3_activation",
         "bootstrap::lifecycle::rustfs_managed_s3_group_replacement_and_joined_restart"),
        "S3 standby admission requires fresh quorum and a retained joined voter recovers its exact disk",
        "Fixture capacity and joined voter shutdown. Active joint-reservation recovery, "
        "independent voter processes and sustained load remain separate gates.",
    ),
    "managed-joint-restart": (
        "native_cell_groups_managed",
        ("bootstrap::lifecycle::joint_restart::rustfs_managed_joint_replacement_restart_settles_original_debit",
         "bootstrap::lifecycle::joint_restart::rustfs_managed_joint_seed_owner_restart_settles_original_debit"),
        "retained replacement and original source boots finish and settle the exact joint move",
        "Fixture capacity and joined shutdown of one voter at a time. "
        "Independent process crashes and simultaneous reboots remain separate gates.",
    ),
    "managed-liveness": (
        "native_cell_groups_managed",
        ("bootstrap::lifecycle::s3::rustfs_managed_s3_seed_refusal_and_source_failover",
         "bootstrap::lifecycle::maintenance::rustfs_managed_archive_maintenance_stop_return_and_resume"),
        "bounded successor election after S3 source close and archived maintenance settlement",
        "The source is stopped through joined close. Fixture capacity and host-native voters "
        "leave process crash and platform fencing as separate gates.",
    ),
    "managed-lifecycle": (
        "native_cell_groups_public",
        ("rustfs_managed_group_lifecycle",),
        "managed operator birth, repair debt, handoff, stop, return and resume",
        "Managed boot and capacity attestations use fixture inputs. Production peer "
        "authentication, independent voter processes and sustained load remain separate gates.",
    ),
}

ROUTING_SUITES = ("routing-range-repairs", "routing-authority", "routing-endpoint", "routing-advice", "routing-boot", "routing-removed", "routing-read-cohorts")


def named_binding_observations(output, case):
    """Require the exact case's four completed image/source-policy profiles."""
    if case not in NAMED_BINDING_CASES:
        raise ValueError("unknown named binding evidence case")
    expected = NAMED_BINDING_CASES[case]
    rows = []
    prefix = "named binding configuration "
    for line in output.splitlines():
        if prefix not in line:
            continue
        payload = line.split(prefix, 1)[1]
        match = re.fullmatch(
            r"recovery=(false|true) extents=(false|true) source=(Quorum|S3): PASS", payload,
        )
        if match is None or (match[1] == "true") != expected:
            raise ValueError("incomplete or foreign named binding profile")
        rows.append({"recovery": expected, "extents": match[2] == "true",
                     "source_durability": match[3], "outcome": "PASS"})
    if (len(rows) != 4 or {(row["extents"], row["source_durability"]) for row in rows}
            != {(mode, policy) for mode in (False, True) for policy in ("Quorum", "S3")}):
        raise ValueError("incomplete named binding image/source-policy evidence")
    return rows


def archive_fairness_observations(output, case):
    """Require the joined, independently verified profile for both image forms."""
    if case != ARCHIVE_FAIRNESS_CASE:
        raise ValueError("unknown archive fairness evidence case")
    prefix = "ARCHIVE_FAIRNESS_EVIDENCE "
    rows = []
    for line in output.splitlines():
        if prefix in line:
            payload = line.split(prefix, 1)[1]
            if len(payload) > 8192:
                raise ValueError("oversized archive fairness evidence")
            rows.append(json.loads(payload))
    counters = {"schema": 1, "groups": 3, "restored_images": 14,
                "checkpoint_limit": 1, "tail_operations_after_close": 0}
    proofs = ("runtime_joined", "foreign_hold_refused", "corrupt_tail_refused",
              "healthy_sibling_unchanged", "repaired_tail_same")
    if (len(rows) != 2
            or any(not isinstance(row, dict)
                   or row.get("case") != case.removeprefix("rustfs_")
                   or type(row.get("extents")) is not bool
                   or "shared_extent_roots_min" not in row
                   or (row["extents"] and (type(row.get("shared_extent_roots_min")) is not int
                                           or row["shared_extent_roots_min"] < 14))
                   or (not row["extents"] and row.get("shared_extent_roots_min") is not None)
                   or any(type(row.get(key)) is not int or row[key] != expected
                          for key, expected in counters.items())
                   or any(row.get(key) is not True for key in proofs) for row in rows)
            or {row["extents"] for row in rows} != {False, True}):
        raise ValueError("incomplete archive fairness image/owner evidence")
    return rows


def routing_observations(output, case):
    """Require complete caller/mode output; native cases enforce receipt oracles."""
    if case == READ_COHORT_CASE:
        return read_cohort_observations(output, case)
    if case in SUITES["routing-advice"][1] + SUITES["routing-boot"][1] + SUITES["routing-removed"][1]:
        return advice_observations(output, case)
    if case in ("rustfs_read_first_refreshes_endpoint", "rustfs_unrelated_endpoint_keeps_inflight_reply"):
        return endpoint_observations(output, case)
    if case not in SUITES["routing-range-repairs"][1] + SUITES["routing-authority"][1] + SUITES["routing-endpoint"][1]:
        raise ValueError("unknown routing evidence case")
    membership = case == "rustfs_membership_refresh_keeps_range"
    authority = case == "rustfs_cached_advice_respects_origin_revocation"
    endpoint = case in SUITES["routing-endpoint"][1]
    repairing_leader = case == "rustfs_endpoint_refresh_keeps_membership"
    prefix = ("MEMBERSHIP_ROUTING_EVIDENCE " if membership else
              "AUTHORITY_ROUTING_EVIDENCE " if authority else
              "ENDPOINT_ROUTING_EVIDENCE " if endpoint else "RANGE_ROUTING_EVIDENCE ")
    rows = [json.loads(line.split(prefix, 1)[1])
            for line in output.splitlines() if prefix in line]
    callers = ("SdkNativeAxum",) if case == "rustfs_split_repairs_only_overlap" else ("Sdk", "Native", "Axum")
    events = ("warm", "membership_repaired", "fences_and_receipts_verified") if membership else ("verified",)
    expected = {(caller, mode, event) for caller in callers for mode in (False, True) for event in events}
    if (len(rows) != len(expected)
            or any(not isinstance(row, dict) or type(row.get("schema")) is not int or row.get("schema") != 1
                   or type(row.get("extents")) is not bool
                   or row.get("case") != case.removeprefix("rustfs_") for row in rows)
            or {(row.get("caller"), row.get("extents"), row.get("event")) for row in rows} != expected):
        raise ValueError("incomplete routing caller/image-mode evidence")
    if authority:
        for row in rows:
            before = row.get("callbacks_before")
            stages = row.get("sql_stages_before")
            routing = (row.get("routing_before"), row.get("routing_after_refusal"))
            if (type(before) is not int or not 1 <= before < 2**63 - 1
                    or type(stages) is not int or not 0 <= stages < 2**64
                    or type(row.get("callbacks_after_refusal")) is not int
                    or row["callbacks_after_refusal"] != before
                    or type(row.get("callbacks_after")) is not int
                    or row["callbacks_after"] != before + 1
                    or type(row.get("sql_stages_after_refusal")) is not int
                    or row["sql_stages_after_refusal"] != stages
                    or row.get("required") != "S3" or row.get("achieved") != "S3"
                    or row.get("ambiguous") is not True
                    or any(type(row.get(name)) is not int or row[name] not in (0, 1)
                           for name in ("proven_rejections", "unconfirmed_replies"))
                    or row["proven_rejections"] + row["unconfirmed_replies"] > 1
                    or row.get("result_be") != (before + 1).to_bytes(8, "big").hex()
                    or any(not isinstance(row.get(field), str) or not row[field]
                           for field in ("command", "group", "origin", "version", "witness"))
                    or (row.get("sdk_ambiguous") is not True if row["caller"] == "Sdk"
                        else row.get("sdk_ambiguous") is not None)
                    or any(not isinstance(values, list) or len(values) != 6
                           or any(type(value) is not int or not 0 <= value < 2**64 for value in values)
                           for values in routing)):
                raise ValueError("invalid revoked-origin receipt/owner evidence")
            if [after - before for before, after in zip(routing[0][:5], routing[1][:5])] != [1, 0, 0, 0, 1]:
                raise ValueError("revoked origin did not use one cached direct attempt")
    if endpoint:
        for row in rows:
            before = row.get("callbacks_before")
            stages = row.get("sql_stages_before")
            routing = (row.get("routing_before"), row.get("routing_after"))
            valid_stages = (type(stages) is int and 0 <= stages < 2**64
                            and type(row.get("sql_stages_at_release")) is int
                            and row["sql_stages_at_release"] == stages) if repairing_leader else (
                                stages is None and row.get("sql_stages_at_release") is None
                                and row.get("metadata_released") is False)
            if (type(before) is not int or not 1 <= before < 2**63 - 1
                    or type(row.get("callbacks_after")) is not int
                    or row["callbacks_after"] != before + 1
                    or not valid_stages
                    or row.get("required") != "S3" or row.get("achieved") != "S3"
                    or row.get("result_be") != (before + 1).to_bytes(8, "big").hex()
                    or type(row.get("retry_until_ms")) is not int
                    or not 0 < row["retry_until_ms"] < 2**64
                    or any(not isinstance(row.get(field), str) or not row[field]
                           for field in ("command", "route", "membership_revision", "version", "witness"))
                    or any(not isinstance(row.get(field), str)
                           or not re.fullmatch(r"[0-9a-f]{32}", row[field])
                           for field in ("node", "disk", "old_process", "current_process"))
                    or row["old_process"] == row["current_process"]
                    or any(not isinstance(row.get(field), str)
                           or not re.fullmatch(r"[0-9a-f]{64}", row[field])
                           for field in ("directory_before", "directory_after"))
                    or row["directory_before"] != row["directory_after"]
                    or any(not isinstance(values, list) or len(values) != 7
                           or any(type(value) is not int or not 0 <= value < 2**64 for value in values)
                           for values in routing)):
                raise ValueError("invalid endpoint boot/receipt/owner evidence")
            delta = [after - before for before, after in zip(*routing)]
            if (any(value < 0 for value in delta) or delta[0] != 1
                    or not 1 <= delta[1] <= 4 or delta[2:4] != [0, 0]
                    or (not 1 <= delta[4] <= 4 or not 1 <= delta[5] <= 4
                        or delta[6] > delta[4] if repairing_leader else delta[4:] != [0, 0, 0])):
                raise ValueError("endpoint refresh changed placement or exceeded original attempts")
    return rows, len(callers) * 2


def exact_case_passed(output, case):
    """Keep exact selection strict when validated observations interrupt libtest."""
    pattern = r"cell_group_log_reclamation_observations (\{[^\n]+\})\n"
    observations = [json.loads(value) for value in re.findall(pattern, output)]
    if observations:
        if [value.get("mode") for value in observations] != ["materialized", "extents"]:
            raise ValueError("incomplete or duplicated reclamation observations")
        output = re.sub(pattern, "", output)
    pattern = r"QUEUE_LOAD (\{[^\n]+\})\n"
    observations = [json.loads(value) for value in re.findall(pattern, output)]
    queue_load_limits = {
        "rustfs_queue_load_preserves_acknowledged_messages": (2, 1),
        "rustfs_queue_four_in_flight_preserves_acknowledged_messages": (4, 2),
    }
    load_limits = queue_load_limits.get(case)
    if load_limits and not observations:
        raise ValueError("missing Queue load observations")
    if observations:
        if (not load_limits
                or [value.get("extents") for value in observations] != [False, True]
                or any(not value.get("realRustFS")
                       or value.get("steadyMessages") != 512
                       or value.get("archiveOutageMessages") != 16
                       or value.get("uniqueMessageIds") != 528
                       or value.get("delivered", 0) < 528
                       or value.get("inFlightRequests") != load_limits[0]
                       or value.get("perQueueInFlight") != load_limits[1]
                       or (load_limits[0] == 4 and (
                           value.get("peakInFlightRequests") != 4
                           or value.get("producers") != 4
                           or value.get("queues") != 2
                           or value.get("batchMessages") != 8
                           or value.get("ordinarySlots") != 8
                           or value.get("ordinaryBytes") != 131072
                           or value.get("callbackSlots") != 2))
                       or value.get("elapsedMs", 0) <= 0
                       for value in observations)):
            raise ValueError("incomplete or invalid Queue load observations")
        output = re.sub(pattern, "", output)
    return bool(re.search(rf"^test {re.escape(case)} \.\.\. ok$", output, re.M)
                and "test result: ok. 1 passed; 0 failed; 0 ignored;" in output)


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sources(suite):
    if suite not in SUITES:
        raise ValueError("unknown qualification suite")
    result = subprocess.check_output(["git", "ls-files", "-z"], cwd=ROOT)
    return [
        {"path": name, "sha256": sha(ROOT / name)}
        for name in result.decode().split("\0")
        # Public fixtures compile example modules too. Capture every tracked
        # example rather than maintaining a suite-specific list of includes.
        if name and not name.startswith(("docs/", "plans/", "packages/"))
        and (ROOT / name).is_file()
    ]


def backend(root):
    record = json.loads((root / "environment.json").read_text())
    if (record["phase"] != "backend-ready"
            or not re.fullmatch(r"cellar-multi-raft-[0-9a-f]{16}", record["owner"])
            or not re.fullmatch(r"colima-cellar-multi-raft(?:-[a-z0-9-]+)?", record["context"])
            or not re.fullmatch(r"http://127\.0\.0\.1:[0-9]+", record["endpoint"])):
        raise ValueError("requires an owned, running loopback RustFS backend")
    docker = ["docker", "--context", record["context"]]
    actual = json.loads(command(docker + [
        "inspect", "--format",
        '{"owner":{{json (index .Config.Labels "cellar.multi-raft.run")}},'
        '"image":{{json .Image}},"running":{{json .State.Running}},'
        '"limits":{{json .HostConfig}},"ports":{{json .NetworkSettings.Ports}}}',
        record["container"],
    ]))
    limits = actual["limits"]
    if (actual["owner"] != record["owner"] or actual["image"] != record["image_id"]
            or not actual["running"]
            or (limits["NanoCpus"], limits["Memory"], limits["MemorySwap"])
            != (10**9, 2 * 1024**3, 2 * 1024**3)):
        raise ValueError("backend identity/running state/resource limits changed")
    ports = actual["ports"]["9000/tcp"]
    if ports != [{"HostIp": "127.0.0.1", "HostPort": record["endpoint"].rsplit(":", 1)[1]}]:
        raise ValueError("backend endpoint differs from the captured container")
    credentials = dict(line.split("=", 1) for line in (root / "rustfs.env").read_text().splitlines())
    record["observed_rustfs"] = {
        "owner": actual["owner"], "image": actual["image"],
        "nano_cpus": limits["NanoCpus"], "memory_bytes": limits["Memory"],
        "memory_swap_bytes": limits["MemorySwap"], "ports": ports,
    }
    return record, credentials


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", choices=tuple(SUITES), default="archive",
                        help="fixed required RustFS case set; archive preserves the existing gate")
    parser.add_argument("--backend", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--target-directory", type=Path, required=True,
                        help="absolute checkout-private Cargo target on the workspace volume")
    parser.add_argument("--keep-going", action="store_true",
                        help="collect every standalone primitive case; any failure keeps the campaign failed")
    args = parser.parse_args()
    if args.keep_going and args.suite not in (
        "ordinary-primitives", "named-bindings", "ordinary-sql-bindings",
        "ordinary-kv-bindings", "ordinary-alarms", "queue-primitives"
    ):
        parser.error("--keep-going requires a process-isolated ordinary primitive, SQL, KV or Queue suite")
    target, cases, scope, limitations = SUITES[args.suite]
    if not args.target_directory.is_absolute():
        parser.error("--target-directory must be absolute and unique to this checkout")
    subprocess.run(["git", "diff", "--exit-code", "HEAD", "--", ".",
                    ":(exclude)docs/**", ":(exclude)plans/**"], cwd=ROOT, check=True,
                   stdout=subprocess.DEVNULL)
    untracked = subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard", "-z"], cwd=ROOT)
    if any(name.startswith(("crates/", "tools/")) for name in untracked.decode().split("\0")):
        parser.error("commit qualification sources before running")
    remote, credentials = backend(args.backend.resolve())
    out = args.output.resolve()
    out.mkdir(mode=0o700, parents=True, exist_ok=False)
    prefix = "cell-groups/" + secrets.token_hex(16)
    record = {
        "format": 1, "status": "RUNNING", "scope": scope,
        "suite": args.suite, "test_target": target,
        "source_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "source_inputs": sources(args.suite), "backend": remote, "prefix": prefix, "commands": [],
        "keep_going": args.keep_going, "passed_cases": [], "failed_cases": [],
        "limitations": (("Payload data protocol with RustFS in one local VM; no Raft voters or image fixtures. "
                         if args.suite == "immutable-payloads" else
                         "Host-native three-voter TLS fixture, both image forms, with RustFS in one local VM. ")
                        + limitations + " Independent hosts remain a separate gate."),
    }

    def save():
        (out / "record.json").write_text(json.dumps(record, indent=2) + "\n")

    save()
    try:
        bucket = remote["owner"] + "-cg-" + secrets.token_hex(4)
        record["s3_probe"] = probe(remote["endpoint"], bucket,
                                   credentials["RUSTFS_ACCESS_KEY"], credentials["RUSTFS_SECRET_KEY"])
        record["bucket"] = bucket
        env = dict(os.environ)
        # Avoid inheriting session credentials or arbitrary AWS endpoint overrides.
        for key in list(env):
            if key.startswith(("AWS_", "CELLAR_CELL_GROUPS_RUSTFS_")):
                del env[key]
        # The recorded Cargo CLI must be the one the operator fixture invokes.
        env.pop("CELLAR_RAFT_OPERATOR_CLI", None)
        env.update(
            AWS_ACCESS_KEY_ID=credentials["RUSTFS_ACCESS_KEY"],
            AWS_SECRET_ACCESS_KEY=credentials["RUSTFS_SECRET_KEY"],
            AWS_EC2_METADATA_DISABLED="true",
            CELLAR_CELL_GROUPS_RUSTFS_ENDPOINT=remote["endpoint"],
            CELLAR_CELL_GROUPS_RUSTFS_BUCKET=bucket,
            CELLAR_CELL_GROUPS_RUSTFS_PREFIX=prefix,
            CARGO_TARGET_DIR=str(args.target_directory),
            CARGO_PROFILE_DEV_DEBUG="0", CARGO_PROFILE_TEST_DEBUG="0",
            CARGO_PROFILE_TEST_OPT_LEVEL="1",
            CARGO_PROFILE_TEST_DEBUG_ASSERTIONS="true",
            CARGO_PROFILE_TEST_OVERFLOW_CHECKS="true",
            CARGO_BUILD_JOBS="1", CARGO_TERM_COLOR="never",
        )
        record["cargo_environment"] = {
            key: env[key] for key in (
                "CARGO_TARGET_DIR", "CARGO_PROFILE_DEV_DEBUG", "CARGO_PROFILE_TEST_DEBUG",
                "CARGO_PROFILE_TEST_OPT_LEVEL", "CARGO_PROFILE_TEST_DEBUG_ASSERTIONS",
                "CARGO_PROFILE_TEST_OVERFLOW_CHECKS", "CARGO_BUILD_JOBS", "CARGO_TERM_COLOR",
            )
        }
        if args.suite in ("ordinary-sql-bindings", "ordinary-kv-bindings", "ordinary-alarms", "immutable-payloads", "queue-primitives"):
            record["test_profile"] = {key: env[key] for key in (
                "CARGO_PROFILE_TEST_OPT_LEVEL", "CARGO_PROFILE_TEST_DEBUG_ASSERTIONS",
                "CARGO_PROFILE_TEST_OVERFLOW_CHECKS", "CARGO_PROFILE_TEST_DEBUG")}
        cargo = ["cargo", "+1.98.1", "test", "-p", "cellar", "--locked", "--test", target, "--"]
        operator_cli = None
        for number, selection in enumerate(("--list", *cases)):
            argv = cargo + ([selection] if number == 0 else [selection, "--exact", "--ignored", "--test-threads=1"])
            if number and (args.suite in ROUTING_SUITES
                           or selection in (ARCHIVE_FAIRNESS_CASE, ARCHIVE_OWNER_CASE)
                           or selection in NAMED_BINDING_CASES):
                argv.append("--show-output")
            if (selection in ("rustfs_queue_load_preserves_acknowledged_messages",
                              "rustfs_queue_four_in_flight_preserves_acknowledged_messages")):
                argv.append("--nocapture")
            elif args.suite in ("ordinary-kv-bindings", "ordinary-alarms") and number != 0:
                # Keep successful diagnostics without splitting libtest's exact
                # result line. The strict success verifier remains unchanged.
                argv.append("--show-output")
            print(f"START {number + 1}/{len(cases) + 1}: {selection}", flush=True)
            started = time.monotonic()
            if selection == "rustfs_quorum_continues_bounded_s3_outage":
                control = BackendControl(remote, out / "backend-control")
                result = control.run(argv, ROOT, env)
                control_record = control.directory / "record.json"
                record["backend_controls"] = [{
                    "case": selection, "path": str(control_record.relative_to(out)),
                    "sha256": sha(control_record),
                }]
            else:
                result = subprocess.run(argv, cwd=ROOT, env=env, text=True,
                                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            output = result.stdout
            for secret in credentials.values():
                output = output.replace(secret, "[REDACTED]")
            log = out / f"{number:02}.log"
            log.write_text(output)
            record["commands"].append({"argv": argv, "exit_code": result.returncode,
                                       "elapsed_seconds": round(time.monotonic() - started, 3),
                                       "output": log.name, "sha256": sha(log)})
            match = re.search(rf"Running [^\n]*\(([^\n)]+/{re.escape(target)}-[a-f0-9]+)\)", output)
            if match:
                executable = Path(match[1])
                selected_hash = sha(executable)
                if "executable" in record and record["executable"]["sha256"] != selected_hash:
                    raise RuntimeError("executable changed during qualification")
                destination = out / executable.name
                shutil.copy2(executable, destination)
                record["executable"] = {"path": destination.name, "sha256": selected_hash}
            if args.suite in (
                "managed-ingress", "managed-lifecycle", "ordinary-sql-bindings",
                "ordinary-kv-bindings", "queue-primitives", *ROUTING_SUITES
            ) and result.returncode == 0:
                current_cli = executable.parent.parent / "cellar" if match else None
                if current_cli is None or not current_cli.is_file():
                    raise RuntimeError("missing managed operator CLI provenance")
                cli_hash = sha(current_cli)
                if operator_cli is None:
                    operator_cli = out / "cellar"
                    shutil.copy2(current_cli, operator_cli)
                    record["operator_cli"] = {"path": operator_cli.name, "sha256": cli_hash}
                if cli_hash != record["operator_cli"]["sha256"] or sha(operator_cli) != cli_hash:
                    raise RuntimeError("managed operator CLI changed during qualification")
                env["CELLAR_RAFT_OPERATOR_CLI"] = str(operator_cli)
                if args.suite == "ordinary-sql-bindings":
                    env["CELLAR_CELL_GROUPS_SQL_CLI"] = str(operator_cli)
                if args.suite == "queue-primitives":
                    env["CELLAR_CELL_GROUPS_QUEUE_CLI"] = str(operator_cli)
                if args.suite == "ordinary-kv-bindings":
                    env["CELLAR_CELL_GROUPS_KV_CLI"] = str(operator_cli)
            if args.suite in ("managed-transition-disaster-closure", "managed-disaster-outbox", "managed-disaster-retry"):
                current_operator = executable.parent.parent / "cellar-perf-cell-group" if match else None
                if current_operator is None or not current_operator.is_file():
                    raise RuntimeError("missing cold managed recovery operator provenance")
                operator_hash = sha(current_operator)
                retained = out / current_operator.name
                if "managed_recovery_operator" not in record:
                    shutil.copy2(current_operator, retained)
                    record["managed_recovery_operator"] = {
                        "path": retained.name, "sha256": operator_hash,
                    }
                if (operator_hash != record["managed_recovery_operator"]["sha256"]
                        or sha(retained) != operator_hash):
                    raise RuntimeError("cold managed recovery operator changed during qualification")
            save()
            if not match:
                raise RuntimeError("missing executable provenance")
            if result.returncode or (number != 0 and not exact_case_passed(output, selection)):
                record["failed_cases"].append(selection)
                save()
                if not args.keep_going or number == 0:
                    raise RuntimeError(f"{selection} failed; see {log}")
                # These fixtures run in separate native processes with distinct
                # store prefixes. No managed process/fault suite can use this.
                print(f"FAIL {selection}; continuing independent cases", flush=True)
                continue
            if number == 0:
                for case in cases:
                    if not re.search(rf"^{re.escape(case)}: test$", output, re.M):
                        raise RuntimeError("missing exact RustFS test in executable inventory")
            if args.suite in ROUTING_SUITES and number:
                rows, scenarios = routing_observations(output, selection)
                ledger = out / f"{number:02}-routing-evidence.json"
                ledger.write_text(json.dumps(rows, indent=2) + "\n")
                record.setdefault("routing_evidence", []).append({
                    "case": selection, "path": ledger.name, "sha256": sha(ledger),
                    "records": len(rows), "scenarios": scenarios,
                })
            if number and selection == ARCHIVE_OWNER_CASE:
                rows = archive_owner_observations(output, selection)
                ledger = out / f"{number:02}-physical-owner-evidence.json"
                ledger.write_text(json.dumps(rows, indent=2) + "\n")
                record.setdefault("archive_owner_evidence", []).append({
                    "case": selection, "path": ledger.name, "sha256": sha(ledger), "records": len(rows),
                })
            if number and selection == ARCHIVE_FAIRNESS_CASE:
                rows = archive_fairness_observations(output, selection)
                ledger = out / f"{number:02}-fairness-evidence.json"
                ledger.write_text(json.dumps(rows, indent=2) + "\n")
                record.setdefault("archive_fairness_evidence", []).append({
                    "case": selection, "path": ledger.name, "sha256": sha(ledger), "records": len(rows),
                })
            if number and selection in NAMED_BINDING_CASES:
                try:
                    rows = named_binding_observations(output, selection)
                except ValueError as error:
                    record["failed_cases"].append(selection)
                    record.setdefault("evidence_failures", []).append({
                        "case": selection, "reason": str(error),
                    })
                    save()
                    if not args.keep_going:
                        raise
                    print(f"FAIL {selection}: incomplete binding evidence; continuing independent cases",
                          flush=True)
                    continue
                ledger = out / f"{number:02}-binding-evidence.json"
                ledger.write_text(json.dumps(rows, indent=2) + "\n")
                record.setdefault("named_binding_evidence", []).append({
                    "case": selection, "path": ledger.name, "sha256": sha(ledger), "records": len(rows),
                })
            if number:
                record["passed_cases"].append(selection)
            print(f"PASS {selection}", flush=True)
        if sources(args.suite) != record["source_inputs"]:
            raise RuntimeError("source changed during qualification")
        backend(args.backend.resolve())
        if record["failed_cases"]:
            raise RuntimeError(f"primitive campaign failed: {', '.join(record['failed_cases'])}")
        record["status"] = "PASSED"
        record["passed_cases"] = list(cases)
    except Exception:
        record["status"] = "FAILED"
        raise
    finally:
        save()
    print(json.dumps({"record": str(out / "record.json"), "status": record["status"],
                      "rustfs_cases": len(cases),
                      "image_modes_per_case": 0 if args.suite == "immutable-payloads" else 2}), flush=True)


if __name__ == "__main__":
    main()
