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

ROOT = Path(__file__).resolve().parents[2]
TARGET = "native_cell_groups_archive_controller"
CASES = (
    "rustfs_quorum_continues_bounded_s3_outage",
    "rustfs_group_archive_requires_all_dependencies",
    "rustfs_group_reclaim_restart_keeps_history",
    "rustfs_archive_coverage_gates_local_compaction",
    "rustfs_short_archived_groups_checkpoint_and_recover_independently",
    "rustfs_s3_remote_outputs_preserve_durability_and_creation",
    "rustfs_s3_remote_deadlines_preserve_ambiguity_and_callbacks",
    "rustfs_s3_remote_slow_delivery_retains_proofs_and_bounds",
    "rustfs_controller_archive_publishes_stable_cuts",
    "rustfs_controller_archive_reconciles_exact_intent_and_joins_close",
    "rustfs_unproved_s3_responses_refuse",
    "rustfs_s3_commands_require_exact_archived_results",
    "rustfs_s3_replays_from_peer_archive_without_new_upload",
    "rustfs_s3_command_deadlines_do_not_quarantine_committed_sql",
    "rustfs_s3_command_requires_the_sibling_archive_dependencies",
    "rustfs_s3_namespace_minimum_cannot_be_downgraded",
    "rustfs_s3_enrollment_requires_the_original_archived_creation",
    "rustfs_s3_enrollment_deadlines_preserve_resolved_ownership",
    "rustfs_s3_receipt_lookup_tracks_archived_observations",
    "rustfs_s3_receipt_lookup_honors_namespace_and_stored_minimums",
    "rustfs_s3_receipt_lookup_preserves_typed_failures_and_result_admission",
    "rustfs_s3_receipt_lookup_deadlines_refund_read_admission",
)

SUITES = {
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
         "automatic_topology::rustfs_configured_topology_preserves_recovery_room"),
        "configured whole-group balancing resumes its durable intent after controller replacement",
        "Both image modes, six host-native TLS voters and two engine slots per node. "
        "The positive case declares 1-GiB logical admission within the unchanged 1-GiB disk/3-GiB memory fixture attestation; "
        "the original 512-MiB logical profile must refuse all moves and continue quorum SQL. "
        "Startup waits for its exact boot capacity activation. Two occupied shared groups rebalance "
        "with unchanged cell routing, receipt replay, linearizable SQL reads and fresh quorum writes. "
        "Explicit operator policy takes precedence and hands back through a canonical request. "
        "Failure-domain loss, independent voter processes, sustained load, larger inventories and measured peaks remain separate gates.",
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
        ("bootstrap::requests::rustfs_managed_native_axum_read_write_and_retained_restart",),
        "managed operator provisioning, native/Axum authority, quorum requests and retained restart",
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


def exact_case_passed(output, case):
    """Keep exact selection strict when validated observations interrupt libtest."""
    pattern = r"cell_group_log_reclamation_observations (\{[^\n]+\})\n"
    observations = [json.loads(value) for value in re.findall(pattern, output)]
    if observations:
        if [value.get("mode") for value in observations] != ["materialized", "extents"]:
            raise ValueError("incomplete or duplicated reclamation observations")
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
    result = subprocess.check_output(["git", "ls-files", "-z"], cwd=ROOT)
    return [
        {"path": name, "sha256": sha(ROOT / name)}
        for name in result.decode().split("\0")
        if name and (not name.startswith(("docs/", "plans/", "packages/", "examples/"))
                     or (suite in ("managed-automatic-repair", "managed-automatic-topology", "managed-automatic-partitions", "managed-partition-cancellation", "managed-partition-service", "managed-automatic-selection", "managed-transition-capacity", "managed-live-retirement", "managed-transition-service", "managed-partition-scheduler", "managed-ingress", "managed-lifecycle", "managed-liveness", "managed-transition",
                                   "managed-split-merge", "managed-partial-merge", "managed-precopy-resources", "managed-movement-ledger", "managed-retirement", "managed-phase-restart", "managed-effect-transfer", "managed-publication-loss")
                         and name == "examples/raft-counter/counter.rs"))
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
    args = parser.parse_args()
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
        "limitations": ("Host-native three-voter TLS fixture, both image forms, with RustFS in one local VM. "
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
        env.update(
            AWS_ACCESS_KEY_ID=credentials["RUSTFS_ACCESS_KEY"],
            AWS_SECRET_ACCESS_KEY=credentials["RUSTFS_SECRET_KEY"],
            AWS_EC2_METADATA_DISABLED="true",
            CELLAR_CELL_GROUPS_RUSTFS_ENDPOINT=remote["endpoint"],
            CELLAR_CELL_GROUPS_RUSTFS_BUCKET=bucket,
            CELLAR_CELL_GROUPS_RUSTFS_PREFIX=prefix,
            CARGO_TARGET_DIR=str(args.target_directory),
            CARGO_PROFILE_DEV_DEBUG="0", CARGO_PROFILE_TEST_DEBUG="0",
        )
        cargo = ["cargo", "+1.98.1", "test", "-p", "cellar", "--locked", "--test", target, "--"]
        operator_cli = None
        for number, selection in enumerate(("--list", *cases)):
            argv = cargo + ([selection] if number == 0 else [selection, "--exact", "--ignored", "--test-threads=1"])
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
            if args.suite in ("managed-ingress", "managed-lifecycle") and result.returncode == 0:
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
            save()
            if result.returncode:
                raise RuntimeError(f"{selection} failed; see {log}")
            if number == 0:
                for case in cases:
                    if not re.search(rf"^{re.escape(case)}: test$", output, re.M):
                        raise RuntimeError("missing exact RustFS test in executable inventory")
            elif not exact_case_passed(output, selection):
                raise RuntimeError("exact RustFS test was not executed successfully")
            if not match:
                raise RuntimeError("missing executable provenance")
            print(f"PASS {selection}", flush=True)
        if sources(args.suite) != record["source_inputs"]:
            raise RuntimeError("source changed during qualification")
        backend(args.backend.resolve())
        record["status"] = "PASSED"
        record["passed_cases"] = list(cases)
    except Exception:
        record["status"] = "FAILED"
        raise
    finally:
        save()
    print(json.dumps({"record": str(out / "record.json"), "status": record["status"],
                      "rustfs_cases": len(cases), "image_modes_per_case": 2}), flush=True)


if __name__ == "__main__":
    main()
