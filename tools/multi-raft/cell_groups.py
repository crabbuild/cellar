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
    "archive": (
        TARGET, CASES,
        "shared-group RustFS archive/setup and local/remote command/enrollment/receipt S3 settlement",
        "The bounded full-backend outage case controls only this owned container. "
        "Logical retained-entry admission and controlled-geometry Raft file purge are tested. "
        "Global physical disk bounds, process-kill restart, fencing/DR and sustained performance "
        "remain separate gates.",
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
        "native_cell_groups_managed",
        ("bootstrap::transition::rustfs_managed_split_plan",
         "bootstrap::transition::rustfs_managed_split_authorization"),
        "managed split cancellation, sealed archive replay, child installation and retained restart",
        "Host-native voters and fixture capacity. Child attachment, fresh quorum, publication "
        "and activation remain separate gates.",
    ),
    "managed-split-merge": (
        "native_cell_groups_managed",
        ("bootstrap::transition::merge::rustfs_managed_recovery_barrier_fences_transfer_and_reopens",
         "bootstrap::transition::merge::rustfs_managed_split_merge"),
        "managed split and merge through retirement, with restored SQL after source disk erasure",
        "Host-native voters, joined source shutdown and erasure of test-owned disks. "
        "Fixture capacity and fencing authority. Permanent Docker launch revocation "
        "and sustained load remain separate gates.",
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
                     or (suite in ("managed-ingress", "managed-lifecycle", "managed-liveness", "managed-transition",
                                   "managed-split-merge")
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
