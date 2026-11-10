#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Run the managed native daemon fixture in separate constrained Linux voters."""

import argparse
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import subprocess
import sys
import time

from backend_control import atomic_json
from cell_groups import backend
from store_probe import probe

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools/perf"))
from raft_run import input_digest, parse_manifest, require, source_manifest

PROFILE = "cellar-multi-raft"
CONTEXT = "colima-" + PROFILE
LABEL = "io.cellar.daemon.run"
BIN = "/usr/local/bin"
NAMES = ("cellar-raft-daemon", "cellar")


def docker(*args, stderr=None):
    return subprocess.check_output(
        ["docker", "--context", CONTEXT, *args], text=True, stderr=stderr).strip()


def guest(script):
    return subprocess.run(["colima", "ssh", "--profile", PROFILE, "--", "sh", "-s"],
                          input=script, text=True, stdout=subprocess.PIPE, cwd=Path.home(),
                          stderr=subprocess.STDOUT, timeout=14 * 60, check=False)


def verify_image(image):
    require(re.fullmatch(r"sha256:[0-9a-f]{64}", image), "immutable Docker image ID required")
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    require(docker("image", "inspect", "--format",
                   '{{index .Config.Labels "org.opencontainers.image.revision"}}', image) == revision,
            "image source revision differs")
    isolated = ("run", "--rm", "--network", "none", "--entrypoint")
    manifest = parse_manifest(docker(*isolated, "cat", image, f"{BIN}/raft-source.sha256"))
    require(manifest == source_manifest(), "image source inputs differ")
    binaries = {name: docker(*isolated, "sha256sum", image, f"{BIN}/{name}").split()[0] for name in NAMES}
    require(all(re.fullmatch(r"[0-9a-f]{64}", digest) for digest in binaries.values()), "invalid executable digest")
    return {"image": image, "source_revision": revision, "source_manifest_sha256": input_digest(manifest),
            "binaries": binaries}


def inventory(run, image):
    identifiers = docker("ps", "-aq", "--no-trunc", "--filter", f"label={LABEL}={run}").splitlines()
    rows = []
    for identifier in identifiers:
        row = json.loads(docker("inspect", "--type", "container", "--format",
                                '{"id":{{json .Id}},"image":{{json .Image}},'
                                '"run":{{json (index .Config.Labels "io.cellar.daemon.run")}},'
                                '"running":{{json .State.Running}},"exit_code":{{json .State.ExitCode}}}',
                                identifier))
        require(row["id"] == identifier and row["image"] == image and row["run"] == run,
                "foreign voter in lab inventory")
        rows.append(row)
    return rows


def verify_archive_outage(events):
    expected = [
        ("archive_writes_blocked", None),
        ("archive_outage_quorum", "native"),
        ("archive_outage_linearizable", "axum"),
        ("archive_outage_s3_refused", "native"),
        ("archive_outage_s3_refused", "axum"),
        ("archive_outage_denied_writes", None),
        ("archive_writes_restored", None),
        ("archive_outage_s3_replayed", "native"),
        ("archive_outage_s3_replayed", "axum"),
    ]
    phases = {phase for phase, _ in expected}
    records = [event for event in events if event["phase"] in phases]
    require([(event["phase"], event.get("ingress")) for event in records] == expected,
            "missing ordered archive outage proof")
    blocked, quorum, read, native_refusal, axum_refusal, denied, _, *replayed = records
    require(blocked["cell"] == "one" and blocked["delta"] == 0
            and blocked["retry_until"].isdigit()
            and quorum["receipt"]["durability"] == "quorum"
            and quorum["receipt"]["value"] == 11, "missing quorum receipt during archive outage")
    require(read["receipt"]["value"] == quorum["receipt"]["value"]
            and read["receipt"]["group"] == quorum["receipt"]["group"]
            and read["receipt"]["index"] >= quorum["receipt"]["index"],
            "linearizable read missed the outage quorum receipt")
    require(denied["members"] and set(denied["members"]) <= {1, 2, 3},
            "no voter confirmed an archive write refusal")
    require(all(event["command"] == blocked["command"]
                for event in [quorum, read, native_refusal, axum_refusal, *replayed]),
            "archive outage retry changed command identity")
    require(all(event["status"] == 503 and event["body"].startswith("Ambiguous { command:")
                for event in [native_refusal, axum_refusal]), "S3 refusal lost ambiguity")
    for event in replayed:
        receipt = event["receipt"]
        require(receipt["durability"] == "s3" and all(
            receipt[field] == quorum["receipt"][field]
            for field in ("group", "group_incarnation", "incarnation", "cell", "sequence", "value")
        ), "S3 recovery did not acknowledge the original result")


def verify_park_wake(events):
    phases = ("managed_park_delivered", "managed_park_wake", "managed_park_read", "managed_park_read")
    records = [event for event in events if event["phase"] in phases]
    require(tuple(event["phase"] for event in records) == phases, "missing ordered park and wake proof")
    parked, wake, native, axum = records
    require(parked["members"] == 3 and parked["generation"] > 0
            and wake["generation"] == parked["generation"] and wake["index"] > parked["generation"]
            and wake["term"] > parked["term"]
            and all(event["operation"] == parked["operation"] for event in records),
            "wake did not prove fresh quorum for the parked generation")
    require([native["ingress"], axum["ingress"]] == ["native", "axum"]
            and all(event["receipt"]["value"] == 11 for event in (native, axum)),
            "post-wake reads did not preserve both ingress paths and the cell value")


def verify_leader_loss(events):
    phases = ("before_hard_kill", "hard_kill_joined", "successor_before_restart", "restarted")
    records = [event for event in events if event["phase"] in phases]
    require(tuple(event["phase"] for event in records) == phases, "missing ordered leader-loss proof")
    before, joined, after, restarted = records
    killed = before["member"]
    successor = after["member"]
    require(killed in (1, 2, 3) and successor != killed and after["term"] > before["term"]
            and before["pid"] == joined["pid"] == restarted["old_pid"]
            and restarted["member"] == killed and restarted["new_pid"] != before["pid"],
            "successor was not selected before the killed leader restarted")
    for sample, members in ((before, {1, 2, 3}), (after, {1, 2, 3} - {killed})):
        voters = sample["voters"]
        require(len(voters) == len(members) and {voter["member"] for voter in voters} == members
                and sample["member"] in members
                and len({(voter["group"], voter["group_incarnation"]) for voter in voters}) == 1
                and all(voter["term"] == sample["term"] and voter["leader"] == sample["member"]
                        and voter["is_leader"] == (voter["member"] == sample["member"])
                        and (not voter["is_leader"] or voter["ready"]) and voter["cells"] == 2
                        for voter in voters), "incomplete local Raft leadership observations")
    require((before["voters"][0]["group"], before["voters"][0]["group_incarnation"])
            == (after["voters"][0]["group"], after["voters"][0]["group_incarnation"]),
            "leadership changed the physical group history")


def verify_maintenance(events):
    phases = ("maintenance_ready", "maintenance_offline", "maintenance_offline_sql",
              "maintenance_return_paused", "maintenance_complete", "maintenance_returned_leader",
              *("maintenance_resumed_sql",) * 4)
    records = [event for event in events if event["phase"] in phases]
    require(tuple(event["phase"] for event in records) == phases, "missing ordered maintenance proof")
    ready, offline, during, paused, complete, leader, *replayed = records
    target = ready["member"]
    require(target in (2, 3) and offline["successor"] in {1, 2, 3} - {target}
            and (ready["leader"] != target or offline["term"] > ready["term"])
            and ready["pid"] == offline["pid"] != complete["pid"]
            and paused["engine"] == ready["engine"] and complete["revision"] > 0
            and all(event["operation"] == ready["operation"] and event["member"] == target
                    for event in (offline, paused, complete)), "maintenance physical lineage differs")
    require(leader["member"] == target and leader["term"] > offline["term"]
            and during["member"] == offline["successor"] and during["ingress"] == "native"
            and during["receipt"]["durability"] == "quorum"
            and [(event["ingress"], event["receipt"]["durability"]) for event in replayed]
            == [(ingress, durability) for durability in ("quorum", "s3") for ingress in ("native", "axum")]
            and all(event["member"] == target for event in replayed),
            "missing SQL on the survivor and returned leader")
    for event in [during, *replayed]:
        receipt = event["receipt"]
        require(event["command"] == during["command"] and receipt["value"] == 23
                and all(
                    receipt[field] == during["receipt"][field]
                    for field in ("group", "group_incarnation", "cell", "incarnation", "sequence")
                ), "maintenance changed the acknowledged command or history")


def verify_standby(events):
    phases = ("repair_standby_ready", "repair_observer_ready", "repair_standby_joined")
    records = [event for event in events if event["phase"] in phases]
    require(tuple(event["phase"] for event in records) == phases, "missing ordered standby proof")
    ready, observer, joined = records
    require(ready["docker_bound"] and ready["capacity_revision"] > 0 and ready["pid"] > 0
            and all(re.fullmatch(r"[0-9a-f]{32}", ready[field]) for field in ("node", "process", "engine"))
            and observer["local_groups"] == observer["standby_groups"] == 0
            and observer["revision"] > ready["capacity_revision"]
            and joined["startup_cancelled"], "standby boot, binding or joined startup proof differs")


def verify_repair(events):
    phases = ("repair_standby_ready", "repair_worker_ready", "repair_failure_joined", "repair_removed",
              "repair_replacement_sql", "repair_replacement_sql", "repair_drain_complete")
    records = [event for event in events if event["phase"] in phases]
    require(tuple(event["phase"] for event in records) == phases, "missing ordered replacement proof")
    standby, worker, failed, removed, native, axum, drain = records
    require(standby["docker_bound"] and standby["capacity_revision"] > 0
            and worker["member"] == 2 and worker["revision"] >= standby["capacity_revision"]
            and re.fullmatch(r"[0-9a-f]{32}", worker["boot"]) and worker["boot"] != failed["boot"]
            and failed["member"] == removed["member"] == 1 and failed["pid"] > 0
            and re.fullmatch(r"[0-9a-f]{32}", failed["boot"])
            and removed["replacement"] == 4 and removed["voters"] == [2, 3, 4]
            and removed["revision"] > standby["capacity_revision"] and removed["retained_files"] > 0
            and re.fullmatch(r"[0-9a-f]{64}", removed["isolation_digest"]),
            "replacement did not retain the failed disk and restore RF3")
    require([event["ingress"] for event in (native, axum)] == ["native", "axum"]
            and native["member"] == axum["member"] == 4 and native["term"] == axum["term"],
            "replacement did not serve both adapters as leader")
    handoff = drain["handoff"]
    successor = drain["successor"]
    require(drain["member"] == 4 and drain["term"] >= native["term"]
            and successor in (2, 3) and drain["successor_term"] > drain["term"]
            and handoff["groups_examined"] == handoff["accepted"] == handoff["leadership_released"] == 1
            and handoff["unavailable_groups"] == 0 and handoff["deadline_elapsed"] is False
            and [voter["member"] for voter in drain["voters"]] == [2, 3]
            and all(voter["leader"] == successor and voter["term"] == drain["successor_term"]
                    and voter["is_leader"] == (voter["member"] == successor) for voter in drain["voters"]),
            "missing joined drain and successor proof after replacement")
    archived = failed["receipt"]
    for event in (native, axum):
        replayed, written = event["replayed"], event["written"]
        require(archived["durability"] == replayed["durability"] == "s3" and written["durability"] == "quorum"
                and written["sequence"] == archived["sequence"] + 1
                and all(receipt[key] == archived[key] for receipt in (replayed, written)
                        for key in ("group", "group_incarnation", "cell", "incarnation", "value")),
                "replacement changed the archived receipt or cell history")


def execute(mode, output, remote, credentials, build, engine, standby=False, repair=False):
    run = secrets.token_hex(8)
    directory = f"/tmp/cellar-daemon-{run}"
    result = {"mode": mode, "run": run, "guest_directory": directory, "status": "RUNNING"}
    report = output / f"{mode}.json"
    atomic_json(report, result)
    start = time.monotonic()
    bucket = f"{remote['owner']}-cli-{run[:8]}"
    access = credentials["RUSTFS_ACCESS_KEY"]
    secret = credentials["RUSTFS_SECRET_KEY"]
    image = build["image"]
    env = {
        "AWS_ACCESS_KEY_ID": access, "AWS_SECRET_ACCESS_KEY": secret, "AWS_EC2_METADATA_DISABLED": "true",
        "CELLAR_CELL_GROUPS_RUSTFS_ENDPOINT": remote["endpoint"], "CELLAR_CELL_GROUPS_RUSTFS_BUCKET": bucket,
        "CELLAR_CELL_GROUPS_RUSTFS_PREFIX": f"managed-linux-daemon/{run}",
        "CELLAR_RAFT_PROCESS_IMAGE_MODE": mode, "CELLAR_RAFT_PROCESS_EVIDENCE": f"{directory}/process.jsonl",
        "CELLAR_RAFT_DOCKER_RUN": run, "CELLAR_RAFT_DOCKER_IMAGE": image,
        "CELLAR_RAFT_OPERATOR_CLI": f"{directory}/cellar", "TMPDIR": directory,
    }
    if standby:
        env["CELLAR_RAFT_DAEMON_STANDBY_GATE"] = "1"
    if repair:
        env["CELLAR_RAFT_DAEMON_REPAIR_GATE"] = "1"
    quoted = shlex.quote(directory)
    try:
        result["signed_s3"] = probe(remote["endpoint"], bucket, access, secret)
        lines = ["set -eu", "umask 077", "unset DOCKER_CONTEXT DOCKER_HOST",
                 f"test \"$(docker info --format '{{{{.ID}}}}')\" = {shlex.quote(engine)}",
                 f"mkdir -m 700 {quoted}",
                 f'helper=$(docker create --network none --label io.cellar.daemon.artifact={run} '
                 f'--entrypoint /bin/true {shlex.quote(image)})']
        for name, digest in build["binaries"].items():
            path = f"{directory}/{name}"
            lines += [f'docker cp "$helper:{BIN}/{name}" {shlex.quote(path)}',
                      f"printf '%s  %s\\n' {digest} {shlex.quote(path)} | sha256sum --check -"]
        lines += [f"export {key}={shlex.quote(value)}" for key, value in env.items()]
        lines += [f"exec timeout --signal=TERM --kill-after=10s 12m {quoted}/cellar-raft-daemon"]
        completed = guest("\n".join(lines) + "\n")
        log = completed.stdout.replace(access, "[REDACTED]").replace(secret, "[REDACTED]")
        (output / f"{mode}.log").write_text(log)
        rows = inventory(run, image)
        result["voters"] = rows
        result["fixture_exit"] = completed.returncode
        ledger = guest(f"cat {quoted}/process.jsonl\n")
        if ledger.returncode == 0:
            ledger_text = ledger.stdout.replace(access, "[REDACTED]").replace(secret, "[REDACTED]")
            (output / f"{mode}.jsonl").write_text(ledger_text)
        require(completed.returncode == 0 and "production Raft CLI qualification passed" in log,
                f"Linux daemon fixture failed in {mode}")
        expected_exits = [0, 0, 0, 0, 137]
        if standby:
            expected_exits.insert(4, 1)
        if repair:
            expected_exits.append(137)
        require(all(not row["running"] for row in rows)
                and sorted(row["exit_code"] for row in rows) == expected_exits, "voter shutdown inventory differs")
        require(ledger.returncode == 0, "process ledger missing")
        events = [json.loads(line) for line in ledger_text.splitlines()]
        verify_park_wake(events)
        verify_archive_outage(events)
        verify_leader_loss(events)
        verify_maintenance(events)
        if standby:
            verify_standby(events)
        if repair:
            verify_repair(events)
        modes = ("native", "axum")
        cells = (("one", "native"), ("two", "axum"))
        require(events[0]["store"] == "rustfs_catalog_archives_and_node_leases"
                and events[0]["operator"] == "cli_admin", "missing RustFS CLI provisioning proof")
        require([event["voters"] for event in events if event["phase"] == "rustfs_node_leases"] == [3],
                "missing live RustFS node lease proof")
        for phase in ("acknowledged", "post_resume_write_verified"):
            outcomes = [(event["cell"], event["ingress"]) for event in events
                        if event["phase"] == phase and (phase != "acknowledged" or event.get("delta") == 11)]
            require(tuple(outcomes) == cells, f"missing native/Axum proof for {phase}")
        resumed = [(event["cell"], event["ingress"]) for event in events
                   if event["phase"] == "resumed_local_read_verified" and event["receipt"]["value"] == 23]
        require(resumed == [(cell, ingress) for cell, _ in cells for ingress in modes],
                "missing final retained-image reads on both ingress adapters")
        summaries = [event for event in events if event["phase"] == "retained_disk_hard_kill_passed"]
        require(len(summaries) == 1 and summaries[0]["physical_groups"] == 1
                and summaries[0]["logical_cells"] == 2, "physical group and logical cell counts differ")
        refusals = ("no_quorum_linearizable", "no_quorum_write_send",
                    "no_quorum_write", "no_quorum_historical")
        phases = {"pre_loss_linearizable", "voter_shutdown_joined", *refusals}
        boundary = [event for event in events if event["phase"] in phases]
        require([(event["phase"], event.get("member"), event.get("ingress")) for event in boundary] == [
            *(("pre_loss_linearizable", None, ingress) for ingress in modes),
            ("voter_shutdown_joined", 4 if repair else 1, None), ("voter_shutdown_joined", 2, None),
            *((phase, 3, ingress) for ingress in modes for phase in refusals),
            ("voter_shutdown_joined", 3, None),
        ], "missing observed quorum-loss boundary for native and Axum")
        commands = []
        for ingress in modes:
            before, read, send, write, historical = (event for event in boundary
                                                      if event.get("ingress") == ingress)
            require(read["status"] == write["status"] == 503
                    and send["command"] == write["command"]
                    and before["reply"]["value"] == historical["reply"]["value"] == 23
                    and historical["reply"]["served_by"] == 3,
                    f"{ingress} quorum-loss observations differ")
            commands.append((send["command"], send["retry_until"], send["delta"]))
        require(commands[0] == commands[1], "quorum-loss retry changed command identity")
        observations = guest(f'''find {quoted} -maxdepth 2 -name '*-docker.json' -type f -exec sh -c '\nfor file do tr -d "\\n" < "$file"; printf "\\n"; done' sh {{}} +\n''')
        require(observations.returncode == 0, "could not capture cgroup observations")
        proof = [json.loads(line) for line in observations.stdout.splitlines()]
        kills = 2 if repair else 1
        require(len(proof) == len(rows) + kills and {row["id"] for row in proof if row["running"]} == {row["id"] for row in rows}
                and sum(row["exit_code"] == 137 and not row["running"] for row in proof) == kills,
                "missing live cgroup or SIGKILL observation")
        result.update(status="PASSED", resources=proof)
    except Exception:
        result["status"] = "FAILED"
        for row in inventory(run, image):
            if row["running"]:
                docker("kill", "--signal", "KILL", row["id"])
        raise
    finally:
        result["elapsed_seconds"] = round(time.monotonic() - start, 3)
        atomic_json(report, result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--backend", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    extension = parser.add_mutually_exclusive_group()
    extension.add_argument("--standby", action="store_true", help="qualify and bind a fourth repair standby")
    extension.add_argument("--repair", action="store_true", help="qualify Docker-fenced replacement from RustFS")
    args = parser.parse_args()
    remote, credentials = backend(args.backend.resolve())
    require(remote["context"] == CONTEXT, "requires the dedicated daemon lab context")
    engine, os_type, cgroups, cpus, memory = docker(
        "info", "--format", "{{.ID}} {{.OSType}} {{.CgroupVersion}} {{.NCPU}} {{.MemTotal}}"
    ).split()
    require(cgroups == "2", "effective voter limit proof requires cgroup v2")
    min_memory_gib = 15 if args.repair else 12
    require(os_type == "linux" and int(cpus) >= 6 and int(memory) >= min_memory_gib * 1024**3,
            "insufficient Linux VM resources")
    require(not docker("ps", "-q", "--filter", f"label={LABEL}"), "another daemon campaign is active")
    build = verify_image(args.image)
    output = args.output.resolve()
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    report = {"status": "RUNNING", "build": build, "backend": remote["observed_rustfs"],
              "scope": "three Linux voters with separate cgroups and disks in one VM",
              "repair_standby": args.standby, "failed_voter_repair": args.repair}
    atomic_json(output / "record.json", report)
    try:
        for mode in ("materialized", "extents"):
            execute(mode, output, remote, credentials, build, engine, standby=args.standby, repair=args.repair)
        report["status"] = "PASSED"
    except Exception:
        report["status"] = "FAILED"
        raise
    finally:
        atomic_json(output / "record.json", report)


if __name__ == "__main__":
    os.umask(0o077)
    main()
