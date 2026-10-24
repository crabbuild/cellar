#!/usr/bin/env python3
"""Measure packaged FastAPI durable reads at independent offered rates.

Seed mutations run once, outside measurement. Every scheduled read retains its
outcome and timing; complete accounting does not imply an application SLO pass.
"""
import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
from types import SimpleNamespace
from urllib.parse import urlsplit

from perf import (CREATE, Connection, batch, executable_path, process_sample,
                  require, save, start_host, stop_host)

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools/python"))
from baseline import digest
from offered import summarize
import affinity


def health(origin, inventory):
    connection = Connection(origin)
    try:
        value = connection.request("GET", "/health")[0]
    finally:
        connection.close()
    free = inventory["soabi"].split("-")[1].endswith("t")
    require(value["free_threaded"] == free and value["gil_enabled"] != free,
            "running Python ABI/GIL state differs from the packaged runtime")
    return value


def seed(origin):
    paths = [f"/orders/read-{index}" for index in range(32)]
    created = batch(origin, [("POST", path, CREATE, 201) for path in paths])
    parsed = urlsplit(origin)
    connection = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=20)
    targets = []
    try:
        for path in paths:
            order = created[path]
            require(order["order_id"] == path.rsplit("/", 1)[1] and order["version"] == 1
                    and order["status"] == "pending" and order["total_cents"] == 2500,
                    "invalid seed acknowledgement")
            connection.request("GET", path)
            response = connection.getresponse()
            raw = response.read(65_537)
            require(response.status == 200 and len(raw) <= 65_536 and json.loads(raw) == order,
                    "seed read differs from acknowledged creation")
            targets.append(dict(path=path, expected_body=list(raw)))
    finally:
        connection.close()
    return targets


def measure(args, origin, targets, manifest, rate, duration, output):
    cpus = args.cpu_placement["client"] if args.cpu_placement else None
    evidence = output.with_suffix(".affinity.json")
    command = [*affinity.prefix(cpus, evidence), str(args.client), origin, str(rate), str(duration), str(args.max_inflight),
               "0", str(args.timeout_ms), str(args.max_lag_ms), str(output), str(manifest)]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            timeout=(duration + args.timeout_ms) / 1000 + 30)
    output.with_suffix(".log").write_bytes(result.stdout)
    require(result.returncode == 0, f"arrival client failed ({result.returncode}); see {output}")
    if cpus is not None:
        affinity.verify(evidence, cpus)
    raw = json.loads(output.read_text())
    require(raw["debug_assertions"] == (args.client_profile == "dev"), "client profile mismatch")
    require(raw["workload"] == dict(url=origin.rstrip("/") + "/", rate=rate, duration_ms=duration,
            max_inflight=args.max_inflight, body_bytes=0, timeout_ms=args.timeout_ms,
            max_lag_ms=args.max_lag_ms, reads=targets), "client workload differs from seeded reads")
    return summarize(raw)


def run_case(args, project, root, case):
    config = json.loads(project.read_text())
    inventory_path = (project.parent / config["python_environment"]["inventory"]).resolve(strict=True)
    inventory = json.loads(inventory_path.read_text())
    case.update(profile=config["profile"], inventory_sha256=digest(inventory_path),
                soabi=inventory["soabi"], source_sha256=inventory["source"]["sha256"],
                load_average_before=list(os.getloadavg()), phases=[])
    cpus = args.cpu_placement["host"] if args.cpu_placement else None
    host, origin, pid = start_host(SimpleNamespace(cellar=args.cellar, project=project,
                                                  rust_threads=args.rust_threads), root, host_cpus=cpus)
    try:
        if cpus is not None:
            case["cpu_affinity"] = affinity.verify(root / "host-affinity.json", cpus, host.child.pid)
            case["native_threads_at_start"] = affinity.verify_threads(pid, cpus)
        case.update(origin=origin, executable_sha256=digest(executable_path(pid)),
                    python_runtime=health(origin, inventory))
        targets = seed(origin)
        manifest = root / "reads.json"
        save(manifest, targets)
        case["verified_seed_writes"] = len(targets)
        for rate in args.rates:
            phase = dict(rate=rate)
            case["phases"].append(phase)
            phase["warmup"] = measure(args, origin, targets, manifest, rate, args.warmup_ms,
                                      root / f"warmup-{rate}.json")
            phase["before"] = process_sample(pid)
            if cpus is not None:
                phase["affinity_before"] = affinity.verify_threads(pid, cpus)
            phase["raw"] = f"{root.name}/measured-{rate}.json"
            phase["measurement"] = measure(args, origin, targets, manifest, rate, args.duration_ms,
                                           args.output / phase["raw"])
            phase["after"] = process_sample(pid)
            if cpus is not None:
                phase["affinity_after"] = affinity.verify_threads(pid, cpus)
            require(health(origin, inventory) == case["python_runtime"], "runtime changed under load")
            save(args.output / "results.json", args.report)
            print(f"{root.name} offered={rate}: {phase['measurement']['outcomes']}", flush=True)
        case["load_average_after"] = list(os.getloadavg())
    except BaseException as error:
        case["failure"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        try:
            case["shutdown"] = stop_host(host)
        except BaseException as error:
            case["shutdown_failure"] = f"{type(error).__name__}: {error}"
            if "failure" not in case:
                raise
    case["complete"] = True


def render(report):
    placement = report["configuration"]["cpu_placement"]
    placement_text = (f"Host CPUs: {placement['host']}; client CPUs: {placement['client']}."
                      if placement else "Host and client share the caller's CPU placement.")
    lines = ["# FastAPI independent-arrival reads", "", f"Experiment: {report['status']}", "", placement_text, "",
             "| Runtime / repetition | Offered/s | Success % | Completed/s in window | Request p99 ms | Scheduled p99 ms | Outcomes |",
             "| --- | ---: | ---: | ---: | ---: | ---: | --- |"]
    for case in report["cases"]:
        for phase in case["phases"]:
            if "measurement" not in phase:
                continue
            raw = json.loads((Path(report["output"]) / phase["raw"]).read_text())
            value = summarize(raw)
            require(value == phase["measurement"], "summary differs from raw arrival evidence")
            def p99(key):
                return f"{value[key]['p99'] / 1e6:.2f}" if value[key] else "—"
            lines.append(f"| {case['runtime']} / {case['repetition']} | {phase['rate']} | "
                         f"{100 * value['success_fraction']:.2f} | "
                         f"{value['completed_in_window_per_second']:.1f} | "
                         f"{p99('successful_request_latency_ns')} | "
                         f"{p99('successful_scheduled_latency_ns')} | {value['outcomes']} |")
    lines += ["", *report["limits"], ""]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", type=Path, required=True)
    parser.add_argument("--client-profile", choices=("dev", "lab", "release"), required=True)
    parser.add_argument("--cellar", type=Path, required=True)
    parser.add_argument("--project", action="append", required=True, metavar="LABEL=PATH")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rates", type=int, nargs="+", default=[1000, 4000, 8000])
    parser.add_argument("--duration-ms", type=int, default=5000)
    parser.add_argument("--warmup-ms", type=int, default=500)
    parser.add_argument("--timeout-ms", type=int, default=2000)
    parser.add_argument("--max-lag-ms", type=int, default=5)
    parser.add_argument("--max-inflight", type=int, default=128)
    parser.add_argument("--rust-threads", type=int, default=4)
    parser.add_argument("--repetitions", type=int, default=2)
    parser.add_argument("--host-cpus", help="Linux logical CPUs for the CLI and its native host, e.g. 0,1")
    parser.add_argument("--client-cpus", help="disjoint Linux logical CPUs for native arrival clients, e.g. 2,3")
    args = parser.parse_args()
    args.cpu_placement = affinity.placement(args.host_cpus, args.client_cpus)
    require(1 <= args.repetitions <= 5 and 1 <= args.max_inflight <= 512 and 1 <= args.rust_threads <= 64
            and 1 <= args.timeout_ms <= 10000 and 1 <= args.max_lag_ms <= 1000, "invalid limits")
    require(len(set(args.rates)) == len(args.rates) and all(1 <= rate <= 100000 for rate in args.rates)
            and all(1 <= duration <= 120000 and all(1 <= rate * duration // 1000 <= 1000000
                    for rate in args.rates) for duration in (args.duration_ms, args.warmup_ms)),
            "invalid rates/durations")
    projects = []
    for argument in args.project:
        label, path = argument.split("=", 1)
        require(label and all(c.isascii() and (c.isalnum() or c in "-_") for c in label), "invalid label")
        projects.append((label, Path(path).resolve(strict=True)))
    require(len({label for label, _ in projects}) == len(projects), "duplicate runtime label")
    args.client, args.cellar = args.client.resolve(strict=True), args.cellar.resolve(strict=True)
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    args.report = report = dict(format=1, status="running", output=str(args.output), cases=[],
        source=subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        source_diff_sha256=hashlib.sha256(subprocess.check_output(["git", "diff", "HEAD"])).hexdigest(),
        harness_sha256=digest(Path(__file__)), client_sha256=digest(args.client), cellar_sha256=digest(args.cellar),
        measurement_sources={str(path): digest(path) for path in
            (Path(__file__).with_name("perf.py"), Path("tools/python/offered.py"),
             Path("tools/python/offered_load.rs"), Path("tools/python/affinity.py"), Path("examples/smoke.py"))},
        platform=platform.platform(), cpu_count=os.cpu_count(),
        native_policy_environment={name: os.environ.get(name) for name in (
            "CELLAR_MAX_REQUESTS", "CELLAR_ADMISSION_WAIT_MS", "CELLAR_MAX_STATELESS_ISOLATES",
            "CELLAR_MAX_CELL_REQUESTS", "CELLAR_HANDLER_BUDGET_S")},
        configuration=dict(rates=args.rates, duration_ms=args.duration_ms, warmup_ms=args.warmup_ms,
                           max_inflight=args.max_inflight, max_lag_ms=args.max_lag_ms,
                           timeout_ms=args.timeout_ms, rust_threads=args.rust_threads,
                           repetitions=args.repetitions, client_profile=args.client_profile,
                           cpu_placement=args.cpu_placement),
        limits=["Fresh private host per runtime/repetition; 32 seeded durable orders. Seed writes are sent once and read back.",
                "Every scheduled read has an outcome. Exact HTTP 200 bodies, including EOF, are checked against seeded receipts.",
                "Percentiles describe successes only; inspect success fraction, failures, dispatch delay and client drops together.",
                "Completed experiment means accounting and shutdown succeeded, not that an application SLO passed.",
                "Native policy environment records inherited overrides; null means the CLI default applies.",
                "Loopback and a private SQLite development bucket; no remote storage or fleet capacity claim.",
                "Optional CPU affinity restricts eligible logical CPUs, not exclusive cores, quotas or memory; other work can still compete.",
                "Warmup and measurement use separate native client processes/pools. Rates may be affected by timer granularity."])
    try:
        for repetition in range(args.repetitions):
            offset = repetition % len(projects)
            for label, project in projects[offset:] + projects[:offset]:
                case = dict(runtime=label, repetition=repetition + 1, complete=False, phases=[])
                report["cases"].append(case)
                save(args.output / "results.json", report)
                run_case(args, project, args.output / f"{label}-{repetition + 1}", case)
                save(args.output / "results.json", report)
        report["status"] = "complete"
    except BaseException as error:
        report.update(status="failed", failure=f"{type(error).__name__}: {error}")
        raise
    finally:
        save(args.output / "results.json", report)
        (args.output / "report.md").write_text(render(report))


if __name__ == "__main__":
    main()
