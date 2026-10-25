#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Validate default-limit evidence before rendering workload measurements."""
from __future__ import annotations

import argparse
from collections import defaultdict
import gzip
import json
import math
from pathlib import Path
import statistics

from baseline_report import require


def bridge_settled(bridge):
    require(type(bridge.get("native_invocations")) is int and bridge["native_invocations"] == 0,
            "unreleased or unobserved native invocation admissions")
    require(bridge["queued_bytes"] == bridge["pending_reservations"] == 0, "unreleased bridge bytes/reservations")
    operations = bridge["operations"]
    require(all(operations[key] == 0 for key in ("pending", "queued", "running", "replies_pending")), "unsettled native operations")
    require(operations["admitted"] == sum(operations[key] for key in ("delivered", "abandoned_queued", "abandoned_running", "undelivered"))
            and operations["started"] == sum(operations[key] for key in ("succeeded", "failed", "abandoned_running")), "operation conservation failed")


def observations(observation, limits, settled=False):
    runtime = observation["runtime"]
    require(runtime["kind"] == "python" and runtime["limits"] == limits, "runtime limits changed")
    require(0 <= observation["memory_sample_age_ns"] and observation["memory"]["sample"]["rss_bytes"] > 0, "missing memory evidence")
    require(0 <= runtime["bridge"]["queued_bytes"] <= runtime["bridge"]["peak_queued_bytes"] <= limits["max_bridge_bytes"], "bridge byte limit exceeded")
    if settled:
        bridge_settled(runtime["bridge"])


def acknowledged(row):
    require(row["status"] == 200 and row["outcome"] == "acknowledged" and row["latency_ns"] > 0, "unknown/failed command cannot prove acceptance")


def verify(document):
    require(document["format"] == 1 and document["status"] == "complete", "incomplete default qualification")
    workload = document["workload"]
    require(workload["count"] >= 4 and workload["count"] % 4 == 0, "missing mixed operation cycle")
    artifacts = {artifact["label"] for artifact in document["artifacts"]}
    require(len(artifacts) == len(document["artifacts"]), "duplicate artifact")
    keys = {(label, repeat) for label in artifacts for repeat in range(workload["repetitions"])}
    seen = set()
    for case in document["cases"]:
        identity = case["runtime"], case["repetition"]
        require(identity in keys and identity not in seen and case["status"] == "complete", "incomplete/duplicate case")
        seen.add(identity)
        limits = case["limits"]
        require(all(isinstance(value, int) and value > 0 for value in limits.values()), "invalid default limits")
        host = case["host"]
        require(host["mode"] == "python-defaults" and host["embed_compiled"] is True and
                host["native_max_stateless_requests"] > limits["max_invocations"] and host["native_admission_wait_ms"] == 0,
                "native admission obscures Python admission")
        abi = case["python_runtime"]
        require(abi["free_threaded"] != abi["gil_enabled"] and
                case["python_runtime_after"] == case["python_runtime_recovered"] == abi, "wrong/changed actual ABI/GIL")
        require([phase["concurrency"] for phase in case["phases"]] == workload["concurrency"], "missing concurrency phase")
        ledger = defaultdict(int)
        for phase in case["phases"]:
            observations(phase["before"], limits, settled=True)
            observations(phase["after"], limits, settled=True)
            concurrency = phase["concurrency"]
            require(set(phase["attempts"]) == {str(worker) for worker in range(concurrency)}, "missing raw worker ledger")
            samples = []
            for worker in range(concurrency):
                rows = phase["attempts"][str(worker)]
                require(len(rows) == workload["count"], "missing raw request")
                for ordinal, row in enumerate(rows):
                    acknowledged(row)
                    kind = ("echo-0", "counter", "echo-4096", "cpu")[ordinal % 4]
                    require(row["worker"] == worker and row["ordinal"] == ordinal and row["kind"] == kind, "wrong raw operation identity")
                    if kind == "counter":
                        ledger[worker] += 1
                        require(row["value"] == ledger[worker] and row["route"] == f"/cells/defaults-{worker}", "counter ledger disagrees")
                    samples.append(row["latency_ns"])
            samples.sort()
            require(phase["elapsed_ns"] >= samples[-1] and math.isclose(phase["requests_per_second"], len(samples) * 1e9 / phase["elapsed_ns"], rel_tol=1e-12), "invalid throughput")
            require(phase["latency_ns"] == {name: samples[math.ceil(q * len(samples)) - 1] for name, q in (("p50", .5), ("p95", .95), ("p99", .99), ("max", 1))}, "invalid latency percentiles")
            for key in ("input_copied_bytes", "output_copied_bytes"):
                require(phase["after"]["runtime"]["bridge"][key] > phase["before"]["runtime"]["bridge"][key], "missing copy observations")
        boundary = case["body_boundary"]
        require(len(boundary) == 2, "missing body boundaries")
        acknowledged(boundary[0])
        require(boundary[0]["request_bytes"] == boundary[0]["response_bytes"] == limits["max_body_bytes"], "exact body boundary not accepted")
        require(boundary[1]["request_bytes"] == limits["max_body_bytes"] + 1 and boundary[1]["status"] >= 500 and
                boundary[1]["outcome"] == "refused" and boundary[1]["reason"] == "Python request body exceeds limit" and
                boundary[1]["reason"] in boundary[1]["error_body"], "over-limit body not refused")
        saturation = case["saturation"]
        target = saturation["target"]
        require(target == limits["max_invocations"] and len(saturation["held"]) == target and
                {row["worker"] for row in saturation["held"]} == set(range(target)), "missing default-cap participants")
        for row in saturation["held"]:
            acknowledged(row)
            require(row["request_bytes"] == row["response_bytes"] == 65536, "wrong held payload")
        require(len(saturation["rejected"]) == 16, "missing excess admission attempts")
        for ordinal, row in enumerate(saturation["rejected"]):
            require(row["ordinal"] == ordinal and row["status"] == 503 and row["outcome"] == "refused" and
                    row["reason"] == "Python invocation capacity exhausted" and
                    json.loads(row["error_body"]) == {"error": "application admission refused", "code": "Overloaded", "outcome": "unknown"},
                    "wrong admission refusal")
        full = False
        observations(saturation["before"], limits, settled=True)
        for observation in saturation["observations"]:
            observations(observation, limits)
            snapshot = observation["runtime"]["interpreter"]
            if snapshot is not None and snapshot["age_ns"] < 1_000_000_000 and snapshot["work"]["active_invocations"] == target:
                full = True
        require(full, "default capacity was not directly observed full")
        observations(saturation["after"], limits, settled=True)
        require(len(saturation["subsequent"]) == 1, "missing capacity reuse")
        acknowledged(saturation["subsequent"][0])
        require(case["owner_disk_deleted"] is True, "owner-local state not deleted")
        for key, increment in (("reads", 0), ("new_writes", 1)):
            rows = case["recovery"][key]
            require(len(rows) == len(ledger) and {row["worker"] for row in rows} == set(ledger), "missing recovery receipt")
            for row in rows:
                acknowledged(row)
                require(row["value"] == ledger[row["worker"]] + increment, "recovery disagrees with acknowledged commands")
        observations(case["recovery"]["observations"], limits, settled=True)
        shutdown = case["shutdown"]
        native, python = shutdown["native"], shutdown["python"]
        require(shutdown["complete"] is True and native["complete"] is True and
                all(native[key] is True for key in ("ingress_drained", "application_work_drained", "cells_drained", "log_quiesced", "local_durability_closed", "clean_reload_prepared")) and
                native["pending_application_events"] == native["pending_dispatches"] == 0 and not native["drain_timed_out"] and not native["deadline_elapsed"], "incomplete native drain")
        require(not python["host_failed"] and not python["runtime_failed"] and
                python["pending_tasks"] == python["pending_objects"] == python["pending_threads"] == 0 and
                python["objects_created"] == python["objects_retired"] and all(value == 0 for value in python["work"].values()), "incomplete Python drain")
        bridge_settled(python["bridge"])
    require(seen == keys, "missing runtime repetitions")


def tables(document):
    verify(document)
    lines = ["Median per-run measurements on a shared development machine; not production SLOs.", "",
             "| Runtime | Concurrency | p50 ms | p99 ms | Requests/s |", "| --- | ---: | ---: | ---: | ---: |"]
    for artifact in document["artifacts"]:
        cases = [case for case in document["cases"] if case["runtime"] == artifact["label"]]
        for concurrency in document["workload"]["concurrency"]:
            rows = [next(phase for phase in case["phases"] if phase["concurrency"] == concurrency) for case in cases]
            values = [statistics.median(row["latency_ns"][key] / 1e6 for row in rows) for key in ("p50", "p99")]
            rate = statistics.median(row["requests_per_second"] for row in rows)
            lines.append(f"| {artifact['label']} | {concurrency} | {values[0]:.3f} | {values[1]:.3f} | {rate:.1f} |")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    args = parser.parse_args()
    data = args.input.read_bytes()
    print(tables(json.loads(gzip.decompress(data) if args.input.suffix == ".gz" else data)), end="")


if __name__ == "__main__":
    main()
