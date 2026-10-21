#!/usr/bin/env python3
"""Validate baseline receipts/raw samples and render measured comparison tables."""
from __future__ import annotations

import argparse
from collections import defaultdict
import gzip
import json
import math
from pathlib import Path
import statistics


def require(condition, message):
    if not condition:
        raise ValueError(message)


def verify(document):
    require(document["format"] == 1 and document["status"] == "complete", "incomplete baseline")
    workload = document["workload"]
    artifacts = {a["label"]: a for a in document["artifacts"]}
    require(len(artifacts) == len(document["artifacts"]), "duplicate artifact label")
    keys = {(label, repetition) for label in artifacts for repetition in range(workload["repetitions"])}
    seen = set()
    expected_phases = {(kind, size, concurrency) for kind, size in (("echo", 0), ("echo", 4096), ("counter", 0))
                       for concurrency in {1, workload["concurrency"]}}
    for case in document["cases"]:
        identity = case["runtime"], case["repetition"]
        require(identity in keys and identity not in seen and case["status"] == "complete", "missing/duplicate/incomplete runtime case")
        seen.add(identity)
        python = artifacts[case["runtime"]]["mode"] == "python"
        require(case["host"]["embed_compiled"] == python, "embedded native reference")
        if python:
            runtime = case["python_runtime"]
            require(runtime["gil_enabled"] != runtime["free_threaded"], "unexpected actual GIL state")
        else:
            require(case["python_runtime"] is None, "native reference has Python runtime")
        require({(p["kind"], p["payload_bytes"], p["concurrency"]) for p in case["phases"]} == expected_phases
                and len(case["phases"]) == len(expected_phases), "workload phase missing/duplicated")
        acknowledged = defaultdict(int)
        for phase in case["phases"]:
            attempts = case["raw_attempts"][phase["name"]]
            require(set(attempts) == {str(i) for i in range(phase["concurrency"])}, "missing worker attempts")
            samples = []
            for worker in range(phase["concurrency"]):
                rows = attempts[str(worker)]
                require(len(rows) == workload["warmup"] + workload["count"], "missing raw request")
                for ordinal, row in enumerate(rows):
                    require(row["worker"] == worker and row["ordinal"] == ordinal
                            and row["warmup"] == (ordinal < workload["warmup"])
                            and row["phase"] == phase["name"] and row["outcome"] == "acknowledged"
                            and row["latency_ns"] > 0, "unproven or malformed raw request")
                    if phase["kind"] == "counter":
                        acknowledged[worker] += 1
                    if not row["warmup"]:
                        samples.append(row)
            require(phase["samples"] == samples, "published samples differ from raw attempts")
            require(len(samples) == phase["requests"] == workload["count"] * phase["concurrency"], "wrong measured request count")
            ordered = sorted(row["latency_ns"] for row in samples)
            for name, quantile in (("p50", .5), ("p95", .95), ("p99", .99)):
                require(phase["latency_ns"][name] == ordered[math.ceil(quantile * len(ordered)) - 1], "incorrect published percentile")
            require(phase["latency_ns"]["max"] == ordered[-1], "incorrect maximum")
            require(phase["elapsed_ns"] >= ordered[-1]
                    and math.isclose(phase["requests_per_second"], len(samples) * 1e9 / phase["elapsed_ns"], rel_tol=1e-12), "incorrect throughput window")
            copies = phase["bridge_copy_delta_including_warmup"]
            if python:
                for key in ("input_copied_bytes", "output_copied_bytes"):
                    require(copies[key] == phase["after"]["runtime"]["bridge"][key] - phase["before"]["runtime"]["bridge"][key]
                            and copies[key] >= 0, "incorrect copy delta")
            else:
                require(copies is None, "native bridge copies were invented")
        recovery = case["recovery"]
        for key, increment in (("reads", 0), ("new_writes", 1)):
            rows = recovery[key]
            require(len(rows) == len(acknowledged) and len({r["worker"] for r in rows}) == len(rows), "missing recovery receipt")
            require({r["worker"]: r["value"] for r in rows} == {w: n + increment for w, n in acknowledged.items()},
                    "recovered receipt differs from acknowledged command ledger")
        require(recovery["kill_to_all_verified_reads_ns"] >= sum(r["latency_ns"] for r in recovery["reads"]), "invalid recovery duration")
        shutdown = case["shutdown"]
        require(shutdown["complete"] and shutdown["native"]["complete"], "incomplete native shutdown")
        native = shutdown["native"]
        require(all(native[key] is True for key in ("ingress_drained", "application_work_drained", "cells_drained",
                    "log_quiesced", "local_durability_closed", "clean_reload_prepared"))
                and native["pending_application_events"] == native["pending_dispatches"] == 0
                and not native["drain_timed_out"] and not native["deadline_elapsed"], "unproven native shutdown phase")
        if python:
            report = shutdown["python"]
            require(not report["host_failed"] and not report["runtime_failed"]
                    and report["pending_tasks"] == report["pending_objects"] == report["pending_threads"] == 0
                    and report["objects_created"] == report["objects_retired"]
                    and all(value == 0 for value in report["work"].values())
                    and report["bridge"]["pending_reservations"] == report["bridge"]["queued_bytes"] == 0,
                    "incomplete Python shutdown")
    require(seen == keys, "runtime repetitions missing")


def tables(document):
    verify(document)
    labels = [artifact["label"] for artifact in document["artifacts"]]
    cases = {label: [case for case in document["cases"] if case["runtime"] == label] for label in labels}
    lines = [f"Median of {document['workload']['repetitions']} per-run measurements; p50/p99 are medians of each run's", "percentiles, not percentiles of a pooled distribution.", "",
             "| Workload | Runtime | p50 (ms) | p99 (ms) | Requests/s |",
             "| --- | --- | ---: | ---: | ---: |"]
    for phase in document["cases"][0]["phases"]:
        name = phase["name"]
        for label in labels:
            rows = [next(p for p in case["phases"] if p["name"] == name) for case in cases[label]]
            p50 = statistics.median(p["latency_ns"]["p50"] / 1e6 for p in rows)
            p99 = statistics.median(p["latency_ns"]["p99"] / 1e6 for p in rows)
            rate = statistics.median(p["requests_per_second"] for p in rows)
            lines.append(f"| {name} | {label} | {p50:.3f} | {p99:.3f} | {rate:.1f} |")
    lines += ["", "| Runtime | Sampled RSS before kill (MiB) | Initial startup (ms) | Cold cell read (ms) | Kill to all verified reads (ms) |",
              "| --- | ---: | ---: | ---: | ---: |"]
    for label in labels:
        rows = cases[label]
        rss = statistics.median(c["before_kill"]["memory"]["sample"]["rss_bytes"] / 2**20 for c in rows)
        startup = statistics.median(c["startup_ns"] / 1e6 for c in rows)
        cold = statistics.median(statistics.median(r["latency_ns"] / 1e6 for r in c["recovery"]["reads"]) for c in rows)
        total = statistics.median(c["recovery"]["kill_to_all_verified_reads_ns"] / 1e6 for c in rows)
        lines.append(f"| {label} | {rss:.2f} | {startup:.1f} | {cold:.1f} | {total:.1f} |")
    lines += ["", "RSS is a cached process point sample. Recovery totals include a 3,100 ms", "lease-expiry wait; neither is a per-cell limit or a production SLO.", "",
              "| Workload | Runtime | Bridge input bytes/request | Bridge output bytes/request |",
              "| --- | --- | ---: | ---: |"]
    for phase in document["cases"][0]["phases"]:
        for label in labels:
            rows = [next(p for p in case["phases"] if p["name"] == phase["name"]) for case in cases[label]]
            if rows[0]["bridge_copy_delta_including_warmup"] is None:
                incoming = outgoing = "n/a"
            else:
                def per_request(row, key):
                    n = row["requests"] + row["warmup_per_worker"] * row["concurrency"]
                    return row["bridge_copy_delta_including_warmup"][key] / n
                incoming = f"{statistics.median(per_request(r, 'input_copied_bytes') for r in rows):.1f}"
                outgoing = f"{statistics.median(per_request(r, 'output_copied_bytes') for r in rows):.1f}"
            lines.append(f"| {phase['name']} | {label} | {incoming} | {outgoing} |")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    args = parser.parse_args()
    data = args.input.read_bytes()
    document = json.loads(gzip.decompress(data) if args.input.suffix == ".gz" else data)
    print(tables(document), end="")


if __name__ == "__main__":
    main()
