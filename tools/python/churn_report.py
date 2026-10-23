#!/usr/bin/env python3
"""Validate durable churn receipts and resource/drain evidence before reporting."""
import argparse
import json
import math
from pathlib import Path
import statistics

from baseline_report import require
from operating_defaults_report import bridge_settled


def observation(value, python, resident):
    require(0 <= value["memory_sample_age_ns"] < 1_000_000_000 and
            value["memory"]["sample"]["rss_bytes"] > 0, "stale/missing memory sample")
    runtime = value["runtime"]
    require(runtime["kind"] == ("python" if python else "native"), "wrong runtime observation")
    if python:
        sample = runtime["interpreter"]
        require(sample is not None and 0 <= sample["age_ns"] < 1_000_000_000, "stale interpreter sample")
        work = sample["work"]
        require(work["active_tasks"] == work["active_invocations"] == work["pending_retirements"] == 0
                and 0 <= work["resident_objects"] <= resident, "unsettled/unbounded Python work")
        bridge_settled(runtime["bridge"])
        require(runtime["bridge"]["peak_queued_bytes"] <= runtime["limits"]["max_bridge_bytes"], "bridge byte bound exceeded")


def shutdown(report, python):
    native = report["native"]
    require(report["complete"] is True and native["complete"] is True and
            all(native[key] is True for key in ("ingress_drained", "application_work_drained", "cells_drained",
                                               "log_quiesced", "local_durability_closed", "clean_reload_prepared")) and
            native["pending_application_events"] == native["pending_dispatches"] == 0 and
            not native["drain_timed_out"] and not native["deadline_elapsed"], "incomplete native drain")
    if python:
        value = report["python"]
        require(not value["host_failed"] and not value["runtime_failed"] and
                value["pending_tasks"] == value["pending_objects"] == value["pending_threads"] == 0 and
                value["objects_created"] == value["objects_retired"] and
                all(n == 0 for n in value["work"].values()), "incomplete Python join")
        bridge_settled(value["bridge"])
    else:
        require(report["python"] is None, "native reference includes Python")


def verify(document):
    require(document["format"] == 1 and document["status"] == "complete", "failed/incomplete churn qualification")
    workload = document["workload"]
    population, rounds = workload["population"], workload["rounds"]
    concurrency, repetitions = workload["concurrency"], workload["repetitions"]
    require(65 <= population <= 1024 and 2 <= rounds <= 128 and 1 <= concurrency <= 16 and
            1 <= repetitions <= 5, "invalid workload limits")
    artifacts = {a["label"]: a for a in document["artifacts"]}
    require(len(artifacts) == len(document["artifacts"]) and
            {a["mode"] for a in artifacts.values()} == {"native", "python"}, "missing/duplicate runtime artifacts")
    expected = {(label, r) for label in artifacts for r in range(repetitions)}
    seen = set()
    for case in document["cases"]:
        identity = case["runtime"], case["repetition"]
        require(identity in expected and identity not in seen and case["status"] == "complete" and
                not any(key in case for key in ("load_failure", "failure", "cleanup_failures")), "failed/duplicate/incomplete case")
        seen.add(identity)
        python = artifacts[case["runtime"]]["mode"] == "python"
        host = case["host"]
        require(host["embed_compiled"] == python and host["debug_assertions"] == (workload["build_profile"] == "dev")
                and host["max_resident"] == 64 < population and host["durability"] == "bucket" and
                host["lease_ttl_ms"] == 3000 and host["handler_budget_ms"] == 5000 and host["host_threads"] == 4,
                "host policy/profile changed")
        require(case["owner_exit_before_kill"] is None and case["owner_exit_after_kill"] == -9 and
                case["owner_disk_removed"] is True, "owner failed early or disk-loss drill was skipped")
        observation(case["before"], python, host["max_resident"])
        require([r["iteration"] for r in case["rounds"]] == list(range(rounds)), "missing/reordered rounds")
        for iteration, stage in enumerate(case["rounds"]):
            require(stage["expected_value"] == iteration + 1 and len(stage["attempts"]) == concurrency, "wrong round/worker count")
            samples = []
            for worker, rows in enumerate(stage["attempts"]):
                require([r["cell"] for r in rows] == list(range(worker, population, concurrency)), "missing/repeated mutation")
                for row in rows:
                    require(row["outcome"] == "acknowledged" and row["response"] == {"value": iteration + 1} and
                            type(row["response"]["value"]) is int and type(row["latency_ns"]) is int and row["latency_ns"] > 0,
                            "unproven acknowledgement/timing")
                    samples.append(row["latency_ns"])
            samples.sort()
            require(stage["elapsed_ns"] >= samples[-1], "invalid measurement interval")
            for name, q in (("p50", .5), ("p99", .99), ("max", 1)):
                require(stage["latency_ns"][name] == samples[math.ceil(q * population) - 1], "incorrect percentile")
            observation(stage["after"], python, host["max_resident"])
        recovery = case["recovery"]
        for name, increment in (("reads", 0), ("new_writes", 1)):
            rows = recovery[name]
            require([r["cell"] for r in rows] == list(range(population)), "missing/repeated recovered cell")
            for row in rows:
                require(row["outcome"] == "acknowledged" and type(row["value"]) is int and row["value"] == rounds + increment,
                        "lost or fabricated recovered state")
                if name == "reads":
                    require(row["acknowledged"] == row["attempted"] == rounds, "recovery disagrees with accepted ledger")
                else:
                    require(row["expected_value"] == rounds + 1, "wrong fresh mutation after recovery")
        observation(recovery["after"], python, host["max_resident"])
        require(recovery["python_runtime"] == case["python_runtime"], "ABI changed during recovery")
        if python:
            require(case["python_runtime"]["free_threaded"] != case["python_runtime"]["gil_enabled"], "wrong actual GIL state")
        else:
            require(case["python_runtime"] is None, "native reference includes Python")
        shutdown(case["shutdown"], python)
    require(seen == expected, "missing artifact/repetition")


def tables(document):
    verify(document)
    lines = ["Closed-loop activation churn on a private SQLite bucket; not production capacity.", "",
             "Per-runtime entries are medians of per-run throughput and p99, not pooled percentiles.", "",
             "| Runtime | Writes/s | p99 ms | Maximum sampled RSS MiB |", "| --- | ---: | ---: | ---: |"]
    for artifact in document["artifacts"]:
        cases = [c for c in document["cases"] if c["runtime"] == artifact["label"]]
        rates, percentiles, memory = [], [], []
        for case in cases:
            samples = sorted(row["latency_ns"] for stage in case["rounds"] for group in stage["attempts"] for row in group)
            rates.append(len(samples) * 1e9 / sum(s["elapsed_ns"] for s in case["rounds"]))
            percentiles.append(samples[math.ceil(.99 * len(samples)) - 1] / 1e6)
            memory.append(max(s["after"]["memory"]["sample"]["rss_bytes"] for s in case["rounds"]) / 2**20)
        lines.append(f"| {artifact['label']} | {statistics.median(rates):.1f} | {statistics.median(percentiles):.2f} | {max(memory):.2f} |")
    lines += ["", "Warmup, quiescence observations and owner-loss recovery are outside timing.",
              "RSS is cached sampling, not an exact peak or a Python-heap leak bound."]
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    args = parser.parse_args()
    print(tables(json.loads(args.input.read_text())), end="")
