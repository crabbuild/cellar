#!/usr/bin/env python3
"""Summarize a run directory and optionally plot using matplotlib.

Usage: python3 tools/perf/analyze.py RUN_DIRECTORY OUTPUT_PREFIX [--plot]
"""

import argparse
import collections
import hashlib
import json
import pathlib
import statistics
import re
from run import routing_cache_checkpoints, validate_routing_cache, validate_python_outcomes


def summarize(root):
    metadata = json.loads((root / "metadata.json").read_text())
    results = json.loads((root / "results.json").read_text())
    # A failed load never returned to the caller's successful-measurement list
    # in older runs. Read its saved sample too, so analysis cannot hide errors.
    failures = {(v["scenario"], v["label"]): v for v in results.get("failures", [])}
    for file in root.glob("*.json"):
        value = json.loads(file.read_text())
        if isinstance(value, dict) and value.get("operation") in ("write", "verify") and (value.get("errors") or value.get("mismatches")):
            failures[(value["scenario"], value["label"])] = value
    cache_snapshots = []
    cache_limit = metadata["arguments"].get("max_cached_routes")
    if cache_limit is not None:
        for checkpoint in routing_cache_checkpoints(metadata["arguments"].get("scenario")):
            try:
                state = json.loads((root / checkpoint["file"]).read_text())
                validate_routing_cache(state, cache_limit)
                cache_snapshots.append(dict(checkpoint, cached_routes=state["cached_routes"],
                                            tracked_cells=sum(state["phases"].values()),
                                            occupied=state["occupied"], owned_cells=state["owned_cells"]))
            except Exception as error:
                label = f"{checkpoint['label']}-node{checkpoint['node']}"
                failures[(checkpoint["scenario"], label)] = {"operation": "state_snapshot",
                    "scenario": checkpoint["scenario"], "label": label, "error": str(error)}
    if metadata["arguments"].get("runtime") == "python":
        try:
            validate_python_outcomes(root, results.get("shutdown_outcomes", []),
                metadata["arguments"].get("scenario"), metadata["arguments"]["max_requests"],
                metadata["arguments"]["shutdown_ms"])
        except Exception as error:
            failures[("python", "shutdown")] = {"operation": "python_shutdown", "error": str(error)}
    groups = collections.defaultdict(list)
    for measurement in results["measurements"]:
        groups[(measurement["scenario"], measurement["concurrency"])].append(measurement)
    summary = []
    for (scenario, concurrency), runs in sorted(groups.items()):
        successful = sum(run["successful_writes"] for run in runs)
        elapsed = sum(run["elapsed_s"] for run in runs)
        summary.append({
            "scenario": scenario, "concurrency": concurrency, "repetitions": len(runs),
            "successful_writes": successful, "elapsed_s": elapsed,
            "writes_per_s": successful / elapsed,
            "writes_per_s_range": [min(r["writes_per_s"] for r in runs), max(r["writes_per_s"] for r in runs)],
            "latency_ms_ranges": {quantile: [min(r["latency_ms"][quantile] for r in runs), max(r["latency_ms"][quantile] for r in runs)] for quantile in ("p50", "p95", "p99", "max")},
            "errors": sum(sum(r["errors"].values()) for r in runs),
        })
    nodes = []
    for file in sorted(root.glob("*-measured-node*-state.json")):
        state = json.loads(file.read_text())
        disk_cpu = file.with_name(file.name.removesuffix("state.json") + "disk-cpu.txt").read_text()
        lines = disk_cpu.splitlines()
        filesystem = lines[1].split()
        directories = []
        for line in lines[2:]:
            if "\t" in line:
                size, name = line.split("\t", 1)
                directories.append({"bytes": int(size), "path": name})
        peak_memory = next((int(line) for line in lines if line.isdigit()), None)
        events = {parts[0]: int(parts[1]) for line in lines if len(parts := line.split()) == 2 and parts[0] in ("low", "high", "max", "oom", "oom_kill", "oom_group_kill")}
        nodes.append({
            "snapshot": file.name, "owned_cells": state["owned_cells"], "resident_cells": len(state["residents"]),
            "tracked_cells": sum(state["phases"].values()) if "phases" in state else None,
            "cached_routes": state.get("cached_routes"),
            "rss_bytes": state["rss_bytes"], "working_set_bytes": state["cgroup_working_set_bytes"],
            "cgroup_current_bytes": state["cgroup_current_bytes"],
            "peak_cgroup_memory_bytes": peak_memory, "memory_events": events,
            "shed_cells": state["node_load"]["shed_cells"], "stops": state["stops"],
            "filesystem_bytes": int(filesystem[1]), "filesystem_used_bytes": int(filesystem[2]),
            "state_directory_bytes": next((d["bytes"] for d in directories if d["path"] == "/data/state"), None),
            "peerlog_bytes": next((d["bytes"] for d in directories if d["path"] == "/data/state/peerlog"), None),
        })
    cpu = collections.defaultdict(list)
    resource_lines = []
    for source in root.glob("resources*.jsonl"):
        resource_lines.extend(source.read_text().splitlines())
    for line in resource_lines:
        sample = json.loads(line)
        if "stats" not in sample or "-c" not in sample["phase"]:
            continue
        name = sample["stats"]["Name"]
        category = "rustfs" if name.endswith("-rustfs") else "load" if name.endswith("-load") else name.rsplit("-", 1)[-1]
        cpu[(sample["phase"].split("-c", 1)[0], category)].append(float(sample["stats"]["CPUPerc"].removesuffix("%")))
    cpu_summary = [{"scenario": scenario, "component": category, "samples": len(values), "median_cpu_percent": statistics.median(values), "max_cpu_percent": max(values)} for (scenario, category), values in sorted(cpu.items())]
    stalls = []
    for file in sorted(root.glob("*-drained-node*.log")):
        for number, line in enumerate(file.read_text().splitlines(), 1):
            if "shutdown_handoff_stalled" in line:
                stalls.append({"log": file.name, "line": number, "event": "shutdown_handoff_stalled",
                    "counters": {key: int(value) for key, value in re.findall(r'\b(drain_ms|shutdown_total_ms|occupied|activating|quiescing|evicting|releasing|adopting|handed_off)=(\d+)', line.split('drain_state=', 1)[0])}})
    return {
        "started_utc": metadata["started_utc"], "revision": metadata["revision"], "source_sha256": metadata["source_sha256"],
        "analysis_sha256": hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
        "source_dirty": metadata["dirty"], "image_labels": metadata.get("image_labels"),
        "completed": False if failures else results.get("completed"), "failures": list(failures.values()),
        "executable_sha256": metadata.get("executable_sha256"),
        "cellar_image": metadata["image"], "rustfs_image": metadata["rustfs_image"],
        "environment": {"architecture": metadata["docker"]["Architecture"], "vm_cpus": metadata["docker"]["NCPU"],
            "vm_memory_bytes": metadata["docker"]["MemTotal"], "node_limits": metadata["node_limits"],
            "load_generator_limits": metadata["load_generator_limits"], "rustfs_limits": metadata["rustfs_limits"]},
        "workload": {"runtime": metadata["arguments"].get("runtime", "native"),
            "cells": metadata["arguments"]["cells"], "payload_bytes": metadata["arguments"]["bytes"],
            "phase_seconds": metadata["arguments"]["seconds"], "repetitions": metadata["arguments"]["repeats"],
            "concurrency": metadata["arguments"]["concurrency"], "warmup_seconds": metadata["arguments"].get("warmup_seconds", 10),
            "max_resident_cells_per_node": metadata["arguments"].get("max_resident", 256),
            "max_cached_routes_per_node": cache_limit,
            "max_requests_per_script_pool": metadata["arguments"].get("max_requests", 64 if "sdk::Node" in metadata.get("execution", "") else None),
            "execution": metadata.get("execution", "process-owning CLI host"),
            "operation": "transaction: increment persistent counter and overwrite JSON value with changing seeded random ASCII payload (64-symbol alphabet)",
            "load_model": "closed loop; one in-flight request per worker; disjoint cell partitions; rotating ingress; no write retries",
            "latency": "request start through complete HTTP response JSON; warmup excluded; percentile ranges across repetitions, not pooled percentiles",
            "build": "Linux ARM64; Rust 1.98.1; repository release profile (opt-level=s, fat LTO)",
            "rebalance_interval_ms": 0},
        "summary": summary, "measurements": results["measurements"], "recovery": results["recovery"], "node_snapshots": nodes, "cpu_samples": cpu_summary,
        "shutdown_handoff_stalls": stalls,
        "shutdown_outcomes": results.get("shutdown_outcomes", []),
        "node_policies": results.get("node_policies", []),
        "routing_cache_snapshots": cache_snapshots,
        "cpu_sampling_coverage": sorted({item["scenario"] for item in cpu_summary}),
        "limitations": ["All containers share one Apple M2 Max / Colima VM and backing disk; CPU affinity and quotas do not isolate disk or VM overhead.",
            "RustFS is a local single-node S3-compatible store, not production S3 latency or durability.",
            "The 5 GiB loop image has approximately 4.84 GiB usable ext4 capacity; it bounds node data, not the read-only container image.",
            f"Repeated overwrite workload with {metadata['arguments']['cells']} total cells; no growing dataset, hot-cell skew or long soak.",
            "Each scenario uses a fresh bucket on a RustFS instance shared by the scenarios in that run.",
            "Failures and rolling restarts occur between write phases; concurrent ambiguous write acknowledgements are not exercised.",
            "Closed-loop measurements characterize saturation and do not correct coordinated omission for an independently arriving workload.",
            "Fleet durability can fall back to bucket proof; peerlog files demonstrate replication, but acknowledgement source is not separately instrumented."]
    }


def plot(report, prefix):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    labels = {"one_node_bucket": "1 node · bucket", "three_node_bucket": "3 nodes · bucket", "three_node_fleet": "3 nodes · fleet"}
    colors = {"one_node_bucket": "#2563eb", "three_node_bucket": "#d97706", "three_node_fleet": "#059669"}
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.7), constrained_layout=True)
    for scenario in labels:
        rows = [row for row in report["summary"] if row["scenario"] == scenario]
        if not rows:
            continue
        x = [r["concurrency"] for r in rows]
        y = [r["writes_per_s"] for r in rows]
        axes[0].plot(x, y, marker="o", label=labels[scenario], color=colors[scenario])
        axes[0].fill_between(x, [r["writes_per_s_range"][0] for r in rows], [r["writes_per_s_range"][1] for r in rows], alpha=.15, color=colors[scenario])
        bounds = [r["latency_ms_ranges"]["p99"] for r in rows]
        axes[1].plot(x, [(lo + hi) / 2 for lo, hi in bounds], marker="o", label=labels[scenario], color=colors[scenario])
        axes[1].fill_between(x, [b[0] for b in bounds], [b[1] for b in bounds], alpha=.15, color=colors[scenario])
    for ax in axes:
        ax.set_xscale("log", base=2)
        ax.set_xticks(report["workload"]["concurrency"], labels=report["workload"]["concurrency"])
        ax.set_xlabel("Concurrent write requests")
        ax.grid(alpha=.2)
        ax.spines[["top", "right"]].set_visible(False)
        ax.legend(frameon=False, fontsize=9)
    axes[0].set_title("Acknowledged write throughput")
    axes[0].set_ylabel("Writes / second")
    axes[0].set_ylim(bottom=0)
    axes[1].set_title("Per-run p99 response latency")
    axes[1].set_ylabel("Milliseconds")
    axes[1].set_ylim(bottom=0)
    workload = report["workload"]
    fig.suptitle(f"Cellar · {workload['cells']} cells · {workload['payload_bytes'] // 1024} KiB transactional writes\nEach node: 1 CPU, 2 GiB RAM, 5 GiB data disk · shaded ranges: {workload['repetitions']} × {workload['phase_seconds']} s runs", fontsize=12)
    fig.savefig(str(prefix) + ".png", dpi=160)
    fig.savefig(str(prefix) + ".svg")
    svg = pathlib.Path(str(prefix) + ".svg")
    svg.write_text("\n".join(line.rstrip() for line in svg.read_text().splitlines()) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_directory", type=pathlib.Path)
    parser.add_argument("output_prefix", type=pathlib.Path)
    parser.add_argument("--plot", action="store_true")
    args = parser.parse_args()
    report = summarize(args.run_directory)
    args.output_prefix.parent.mkdir(parents=True, exist_ok=True)
    args.output_prefix.with_suffix(".json").write_text(json.dumps(report, indent=2) + "\n")
    if args.plot:
        plot(report, args.output_prefix)
    for row in report["summary"]:
        print(row["scenario"], row["concurrency"], round(row["writes_per_s"]), row["latency_ms_ranges"]["p99"])


if __name__ == "__main__":
    main()
