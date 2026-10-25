# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Readable capacity report from fixed-rate measurements and explicit SLOs."""
import statistics


def rows(report, p99_ms=1000, minimum_success=.999):
    grouped = {}
    for measurement in report["measurements"]:
        key = (measurement["host"], measurement["durability"], measurement["mix"], measurement["skew"], measurement["rate"])
        values = list(measurement["summary"].values())
        attempts = sum(v["attempts"] for v in values)
        successes = sum(v["successful"] for v in values)
        missing = not values or not attempts or any(v["p99_ms"] is None or v["scheduling_debt_p99_ms"] is None for v in values)
        debt = max((v["scheduling_debt_p99_ms"] or 0 for v in values), default=0)
        saturation = any(v["outcomes"].get("generator_saturated",0) for v in values)
        protocol = any(v["outcomes"].get("error:protocol",0) for v in values)
        latency = max((v["p99_ms"] or 0 for v in values), default=0)
        grouped.setdefault(key, []).append({"success":successes/max(1,attempts),
            "rps":sum(v["successful_rps"] for v in values), "p99":latency,
            "valid":not missing and not saturation and not protocol and debt <= 10,
            "qualified":successes/max(1,attempts) >= minimum_success and latency <= p99_ms,
            "confident":bool(values) and all(v["p99_confident"] for v in values)})
    result = []
    for key, repeats in sorted(grouped.items()):
        result.append({"key":key,"rps":statistics.median(v["rps"] for v in repeats),
            "success":min(v["success"] for v in repeats),"p99":statistics.median(v["p99"] for v in repeats),
            "valid":all(v["valid"] for v in repeats),
            "qualified":report.get("completed", False) and all(v["qualified"] and v["valid"] for v in repeats),
            "confident":all(v["confident"] for v in repeats)})
    return result


def markdown(report, p99_ms=1000, minimum_success=.999):
    inventory = {"smoke":100,"capacity":3000,"full":10000}[report["metadata"]["profile"]]
    resources = report["metadata"].get("resources", {})
    capacity = resources.get("node_resource_class") == "capacity"
    text = ["# FleetCare performance results", "",
        f"Suite completed: **{report['completed']}**. Inventory: **{inventory:,} cells** on three nodes.", "",
        ("Each application container has 2 vCPU / 4 GiB; child diagnostics run outside measurements. " if capacity else
        "Each node's aggregate application + cell container budget is 1 vCPU / 2 GiB. "
        "The application reserves 1/16 vCPU and 256 MiB for one cell container. ") +
        "RustFS, the independent generator and technician simulator have separate quotas.", "",
        f"Qualification: success ≥ {minimum_success:.2%}, every operation's all-attempt p99 ≤ {p99_ms:g} ms, "
        "generator scheduling debt p99 ≤ 10 ms, no generator saturation or protocol errors, on every repeat.", "",
        "Latency includes client scheduling delay and unsuccessful attempts. The table uses the median "
        "of each repeat's worst operation p99; see results.json for individual operations and outcomes. "
        "Low sample counts are marked provisional; these runs do not establish a reliable p99.", "",
        "| Host | Proof policy | Mix | Skew | Offered/s | Successful/s | Min success | p99 ms | Qualified | Samples |",
        "|---|---|---|---|---:|---:|---:|---:|---|---|"]
    capacities = {}
    for row in rows(report,p99_ms,minimum_success):
        host, durability, mix, skew, rate = row["key"]
        text.append(f"| {host} | {durability} | {mix} | {skew} | {rate} | {row['rps']:.2f} | "
            f"{row['success']:.2%} | {row['p99']:.2f} | {'yes' if row['qualified'] else 'no'} | "
            f"{'≥10k per operation' if row['confident'] else 'provisional'} |")
        if row["qualified"]:
            capacities[(host,durability,mix,skew)] = max(rate,capacities.get((host,durability,mix,skew),0))
    text += ["", "Largest qualifying tested rate (a measured lower bound, not extrapolated capacity):", ""]
    text += [f"- {' / '.join(key)}: {rate}/s" for key,rate in sorted(capacities.items())] or ["- None."]
    text += ["", "Functional gates cover state machines, durable receipts, projections, workflows, queues/DLQ, "
        "alarms, streaming attachments, subscriptions, SQL/KV, facets, services, assets, loaders, web/crypto, "
        "TCP, outbound HTTP/WebSockets and cell containers. Standard additionally injects a process crash "
        "and a RustFS outage and restores from the bucket after discarding all local state.", "",
        "Coverage limits: private TLS is tested for certificate rejection and STARTTLS; trusted TLS throughput "
        "is not measured because the production TCP service uses public WebPKI roots. Cache is currently an "
        "always-miss compatibility facade; SQL dump is unsupported. Extended workflow sensitive/rollback, "
        "batch/configuration, history and byte-stream APIs are outside this workload's coverage. "
        "These capabilities are not reported as successful cache hits or trusted TLS traffic.", ""]
    if report.get("failures"):
        text += ["Failures (the suite remains incomplete):", ""]
        text += [f"- {failure['operation']}: {failure['error']}" for failure in report["failures"]]
        text += [""]
    return "\n".join(text)
