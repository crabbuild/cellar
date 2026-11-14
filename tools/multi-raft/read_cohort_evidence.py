# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Require complete public RO-11 histories, including joined physical owners."""

from collections import Counter
import json
import re

CASE = "rustfs_read_cohort_has_fresh_barrier"
PREFIX = "PUBLIC_READ_EVIDENCE "


def require(condition, reason):
    if not condition:
        raise ValueError("read cohort evidence: " + reason)


def natural(value):
    require(type(value) is int and value >= 0, "invalid counter")
    return value


def segments(full):
    result = []
    if full:
        for ingress in ("Sdk", "Native", "Axum"):
            for cancel in (False, True):
                phases = [f"prequeued_start_{ingress}_cancel={str(cancel).lower()}",
                          "prequeued_admitted_behind_sync", "shared_barrier_independent_pins"]
                if cancel:
                    phases.append("cancelled_waiter_retains_physical_pins")
                result.append(phases + ["prequeued_drained"])
            result.append([f"late_start_{ingress}", "late_arrival_has_second_barrier_and_pin", "late_drained"])
            result.append([f"pending_late_start_{ingress}", "pending_late_sync_entered",
                           "pending_late_first_barrier_held", "pending_late_two_barriers_before_any_callback",
                           "pending_late_drained"])
            result.append([f"term_start_{ingress}", "term_reads_admitted_behind_sync", "term_refused_and_drained"])
    callers = "Sdk, Native" if full else "Axum, Sdk"
    result.append([f"configuration_start_[{callers}]", "configuration_reads_admitted_behind_apply",
                   "configuration_committed_and_data_admitted_before_apply", "configuration_refused_and_drained",
                   "configuration_fresh_reads_verified"])
    return result


def snapshot(row):
    index = natural(row["request_node"])
    require(index in (0, 1) and len(row["nodes"]) >= 3, "missing selected voter")
    node = row["nodes"][index]
    for field in ("node", "disk", "process"):
        require(isinstance(node[field], str) and re.fullmatch(r"[0-9a-f]{32}", node[field])
                and node[field] != "0" * 32, "missing physical identity")
    require(isinstance(node["fleet_boot"], str) and node["fleet_boot"], "missing boot identity")
    for owner in (node["owners"]["runtime"]["read_pins"], node["owners"]["host"]["controller"]["ordinary"]):
        require(natural(owner["items"]) <= natural(owner["max_items"])
                and natural(owner["bytes"]) <= natural(owner["max_bytes"]), "owner exceeds admission")
    ordinary_bound = node["owners"]["host"]["controller"]["ordinary"]
    image_bound = node["owners"]["runtime"]["read_pins"]
    require((ordinary_bound["max_items"], ordinary_bound["max_bytes"]) == (2, 65536)
            and (image_bound["max_items"], image_bound["max_bytes"]) == (256, 64 * 1024 * 1024), "changed owner bounds")
    pins = node["owners"]["runtime"]["read_pins"]["items"]
    ordinary = node["owners"]["host"]["controller"]["ordinary"]["items"]
    counters = node["counters"]
    barriers = tuple(natural(counters[name]) for name in
                     ("cell_linearizable_read_cohorts_started", "cell_linearizable_read_cohort_joins"))
    queries = natural(row["callbacks"]["queries"])
    group = node["group"]
    require(isinstance(group["history"], str) and group["history"], "missing group history")
    return pins, ordinary, barriers, queries, natural(group["term"]), natural(group["membership_revision"])


def owners(rows, full):
    expected = segments(full)
    require([row["phase"] for row in rows] == [phase for part in expected for phase in part],
            "partial or reordered phase history")
    identities = None
    group_history = None
    for row in rows:
        current = [(node["node"], node["disk"], node["process"], node["fleet_boot"])
                   for node in row["nodes"]]
        require(identities is None or identities == current, "physical fixture identity changed")
        identities = current
        for node in row["nodes"][:3]:
            history = node["group"]["history"]
            require(group_history is None or group_history == history, "foreign group history")
            group_history = history
    offset = 0
    for phases in expected:
        part = rows[offset:offset + len(phases)]
        offset += len(phases)
        values = [snapshot(row) for row in part]
        before, after = values[0], values[-1]
        require(before[:2] == after[:2] == (0, 0), "physical owners did not drain")
        name = phases[0]
        if name.startswith("prequeued_"):
            require(all(value[:2] == (2, 2) for value in values[1:-1]), "cancelled or queued image released early")
            require(tuple(b - a for a, b in zip(before[2], values[2][2])) == (1, 1), "prequeued reads did not share one fresh barrier")
        elif name.startswith("late_start_"):
            require(values[1][:2] == (2, 2), "late read lost its image")
            require(tuple(b - a for a, b in zip(before[2], values[1][2])) == (2, 0), "late arrival reused a completed barrier")
        elif name.startswith("pending_late_"):
            require(all(row["request_node"] == 1 for row in part), "pending barrier did not use follower")
            require(values[2][:2] == (1, 1) and values[3][:2] == (2, 2), "pending reads lost their owners")
            require(tuple(b - a for a, b in zip(before[2], values[2][2])) == (1, 0)
                    and tuple(b - a for a, b in zip(before[2], values[3][2])) == (2, 0), "pending late read shared old barrier")
            require(values[2][3] == values[3][3] == before[3], "query entered before barrier completion")
        elif name.startswith("term_start_"):
            require(values[1][:2] == (2, 2) and after[4] > values[1][4]
                    and after[3] == before[3], "higher term did not refuse queued reads")
            continue
        else:
            require(values[1][:2] == values[2][:2] == (2, 2), "configuration cut lost queued owners")
            require(values[1][4:] == values[2][4:] and values[3][4] == values[2][4]
                    and values[3][5] > values[2][5], "configuration cut did not change membership in the same term")
            require(values[3][:2] == (0, 0) and values[3][3] == before[3], "invalidated configuration entered a query")
        require(after[3] == before[3] + 2, "physical query callback count changed")


def observations(output, case, *, external_store=True):
    require(case == CASE, "unknown case")
    try:
        rows = [json.loads(line.split(PREFIX, 1)[1]) for line in output.splitlines() if PREFIX in line]
        runs = {f"extents={str(mode).lower()},full={str(full).lower()}": full
                for mode in (False, True) for full in (True, False)}
        require(rows and all(type(row) is dict and type(row["schema"]) is int and row["schema"] == 1
                             and row["case"] == CASE.removeprefix("rustfs_") and row["run"] in runs for row in rows),
                "invalid schema or fixture")
        require({row["run"] for row in rows} == set(runs), "missing image or configuration fixture")
        for run, full in runs.items():
            history = [row for row in rows if row["run"] == run]
            expected = Counter(owners=65 if full else 5, request=34 if full else 4,
                               result=31 if full else 4, verified_result=23 if full else 2,
                               receipt=13 if full else 4, completed=1)
            if full:
                expected.update(invalid_request=12, caller_cancelled=3, pending_barrier_cut=3)
            require(Counter(row["event"] for row in history) == expected, "incomplete or duplicated event history")
            complete = history[-1]
            require(complete["event"] == "completed" and complete["joined"] is True
                    and complete["external_store"] is external_store
                    and natural(complete["commands"]) == (10 if full else 4)
                    and natural(complete["queries"]) == (26 if full else 2), "unjoined or wrong-backend completion")
            selected = lambda event: [row for row in history if row["event"] == event]
            requests = {row["invocation"]: row for row in selected("request")}
            require(len(requests) == expected["request"] and all(re.fullmatch(r"[0-9a-f]{32}", key)
                    and key != "0" * 32 for key in requests), "duplicate or invalid request identity")
            for row in requests.values():
                require(row["required"] == "linearizable_fresh_quorum" and type(row["timeout_ms"]) is int
                        and row["timeout_ms"] == 5000 and type(row["max_attempts"]) is int and row["max_attempts"] == 1
                        and type(row["attempt_ms"]) is int and row["attempt_ms"] == (5000 if row["ingress"] == "Sdk" else 4000), "read contract changed")
                require(type(row["requested_member"]) is int and row["requested_member"] in (1, 2), "unselected member")
            require({row["ingress"] for row in requests.values()} == ({"Sdk", "Native", "Axum"} if full else {"Sdk", "Axum"}), "missing public ingress")
            require(Counter(row["ingress"] for row in requests.values())
                    == (Counter(Sdk=12, Native=12, Axum=10) if full else Counter(Sdk=2, Axum=2)), "incomplete caller matrix")
            results = {row["invocation"]: row for row in selected("result")}
            cancelled = {row["invocation"] for row in selected("caller_cancelled")}
            require(len(results) == expected["result"] and len(cancelled) == expected["caller_cancelled"]
                    and not results.keys() & cancelled and results.keys() | cancelled == requests.keys(), "lost or duplicated disposition")
            require(all(row["actual_invocation"] == row["invocation"] for row in results.values())
                    and all(row["waiter_joined"] is True for row in selected("caller_cancelled")), "invocation or cancellation changed")
            require(Counter(row["disposition"] for row in results.values())
                    == Counter(selected=23 if full else 2, not_leader=8 if full else 2), "wrong term/configuration refusal")
            verified = {row["invocation"]: row for row in selected("verified_result")}
            require(len(verified) == expected["verified_result"]
                    and verified.keys() == {key for key, row in results.items() if row["disposition"] == "selected"}, "unverified query result")
            require(Counter(requests[key]["ingress"] for key in verified)
                    == (Counter(Sdk=8, Native=8, Axum=7) if full else Counter(Sdk=1, Axum=1)), "missing caller results")
            require(Counter(requests[key]["ingress"] for key in cancelled)
                    == (Counter(Sdk=1, Native=1, Axum=1) if full else Counter()), "missing caller cancellations")
            receipts = {}
            proofs = set()
            for row in history:
                if row["event"] == "receipt":
                    require(row["required"] == row["achieved"] == "S3", "weakened durable receipt")
                    require(isinstance(row["id"], str) and row["id"] and type(row["value"]) is int, "invalid receipt identity or value")
                    proof = (row["value"], row["version"], row["witness"])
                    require(all(isinstance(value, str) and value for value in proof[1:]), "missing exact image proof")
                    require(row["id"] not in receipts or receipts[row["id"]] == proof, "receipt replay changed")
                    receipts[row["id"]] = proof
                    proofs.add(proof)
                elif row["event"] == "verified_result":
                    key = row["invocation"]
                    requested_history = requests[key]["history"]
                    sequence = re.search(r"sequence: MutationSequence\((\d+)\)", row["version"])
                    require(isinstance(requested_history, str) and requested_history.startswith("Ok(")
                            and requested_history.endswith(")") and requested_history[3:-1] in row["version"]
                            and results[key]["cell"] in ("Counter:one", "Counter:two")
                            and 'cell: CellId("' + results[key]["cell"] + '")' in requested_history
                            and sequence and int(sequence[1]) == natural(results[key]["sequence"]), "foreign cell or image sequence")
                    require((row["value"], row["version"], row["witness"]) in proofs
                            and type(row["value"]) is type(results[key]["value"]) is int
                            and row["value"] == results[key]["value"] and type(row["served_by"]) is int
                            and row["served_by"] == requests[key]["requested_member"], "query lost original receipt or selected member")
            require(len(receipts) == complete["commands"], "command callback/receipt mismatch")
            for row in selected("pending_barrier_cut"):
                before = tuple(natural(value) for value in row["before"])
                after = tuple(natural(value) for value in row["after"])
                require(len(before) == len(after) == 2 and tuple(b - a for a, b in zip(before, after)) == (2, 0)
                        and natural(row["callbacks_before"]) == natural(row["callbacks_after"]), "pending barrier cut lost isolation")
            require(all(type(row["status"]) is int and row["status"] in (400, 405)
                        for row in selected("invalid_request")), "invalid wire request admitted")
            owners(selected("owners"), full)
        return rows, len(runs)
    except (KeyError, TypeError, IndexError, json.JSONDecodeError) as error:
        raise ValueError("read cohort evidence: malformed record") from error
