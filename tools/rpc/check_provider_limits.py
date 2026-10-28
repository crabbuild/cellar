# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Audit native provider policy, committed receipts and every generated pair."""
import copy
import json
from pathlib import Path
import re
import sys

LANGUAGES = ("rust", "python", "v8", "node")
CLASSES = dict(zip(LANGUAGES, ("RustCounter", "PythonCounter", "V8Counter", "NodeCounter")))


def require(condition, message):
    if not condition:
        raise ValueError(message)


def validate(report):
    if type(report) is dict and report.get("suite") == "all":
        report = report.get("provider_limits")
    require(type(report) is dict and type(report.get("format")) is int and report.get("format") == 1, "invalid provider-limit format")
    cases = report.get("cases")
    require(type(cases) is list and type(report.get("case_count")) is int and len(cases) == report.get("case_count") == 32, "incomplete provider-limit matrix")
    expected = {(route, caller, provider) for route in ("local", "signed-peer")
                for caller in LANGUAGES for provider in LANGUAGES}
    seen = set()
    identities = {}
    for case in cases:
        require(type(case) is dict, "invalid case record")
        key = (case.get("route"), case.get("caller"), case.get("provider"))
        require(key in expected and key not in seen, "missing, duplicated or unknown provider pair")
        seen.add(key)
        route, caller, provider = key
        require(case.get("class") == CLASSES[provider] and case.get("owner") == "provider-limit-owner",
                "wrong provider authority")
        require(case.get("source") == ("provider-limit-source" if route == "signed-peer" else "provider-limit-owner"),
                "wrong source authority")
        scope, epoch = case.get("scope"), case.get("epoch")
        require(type(scope) is str and re.fullmatch(re.escape(CLASSES[provider]) + r":[a-f0-9]{64}", scope)
                and type(epoch) is int and epoch > 0, "invalid scope or epoch")
        require(identities.setdefault(provider, (scope, epoch)) == (scope, epoch), "provider authority changed")
        require(type(case.get("provider_max_decoded_slots")) is int and case.get("provider_max_decoded_slots") == 7
                and type(case.get("generated_max_decoded_slots")) is int and case.get("generated_max_decoded_slots") == 1000000,
                "provider and generated policy were not distinct")
        require(case.get("first_reply") == {"code": "InvalidResponse", "outcome": "unknown"},
                "provider leaked an invalid success or rollback proof")
        before = (0 if route == "local" else 4) + LANGUAGES.index(caller)
        require(case.get("counter_before") == str(before) and case.get("counter_after") == str(before + 1),
                "committed count or receipt deduplication changed")
        require(case.get("conflict") == {"error": "operation_conflict"}, "atomic receipt conflict missing")
        require(type(case.get("explicit_retries")) is int and case["explicit_retries"] == 2
                and type(case.get("handler_count")) is int and case["handler_count"] == 4
                and type(case.get("factory_count_change")) is int and case["factory_count_change"] == 0,
                "unexpected retry, handler or factory count")
        require(case.get("automatic_replay") is False and case.get("closed_transaction") is True,
                "replay or open transaction observed")
    require(seen == expected, "provider-limit matrix is incomplete")
    require(report.get("native_nodes_joined") is True
            and type(report.get("native_reservations_after_join")) is int
            and report["native_reservations_after_join"] == 0, "native cleanup is incomplete")
    require(report.get("all_memory_qualified") is False and report.get("release_qualified") is False,
            "provider policy alone cannot qualify the release")
    return len(cases)


def probe_auditor(report):
    source = report["provider_limits"] if report.get("suite") == "all" else report
    rejected = 0
    changes = [
        ("format", True), ("case_count", 32.0), ("native_nodes_joined", False),
        ("native_reservations_after_join", 1), ("native_reservations_after_join", False),
        ("all_memory_qualified", True), ("release_qualified", True),
    ]
    case_changes = [
        ("route", "remote"), ("caller", "invalid"), ("provider", "invalid"),
        ("class", "Wrong"), ("source", "wrong"), ("owner", "wrong"), ("scope", "wrong"),
        ("epoch", True), ("provider_max_decoded_slots", 7.0), ("generated_max_decoded_slots", 7),
        ("first_reply", {"value":"1"}), ("first_reply", {"code":"InvalidResponse","outcome":"not_committed"}),
        ("counter_before", "1"), ("counter_after", "2"), ("conflict", {}),
        ("explicit_retries", True), ("handler_count", 3), ("factory_count_change", True),
        ("automatic_replay", True), ("closed_transaction", False),
    ]
    corrupted = []
    for key, value in changes:
        changed = copy.deepcopy(source); changed[key] = value; corrupted.append(changed)
    for key, value in case_changes:
        changed = copy.deepcopy(source); changed["cases"][0][key] = value; corrupted.append(changed)
    changed = copy.deepcopy(source); changed["cases"][1] = copy.deepcopy(changed["cases"][0]); corrupted.append(changed)
    changed = copy.deepcopy(source); changed["cases"].pop(); corrupted.append(changed)
    for changed in corrupted:
        try:
            validate(changed)
        except ValueError:
            rejected += 1
        else:
            raise ValueError("provider-limit auditor accepted a corrupted report")
    return rejected


if __name__ == "__main__":
    require(len(sys.argv) == 2, "usage: check_provider_limits.py report.json")
    report = json.loads(Path(sys.argv[1]).read_text())
    print(f"Validated {validate(report)} native provider-limit cases; rejected {probe_auditor(report)} corrupted reports")
