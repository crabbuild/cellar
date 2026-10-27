#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Audit guest metadata refusals and subsequent generated-client progress."""
import copy
import itertools
import json
import re
import sys
from pathlib import Path

CALLERS = ("python", "v8", "node")
CLASSES = {"rust": "RustCounter", "python": "PythonCounter", "v8": "V8Counter", "node": "NodeCounter"}


def audit(report):
    assert report["format"] == 1
    assert report["native_nodes_drained"] is True
    assert type(report["native_reservations_after_join"]) is int
    assert report["native_reservations_after_join"] == 0
    assert report["all_memory_qualified"] is report["release_qualified"] is False
    expected = set(itertools.product(("local", "signed-peer"), CALLERS, CLASSES, ("unknown", "oversize")))
    cases = report["cases"]
    keys = [(case["route"], case["caller"], case["provider"], case["mode"]) for case in cases]
    assert len(cases) == report["case_count"] == len(expected) == 48
    assert len(set(keys)) == len(keys) and set(keys) == expected
    scopes = {}
    for case in cases:
        assert case["refusal"] == {"code": "InvalidArgument", "outcome": "not_committed"}
        for name in ("provider_calls_on_refusal", "factories_on_refusal"):
            assert type(case[name]) is int and case[name] == 0
        assert case["source"] == ("guest-control-owner" if case["route"] == "local" else "guest-control-source")
        assert case["owner"] == "guest-control-owner" and case["class"] == CLASSES[case["provider"]]
        assert type(case["epoch"]) is int and case["epoch"] > 0
        assert re.fullmatch(re.escape(case["class"]) + r":[0-9a-f]{64}", case["scope"])
        identity = (case["scope"], case["epoch"])
        if case["provider"] in scopes:
            assert scopes[case["provider"]] == identity
        scopes[case["provider"]] = identity
        assert case["fresh_generated_read"] == "0"
    assert len(scopes) == len({scope for scope, _ in scopes.values()}) == 4


def negative_probes(report):
    mutations = (
        lambda r: r["cases"].pop(),
        lambda r: r["cases"].__setitem__(0, copy.deepcopy(r["cases"][1])),
        lambda r: r["cases"][0].__setitem__("caller", "rust"),
        lambda r: r["cases"][0].__setitem__("provider", "unknown"),
        lambda r: r["cases"][0].__setitem__("mode", "other"),
        lambda r: r["cases"][0]["refusal"].__setitem__("code", "Ambiguous"),
        lambda r: r["cases"][0]["refusal"].__setitem__("outcome", "unknown"),
        lambda r: r["cases"][0].__setitem__("provider_calls_on_refusal", 1),
        lambda r: r["cases"][0].__setitem__("factories_on_refusal", 1),
        lambda r: r["cases"][-1].__setitem__("source", "guest-control-owner"),
        lambda r: r["cases"][0].__setitem__("owner", "guest-control-source"),
        lambda r: r["cases"][0].__setitem__("class", "OtherCounter"),
        lambda r: r["cases"][0].__setitem__("scope", "RustCounter:" + "0" * 64),
        lambda r: r["cases"][0].__setitem__("epoch", 0),
        lambda r: r["cases"][0].__setitem__("fresh_generated_read", "1"),
        lambda r: r.__setitem__("native_nodes_drained", False),
        lambda r: r.__setitem__("native_reservations_after_join", 1),
        lambda r: r.__setitem__("all_memory_qualified", True),
        lambda r: r.__setitem__("release_qualified", True),
    )
    for mutation in mutations:
        damaged = copy.deepcopy(report)
        mutation(damaged)
        try:
            audit(damaged)
        except (AssertionError, KeyError, TypeError):
            continue
        raise AssertionError("guest-control auditor accepted corrupted evidence")
    return len(mutations)


if __name__ == "__main__":
    report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    audit(report)
    count = negative_probes(report)
    print(f"Verified 48 guest-control cases; rejected {count} corrupt reports")
