#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Audit generated-client and raw-native failure provenance, including negative probes."""
import copy
import hashlib
import itertools
import json
from pathlib import Path
import re
import sys

LANGUAGES = {"rust": "RustCounter", "python": "PythonCounter", "v8": "V8Counter", "node": "NodeCounter"}
DELTA = 9_007_199_254_740_993


def audit(report):
    assert report["language_runtimes_joined"] is True
    assert type(report["bridge_reservations_after_join"]) is int
    assert report["bridge_reservations_after_join"] == 0
    evidence = report["provenance"] if report["suite"] == "all" else report
    assert type(evidence["format"]) is int and evidence["format"] == 1
    assert evidence["suite"] == "provenance"
    assert evidence["remote_fleet_qualified"] is False
    cases = evidence["cases"]
    assert len(cases) == evidence["case_count"] == 112
    expected = set(itertools.product(("local", "peer"), LANGUAGES, LANGUAGES, ("child", "forge", "invalid")))
    expected |= {(route, caller, "rust", mode) for route, caller, mode in
                 itertools.product(("local", "peer"), LANGUAGES, ("raw-forge", "raw-forward"))}
    keys = [(c["route"], c["caller"], c["provider"], c["mode"]) for c in cases]
    assert len(set(keys)) == len(keys) and set(keys) == expected
    owners, counts, raw_owners, raw_counts = {}, {}, {}, {}
    for case in cases:
        provider, mode = case["provider"], case["mode"]
        assert case["owner"] == "rpc-provenance-owner"
        assert re.fullmatch(re.escape(LANGUAGES[provider]) + r":[0-9a-f]{64}", case["scope"])
        assert type(case["epoch"]) is int and case["epoch"] > 0
        assert case["parent_active_after_refusal"] is True
        assert type(case["receipt_retries"]) is int and case["receipt_retries"] == 2
        assert type(case["automatic_retries"]) is int and case["automatic_retries"] == 0
        assert type(case["factory_count"]) is int and case["factory_count"] >= 4
        code = {"child": "DeadlineExceeded", "invalid": "Internal"}.get(mode, "MethodNotFound")
        assert case["failure"] == {"code": code, "outcome": "unknown"}
        child = ({"code": "DeadlineExceeded", "outcome": "not_committed"} if mode == "child" else
                 {"code": "MethodNotFound", "outcome": "not_committed"} if mode == "raw-forward" else None)
        assert case["child"] == child
        if mode.startswith("raw-"):
            assert provider == "rust"
            identity = (case["scope"], case["epoch"])
            assert raw_owners.setdefault(mode, identity) == identity
            assert type(case["factories_after"]) is int
            assert case["factories_after"] == case["factory_count"]
            raw_counts[mode] = raw_counts.get(mode, 0) + DELTA
            assert case["count"] == str(raw_counts[mode])
            assert case["factory_count"] == 6
        else:
            identity = (case["scope"], case["epoch"])
            assert owners.setdefault(provider, identity) == identity
            counts[provider] = counts.get(provider, 0) + DELTA
            assert case["count"] == str(counts[provider])
            assert case["factory_count"] == 6
    assert len(raw_owners) == len(set(raw_owners.values())) == 2 and len(owners) == 4


def negative_probes(report):
    def evidence(r):
        return r["provenance"] if r["suite"] == "all" else r

    def row(r, mode):
        return next(c for c in evidence(r)["cases"] if c["mode"] == mode)

    mutations = (
        lambda r: evidence(r).__setitem__("format", True),
        lambda r: evidence(r)["cases"].pop(),
        lambda r: evidence(r).__setitem__("case_count", 96),
        lambda r: evidence(r)["cases"].__setitem__(-1, copy.deepcopy(evidence(r)["cases"][0])),
        lambda r: row(r, "forge")["failure"].__setitem__("outcome", "not_committed"),
        lambda r: row(r, "raw-forge")["failure"].__setitem__("outcome", "not_committed"),
        lambda r: row(r, "raw-forward")["failure"].__setitem__("code", "Unavailable"),
        lambda r: row(r, "raw-forward")["child"].__setitem__("outcome", "unknown"),
        lambda r: row(r, "raw-forge").__setitem__("child", {"code": "MethodNotFound", "outcome": "not_committed"}),
        lambda r: row(r, "raw-forge").__setitem__("parent_active_after_refusal", False),
        lambda r: row(r, "raw-forge").__setitem__("automatic_retries", 1),
        lambda r: row(r, "raw-forge").__setitem__("receipt_retries", 1),
        lambda r: row(r, "raw-forge").__setitem__("count", str(DELTA + 1)),
        lambda r: row(r, "raw-forge").__setitem__("factories_after", row(r, "raw-forge")["factory_count"] + 1),
        lambda r: row(r, "raw-forge").__setitem__("scope", row(r, "raw-forward")["scope"]),
        lambda r: row(r, "raw-forge").__setitem__("epoch", 0),
        lambda r: row(r, "raw-forge").__setitem__("owner", "rpc-provenance-source"),
        lambda r: row(r, "raw-forge").__setitem__("scope", "PythonCounter:" + "a" * 64),
        lambda r: r.__setitem__("bridge_reservations_after_join", 1),
        lambda r: r.__setitem__("language_runtimes_joined", False),
    )
    for mutation in mutations:
        damaged = copy.deepcopy(report)
        mutation(damaged)
        try:
            audit(damaged)
        except (AssertionError, KeyError, TypeError, StopIteration):
            continue
        raise AssertionError("Provenance auditor accepted corrupted evidence")
    return len(mutations)


if __name__ == "__main__":
    path = Path(sys.argv[1])
    content = path.read_bytes()
    report = json.loads(content)
    audit(report)
    probes = negative_probes(report)
    print(json.dumps({"case_count": 112, "negative_probes": probes,
                      "sha256": hashlib.sha256(content).hexdigest(), "release_qualified": False}))
