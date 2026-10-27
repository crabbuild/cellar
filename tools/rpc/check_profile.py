#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Independently audit live profile coverage, authority and cleanup evidence."""
import copy
import itertools
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
LANGUAGES = ("rust", "python", "v8", "node")
CLASSES = {"rust": "RustCounter", "python": "PythonCounter", "v8": "V8Counter", "node": "NodeCounter"}


def audit(report):
    vectors = {vector["id"]: vector for vector in json.loads((ROOT / "fixtures/profile-vectors.json").read_text())}
    descriptor = json.loads((ROOT / "fixtures/profile-generated/rpc.json").read_text())
    assert report["format"] == 1 and report["contract_digest"] == descriptor["digest"]
    assert report["vector_count"] == len(vectors) == 18
    assert report["native_nodes_drained"] is report["language_runtimes_joined"] is True
    for name in ("native_reservations_after_join", "bridge_reservations_after_join"):
        assert type(report[name]) is int and report[name] == 0
    assert report["all_memory_qualified"] is report["python_modes_qualified"] is report["release_qualified"] is False
    cases = report["cases"]
    expected = set(itertools.product(("local", "signed-peer"), LANGUAGES, LANGUAGES, vectors))
    keys = [(case["route"], case["caller"], case["provider"], case["id"]) for case in cases]
    assert len(cases) == report["case_count"] == len(expected) == 576
    assert len(set(keys)) == len(keys) and set(keys) == expected
    scopes = {}
    for case in cases:
        vector = vectors[case["id"]]
        assert case["method"] == vector["method"] and case["type"] == vector["type"]
        assert case["source"] == ("profile-owner" if case["route"] == "local" else "profile-source")
        assert case["owner"] == "profile-owner" and case["class"] == CLASSES[case["provider"]]
        assert type(case["epoch"]) is int and case["epoch"] > 0
        assert re.fullmatch(re.escape(case["class"]) + r":[0-9a-f]{64}", case["scope"])
        key = (case["provider"], case["id"])
        if key in scopes:
            assert scopes[key] == case["scope"]
        scopes[key] = case["scope"]
        assert case["request_semantics_verified"] is case["reply_semantics_verified"] is case["caller_verified"] is True
    assert len(scopes) == len(set(scopes.values())) == 4 * 18


def negative_probes(report):
    mutations = (
        lambda r: r["cases"].pop(),
        lambda r: r["cases"].__setitem__(0, copy.deepcopy(r["cases"][1])),
        lambda r: r["cases"][0].__setitem__("id", "unknown-vector"),
        lambda r: r["cases"][0].__setitem__("caller", "unknown-language"),
        lambda r: r["cases"][0].__setitem__("provider", "unknown-language"),
        lambda r: r["cases"][0].__setitem__("route", "http-proxy"),
        lambda r: r["cases"][0].__setitem__("method", "Other"),
        lambda r: r["cases"][0].__setitem__("type", "Other"),
        lambda r: r["cases"][-1].__setitem__("source", "profile-owner"),
        lambda r: r["cases"][0].__setitem__("owner", "profile-source"),
        lambda r: r["cases"][0].__setitem__("epoch", 0),
        lambda r: r["cases"][0].__setitem__("scope", "RustCounter:" + "0" * 64),
        lambda r: r["cases"][0].__setitem__("request_semantics_verified", False),
        lambda r: r["cases"][0].__setitem__("reply_semantics_verified", False),
        lambda r: r["cases"][0].__setitem__("caller_verified", False),
        lambda r: r.__setitem__("native_nodes_drained", False),
        lambda r: r.__setitem__("language_runtimes_joined", False),
        lambda r: r.__setitem__("native_reservations_after_join", 1),
        lambda r: r.__setitem__("bridge_reservations_after_join", 1),
        lambda r: r.__setitem__("all_memory_qualified", True),
        lambda r: r.__setitem__("python_modes_qualified", True),
        lambda r: r.__setitem__("release_qualified", True),
    )
    for mutation in mutations:
        damaged = copy.deepcopy(report)
        mutation(damaged)
        try:
            audit(damaged)
        except (AssertionError, KeyError, TypeError):
            continue
        raise AssertionError("profile auditor accepted corrupted evidence")
    return len(mutations)


if __name__ == "__main__":
    report = json.loads(Path(sys.argv[1]).read_text())
    audit(report)
    count = negative_probes(report)
    print(f"Verified 576 live profile cases; rejected {count} corrupt reports")
