#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Audit a completed matrix's loaded Python mode, separately from its cases."""
import argparse
import copy
import json
from pathlib import Path
import re


def audit(report, expected):
    assert expected in ("gil", "free-threaded")
    assert report["language_runtimes_joined"] is True
    assert type(report["bridge_reservations_after_join"]) is int
    assert report["bridge_reservations_after_join"] == 0
    runtime = report["python_runtime"]
    assert runtime["expected_mode"] == expected
    startup, after = runtime["startup"], runtime["after_matrix"]
    assert startup == after and type(startup) is dict
    free = expected == "free-threaded"
    assert startup["free_threaded"] is free and startup["gil_enabled"] is (not free)
    assert startup["implementation"] == "cpython"
    version = startup["version"]
    assert type(version) is list and len(version) == 3
    assert all(type(part) is int and part >= 0 for part in version)
    assert version[:2] == [3, 14]
    prefix = r"(?:cpython-314t|cp314t)" if free else r"(?:cpython-314|cp314)"
    assert re.fullmatch(prefix + r"-[a-z0-9_-]+", startup["soabi"])
    assert startup["protobuf_version"] == "7.34.2"
    assert startup["protobuf_backend"] in ("python", "upb")


def negative_probes(report, expected):
    opposite = "gil" if expected == "free-threaded" else "free-threaded"

    def both(value, field, changed):
        for phase in ("startup", "after_matrix"):
            value["python_runtime"][phase][field] = changed

    mutations = (
        lambda r: r.pop("language_runtimes_joined"),
        lambda r: r.__setitem__("language_runtimes_joined", False),
        lambda r: r.__setitem__("bridge_reservations_after_join", 1),
        lambda r: r.__setitem__("bridge_reservations_after_join", False),
        lambda r: r.pop("python_runtime"),
        lambda r: r["python_runtime"].__setitem__("expected_mode", opposite),
        lambda r: r["python_runtime"].__setitem__("after_matrix", None),
        lambda r: r["python_runtime"]["after_matrix"].__setitem__("protobuf_backend", "changed"),
        lambda r: both(r, "free_threaded", expected != "free-threaded"),
        lambda r: both(r, "free_threaded", int(expected == "free-threaded")),
        lambda r: both(r, "gil_enabled", expected == "free-threaded"),
        lambda r: both(r, "gil_enabled", int(expected != "free-threaded")),
        lambda r: both(r, "version", [3, 13, 5]),
        lambda r: both(r, "version", [3, 14]),
        lambda r: both(r, "version", [3, 14, True]),
        lambda r: both(r, "implementation", "pypy"),
        lambda r: both(r, "soabi", "cpython-313-darwin"),
        lambda r: both(r, "soabi", "cpython-314t-darwin" if expected == "gil" else "cpython-314-darwin"),
        lambda r: both(r, "protobuf_version", "7.34.1"),
        lambda r: both(r, "protobuf_backend", "unknown"),
    )
    for mutation in mutations:
        damaged = copy.deepcopy(report)
        mutation(damaged)
        try:
            audit(damaged, expected)
        except (AssertionError, KeyError, TypeError):
            continue
        raise AssertionError("Python mode auditor accepted corrupted evidence")
    return len(mutations)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected", choices=("gil", "free-threaded"), required=True)
    parser.add_argument("report", type=Path)
    arguments = parser.parse_args()
    report = json.loads(arguments.report.read_text(encoding="utf-8"))
    audit(report, arguments.expected)
    count = negative_probes(report, arguments.expected)
    print(f"Verified loaded {arguments.expected} CPython before/after matrix and runtime cleanup; rejected {count} corrupt reports")
