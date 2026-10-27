# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Independently audit source admission coverage, provenance and cleanup."""
import copy
import hashlib
import itertools
import json
import re
import sys
from pathlib import Path

LANGUAGES = {"rust": "RustCounter", "python": "PythonCounter", "v8": "V8Counter", "node": "NodeCounter"}


def audit(report):
    assert report["format"] == 1
    assert report["topology"] == "local-and-signed-peer-rpc-saturation"
    assert re.fullmatch(r"[0-9a-f]{64}", report["contract_digest"])
    assert report["native_nodes_drained"] is True
    assert report["source_reservations_after_drain"] == report["receiver_reservations_after_drain"] == report["receiver_tunnels_after_drain"] == 0
    assert report["all_memory_qualified"] is report["release_qualified"] is False
    cases = report["cases"]
    assert len(cases) == report["case_count"] == 96
    expected = {(side, route, limit, caller, provider) for side, route in (("source", "local"), ("source", "signed-peer"), ("receiver", "signed-peer"))
                for limit, caller, provider in itertools.product(("requests", "bytes"), LANGUAGES, LANGUAGES)}
    keys = [(c["side"], c["route"], c["limit"], c["caller"], c["provider"]) for c in cases]
    assert len(set(keys)) == len(keys) and set(keys) == expected
    identities = {}
    counts = {}
    zero = {"active_requests": 0, "reserved_request_bytes": 0}
    for case in cases:
        route, limit, caller, provider = case["route"], case["limit"], case["caller"], case["provider"]
        assert case["class"] == LANGUAGES[provider]
        assert case["source"] == "saturation-source"
        assert case["owner"] == ("saturation-owner" if route == "signed-peer" else case["source"])
        assert isinstance(case["scope"], str) and case["scope"]
        assert type(case["epoch"]) is int and case["epoch"] > 0
        key = case["side"], route, limit, provider
        identity = case["scope"], case["epoch"]
        assert identities.setdefault(key, identity) == identity
        before, after = int(case["counter_before"]), int(case["counter_after"])
        assert str(before) == case["counter_before"] and str(after) == case["counter_after"]
        assert before == counts.get(key, 0) and after == before + 2
        counts[key] = after
        held = case["held"]
        assert held == {"node": case["owner"], "class": case["class"], "scope": case["scope"],
                        "epoch": case["epoch"], "count": str(before), "transaction_open": False}
        usage = case["usage_while_held"]
        assert usage["active_requests"] == 1
        assert type(usage["reserved_request_bytes"]) is int
        assert 65536 < usage["reserved_request_bytes"] <= case["max_rpc_bytes"]
        if limit == "requests":
            assert case["max_rpc_requests"] == 1
            assert case["max_rpc_bytes"] == 256 * 1024 * 1024
        else:
            assert case["max_rpc_requests"] == 128
            assert case["max_rpc_bytes"] == 128 * 1024
        assert case["first_refusal"] == {"code": "ResourceExhausted", "outcome": "not_committed" if case["side"] == "source" else "unknown"}
        assert case["refused_handler_count"] == case["factory_count_change"] == 0
        assert case["total_handler_count"] == 4
        assert case["explicit_receipt_retries"] == 2
        assert case["changed_command_refused"] is True and case["automatic_replay"] is False
        assert case["usage_after_case"] == zero
    assert set(counts.values()) == {8}


def negative_probes(report):
    mutations = (
        lambda r: r["cases"].pop(),
        lambda r: r["cases"].__setitem__(1, r["cases"][0]),
        lambda r: r["cases"][0]["first_refusal"].__setitem__("outcome", "forged"),
        lambda r: r["cases"][0].__setitem__("refused_handler_count", 1),
        lambda r: r["cases"][0]["usage_after_case"].__setitem__("reserved_request_bytes", 1),
        lambda r: r["cases"][0]["usage_while_held"].__setitem__("active_requests", 0),
        lambda r: r["cases"][0].__setitem__("max_rpc_requests", 0),
        lambda r: r["cases"][0]["held"].__setitem__("epoch", 0),
        lambda r: r["cases"][0].__setitem__("automatic_replay", True),
        lambda r: r.__setitem__("native_nodes_drained", False),
    )
    for mutation in mutations:
        damaged = copy.deepcopy(report)
        mutation(damaged)
        try:
            audit(damaged)
        except (AssertionError, KeyError, TypeError, ValueError):
            continue
        raise AssertionError("saturation auditor accepted corrupted evidence")
    return len(mutations)


def main():
    path = Path(sys.argv[1])
    data = path.read_bytes()
    report = json.loads(data)
    audit(report)
    probes = negative_probes(report)
    print(json.dumps({"case_count": report["case_count"], "negative_probes": probes,
                      "sha256": hashlib.sha256(data).hexdigest(), "release_qualified": False}))


if __name__ == "__main__":
    main()
